// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package retry

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/xige-16/storage-test/pkg/log"
	"github.com/xige-16/storage-test/pkg/util/funcutil"
	"github.com/xige-16/storage-test/pkg/util/merr"
)

// Do will run function with retry mechanism.
// fn is the func to run.
// Option can control the retry times and timeout.
func Do(ctx context.Context, fn func() error, opts ...Option) error {
	if !funcutil.CheckCtxValid(ctx) {
		return ctx.Err()
	}

	log := log.Ctx(ctx)
	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	var el error

	for i := uint(0); i < c.attempts; i++ {
		if err := fn(); err != nil {
			if i%4 == 0 {
				log.Error("retry func failed", zap.Uint("retry time", i), zap.Error(err))
			}

			err = errors.Wrapf(err, "attempt #%d", i)
			el = merr.Combine(el, err)

			if !IsRecoverable(err) {
				return el
			}

			if c.isRetryErr != nil && !c.isRetryErr(err) {
				return err
			}

			deadline, ok := ctx.Deadline()
			if ok && time.Until(deadline) < c.sleep {
				// to avoid sleep until ctx done
				return el
			}

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				return merr.Combine(el, ctx.Err())
			}

			c.sleep *= 2
			if c.sleep > c.maxSleepTime {
				c.sleep = c.maxSleepTime
			}
		} else {
			return nil
		}
	}
	return el
}

// Do will run function with retry mechanism.
// fn is the func to run, return err and shouldRetry flag.
// Option can control the retry times and timeout.
func Handle(ctx context.Context, fn func() (bool, error), opts ...Option) error {
	if !funcutil.CheckCtxValid(ctx) {
		return ctx.Err()
	}

	log := log.Ctx(ctx)
	c := newDefaultConfig()

	for _, opt := range opts {
		opt(c)
	}

	var lastErr error
	for i := uint(0); i < c.attempts; i++ {
		if shouldRetry, err := fn(); err != nil {
			if i%4 == 0 {
				log.Warn("retry func failed", zap.Uint("retried", i), zap.Error(err))
			}

			if !shouldRetry {
				if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) && lastErr != nil {
					return lastErr
				}
				return err
			}

			deadline, ok := ctx.Deadline()
			if ok && time.Until(deadline) < c.sleep {
				// to avoid sleep until ctx done
				if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) && lastErr != nil {
					return lastErr
				}
				return err
			}

			lastErr = err

			select {
			case <-time.After(c.sleep):
			case <-ctx.Done():
				return lastErr
			}

			c.sleep *= 2
			if c.sleep > c.maxSleepTime {
				c.sleep = c.maxSleepTime
			}
		} else {
			return nil
		}
	}
	return lastErr
}

// errUnrecoverable is error instance for unrecoverable.
var errUnrecoverable = errors.New("unrecoverable error")

// Unrecoverable method wrap an error to unrecoverableError. This will make retry
// quick return.
func Unrecoverable(err error) error {
	return merr.Combine(err, errUnrecoverable)
}

// IsRecoverable is used to judge whether the error is wrapped by unrecoverableError.
func IsRecoverable(err error) bool {
	return !errors.Is(err, errUnrecoverable)
}
