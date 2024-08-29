package minio

import (
	"bytes"
	"context"
	"github.com/xige-16/storage-test/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"path"
	"strconv"
	"time"

	"github.com/xige-16/storage-test/internal/storage"
	"github.com/xige-16/storage-test/pkg/common"
)

func PutObject(ctx context.Context,
	minioClient storage.ObjectStorage,
	bucketName, rootPath string,
	qps, size, reqTime int64) error {

	input := make([]byte, size, size)
	log.Info("PutObject init input data done", zap.Int("size", len(input)))
	// 创建存储延迟的切片
	var latencies []time.Duration

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(reqTime)*time.Second)
	defer cancel()

	ticker := time.NewTicker(1000 * time.Millisecond / time.Duration(qps))
	log.Info("PutObject start...", zap.Int64("qps", qps))

	stop := true
	offset := 0
	group, _ := errgroup.WithContext(ctx)
	for stop {
		select {
		case <-timeoutCtx.Done():
			stop = false
			ticker.Stop()
		case <-ticker.C:
			group.Go(func() error {
				startTime := time.Now()
				err := minioClient.PutObject(ctx, bucketName, path.Join(rootPath, common.SegmentInsertLogPath, strconv.Itoa(offset)), bytes.NewReader(input), int64(len(input)))
				if err != nil {
					return err
				}
				latency := time.Since(startTime)
				latencies = append(latencies, latency)
				//log.Debug("PutObject done", zap.String("key", key))

				return nil
			})
			offset += 1
		}
	}

	group.Wait()

	log.Info("PutObject done", zap.Int64("reqTime", reqTime), zap.Int("reqCount", len(latencies)))

	return PrintStatics(latencies, "PutObject")
}
