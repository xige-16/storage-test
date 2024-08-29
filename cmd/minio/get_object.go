package minio

import (
	"context"
	"errors"
	"path"
	"strconv"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/xige-16/storage-test/internal/storage"
	"github.com/xige-16/storage-test/pkg/common"
	"github.com/xige-16/storage-test/pkg/log"
	"github.com/xige-16/storage-test/pkg/util/timerecord"
)

func GetObject(ctx context.Context,
	minioClient storage.ObjectStorage,
	bucketName, rootPath string,
	qps, reqTime int64) error {

	// 创建存储延迟的切片
	var statLatencies []time.Duration
	var getLatencies []time.Duration

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(reqTime)*time.Second)
	defer cancel()

	ticker := time.NewTicker(1000 * time.Millisecond / time.Duration(qps))
	log.Info("GetObject start...", zap.Int64("qps", qps), zap.Int64("reqTime", reqTime))

	stop := true
	offset := 0
	group, _ := errgroup.WithContext(ctx)
	for stop {
		select {
		case <-timeoutCtx.Done():
			stop = false
			ticker.Stop()
		case <-ticker.C:
			key := path.Join(rootPath, common.SegmentInsertLogPath, strconv.Itoa(offset))
			group.Go(func() error {
				tr := timerecord.NewTimeRecorder("GetObject")

				size, err := minioClient.StatObject(ctx, bucketName, key)
				if err != nil {
					return err
				}
				duration := tr.RecordSpan()
				statLatencies = append(statLatencies, duration)

				object, err := minioClient.GetObject(ctx, bucketName, key, int64(0), int64(0))
				if err != nil {
					return err
				}

				output, err := storage.Read(object, size)
				if err != nil {
					return err
				}
				if len(output) == 0 {
					return errors.New("GetObject output len is 0")
				}

				duration = tr.RecordSpan()
				getLatencies = append(getLatencies, duration)
				//log.Info("GetObject get done", zap.String("key", key))

				return nil
			})
			offset += 1
		}
	}

	group.Wait()

	log.Info("StatObject done", zap.Int64("reqTime", reqTime), zap.Int("reqCount", len(statLatencies)))
	log.Info("GetObject done", zap.Int64("reqTime", reqTime), zap.Int("reqCount", len(getLatencies)))

	err := PrintStatics(statLatencies, "StatObject")
	if err != nil {
		return err
	}

	return PrintStatics(getLatencies, "GetObject")
}
