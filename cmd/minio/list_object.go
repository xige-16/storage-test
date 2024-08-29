package minio

import (
	"context"
	"github.com/xige-16/storage-test/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"path"
	"time"

	"github.com/xige-16/storage-test/internal/storage"
	"github.com/xige-16/storage-test/pkg/common"
)

func ListObject(ctx context.Context,
	minioClient storage.ObjectStorage,
	bucketName, rootPath string,
	qps, reqTime int64) error {

	// 创建存储延迟的切片
	var latencies []time.Duration
	ticker := time.NewTicker(1000 * time.Millisecond / time.Duration(qps))

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(reqTime)*time.Second)
	defer cancel()

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
				_, _, err := minioClient.ListObjects(ctx, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), true)
				if err != nil {
					return err
				}
				latency := time.Since(startTime)
				latencies = append(latencies, latency)
				//log.Debug("list keys", zap.Strings("keys", keys), zap.String("path", rootPath))

				return nil
			})
			offset += 1
		}
	}

	group.Wait()
	log.Debug("list keys done", zap.Int64("reqTime", reqTime), zap.Int("reqCount", len(latencies)))

	return PrintStatics(latencies, "ListObject")
}
