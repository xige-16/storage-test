package minio

import (
	"context"
	"github.com/xige-16/storage-test/pkg/log"
	"github.com/xige-16/storage-test/pkg/util/timerecord"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"path"
	"strconv"
	"time"

	"github.com/xige-16/storage-test/internal/storage"
	"github.com/xige-16/storage-test/pkg/common"
)

func RemoveObject(ctx context.Context,
	minioClient storage.ObjectStorage,
	bucketName, rootPath string,
	qps, reqTime int64) error {

	// 创建存储延迟的切片
	var listLatencies []time.Duration
	var removeLatencies []time.Duration

	ticker := time.NewTicker(1000 * time.Millisecond / time.Duration(qps))

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(reqTime)*time.Second)
	defer cancel()

	log.Info("RemoveObject start...", zap.Int64("qps", qps), zap.Int64("reqTime", reqTime))

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

				tr := timerecord.NewTimeRecorder("RemoveObject")
				_, _, err := minioClient.ListObjects(ctx, bucketName, path.Join(rootPath, common.SegmentInsertLogPath), true)
				if err != nil {
					return err
				}
				duration := tr.RecordSpan()
				listLatencies = append(listLatencies, duration)

				err = minioClient.RemoveObject(ctx, bucketName, key)
				if err != nil {
					return err
				}
				duration = tr.RecordSpan()
				removeLatencies = append(removeLatencies, duration)
				//log.Debug("RemoveObject done", zap.String("key", key))

				return nil
			})
			offset += 1
		}
	}

	group.Wait()
	log.Info("ListObjects done", zap.Int64("reqTime", reqTime), zap.Int("reqCount", len(listLatencies)))
	log.Info("RemoveObject done", zap.Int64("reqTime", reqTime), zap.Int("reqCount", len(removeLatencies)))

	err := PrintStatics(listLatencies, "ListObject")
	if err != nil {
		return err
	}

	return PrintStatics(removeLatencies, "RemoveObject")
}
