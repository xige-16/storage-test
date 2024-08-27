package minio

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/xige-16/storage-test/internal/storage"
	"github.com/xige-16/storage-test/pkg/common"
)

func PutObject(ctx context.Context, minioClient storage.ObjectStorage, bucketName, rootPath string, qps int) {

	input := make([]byte, 16000000, 16000000)
	// 创建存储延迟的切片
	var latencies []time.Duration

	ticker := time.NewTicker(time.Second / time.Duration(qps))
	defer ticker.Stop()

	stop := true
	offset := 10
	wg := &sync.WaitGroup{}
	for stop {
		select {
		case <-ctx.Done():
			stop = false
		case <-ticker.C:
			wg.Add(1)
			go func() {
				startTime := time.Now()
				err := minioClient.PutObject(ctx, bucketName, path.Join(rootPath, common.SegmentInsertLogPath, strconv.Itoa(offset)), bytes.NewReader(input), int64(len(input)))
				if err != nil {
					panic(err)
				}
				latency := time.Since(startTime)
				latencies = append(latencies, latency)

				wg.Done()
			}()
			offset += 1
			stop = false
		}
	}
	wg.Wait()

	// 计算平均延迟
	var totalLatency time.Duration
	for _, latency := range latencies {
		totalLatency += latency
	}
	averageLatency := totalLatency / time.Duration(len(latencies))

	// 排序后计算 P99 和 P95
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	fmt.Printf("putObject Average Latency: %v\n", averageLatency)
	fmt.Printf("putObject P95 Latency: %v\n", p95)
	fmt.Printf("putObject P99 Latency: %v\n", p99)
}
