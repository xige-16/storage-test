package minio

import (
	"fmt"
	"github.com/cockroachdb/errors"
	"sort"
	"time"
)

func PrintStatics(latencies []time.Duration, funcName string) error {
	if len(latencies) == 0 {
		return errors.New(funcName + "empty latency results")
	}

	if len(latencies) != 0 {
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

		fmt.Printf("%s Average Latency: %v\n", funcName, averageLatency)
		fmt.Printf("%s P95 Latency: %v\n", funcName, p95)
		fmt.Printf("%s P99 Latency: %v\n", funcName, p99)
	}

	return nil
}
