package merkletree

import (
	"fmt"
	"testing"
)

func BenchmarkInsertStrategies(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Single_%d", size), func(b *testing.B) {
			tree := New[*Account](DefaultConfig())

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := 0; j < size; j++ {
					tree.Insert(NewAccount(uint64(i*size+j), StatusUser))
				}
			}

			b.ReportMetric(float64(size*b.N)/b.Elapsed().Seconds(), "ops/sec")
		})

		b.Run(fmt.Sprintf("Batch_%d", size), func(b *testing.B) {
			tree := New[*Account](DefaultConfig())

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				items := make([]*Account, size)
				for j := 0; j < size; j++ {
					items[j] = NewAccount(uint64(i*size+j), StatusUser)
				}
				tree.InsertBatch(items)
			}

			b.ReportMetric(float64(size*b.N)/b.Elapsed().Seconds(), "ops/sec")
		})
	}
}
