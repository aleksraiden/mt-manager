package merkletree

import (
	"math/rand"
	"testing"
	"time"
)

// Бенчмарки
func BenchmarkTreeInsert(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc := NewAccount(uint64(i), StatusUser)
		if err := tree.Insert(acc); err != nil {
			b.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}
}

func BenchmarkTreeInsertBatch(b *testing.B) {
	tree := New[*Account](DefaultConfig())
	batchSize := 1000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		accounts := make([]*Account, batchSize)
		for j := range accounts {
			accounts[j] = NewAccount(uint64(i*batchSize+j), StatusUser)
		}
		b.StartTimer()

		tree.InsertBatch(accounts)
	}
}

func BenchmarkTreeGet(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	// Подготовка данных
	for i := uint64(0); i < 1000000; i++ {
		acc := NewAccount(i, StatusUser)
		if err := tree.Insert(acc); err != nil {
			b.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(uint64(i % 1000000))
	}
}

func BenchmarkTreeGetCached(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	// Подготовка данных
	for i := uint64(0); i < 1000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	// Прогреваем кеш
	for i := uint64(0); i < 100; i++ {
		tree.Get(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(uint64(i % 100))
	}
}

func BenchmarkTreeComputeRoot(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	// Подготовка данных
	for i := uint64(0); i < 10000; i++ {
		acc := NewAccount(i, StatusUser)
		if err := tree.Insert(acc); err != nil {
			b.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.ComputeRoot()
	}
}

func BenchmarkConcurrentGetAndRootHighContention(b *testing.B) {
	tree := New[*Account](DefaultConfig())
	// Заполняем большим количеством элементов
	for i := uint64(0); i < 500_000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(b.N)))
		for pb.Next() {
			uid := uint64(rng.Intn(500_000))
			_, _ = tree.Get(uid)

			// 1% шанс вызвать ComputeRoot (симулируем CometBFT finality)
			if rng.Intn(100) == 0 {
				_ = tree.ComputeRoot()
			}
		}
	})
}
