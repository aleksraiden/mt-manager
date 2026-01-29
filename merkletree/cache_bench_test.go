package merkletree

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// BenchmarkCacheHit - бенчмарк попадания в кеш
func BenchmarkCacheHit(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			cfg := &Config{
				MaxDepth:    3,
				CacheSize:   size,
				CacheShards: 8,
			}

			tree := New[*Account](cfg)

			// Заполняем дерево
			for i := uint64(0); i < uint64(size); i++ {
				tree.Insert(NewAccount(i, StatusUser))
			}

			// Прогреваем кеш
			for i := uint64(0); i < uint64(size/10); i++ {
				tree.Get(i)
			}

			b.ResetTimer()

			// Бенчмарк попаданий в кеш
			for i := 0; i < b.N; i++ {
				uid := uint64(i % (size / 10))
				tree.Get(uid)
			}
		})
	}
}

// BenchmarkCacheMiss - бенчмарк промахов кеша
func BenchmarkCacheMiss(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			cfg := &Config{
				MaxDepth:    3,
				CacheSize:   size / 10, // Маленький кеш
				CacheShards: 8,
			}

			tree := New[*Account](cfg)

			// Заполняем дерево
			for i := uint64(0); i < uint64(size); i++ {
				tree.Insert(NewAccount(i, StatusUser))
			}

			b.ResetTimer()

			// Бенчмарк промахов кеша
			for i := 0; i < b.N; i++ {
				uid := uint64(i % size)
				tree.Get(uid)
			}
		})
	}
}

// BenchmarkCacheVsNoCache - сравнение с кешем и без
func BenchmarkCacheVsNoCache(b *testing.B) {
	dataSize := 100000

	b.Run("WithCache", func(b *testing.B) {
		cfg := &Config{
			MaxDepth:    3,
			CacheSize:   10000,
			CacheShards: 8,
		}

		tree := New[*Account](cfg)

		for i := uint64(0); i < uint64(dataSize); i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		// Прогрев кеша
		for i := uint64(0); i < 1000; i++ {
			tree.Get(i)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			uid := uint64(i % 1000)
			tree.Get(uid)
		}
	})

	b.Run("NoCache", func(b *testing.B) {
		cfg := &Config{
			MaxDepth:    3,
			CacheSize:   0, // Без кеша
			CacheShards: 0,
		}

		tree := New[*Account](cfg)

		for i := uint64(0); i < uint64(dataSize); i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			uid := uint64(i % 1000)
			tree.Get(uid)
		}
	})
}

// BenchmarkCacheShards - влияние количества шардов
func BenchmarkCacheShards(b *testing.B) {
	shardPowers := []uint{4, 6, 8, 10, 12} // 16, 64, 256, 1024, 4096 шардов

	for _, power := range shardPowers {
		numShards := 1 << power
		b.Run(fmt.Sprintf("Shards_%d", numShards), func(b *testing.B) {
			cfg := &Config{
				MaxDepth:    3,
				CacheSize:   100000,
				CacheShards: power,
			}

			tree := New[*Account](cfg)

			// Заполняем
			for i := uint64(0); i < 100000; i++ {
				tree.Insert(NewAccount(i, StatusUser))
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					uid := uint64(rng.Intn(100000))
					tree.Get(uid)
				}
			})
		})
	}
}

// BenchmarkCacheResize - производительность изменения размера
func BenchmarkCacheResize(b *testing.B) {
	cfg := &Config{
		MaxDepth:    3,
		CacheSize:   10000,
		CacheShards: 8,
	}

	tree := New[*Account](cfg)

	// Заполняем
	for i := uint64(0); i < 10000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Изменяем размер кеша
		if i%2 == 0 {
			tree.cache.Resize(50000)
		} else {
			tree.cache.Resize(10000)
		}
	}
}

// BenchmarkLRUEviction - производительность вытеснения из LRU
func BenchmarkLRUEviction(b *testing.B) {
	cfg := &Config{
		MaxDepth:    3,
		CacheSize:   1000, // Маленький кеш для частого вытеснения
		CacheShards: 6,
	}

	tree := New[*Account](cfg)

	// Заполняем больше элементов, чем размер кеша
	for i := uint64(0); i < 10000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	b.ResetTimer()

	// Случайный доступ вызывает вытеснения
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < b.N; i++ {
		uid := uint64(rng.Intn(10000))
		tree.Get(uid)
	}
}

// BenchmarkCacheContention - lock contention в кеше
func BenchmarkCacheContention(b *testing.B) {
	configs := []struct {
		name   string
		shards uint
	}{
		{"LowShards_16", 4},
		{"MediumShards_256", 8},
		{"HighShards_4096", 12},
	}

	for _, tc := range configs {
		b.Run(tc.name, func(b *testing.B) {
			cfg := &Config{
				MaxDepth:    3,
				CacheSize:   100000,
				CacheShards: tc.shards,
			}

			tree := New[*Account](cfg)

			// Заполняем
			for i := uint64(0); i < 100000; i++ {
				tree.Insert(NewAccount(i, StatusUser))
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					uid := uint64(rng.Intn(100000))
					tree.Get(uid)
				}
			})
		})
	}
}
