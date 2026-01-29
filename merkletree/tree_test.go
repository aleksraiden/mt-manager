package merkletree

import (
	"runtime"
	"testing"
	"time"
	"sync"
	"math/rand"
)

func TestTreeBasicOperations(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	// Вставка аккаунтов
	for i := uint64(0); i < 1000; i++ {
		acc := NewAccount(i, StatusUser)
		tree.Insert(acc)
	}

	// Проверка размера
	if tree.Size() != 1000 {
		t.Errorf("Ожидалось 1000 аккаунтов, получено %d", tree.Size())
	}

	// Проверка получения аккаунта
	acc, ok := tree.Get(500)
	if !ok {
		t.Error("Аккаунт 500 должен существовать")
	}
	if acc.UID != 500 {
		t.Errorf("Ожидался UID 500, получен %d", acc.UID)
	}

	// Проверка несуществующего аккаунта
	_, ok = tree.Get(10000)
	if ok {
		t.Error("Аккаунт 10000 не должен существовать")
	}
}

func TestTreeBatchInsert(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	// Подготовка пакета аккаунтов
	accounts := make([]*Account, 1000)
	for i := range accounts {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}

	// Пакетная вставка
	tree.InsertBatch(accounts)

	if tree.Size() != 1000 {
		t.Errorf("Ожидалось 1000 аккаунтов, получено %d", tree.Size())
	}
}

func TestTreeRootHash(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	// Вставка детерминированных аккаунтов
	for i := uint64(0); i < 100; i++ {
		acc := NewAccountDeterministic(i, StatusUser) // <-- Изменено
		tree.Insert(acc)
	}

	root1 := tree.ComputeRoot()

	// Создаем второе дерево с теми же данными
	tree2 := New[*Account](DefaultConfig())
	for i := uint64(0); i < 100; i++ {
		acc := NewAccountDeterministic(i, StatusUser) // <-- Изменено
		tree2.Insert(acc)
	}

	root2 := tree2.ComputeRoot()

	// Корни должны совпадать
	if root1 != root2 {
		t.Errorf("Корневые хеши должны совпадать для идентичных деревьев\nRoot1: %x\nRoot2: %x", root1[:16], root2[:16])
	}
}

func TestCacheHit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheSize = 100
	tree := New[*Account](cfg)

	// Вставка аккаунтов
	for i := uint64(0); i < 1000; i++ {
		acc := NewAccount(i, StatusUser)
		tree.Insert(acc)
	}

	// Первое чтение - кеш промах
	acc1, _ := tree.Get(100)

	// Второе чтение - должно быть из кеша
	acc2, _ := tree.Get(100)

	if acc1.UID != acc2.UID {
		t.Error("Кеш должен возвращать тот же объект")
	}
}

func TestTreeClear(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	if tree.Size() != 100 {
		t.Errorf("Ожидалось 100, получено %d", tree.Size())
	}

	tree.Clear()

	if tree.Size() != 0 {
		t.Errorf("После Clear ожидалось 0, получено %d", tree.Size())
	}
}

func TestTreeStats(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	for i := uint64(0); i < 1000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	stats := tree.GetStats()

	if stats.TotalItems != 1000 {
		t.Errorf("Ожидалось 1000 элементов, получено %d", stats.TotalItems)
	}

	if stats.AllocatedNodes == 0 {
		t.Error("Должны быть аллоцированы узлы")
	}
}

// Бенчмарки
func BenchmarkTreeInsert(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc := NewAccount(uint64(i), StatusUser)
		tree.Insert(acc)
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
		tree.Insert(acc)
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
		tree.Insert(acc)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.ComputeRoot()
	}
}

// Интеграционный тест
func TestLargeScalePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропуск большого теста в short режиме")
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := DefaultConfig()
	cfg.CacheSize = 100000
	tree := New[*Account](cfg)

	t.Log("Заполнение 5M аккаунтов...")
	start := time.Now()

	for i := uint64(0); i < 5_000_000; i++ {
		acc := NewAccount(i, StatusUser)
		tree.Insert(acc)

		if i > 0 && i%1_000_000 == 0 {
			t.Logf("Вставлено %dM аккаунтов", i/1_000_000)
		}
	}

	insertTime := time.Since(start)
	t.Logf("Время вставки: %v (%.0f ops/sec)", insertTime, 5_000_000/insertTime.Seconds())

	// Вычисление корня
	t.Log("Вычисление корневого хеша...")
	start = time.Now()
	root := tree.ComputeRoot()
	rootTime := time.Since(start)
	t.Logf("Root: %x | Время: %v", root[:16], rootTime)

	// Статистика памяти
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("Heap alloc: %v MB", m.HeapAlloc/1024/1024)

	stats := tree.GetStats()
	t.Logf("Статистика дерева: %+v", stats)
}

func TestTreeConcurrentGetAndRoot(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode")
	}

	tree := New[*Account](DefaultConfig())

	// Заполняем
	for i := uint64(0); i < 100_000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	// Параллельные геты + периодические ComputeRoot
	const workers = 32
	const rounds = 1000

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for j := 0; j < rounds; j++ {
				uid := uint64(rng.Intn(100_000))
				_, _ = tree.Get(uid)

				if j%50 == 0 {
					_ = tree.ComputeRoot()
				}
			}
		}(i)
	}

	wg.Wait()
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

func TestRootChangesAfterMutation(t *testing.T) {
    tree := New[*Account](DefaultConfig())
    acc1 := NewAccount(1, StatusUser)
    tree.Insert(acc1)

    root1 := tree.ComputeRoot()

    acc2 := NewAccount(2, StatusUser)
    tree.Insert(acc2)

    root2 := tree.ComputeRoot()

    if root1 == root2 {
        t.Error("Root hash должен измениться после вставки")
    }
}