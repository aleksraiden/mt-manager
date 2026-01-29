package merkletree

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// ============================================
// Главный стресс-тест менеджера
// ============================================

func TestManagerStressTest(t *testing.T) {
	tests := []struct {
		name      string
		numTrees  int
		minItems  int
		maxItems  int
		updates   int
	}{
		{name: "Trees_10", numTrees: 10, minItems: 100000, maxItems: 1000000, updates: 100000},
		{name: "Trees_100", numTrees: 100, minItems: 100000, maxItems: 1000000, updates: 100000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runStressTest(t, tt.numTrees, tt.minItems, tt.maxItems, tt.updates)
		})
	}
}

func runStressTest(t *testing.T, numTrees, minItems, maxItems, numUpdates int) {
	t.Logf("Стресс-тест: %d деревьев", numTrees)

	cfg := DefaultConfig()
	cfg.TopN = 10

	mgr := NewUniversalManager(cfg)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Фаза 1: Создание и заполнение деревьев
	t.Log("Фаза 1: Создание и заполнение деревьев...")
	phase1Start := time.Now()
	totalItems := 0
	treeItemCounts := make(map[string]int)

	for i := 0; i < numTrees; i++ {
		treeID := fmt.Sprintf("tree%d", i)
		tree, _ := CreateTree[*Account](mgr, treeID)

		numItems := minItems + rnd.Intn(maxItems-minItems)
		treeItemCounts[treeID] = numItems

		items := make([]*Account, numItems)
		for j := 0; j < numItems; j++ {
			id := uint64(i*maxItems + j)
			status := AccountStatus(rnd.Intn(3))
			items[j] = NewAccount(id, status)
		}

		tree.InsertBatch(items)
		totalItems += numItems

		if (i+1)%10 == 0 || i == numTrees-1 {
			t.Logf("  Создано %d/%d деревьев, %d элементов", i+1, numTrees, totalItems)
		}
	}

	phase1Duration := time.Since(phase1Start)
	t.Logf("✓ Фаза 1 завершена за %v", phase1Duration)
	t.Logf("  Всего элементов: %d", totalItems)
	t.Logf("  Среднее на дерево: %d", totalItems/numTrees)

	// Фаза 2: Вычисление индивидуальных корней
	t.Log("Фаза 2: Вычисление индивидуальных корней...")
	phase2Start := time.Now()

	initialHashes := make(map[string][32]byte)
	treeIDs := mgr.ListTrees()

	for _, treeID := range treeIDs {
		tree, exists := GetTree[*Account](mgr, treeID)
		if !exists {
			t.Fatalf("Дерево '%s' не найдено", treeID)
		}

		hash := tree.ComputeRoot()
		initialHashes[treeID] = hash
	}

	phase2Duration := time.Since(phase2Start)
	t.Logf("✓ Фаза 2 завершена за %v", phase2Duration)
	t.Logf("  Среднее время на дерево: %v", phase2Duration/time.Duration(numTrees))

	count := 0
	for treeID, hash := range initialHashes {
		if count < 3 {
			t.Logf("  %s: %x", treeID, hash[:16])
		}
		count++
	}

	// Фаза 3: Вычисление глобального корня
	t.Log("Фаза 3: Вычисление глобального корня...")
	phase3Start := time.Now()
	globalRoot := mgr.ComputeGlobalRoot()
	phase3Duration := time.Since(phase3Start)

	t.Logf("✓ Фаза 3 завершена за %v", phase3Duration)
	t.Logf("  Глобальный корень: %x", globalRoot[:16])

	// Фаза 3.5: Проверка TopN
	t.Log("Фаза 3.5: Проверка TopN...")
	phase35Start := time.Now()

	topNChecks := 0
	topNFound := 0

	for _, treeID := range treeIDs {
		tree, _ := GetTree[*Account](mgr, treeID)

		if !tree.IsTopNEnabled() {
			t.Errorf("TopN должен быть включен для дерева %s", treeID)
			continue
		}
		topNChecks++

		if minItem, ok := tree.GetMin(); ok {
			topNFound++
			if topNChecks <= 2 {
				t.Logf("  %s: Min ID=%d", treeID, minItem.UID)
			}
		}

		if maxItem, ok := tree.GetMax(); ok {
			if topNChecks <= 2 {
				t.Logf("  %s: Max ID=%d", treeID, maxItem.UID)
			}
		}

		topMin := tree.GetTopMin(5)
		topMax := tree.GetTopMax(5)
		if topNChecks <= 2 {
			t.Logf("  %s: TopMin count=%d, TopMax count=%d", treeID, len(topMin), len(topMax))
		}
	}

	phase35Duration := time.Since(phase35Start)
	t.Logf("✓ Фаза 3.5 завершена за %v", phase35Duration)
	t.Logf("  Проверено %d деревьев, TopN найден в %d", topNChecks, topNFound)

	// Фаза 4: Обновления
	t.Log("Фаза 4: Обновления элементов...")
	phase4Start := time.Now()

	updateBatches := 0
	updatesPerformed := 0

	for updatesPerformed < numUpdates {
		batchSize := rnd.Intn(500) + 1
		if updatesPerformed+batchSize > numUpdates {
			batchSize = numUpdates - updatesPerformed
		}

		treeIdx := rnd.Intn(numTrees)
		treeID := fmt.Sprintf("tree%d", treeIdx)
		tree, _ := GetTree[*Account](mgr, treeID)

		items := make([]*Account, batchSize)
		for i := 0; i < batchSize; i++ {
			id := uint64(treeIdx*maxItems + rnd.Intn(treeItemCounts[treeID]))
			status := AccountStatus(rnd.Intn(3))
			items[i] = NewAccount(id, status)
		}

		tree.InsertBatch(items)
		updateBatches++
		updatesPerformed += batchSize

		if updatesPerformed%(numUpdates/10) == 0 || updatesPerformed == numUpdates {
			t.Logf("  Обновлено %d/%d элементов (%d батчей)", updatesPerformed, numUpdates, updateBatches)
		}
	}

	phase4Duration := time.Since(phase4Start)
	t.Logf("✓ Фаза 4 завершена за %v", phase4Duration)
	t.Logf("  Батчей: %d", updateBatches)
	t.Logf("  Средний размер батча: %d", numUpdates/updateBatches)
	t.Logf("  Производительность: %.0f ops/sec", float64(numUpdates)/phase4Duration.Seconds())

	// Фаза 5: Удаления
	t.Log("Фаза 5: Удаление элементов...")
	phase5Start := time.Now()

	totalDeleted := 0
	deleteBatches := 0

	for i := 0; i < numTrees; i++ {
		treeID := fmt.Sprintf("tree%d", i)
		tree, _ := GetTree[*Account](mgr, treeID)

		numDeletes := 100 + rnd.Intn(9900)
		if numDeletes > treeItemCounts[treeID] {
			numDeletes = treeItemCounts[treeID] / 2
		}

		deleted := 0
		for deleted < numDeletes {
			batchSize := rnd.Intn(500) + 1
			if deleted+batchSize > numDeletes {
				batchSize = numDeletes - deleted
			}

			idsToDelete := make([]uint64, batchSize)
			for j := 0; j < batchSize; j++ {
				id := uint64(i*maxItems + rnd.Intn(treeItemCounts[treeID]))
				idsToDelete[j] = id
			}

			count := tree.DeleteBatch(idsToDelete)
			deleted += count
			totalDeleted += count
			deleteBatches++
		}

		if (i+1)%10 == 0 || i == numTrees-1 {
			t.Logf("  Обработано %d/%d деревьев, удалено %d элементов", i+1, numTrees, totalDeleted)
		}
	}

	phase5Duration := time.Since(phase5Start)
	t.Logf("✓ Фаза 5 завершена за %v", phase5Duration)
	t.Logf("  Удалено элементов: %d", totalDeleted)
	t.Logf("  Батчей удаления: %d", deleteBatches)
	if phase5Duration.Seconds() > 0 {
		t.Logf("  Производительность: %.0f ops/sec", float64(totalDeleted)/phase5Duration.Seconds())
	}

	// Фаза 6: Повторное вычисление корней
	t.Log("Фаза 6: Повторное вычисление корней...")
	phase6Start := time.Now()

	changedTrees := 0
	for _, treeID := range treeIDs {
		tree, _ := GetTree[*Account](mgr, treeID)
		newHash := tree.ComputeRoot()
		if newHash != initialHashes[treeID] {
			changedTrees++
		}
	}

	phase6Duration := time.Since(phase6Start)
	t.Logf("✓ Фаза 6 завершена за %v", phase6Duration)
	t.Logf("  Изменено деревьев: %d/%d", changedTrees, numTrees)

	// Фаза 7: Новый глобальный корень
	t.Log("Фаза 7: Новый глобальный корень...")
	phase7Start := time.Now()
	newGlobalRoot := mgr.ComputeGlobalRoot()
	phase7Duration := time.Since(phase7Start)

	t.Logf("✓ Фаза 7 завершена за %v", phase7Duration)
	t.Logf("  Новый глобальный корень: %x", newGlobalRoot[:16])

	if newGlobalRoot == globalRoot {
		t.Error("Глобальный корень не должен остаться неизменным!")
	}

	// Итого
	totalDuration := phase1Duration + phase2Duration + phase3Duration + phase35Duration +
		phase4Duration + phase5Duration + phase6Duration + phase7Duration

	t.Log("==========================================")
	t.Logf("Общее время: %v", totalDuration)
	t.Logf("  Фаза 1 (создание): %v", phase1Duration)
	t.Logf("  Фаза 2 (корни деревьев): %v", phase2Duration)
	t.Logf("  Фаза 3 (глобальный корень): %v", phase3Duration)
	t.Logf("  Фаза 3.5 (TopN проверка): %v", phase35Duration)
	t.Logf("  Фаза 4 (обновления): %v", phase4Duration)
	t.Logf("  Фаза 5 (удаления): %v", phase5Duration)
	t.Logf("  Фаза 6 (пересчет): %v", phase6Duration)
	t.Logf("  Фаза 7 (новый глобал): %v", phase7Duration)

	stats := mgr.GetTotalStats()
	t.Log("==========================================")
	t.Logf("Итоговая статистика:")
	t.Logf("  Деревьев: %d", stats.TreeCount)
	t.Logf("  Элементов: %d", stats.TotalItems)
	t.Logf("  Узлов: %d", stats.TotalNodes)
	t.Logf("  Размер кешей: %d", stats.TotalCacheSize)

	t.Log("==========================================")
	for i := 0; i < min(3, numTrees); i++ {
		treeID := fmt.Sprintf("tree%d", i)
		tree, _ := GetTree[*Account](mgr, treeID)
		treeStats := tree.GetStats()

		t.Logf("Дерево %s:", treeID)
		t.Logf("  Элементов: %d", treeStats.TotalItems)
		t.Logf("  Удалено узлов: %d", treeStats.DeletedNodes)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ============================================
// TopN Cache тесты
// ============================================

func TestTopNCache(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 5
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		// ID не по порядку
		ids := []uint64{50, 10, 90, 30, 70, 20, 80, 40, 60, 100}
		for _, id := range ids {
			tree.Insert(NewAccount(id, StatusUser))
		}

		// Проверяем MinMax
		minItem, ok := tree.GetMin()
		if !ok {
			t.Fatal("GetMin failed")
		}
		if minItem.UID != 10 {
			t.Errorf("Min ID должен быть 10, получен %d", minItem.UID)
		}

		maxItem, ok := tree.GetMax()
		if !ok {
			t.Fatal("GetMax failed")
		}
		if maxItem.UID != 100 {
			t.Errorf("Max ID должен быть 100, получен %d", maxItem.UID)
		}

		// Top-5 Min
		topMin := tree.GetTopMin(5)
		if len(topMin) != 5 {
			t.Errorf("TopMin должен вернуть 5 элементов, получено %d", len(topMin))
		}
		expectedMin := []uint64{10, 20, 30, 40, 50}
		for i, item := range topMin {
			if item.UID != expectedMin[i] {
				t.Errorf("TopMin[%d]: ожидалось %d, получено %d", i, expectedMin[i], item.UID)
			}
		}

		// Top-5 Max
		topMax := tree.GetTopMax(5)
		if len(topMax) != 5 {
			t.Errorf("TopMax должен вернуть 5 элементов, получено %d", len(topMax))
		}
		expectedMax := []uint64{100, 90, 80, 70, 60}
		for i, item := range topMax {
			if item.UID != expectedMax[i] {
				t.Errorf("TopMax[%d]: ожидалось %d, получено %d", i, expectedMax[i], item.UID)
			}
		}
	})

	t.Run("WithDeletes", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 3
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		for i := uint64(10); i <= 100; i += 10 {
			tree.Insert(NewAccount(i, StatusUser))
		}

		// Удаляем минимум
		tree.Delete(10)
		tree.Delete(20)

		minItem, ok := tree.GetMin()
		if !ok {
			t.Fatal("GetMin failed")
		}
		if minItem.UID != 30 {
			t.Errorf("После удаления Min должен быть 30, получен %d", minItem.UID)
		}

		// Удаляем максимум
		tree.Delete(100)
		tree.Delete(90)

		maxItem, ok := tree.GetMax()
		if !ok {
			t.Fatal("GetMax failed")
		}
		if maxItem.UID != 80 {
			t.Errorf("После удаления Max должен быть 80, получен %d", maxItem.UID)
		}
	})

	t.Run("Disabled", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 0
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		// TopN отключен
		if tree.IsTopNEnabled() {
			t.Error("TopN должен быть отключен")
		}

		_, ok := tree.GetMin()
		if ok {
			t.Error("GetMin должен возвращать false когда TopN отключен")
		}

		topMin := tree.GetTopMin(5)
		if topMin != nil {
			t.Error("GetTopMin должен возвращать nil когда TopN отключен")
		}
	})
}

// ============================================
// TopN бенчмарки
// ============================================

func BenchmarkTopNOperations(b *testing.B) {
	b.Run("GetMin", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		for i := uint64(0); i < 100000; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, ok := tree.GetMin()
			if !ok {
				b.Fatal("GetMin failed")
			}
		}
	})

	b.Run("GetMax", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		for i := uint64(0); i < 100000; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, ok := tree.GetMax()
			if !ok {
				b.Fatal("GetMax failed")
			}
		}
	})

	b.Run("GetTopMin", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 20
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		for i := uint64(0); i < 100000; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			items := tree.GetTopMin(10)
			if len(items) == 0 {
				b.Fatal("GetTopMin returned empty")
			}
		}
	})

	b.Run("GetTopMax", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 20
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		for i := uint64(0); i < 100000; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			items := tree.GetTopMax(10)
			if len(items) == 0 {
				b.Fatal("GetTopMax returned empty")
			}
		}
	})
}

func BenchmarkTopNOverhead(b *testing.B) {
	b.Run("InsertWithTopN", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tree.Insert(NewAccount(uint64(i), StatusUser))
		}
	})

	b.Run("InsertWithoutTopN", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 0
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tree.Insert(NewAccount(uint64(i), StatusUser))
		}
	})

	b.Run("DeleteWithTopN", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		for i := 0; i < b.N; i++ {
			tree.Insert(NewAccount(uint64(i), StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tree.Delete(uint64(i))
		}
	})

	b.Run("DeleteWithoutTopN", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 0
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		for i := 0; i < b.N; i++ {
			tree.Insert(NewAccount(uint64(i), StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			tree.Delete(uint64(i))
		}
	})
}

func BenchmarkTopNVsFullScan(b *testing.B) {
	b.Run("TopNGetTop10", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		for i := uint64(0); i < 100000; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			items := tree.GetTopMin(10)
			_ = items
		}
	})

	b.Run("FullScanGetTop10", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 0
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "bench")

		for i := uint64(0); i < 100000; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			allItems := tree.GetAllItems()
			sort.Slice(allItems, func(i, j int) bool {
				return allItems[i].UID < allItems[j].UID
			})
			if len(allItems) > 10 {
				_ = allItems[:10]
			}
		}
	})
}

// ============================================
// Range Query бенчмарки
// ============================================

func BenchmarkManagerRangeQuery(b *testing.B) {
	sizes := []int{10, 25, 50, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			benchmarkManagerRangeQuerySize(b, size)
		})
	}
}

func benchmarkManagerRangeQuerySize(b *testing.B, rangeSize int) {
	mgr := NewUniversalManager(DefaultConfig())

	numTrees := 10
	itemsPerTree := 100000

	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, err := CreateTree[*Account](mgr, treeName)
		if err != nil {
			b.Fatalf("Failed to create tree: %v", err)
		}

		accounts := make([]*Account, itemsPerTree)
		for i := 0; i < itemsPerTree; i++ {
			id := uint64(treeIdx*1000000 + i)
			accounts[i] = NewAccount(id, StatusUser)
		}
		tree.InsertBatch(accounts)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		treeIdx := i % numTrees
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, _ := GetTree[*Account](mgr, treeName)

		startID := uint64(treeIdx*1000000 + (i%3)*itemsPerTree)
		endID := startID + uint64(rangeSize)

		result := tree.RangeQueryByID(startID, endID, true, false)
		if len(result) == 0 {
			b.Logf("Empty result for tree %s, range [%d, %d)", treeName, startID, endID)
		}
	}
}

func BenchmarkManagerRangeQueryParallel(b *testing.B) {
	sizes := []int{10, 25, 50, 100, 1000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			benchmarkManagerRangeQueryParallelSize(b, size)
		})
	}
}

func benchmarkManagerRangeQueryParallelSize(b *testing.B, rangeSize int) {
	mgr := NewUniversalManager(DefaultConfig())

	numTrees := 10
	itemsPerTree := 100000

	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, err := CreateTree[*Account](mgr, treeName)
		if err != nil {
			b.Fatalf("Failed to create tree: %v", err)
		}

		accounts := make([]*Account, itemsPerTree)
		for i := 0; i < itemsPerTree; i++ {
			id := uint64(treeIdx*1000000 + i)
			accounts[i] = NewAccount(id, StatusUser)
		}
		tree.InsertBatch(accounts)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			treeIdx := counter % numTrees
			treeName := fmt.Sprintf("tree%d", treeIdx)
			tree, _ := GetTree[*Account](mgr, treeName)

			startID := uint64(treeIdx*1000000 + (counter%3)*itemsPerTree)
			endID := startID + uint64(rangeSize)

			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
			counter++
		}
	})
}

func BenchmarkManagerRangeQueryCrossTree(b *testing.B) {
	mgr := NewUniversalManager(DefaultConfig())

	numTrees := 10
	itemsPerTree := 100000

	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, err := CreateTree[*Account](mgr, treeName)
		if err != nil {
			b.Fatalf("Failed to create tree: %v", err)
		}

		accounts := make([]*Account, itemsPerTree)
		for i := 0; i < itemsPerTree; i++ {
			id := uint64(treeIdx*1000000 + i)
			accounts[i] = NewAccount(id, StatusUser)
		}
		tree.InsertBatch(accounts)
	}

	rangeSize := 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
			treeName := fmt.Sprintf("tree%d", treeIdx)
			tree, _ := GetTree[*Account](mgr, treeName)

			startID := uint64(treeIdx*1000000 + (i%3)*itemsPerTree)
			endID := startID + uint64(rangeSize)

			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
		}
	}
}

func BenchmarkManagerMixedOps(b *testing.B) {
	mgr := NewUniversalManager(DefaultConfig())

	numTrees := 10
	itemsPerTree := 50000

	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, err := CreateTree[*Account](mgr, treeName)
		if err != nil {
			b.Fatalf("Failed to create tree: %v", err)
		}

		accounts := make([]*Account, itemsPerTree)
		for i := 0; i < itemsPerTree; i++ {
			id := uint64(treeIdx*1000000 + i)
			accounts[i] = NewAccount(id, StatusUser)
		}
		tree.InsertBatch(accounts)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		treeIdx := i % numTrees
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, _ := GetTree[*Account](mgr, treeName)

		if i%5 == 0 {
			// 20% - вставка
			newID := uint64(treeIdx*1000000 + itemsPerTree + i)
			acc := NewAccount(newID, StatusUser)
			tree.Insert(acc)
		} else {
			// 80% - range-запрос
			startID := uint64(treeIdx*1000000 + (i%3)*itemsPerTree)
			endID := startID + 50
			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
		}
	}
}

func BenchmarkManagerRangeQueryDifferentDepths(b *testing.B) {
	configs := []struct {
		name   string
		config *Config
	}{
		{"Depth3Small", SmallConfig()},
		{"Depth3Medium", MediumConfig()},
		{"Depth4Large", LargeConfig()},
		{"Depth5Huge", HugeConfig()},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			mgr := NewUniversalManager(cfg.config)
			tree, err := CreateTree[*Account](mgr, "test")
			if err != nil {
				b.Fatalf("Failed to create tree: %v", err)
			}

			accounts := make([]*Account, 100000)
			for i := 0; i < 100000; i++ {
				accounts[i] = NewAccount(uint64(i), StatusUser)
			}
			tree.InsertBatch(accounts)

			rangeSize := 100

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				startID := uint64((i % 3) * 90000)
				endID := startID + uint64(rangeSize)
				result := tree.RangeQueryByID(startID, endID, true, false)
				if len(result) == 0 {
					b.Logf("Empty result for range [%d, %d)", startID, endID)
				}
			}
		})
	}
}

func BenchmarkManagerRangeQuerySequentialVsRandom(b *testing.B) {
	b.Run("Sequential", func(b *testing.B) {
		mgr := NewUniversalManager(DefaultConfig())
		tree, _ := CreateTree[*Account](mgr, "test")

		accounts := make([]*Account, 100000)
		for i := 0; i < 100000; i++ {
			accounts[i] = NewAccount(uint64(i), StatusUser)
		}
		tree.InsertBatch(accounts)

		rangeSize := 100

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			startID := uint64((i * rangeSize) % 90000)
			endID := startID + uint64(rangeSize)
			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
		}
	})

	b.Run("Random", func(b *testing.B) {
		mgr := NewUniversalManager(DefaultConfig())
		tree, _ := CreateTree[*Account](mgr, "test")

		accounts := make([]*Account, 100000)
		for i := 0; i < 100000; i++ {
			accounts[i] = NewAccount(uint64(i), StatusUser)
		}
		tree.InsertBatch(accounts)

		rangeSize := 100

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			startID := uint64(((i * 13) % (i + 7)) % 90000)
			endID := startID + uint64(rangeSize)
			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
		}
	})
}

func BenchmarkManagerRangeQueryQuick(b *testing.B) {
	sizes := []int{10, 25, 50, 100}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			benchmarkManagerRangeQueryQuickSize(b, size)
		})
	}
}

func benchmarkManagerRangeQueryQuickSize(b *testing.B, rangeSize int) {
	mgr := NewUniversalManager(DefaultConfig())

	numTrees := 3
	itemsPerTree := 10000

	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, err := CreateTree[*Account](mgr, treeName)
		if err != nil {
			b.Fatalf("Failed to create tree: %v", err)
		}

		accounts := make([]*Account, itemsPerTree)
		for i := 0; i < itemsPerTree; i++ {
			id := uint64(treeIdx*100000 + i)
			accounts[i] = NewAccount(id, StatusUser)
		}
		tree.InsertBatch(accounts)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		treeIdx := i % numTrees
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, _ := GetTree[*Account](mgr, treeName)

		startID := uint64(treeIdx*100000 + (i%3)*itemsPerTree)
		endID := startID + uint64(rangeSize)

		result := tree.RangeQueryByID(startID, endID, true, false)
		_ = result
	}
}

func BenchmarkManagerRangeQueryParallelQuick(b *testing.B) {
	mgr := NewUniversalManager(DefaultConfig())

	numTrees := 3
	itemsPerTree := 10000

	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree%d", treeIdx)
		tree, _ := CreateTree[*Account](mgr, treeName)

		accounts := make([]*Account, itemsPerTree)
		for i := 0; i < itemsPerTree; i++ {
			id := uint64(treeIdx*100000 + i)
			accounts[i] = NewAccount(id, StatusUser)
		}
		tree.InsertBatch(accounts)
	}

	rangeSize := 50

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			treeIdx := counter % numTrees
			treeName := fmt.Sprintf("tree%d", treeIdx)
			tree, _ := GetTree[*Account](mgr, treeName)

			startID := uint64(treeIdx*100000 + (counter%3)*itemsPerTree)
			endID := startID + uint64(rangeSize)

			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
			counter++
		}
	})
}

func BenchmarkManagerRangeQuerySingleTree(b *testing.B) {
	sizes := []int{10, 50, 100, 500}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			mgr := NewUniversalManager(DefaultConfig())
			tree, _ := CreateTree[*Account](mgr, "test")

			accounts := make([]*Account, 10000)
			for i := 0; i < 10000; i++ {
				accounts[i] = NewAccount(uint64(i), StatusUser)
			}
			tree.InsertBatch(accounts)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				startID := uint64((i % 3) * 9000)
				endID := startID + uint64(size)
				result := tree.RangeQueryByID(startID, endID, true, false)
				_ = result
			}
		})
	}
}

func BenchmarkManagerRangeQueryTiny(b *testing.B) {
	mgr := NewUniversalManager(DefaultConfig())
	tree, _ := CreateTree[*Account](mgr, "tiny")

	accounts := make([]*Account, 1000)
	for i := 0; i < 1000; i++ {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(accounts)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		startID := uint64((i % 3) * 900)
		endID := startID + 50
		result := tree.RangeQueryByID(startID, endID, true, false)
		_ = result
	}
}

func BenchmarkManagerRangeQueryCompare(b *testing.B) {
	mgr := NewUniversalManager(DefaultConfig())
	tree, _ := CreateTree[*Account](mgr, "compare")

	accounts := make([]*Account, 50000)
	for i := 0; i < 50000; i++ {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(accounts)

	b.Run("SequentialSmall", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			startID := uint64((i % 3) * 40000)
			result := tree.RangeQueryByID(startID, startID+50, true, false)
			_ = result
		}
	})

	b.Run("SequentialLarge", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			startID := uint64((i % 3) * 30000)
			result := tree.RangeQueryByID(startID, startID+1000, true, false)
			_ = result
		}
	})

	b.Run("ParallelSmall", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				startID := uint64((counter % 3) * 40000)
				result := tree.RangeQueryByID(startID, startID+50, true, false)
				_ = result
				counter++
			}
		})
	})

	b.Run("ParallelLarge", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				startID := uint64((counter % 3) * 30000)
				result := tree.RangeQueryByID(startID, startID+1000, true, false)
				_ = result
				counter++
			}
		})
	})
}

func BenchmarkManagerRangeQueryCacheBehavior(b *testing.B) {
	b.Run("HotRange", func(b *testing.B) {
		mgr := NewUniversalManager(DefaultConfig())
		tree, _ := CreateTree[*Account](mgr, "hot")

		accounts := make([]*Account, 10000)
		for i := 0; i < 10000; i++ {
			accounts[i] = NewAccount(uint64(i), StatusUser)
		}
		tree.InsertBatch(accounts)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			result := tree.RangeQueryByID(1000, 1100, true, false)
			_ = result
		}
	})

	b.Run("ColdRange", func(b *testing.B) {
		mgr := NewUniversalManager(DefaultConfig())
		tree, _ := CreateTree[*Account](mgr, "cold")

		accounts := make([]*Account, 10000)
		for i := 0; i < 10000; i++ {
			accounts[i] = NewAccount(uint64(i), StatusUser)
		}
		tree.InsertBatch(accounts)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			startID := uint64((i * 100) % 9000)
			result := tree.RangeQueryByID(startID, startID+100, true, false)
			_ = result
		}
	})
}

// ============================================
// TopN Iterator тесты
// ============================================

func TestTopNIterator(t *testing.T) {
	t.Run("BasicIteration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 5
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		ids := []uint64{50, 10, 90, 30, 70, 20, 80, 40, 60, 100}
		for _, id := range ids {
			tree.Insert(NewAccount(id, StatusUser))
		}

		iter := tree.IterTopMin()
		count := 0
		prev := uint64(0)

		for iter.HasNext() {
			account, ok := iter.Next()
			if !ok {
				t.Fatal("Next() вернул false при HasNext()==true")
			}
			if account.UID <= prev {
				t.Errorf("Порядок нарушен: %d <= %d", account.UID, prev)
			}
			prev = account.UID
			count++
		}

		if count != 5 {
			t.Errorf("Ожидалось 5 элементов, получено %d", count)
		}

		_, ok := iter.Next()
		if ok {
			t.Error("Next() должен вернуть false")
		}
	})

	t.Run("PeekAndNext", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 3
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i*10, StatusUser))
		}

		iter := tree.IterTopMin()

		first, _ := iter.Peek()
		same, _ := iter.Peek()
		if first.UID != same.UID {
			t.Error("Peek должен возвращать один и тот же элемент")
		}

		taken, _ := iter.Next()
		if taken.UID != first.UID {
			t.Error("Next должен вернуть то, что показал Peek")
		}

		second, _ := iter.Peek()
		if second.UID <= first.UID {
			t.Error("После Next, Peek должен показать следующий элемент")
		}
	})

	t.Run("Reset", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 5
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		iter := tree.IterTopMax()

		firstRun := make([]uint64, 0)
		for i := 0; i < 3 && iter.HasNext(); i++ {
			acc, _ := iter.Next()
			firstRun = append(firstRun, acc.UID)
		}

		if len(firstRun) != 3 {
			t.Fatalf("Ожидалось 3 элемента, получено %d", len(firstRun))
		}

		iter.Reset()

		secondRun := make([]uint64, 0)
		for i := 0; i < 3 && iter.HasNext(); i++ {
			acc, _ := iter.Next()
			secondRun = append(secondRun, acc.UID)
		}

		for i := 0; i < 3; i++ {
			if firstRun[i] != secondRun[i] {
				t.Errorf("После Reset элемент %d: %d != %d", i, firstRun[i], secondRun[i])
			}
		}
	})

	t.Run("TakeWhile", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		for i := uint64(1); i <= 20; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		iter := tree.IterTopMin()
		taken := iter.TakeWhile(func(acc *Account) bool {
			return acc.UID <= 5
		})

		if len(taken) != 5 {
			t.Errorf("Ожидалось 5 элементов, получено %d", len(taken))
		}

		for i, acc := range taken {
			expected := uint64(i + 1)
			if acc.UID != expected {
				t.Errorf("taken[%d]: ожидалось %d, получено %d", i, expected, acc.UID)
			}
		}

		next, ok := iter.Peek()
		if !ok || next.UID != 6 {
			t.Errorf("После TakeWhile итератор должен быть на элементе 6")
		}
	})

	t.Run("ForEach", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 5
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i*10, StatusUser))
		}

		iter := tree.IterTopMin()

		collected := make([]uint64, 0)
		iter.ForEach(func(acc *Account) {
			collected = append(collected, acc.UID)
		})

		if len(collected) != 5 {
			t.Errorf("Ожидалось 5 элементов, получено %d", len(collected))
		}

		if iter.HasNext() {
			t.Error("После ForEach итератор должен быть исчерпан")
		}
	})

	t.Run("EmptyIterator", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 0
		mgr := NewUniversalManager(cfg)
		tree, _ := CreateTree[*Account](mgr, "test")

		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}

		iter := tree.IterTopMin()
		if iter.HasNext() {
			t.Error("Итератор должен быть пустым когда TopN отключен")
		}

		if iter.Remaining() != 0 {
			t.Error("Remaining должен вернуть 0")
		}
	})
}

func BenchmarkTopNIterator(b *testing.B) {
	cfg := DefaultConfig()
	cfg.TopN = 20
	mgr := NewUniversalManager(cfg)
	tree, _ := CreateTree[*Account](mgr, "bench")

	for i := uint64(0); i < 100000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	b.Run("Iteration", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iter := tree.IterTopMin()
			count := 0
			for iter.HasNext() {
				_, _ = iter.Next()
				count++
			}
		}
	})

	b.Run("TakeWhile", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iter := tree.IterTopMin()
			_ = iter.TakeWhile(func(acc *Account) bool {
				return acc.UID <= 10
			})
		}
	})

	b.Run("ForEach", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			iter := tree.IterTopMin()
			iter.ForEach(func(acc *Account) {
				_ = acc.UID
			})
		}
	})
}
