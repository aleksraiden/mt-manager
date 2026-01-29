package merkletree

import (
	"fmt"
	"math/rand"
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

	// TopN с 10 элементами
	cfg := DefaultConfig()
	cfg.TopN = 10

	// Создаем универсальный менеджер (КЛЮЧЕВОЕ ИЗМЕНЕНИЕ!)
	mgr := NewUniversalManager(cfg)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Фаза 1: Создание и заполнение деревьев
	t.Log("Фаза 1: Создание и заполнение деревьев...")
	phase1Start := time.Now()
	totalItems := 0
	treeItemCounts := make(map[string]int)

	for i := 0; i < numTrees; i++ {
		treeID := fmt.Sprintf("tree%d", i)
		tree, _ := CreateTree[*Account](mgr, treeID) // Generic функция

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
		tree, exists := GetTree[*Account](mgr, treeID) // Generic функция
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
		tree, _ := GetTree[*Account](mgr, treeID) // Generic функция

		// TopN должен быть включен
		if !tree.IsTopNEnabled() {
			t.Errorf("TopN должен быть включен для дерева %s", treeID)
			continue
		}
		topNChecks++

		// Проверяем min/max
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

		// Top-5
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
		tree, _ := GetTree[*Account](mgr, treeID) // Generic функция

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
		tree, _ := GetTree[*Account](mgr, treeID) // Generic функция

		// Удаляем от 100 до 10K элементов
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
		tree, _ := GetTree[*Account](mgr, treeID) // Generic функция
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

	// Показываем первые 3 дерева
	t.Log("==========================================")
	for i := 0; i < min(3, numTrees); i++ {
		treeID := fmt.Sprintf("tree%d", i)
		tree, _ := GetTree[*Account](mgr, treeID) // Generic функция
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

// Бенчмарк range-запросов
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

	// Создаем 10 деревьев по 100K элементов
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
			// ID для каждого дерева: treeIdx * 1M + i
			id := uint64(treeIdx*1000000 + i)
			accounts[i] = NewAccount(id, StatusUser)
		}
		tree.InsertBatch(accounts)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Делаем range-запросы
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

// Бенчмарк смешанных операций
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

	// 80% range-запросы, 20% вставки
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
