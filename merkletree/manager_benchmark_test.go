package merkletree

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestManagerStressTest(t *testing.T) {
	tests := []struct {
		name     string
		numTrees int
		minItems int
		maxItems int
		updates  int
	}{
		{
			name:     "Trees_10",
			numTrees: 10,
			minItems: 100000,
			maxItems: 1000000,
			updates:  100000,
		},
		{
			name:     "Trees_100",
			numTrees: 100,
			minItems: 100000,
			maxItems: 1000000,
			updates:  100000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runStressTest(t, tt.numTrees, tt.minItems, tt.maxItems, tt.updates)
		})
	}
}

func runStressTest(t *testing.T, numTrees, minItems, maxItems, numUpdates int) {
	t.Logf("=== Тест с %d деревьями ===", numTrees)
	
	// ✅ ИЗМЕНЕНИЕ: Используем конфигурацию с TopN=10
	cfg := DefaultConfig()
	cfg.TopN = 10  // Включаем TopN кеш
	mgr := NewManager[*Account](cfg)
	
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Фаза 1: Создание и заполнение деревьев
	t.Log("Фаза 1: Создание и заполнение деревьев...")
	phase1Start := time.Now()
	totalItems := 0
	treeItemCounts := make(map[string]int)
	
	for i := 0; i < numTrees; i++ {
		treeID := fmt.Sprintf("tree_%d", i)
		tree := mgr.GetOrCreateTree(treeID)
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
			t.Logf("  Создано %d/%d деревьев, всего элементов: %d", i+1, numTrees, totalItems)
		}
	}
	
	phase1Duration := time.Since(phase1Start)
	t.Logf("Фаза 1 завершена за %v", phase1Duration)
	t.Logf("  Всего элементов: %d", totalItems)
	t.Logf("  Средний размер дерева: %d", totalItems/numTrees)

	// Фаза 2: Вычисление хешей всех деревьев
	t.Log("\nФаза 2: Вычисление хешей деревьев...")
	phase2Start := time.Now()
	initialHashes := make(map[string][32]byte)
	treeIDs := mgr.ListTrees()
	
	for _, treeID := range treeIDs {
		tree, exists := mgr.GetTree(treeID)
		if !exists {
			t.Fatalf("Tree %s not found", treeID)
		}
		hash := tree.ComputeRoot()
		initialHashes[treeID] = hash
	}
	
	phase2Duration := time.Since(phase2Start)
	t.Logf("Фаза 2 завершена за %v", phase2Duration)
	t.Logf("  Среднее время на дерево: %v", phase2Duration/time.Duration(numTrees))
	
	// Печать случайных хешей
	count := 0
	for treeID, hash := range initialHashes {
		if count < 3 {
			t.Logf("  %s: %x", treeID, hash[:16])
			count++
		}
	}

	// Фаза 3: Вычисление глобального корня
	t.Log("\nФаза 3: Вычисление глобального корня...")
	phase3Start := time.Now()
	globalRoot := mgr.ComputeGlobalRoot()
	phase3Duration := time.Since(phase3Start)
	t.Logf("Фаза 3 завершена за %v", phase3Duration)
	t.Logf("  Глобальный корень: %x", globalRoot[:16])

	// ✅ НОВАЯ ФАЗА 3.5: Проверка TopN кеша
	t.Log("\nФаза 3.5: Проверка TopN кеша...")
	phase35Start := time.Now()
	topNChecks := 0
	topNFound := 0
	
	for _, treeID := range treeIDs {
		tree, _ := mgr.GetTree(treeID)
		
		// Проверяем, что TopN включен
		if !tree.IsTopNEnabled() {
			t.Errorf("TopN должен быть включен для дерева %s", treeID)
			continue
		}
		
		topNChecks++
		
		// Получаем min/max
		if _, ok := tree.GetMin(); ok {
			topNFound++
			//t.Logf("  %s: Min ID=%d", treeID, minItem.UID)
		}
		
		if _, ok := tree.GetMax(); ok {
			//t.Logf("  %s: Max ID=%d", treeID, maxItem.UID)
		}
		
		// Проверяем Top-5
		topMin := tree.GetTopMin(5)
		topMax := tree.GetTopMax(5)
		
		if topNChecks <= 2 {
			t.Logf("  %s: TopMin count=%d, TopMax count=%d", 
				treeID, len(topMin), len(topMax))
		}
	}
	
	phase35Duration := time.Since(phase35Start)
	t.Logf("Фаза 3.5 завершена за %v", phase35Duration)
	t.Logf("  Проверено деревьев: %d, с TopN данными: %d", topNChecks, topNFound)

	// Фаза 4: Случайные обновления
	t.Log("\nФаза 4: Выполнение случайных обновлений...")
	phase4Start := time.Now()
	updateBatches := 0
	updatesPerformed := 0
	
	for updatesPerformed < numUpdates {
		batchSize := rnd.Intn(500) + 1
		if updatesPerformed+batchSize > numUpdates {
			batchSize = numUpdates - updatesPerformed
		}
		
		treeIdx := rnd.Intn(numTrees)
		treeID := fmt.Sprintf("tree_%d", treeIdx)
		tree, _ := mgr.GetTree(treeID)
		items := make([]*Account, batchSize)
		
		for i := 0; i < batchSize; i++ {
			id := uint64(treeIdx*maxItems + rnd.Intn(treeItemCounts[treeID]))
			status := AccountStatus(rnd.Intn(3))
			items[i] = NewAccount(id, status)
		}
		
		tree.InsertBatch(items)
		updateBatches++
		updatesPerformed += batchSize
		
		if updatesPerformed%(numUpdates/10) == 0 || updatesPerformed >= numUpdates {
			t.Logf("  Выполнено %d/%d обновлений (%d батчей)", updatesPerformed, numUpdates, updateBatches)
		}
	}
	
	phase4Duration := time.Since(phase4Start)
	t.Logf("Фаза 4 завершена за %v", phase4Duration)
	t.Logf("  Батчей обновлений: %d", updateBatches)
	t.Logf("  Средний размер батча: %d", numUpdates/updateBatches)
	t.Logf("  Скорость обновлений: %.0f ops/sec", float64(numUpdates)/phase4Duration.Seconds())

	// Фаза 5: Случайные удаления
	t.Log("\nФаза 5: Выполнение случайных удалений...")
	phase5Start := time.Now()
	totalDeleted := 0
	deleteBatches := 0
	
	for i := 0; i < numTrees; i++ {
		treeID := fmt.Sprintf("tree_%d", i)
		tree, _ := mgr.GetTree(treeID)
		
		// Случайное количество удалений от 100 до 10K
		numDeletes := 100 + rnd.Intn(9900)
		if numDeletes > treeItemCounts[treeID] {
			numDeletes = treeItemCounts[treeID] / 2
		}
		
		// Удаляем батчами
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
			t.Logf("  Обработано %d/%d деревьев, удалено элементов: %d", i+1, numTrees, totalDeleted)
		}
	}
	
	phase5Duration := time.Since(phase5Start)
	t.Logf("Фаза 5 завершена за %v", phase5Duration)
	t.Logf("  Всего удалено: %d элементов", totalDeleted)
	t.Logf("  Батчей удалений: %d", deleteBatches)
	if phase5Duration.Seconds() > 0 {
		t.Logf("  Скорость удалений: %.0f ops/sec", float64(totalDeleted)/phase5Duration.Seconds())
	}

	// Фаза 6: Пересчет хешей после удалений
	t.Log("\nФаза 6: Пересчет хешей после удалений...")
	phase6Start := time.Now()
	changedTrees := 0
	
	for _, treeID := range treeIDs {
		tree, _ := mgr.GetTree(treeID)
		newHash := tree.ComputeRoot()
		if newHash != initialHashes[treeID] {
			changedTrees++
		}
	}
	
	phase6Duration := time.Since(phase6Start)
	t.Logf("Фаза 6 завершена за %v", phase6Duration)
	t.Logf("  Деревьев с измененным хешем: %d/%d", changedTrees, numTrees)

	// Фаза 7: Новый глобальный корень
	t.Log("\nФаза 7: Новый глобальный корень...")
	phase7Start := time.Now()
	newGlobalRoot := mgr.ComputeGlobalRoot()
	phase7Duration := time.Since(phase7Start)
	t.Logf("Фаза 7 завершена за %v", phase7Duration)
	t.Logf("  Новый глобальный корень: %x", newGlobalRoot[:16])
	
	if newGlobalRoot == globalRoot {
		t.Error("Глобальный корень не должен совпадать после удалений!")
	}

	t.Log("\nФаза 8: Проверка TopN после удалений...")
	phase8Start := time.Now()
	topNStillValid := 0

	for i := 0; i < min(3, numTrees); i++ {
		treeID := fmt.Sprintf("tree_%d", i)
		tree, _ := mgr.GetTree(treeID)
		
		// Проверяем, что TopN все еще работает
		minItem, minOk := tree.GetMin()
		maxItem, maxOk := tree.GetMax()
		
		if minOk && maxOk {
			topNStillValid++
			minKey := keyToUint64(minItem.Key())
			maxKey := keyToUint64(maxItem.Key())
			
			t.Logf("  %s: Min ID=%d (key=%d), Max ID=%d (key=%d)", 
				treeID, minItem.UID, minKey, maxItem.UID, maxKey)
			
			// Проверяем, что min < max
			if minKey >= maxKey {
				t.Errorf("  %s: Min key (%d) должен быть < Max key (%d)!", 
					treeID, minKey, maxKey)
			}
		}
		
		// Top-3 для проверки
		topMin := tree.GetTopMin(3)
		topMax := tree.GetTopMax(3)
		
		t.Logf("  %s: TopMin=%d элементов, TopMax=%d элементов", 
			treeID, len(topMin), len(topMax))
		
		// Выводим ключи для отладки
		if len(topMin) > 0 {
			minKeys := make([]uint64, len(topMin))
			for j, item := range topMin {
				minKeys[j] = keyToUint64(item.Key())
			}
			t.Logf("    TopMin keys: %v", minKeys)
		}
		
		if len(topMax) > 0 {
			maxKeys := make([]uint64, len(topMax))
			for j, item := range topMax {
				maxKeys[j] = keyToUint64(item.Key())
			}
			t.Logf("    TopMax keys: %v", maxKeys)
		}
		
		// ✅ ИСПРАВЛЕНИЕ: Проверяем сортировку TopMin (ascending)
		for j := 0; j < len(topMin)-1; j++ {
			keyJ := keyToUint64(topMin[j].Key())
			keyJNext := keyToUint64(topMin[j+1].Key())
			
			// Для ascending: каждый следующий должен быть >= текущего
			if keyJ > keyJNext {
				t.Errorf("  %s: TopMin не отсортирован! [%d]=%d > [%d]=%d", 
					treeID, j, keyJ, j+1, keyJNext)
			}
		}
		
		// ✅ ИСПРАВЛЕНИЕ: Проверяем сортировку TopMax (descending)
		for j := 0; j < len(topMax)-1; j++ {
			keyJ := keyToUint64(topMax[j].Key())
			keyJNext := keyToUint64(topMax[j+1].Key())
			
			// Для descending: каждый следующий должен быть <= текущего
			// Ошибка если следующий БОЛЬШЕ текущего
			if keyJ < keyJNext {
				t.Errorf("  %s: TopMax не отсортирован! [%d]=%d < [%d]=%d", 
					treeID, j, keyJ, j+1, keyJNext)
			}
		}
	}

	phase8Duration := time.Since(phase8Start)
	t.Logf("Фаза 8 завершена за %v", phase8Duration)
	t.Logf("  Деревьев с валидным TopN: %d", topNStillValid)

	// Итоговая статистика
	totalDuration := phase1Duration + phase2Duration + phase3Duration +
		phase35Duration + phase4Duration + phase5Duration + 
		phase6Duration + phase7Duration + phase8Duration
	
	t.Log("\n=== Итоговая статистика ===")
	t.Logf("Общее время: %v", totalDuration)
	t.Logf("  Фаза 1 (заполнение): %v", phase1Duration)
	t.Logf("  Фаза 2 (хеши): %v", phase2Duration)
	t.Logf("  Фаза 3 (глобальный рут): %v", phase3Duration)
	t.Logf("  Фаза 3.5 (проверка TopN): %v", phase35Duration)
	t.Logf("  Фаза 4 (обновления): %v", phase4Duration)
	t.Logf("  Фаза 5 (удаления): %v", phase5Duration)
	t.Logf("  Фаза 6 (пересчет): %v", phase6Duration)
	t.Logf("  Фаза 7 (новый рут): %v", phase7Duration)
	t.Logf("  Фаза 8 (TopN после удалений): %v", phase8Duration)
	
	stats := mgr.GetStats()
	t.Log("\nСтатистика менеджера:")
	t.Logf("  Деревьев: %d", stats.TreeCount)
	t.Logf("  Элементов: %d", stats.TotalItems)
	t.Logf("  Узлов: %d", stats.TotalNodes)
	t.Logf("  Размер кеша: %d", stats.TotalCacheSize)
	
	// Детальная статистика по удалениям
	t.Log("\nСтатистика удалений по деревьям:")
	for i := 0; i < min(3, numTrees); i++ {
		treeID := fmt.Sprintf("tree_%d", i)
		tree, _ := mgr.GetTree(treeID)
		treeStats := tree.GetStats()
		t.Logf("  %s:", treeID)
		t.Logf("    Элементов: %d", treeStats.TotalItems)
		t.Logf("    Удаленных узлов: %d", treeStats.DeletedNodes)
	}
}

// ✅ НОВЫЙ ТЕСТ: Функциональность TopN
func TestTopNCache(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 5
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		// Вставляем элементы с известными ID
		ids := []uint64{50, 10, 90, 30, 70, 20, 80, 40, 60, 100}
		for _, id := range ids {
			tree.Insert(NewAccount(id, StatusUser))
		}
		
		// Проверяем Min/Max
		minItem, ok := tree.GetMin()
		if !ok {
			t.Fatal("GetMin должен вернуть элемент")
		}
		if minItem.UID != 10 {
			t.Errorf("Min ID должен быть 10, получили %d", minItem.UID)
		}
		
		maxItem, ok := tree.GetMax()
		if !ok {
			t.Fatal("GetMax должен вернуть элемент")
		}
		if maxItem.UID != 100 {
			t.Errorf("Max ID должен быть 100, получили %d", maxItem.UID)
		}
		
		// Проверяем Top-5 Min
		topMin := tree.GetTopMin(5)
		if len(topMin) != 5 {
			t.Errorf("TopMin должен вернуть 5 элементов, получили %d", len(topMin))
		}
		
		expectedMin := []uint64{10, 20, 30, 40, 50}
		for i, item := range topMin {
			if item.UID != expectedMin[i] {
				t.Errorf("TopMin[%d] должен быть %d, получили %d", 
					i, expectedMin[i], item.UID)
			}
		}
		
		// Проверяем Top-5 Max
		topMax := tree.GetTopMax(5)
		if len(topMax) != 5 {
			t.Errorf("TopMax должен вернуть 5 элементов, получили %d", len(topMax))
		}
		
		expectedMax := []uint64{100, 90, 80, 70, 60}
		for i, item := range topMax {
			if item.UID != expectedMax[i] {
				t.Errorf("TopMax[%d] должен быть %d, получили %d", 
					i, expectedMax[i], item.UID)
			}
		}
	})
	
	t.Run("WithDeletes", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 3
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		// Вставляем элементы
		for i := uint64(10); i <= 100; i += 10 {
			tree.Insert(NewAccount(i, StatusUser))
		}
		
		// Удаляем минимум
		tree.Delete(10)
		
		// Новый минимум должен быть 20
		minItem, ok := tree.GetMin()
		if !ok {
			t.Fatal("GetMin должен вернуть элемент после удаления")
		}
		if minItem.UID != 20 {
			t.Errorf("Min после удаления должен быть 20, получили %d", minItem.UID)
		}
		
		// Удаляем максимум
		tree.Delete(100)
		
		// Новый максимум должен быть 90
		maxItem, ok := tree.GetMax()
		if !ok {
			t.Fatal("GetMax должен вернуть элемент после удаления")
		}
		if maxItem.UID != 90 {
			t.Errorf("Max после удаления должен быть 90, получили %d", maxItem.UID)
		}
	})
	
	t.Run("Disabled", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 0  // Отключен
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		// Вставляем элементы
		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}
		
		// TopN должен быть отключен
		if tree.IsTopNEnabled() {
			t.Error("TopN должен быть отключен")
		}
		
		// Методы должны возвращать пустые результаты
		_, ok := tree.GetMin()
		if ok {
			t.Error("GetMin должен вернуть false когда TopN отключен")
		}
		
		topMin := tree.GetTopMin(5)
		if topMin != nil {
			t.Error("GetTopMin должен вернуть nil когда TopN отключен")
		}
	})
}

// ✅ НОВЫЕ БЕНЧМАРКИ: TopN операции
func BenchmarkTopNOperations(b *testing.B) {
	b.Run("GetMin", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
		// Заполняем дерево
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
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
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
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
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
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
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

// ✅ БЕНЧМАРК: Накладные расходы TopN на Insert/Delete
func BenchmarkTopNOverhead(b *testing.B) {
	b.Run("Insert_WithTopN", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			tree.Insert(NewAccount(uint64(i), StatusUser))
		}
	})
	
	b.Run("Insert_WithoutTopN", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 0
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			tree.Insert(NewAccount(uint64(i), StatusUser))
		}
	})
	
	b.Run("Delete_WithTopN", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
		// Предварительно заполняем
		for i := 0; i < b.N; i++ {
			tree.Insert(NewAccount(uint64(i), StatusUser))
		}
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			tree.Delete(uint64(i))
		}
	})
	
	b.Run("Delete_WithoutTopN", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 0
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
		// Предварительно заполняем
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

// ✅ БЕНЧМАРК: Сравнение TopN vs полное сканирование
func BenchmarkTopNVsFullScan(b *testing.B) {
	b.Run("TopN_GetTop10", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
		// Заполняем 100K элементов
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
	
	b.Run("FullScan_GetTop10", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.TopN = 0  // Отключаем TopN
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("bench")
		
		// Заполняем 100K элементов
		for i := uint64(0); i < 100000; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}
		
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			// Эмулируем полное сканирование
			allItems := tree.GetAllItems()
			
			// Сортируем (в реальности потребуется sort)
			if len(allItems) < 10 {
				continue
			}
			
			// Просто берем первые 10 для сравнения накладных расходов
			_ = allItems[:10]
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// BenchmarkManagerRangeQuery тестирует range-запросы с разными размерами
func BenchmarkManagerRangeQuery(b *testing.B) {
	sizes := []int{10, 25, 50, 100, 1000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			benchmarkManagerRangeQuerySize(b, size)
		})
	}
}

func benchmarkManagerRangeQuerySize(b *testing.B, rangeSize int) {
	mgr := NewManager[*Account](LargeConfig())
	
	// Создаем 10 деревьев по 100K элементов в каждом
	numTrees := 10
	itemsPerTree := 100000
	
	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, err := mgr.CreateTree(treeName)
		if err != nil {
			b.Fatalf("Failed to create tree: %v", err)
		}
		
		// Вставляем элементы
		accounts := make([]*Account, itemsPerTree)
		for i := 0; i < itemsPerTree; i++ {
			// Уникальный ID = treeIdx * 1M + i
			id := uint64(treeIdx*1000000 + i)
			accounts[i] = NewAccount(id, StatusUser)
		}
		tree.InsertBatch(accounts)
	}
	
	b.ResetTimer()
	b.ReportAllocs()
	
	// Выполняем range-запросы из случайных деревьев
	for i := 0; i < b.N; i++ {
		treeIdx := i % numTrees
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, _ := mgr.GetTree(treeName)
		
		// Случайная начальная позиция
		startID := uint64(treeIdx*1000000 + (i*13)%itemsPerTree)
		endID := startID + uint64(rangeSize)
		
		result := tree.RangeQueryByID(startID, endID, true, false)
		if len(result) == 0 {
			b.Logf("Empty result for tree %s, range [%d, %d)", treeName, startID, endID)
		}
	}
}

// BenchmarkManagerRangeQueryParallel параллельные range-запросы
func BenchmarkManagerRangeQueryParallel(b *testing.B) {
	sizes := []int{10, 25, 50, 100, 1000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			benchmarkManagerRangeQueryParallelSize(b, size)
		})
	}
}

func benchmarkManagerRangeQueryParallelSize(b *testing.B, rangeSize int) {
	mgr := NewManager[*Account](LargeConfig())
	
	numTrees := 10
	itemsPerTree := 100000
	
	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, err := mgr.CreateTree(treeName)
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
	
	// Параллельные запросы из разных деревьев
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			treeIdx := counter % numTrees
			treeName := fmt.Sprintf("tree_%d", treeIdx)
			tree, _ := mgr.GetTree(treeName)
			
			startID := uint64(treeIdx*1000000 + (counter*13)%itemsPerTree)
			endID := startID + uint64(rangeSize)
			
			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
			counter++
		}
	})
}

// BenchmarkManagerRangeQueryCrossTree range-запросы из разных деревьев последовательно
func BenchmarkManagerRangeQueryCrossTree(b *testing.B) {
	mgr := NewManager[*Account](LargeConfig())
	
	numTrees := 10
	itemsPerTree := 100000
	
	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, err := mgr.CreateTree(treeName)
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
	
	// Каждая итерация читает из ВСЕХ деревьев
	for i := 0; i < b.N; i++ {
		for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
			treeName := fmt.Sprintf("tree_%d", treeIdx)
			tree, _ := mgr.GetTree(treeName)
			
			startID := uint64(treeIdx*1000000 + (i*13)%itemsPerTree)
			endID := startID + uint64(rangeSize)
			
			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
		}
	}
}

// BenchmarkManagerMixedOps смешанные операции: вставки + range-запросы
func BenchmarkManagerMixedOps(b *testing.B) {
	mgr := NewManager[*Account](LargeConfig())
	
	numTrees := 10
	itemsPerTree := 50000 // Меньше, т.к. будем добавлять
	
	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, err := mgr.CreateTree(treeName)
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
	
	// 80% range-запросов, 20% вставок
	for i := 0; i < b.N; i++ {
		treeIdx := i % numTrees
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, _ := mgr.GetTree(treeName)
		
		if i%5 == 0 {
			// 20% - вставка нового элемента
			newID := uint64(treeIdx*1000000 + itemsPerTree + i)
			acc := NewAccount(newID, StatusUser)
			tree.Insert(acc)
		} else {
			// 80% - range-запрос
			startID := uint64(treeIdx*1000000 + (i*13)%itemsPerTree)
			endID := startID + 50
			
			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
		}
	}
}

// BenchmarkManagerRangeQueryDifferentDepths range-запросы на разных глубинах дерева
func BenchmarkManagerRangeQueryDifferentDepths(b *testing.B) {
	configs := []struct {
		name   string
		config *Config
	}{
		{"Depth3_Small", SmallConfig()},
		{"Depth3_Medium", MediumConfig()},
		{"Depth4_Large", LargeConfig()},
		{"Depth5_Huge", HugeConfig()},
	}
	
	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			mgr := NewManager[*Account](cfg.config)
			
			tree, err := mgr.CreateTree("test")
			if err != nil {
				b.Fatalf("Failed to create tree: %v", err)
			}
			
			// Вставляем 100K элементов
			accounts := make([]*Account, 100000)
			for i := 0; i < 100000; i++ {
				accounts[i] = NewAccount(uint64(i), StatusUser)
			}
			tree.InsertBatch(accounts)
			
			rangeSize := 100
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				startID := uint64((i * 13) % 90000)
				endID := startID + uint64(rangeSize)
				
				result := tree.RangeQueryByID(startID, endID, true, false)
				if len(result) == 0 {
					b.Logf("Empty result for range [%d, %d)", startID, endID)
				}
			}
		})
	}
}

// BenchmarkManagerRangeQuerySequentialVsRandom последовательные vs случайные диапазоны
func BenchmarkManagerRangeQuerySequentialVsRandom(b *testing.B) {
	b.Run("Sequential", func(b *testing.B) {
		mgr := NewManager[*Account](LargeConfig())
		
		tree, _ := mgr.CreateTree("test")
		
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
		mgr := NewManager[*Account](LargeConfig())
		
		tree, _ := mgr.CreateTree("test")
		
		accounts := make([]*Account, 100000)
		for i := 0; i < 100000; i++ {
			accounts[i] = NewAccount(uint64(i), StatusUser)
		}
		tree.InsertBatch(accounts)
		
		rangeSize := 100
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Случайная позиция с помощью простого PRNG
			startID := uint64((i*13 + i*i*7) % 90000)
			endID := startID + uint64(rangeSize)
			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
		}
	})
}


// ========== БЫСТРЫЕ БЕНЧМАРКИ ==========

// BenchmarkManagerRangeQueryQuick быстрые range-запросы (малые деревья)
func BenchmarkManagerRangeQueryQuick(b *testing.B) {
	sizes := []int{10, 25, 50, 100}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			benchmarkManagerRangeQueryQuickSize(b, size)
		})
	}
}

func benchmarkManagerRangeQueryQuickSize(b *testing.B, rangeSize int) {
	mgr := NewManager[*Account](SmallConfig())
	
	// Только 3 дерева по 10K элементов
	numTrees := 3
	itemsPerTree := 10000
	
	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, err := mgr.CreateTree(treeName)
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
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, _ := mgr.GetTree(treeName)
		
		startID := uint64(treeIdx*100000 + (i*13)%itemsPerTree)
		endID := startID + uint64(rangeSize)
		
		result := tree.RangeQueryByID(startID, endID, true, false)
		_ = result
	}
}

// BenchmarkManagerRangeQueryParallelQuick быстрый параллельный бенчмарк
func BenchmarkManagerRangeQueryParallelQuick(b *testing.B) {
	mgr := NewManager[*Account](SmallConfig())
	
	numTrees := 3
	itemsPerTree := 10000
	
	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("tree_%d", treeIdx)
		tree, _ := mgr.CreateTree(treeName)
		
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
			treeName := fmt.Sprintf("tree_%d", treeIdx)
			tree, _ := mgr.GetTree(treeName)
			
			startID := uint64(treeIdx*100000 + (counter*13)%itemsPerTree)
			endID := startID + uint64(rangeSize)
			
			result := tree.RangeQueryByID(startID, endID, true, false)
			_ = result
			counter++
		}
	})
}

// BenchmarkManagerRangeQuerySingleTree быстрый бенчмарк для одного дерева
func BenchmarkManagerRangeQuerySingleTree(b *testing.B) {
	sizes := []int{10, 50, 100, 500}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			mgr := NewManager[*Account](SmallConfig())
			tree, _ := mgr.CreateTree("test")
			
			// Только 10K элементов
			accounts := make([]*Account, 10000)
			for i := 0; i < 10000; i++ {
				accounts[i] = NewAccount(uint64(i), StatusUser)
			}
			tree.InsertBatch(accounts)
			
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				startID := uint64((i * 13) % 9000)
				endID := startID + uint64(size)
				result := tree.RangeQueryByID(startID, endID, true, false)
				_ = result
			}
		})
	}
}

// BenchmarkManagerRangeQueryTiny совсем маленький бенчмарк для быстрой проверки
func BenchmarkManagerRangeQueryTiny(b *testing.B) {
	mgr := NewManager[*Account](SmallConfig())
	tree, _ := mgr.CreateTree("tiny")
	
	// Только 1000 элементов!
	accounts := make([]*Account, 1000)
	for i := 0; i < 1000; i++ {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(accounts)
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		startID := uint64((i * 13) % 900)
		endID := startID + 50
		result := tree.RangeQueryByID(startID, endID, true, false)
		_ = result
	}
}

// BenchmarkManagerRangeQueryCompare сравнение последовательного и параллельного
func BenchmarkManagerRangeQueryCompare(b *testing.B) {
	mgr := NewManager[*Account](MediumConfig())
	tree, _ := mgr.CreateTree("compare")
	
	// 50K элементов - средний размер
	accounts := make([]*Account, 50000)
	for i := 0; i < 50000; i++ {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(accounts)
	
	b.Run("Sequential_Small", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			startID := uint64((i * 13) % 40000)
			result := tree.RangeQueryByID(startID, startID+50, true, false)
			_ = result
		}
	})
	
	b.Run("Sequential_Large", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			startID := uint64((i * 13) % 30000)
			result := tree.RangeQueryByID(startID, startID+1000, true, false)
			_ = result
		}
	})
	
	b.Run("Parallel_Small", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				startID := uint64((counter * 13) % 40000)
				result := tree.RangeQueryByID(startID, startID+50, true, false)
				_ = result
				counter++
			}
		})
	})
	
	b.Run("Parallel_Large", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				startID := uint64((counter * 13) % 30000)
				result := tree.RangeQueryByID(startID, startID+1000, true, false)
				_ = result
				counter++
			}
		})
	})
}

// BenchmarkManagerRangeQueryCacheBehavior проверка поведения кеша
func BenchmarkManagerRangeQueryCacheBehavior(b *testing.B) {
	b.Run("HotRange", func(b *testing.B) {
		// Один и тот же диапазон - должен быть в кеше
		mgr := NewManager[*Account](MediumConfig())
		tree, _ := mgr.CreateTree("hot")
		
		accounts := make([]*Account, 10000)
		for i := 0; i < 10000; i++ {
			accounts[i] = NewAccount(uint64(i), StatusUser)
		}
		tree.InsertBatch(accounts)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Всегда один и тот же диапазон
			result := tree.RangeQueryByID(1000, 1100, true, false)
			_ = result
		}
	})
	
	b.Run("ColdRange", func(b *testing.B) {
		// Каждый раз новый диапазон - кеш не помогает
		mgr := NewManager[*Account](MediumConfig())
		tree, _ := mgr.CreateTree("cold")
		
		accounts := make([]*Account, 10000)
		for i := 0; i < 10000; i++ {
			accounts[i] = NewAccount(uint64(i), StatusUser)
		}
		tree.InsertBatch(accounts)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Каждый раз новый диапазон
			startID := uint64((i * 100) % 9000)
			result := tree.RangeQueryByID(startID, startID+100, true, false)
			_ = result
		}
	})
}

// Итераторы TopN
func TestTopNIterator(t *testing.T) {
	t.Run("BasicIteration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 5
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		// Вставляем элементы
		ids := []uint64{50, 10, 90, 30, 70, 20, 80, 40, 60, 100}
		for _, id := range ids {
			tree.Insert(NewAccount(id, StatusUser))
		}
		
		// Итератор по минимумам
		iter := tree.IterTopMin()
		
		count := 0
		prev := uint64(0)
		for iter.HasNext() {
			account, ok := iter.Next()
			if !ok {
				t.Fatal("Next вернул false при HasNext=true")
			}
			
			// Проверяем возрастающий порядок
			if account.UID < prev {
				t.Errorf("Порядок нарушен: %d < %d", account.UID, prev)
			}
			prev = account.UID
			count++
		}
		
		if count != 5 {
			t.Errorf("Ожидали 5 элементов, получили %d", count)
		}
		
		// Проверяем что после исчерпания Next возвращает false
		_, ok := iter.Next()
		if ok {
			t.Error("Next должен вернуть false после исчерпания")
		}
	})
	
	t.Run("PeekAndNext", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 3
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i*10, StatusUser))
		}
		
		iter := tree.IterTopMin()
		
		// Peek не должен сдвигать позицию
		first, _ := iter.Peek()
		same, _ := iter.Peek()
		
		if first.UID != same.UID {
			t.Error("Peek должен возвращать один и тот же элемент")
		}
		
		// Next должен сдвинуть
		taken, _ := iter.Next()
		if taken.UID != first.UID {
			t.Error("Next должен вернуть тот же элемент что и Peek")
		}
		
		// Теперь Peek вернет следующий
		second, _ := iter.Peek()
		if second.UID == first.UID {
			t.Error("После Next, Peek должен вернуть новый элемент")
		}
	})
	
	t.Run("Reset", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 5
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}
		
		iter := tree.IterTopMax()
		
		// Берем первые 3
		firstRun := make([]uint64, 0)
		for i := 0; i < 3 && iter.HasNext(); i++ {
			acc, _ := iter.Next()
			firstRun = append(firstRun, acc.UID)
		}
		
		if len(firstRun) != 3 {
			t.Fatalf("Ожидали 3 элемента, получили %d", len(firstRun))
		}
		
		// Reset
		iter.Reset()
		
		// Берем снова первые 3
		secondRun := make([]uint64, 0)
		for i := 0; i < 3 && iter.HasNext(); i++ {
			acc, _ := iter.Next()
			secondRun = append(secondRun, acc.UID)
		}
		
		// Должны совпадать
		for i := 0; i < 3; i++ {
			if firstRun[i] != secondRun[i] {
				t.Errorf("После Reset элемент [%d]: %d != %d", 
					i, firstRun[i], secondRun[i])
			}
		}
	})
	
	t.Run("TakeWhile", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 10
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		// Вставляем 1, 2, 3, ..., 20
		for i := uint64(1); i <= 20; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}
		
		iter := tree.IterTopMin()
		
		// Берем элементы пока ID <= 5
		taken := iter.TakeWhile(func(acc *Account) bool {
			return acc.UID <= 5
		})
		
		if len(taken) != 5 {
			t.Errorf("Ожидали 5 элементов, получили %d", len(taken))
		}
		
		// Проверяем что взяли правильные
		for i, acc := range taken {
			expected := uint64(i + 1)
			if acc.UID != expected {
				t.Errorf("taken[%d]: ожидали %d, получили %d", 
					i, expected, acc.UID)
			}
		}
		
		// Итератор должен остановиться на 6
		next, ok := iter.Peek()
		if !ok || next.UID != 6 {
			t.Errorf("После TakeWhile итератор должен быть на элементе 6")
		}
	})
	
	t.Run("ForEach", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 5
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i*10, StatusUser))
		}
		
		iter := tree.IterTopMin()
		
		// Собираем все через ForEach
		collected := make([]uint64, 0)
		iter.ForEach(func(acc *Account) {
			collected = append(collected, acc.UID)
		})
		
		if len(collected) != 5 {
			t.Errorf("Ожидали 5 элементов, получили %d", len(collected))
		}
		
		// После ForEach итератор исчерпан
		if iter.HasNext() {
			t.Error("После ForEach итератор должен быть исчерпан")
		}
	})
	
	t.Run("EmptyIterator", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.TopN = 0  // Отключен
		mgr := NewManager[*Account](cfg)
		tree, _ := mgr.CreateTree("test")
		
		// Вставляем элементы
		for i := uint64(1); i <= 10; i++ {
			tree.Insert(NewAccount(i, StatusUser))
		}
		
		// Итератор должен быть пустым
		iter := tree.IterTopMin()
		
		if iter.HasNext() {
			t.Error("Итератор должен быть пустым когда TopN отключен")
		}
		
		if iter.Remaining() != 0 {
			t.Error("Remaining должен быть 0 для пустого итератора")
		}
	})
}

// Итераторы
func BenchmarkTopNIterator(b *testing.B) {
	cfg := DefaultConfig()
	cfg.TopN = 20
	mgr := NewManager[*Account](cfg)
	tree, _ := mgr.CreateTree("bench")
	
	// Заполняем
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
				return acc.UID < 10
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

