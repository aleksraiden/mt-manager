package merkletree

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// ============================================
// Базовые тесты снапшотов
// ============================================

func TestSnapshotBasic(t *testing.T) {
	tmpDir := "./test_snapshots_basic"
	defer os.RemoveAll(tmpDir)

	// Создаем универсальный менеджер со снапшотами
	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	// Создаем дерево аккаунтов
	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 1000; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Создаем снапшот
	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	t.Logf("Created snapshot: %x", version[:8])

	// Проверяем метаданные
	metadata, err := mgr.GetSnapshotMetadata()
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	if metadata.Count != 1 {
		t.Errorf("Expected 1 snapshot, got %d", metadata.Count)
	}

	t.Logf("Metadata: %d snapshots, first=%x, last=%x",
		metadata.Count, metadata.FirstVersion[:4], metadata.LastVersion[:4])
}

func TestSnapshotMultipleTrees(t *testing.T) {
	tmpDir := "./test_snapshots_multi"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(MediumConfig(), tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	// Создаем несколько деревьев разных типов
	accountTree, _ := CreateTree[*Account](mgr, "accounts")
	balanceTree, _ := CreateTree[*Balance](mgr, "balances")

	// Заполняем аккаунты
	for i := uint64(0); i < 1000; i++ {
		accountTree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Заполняем балансы
	for i := uint64(0); i < 1000; i++ {
		balanceTree.Insert(NewBalance(i, 1, 1000_000000, 0))
	}

	globalRootBefore := mgr.ComputeGlobalRoot()
	t.Logf("Global root before snapshot: %x", globalRootBefore[:8])

	// Создаем снапшот
	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	t.Logf("Created snapshot: %x", version[:8])

	// Проверяем, что version совпадает с global root
	if version != globalRootBefore {
		t.Errorf("Snapshot version should match global root\nVersion: %x\nGlobal:  %x",
			version[:8], globalRootBefore[:8])
	}

	// Список версий
	versions, err := mgr.ListSnapshotVersions()
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}

	if len(versions) != 1 {
		t.Errorf("Expected 1 version, got %d", len(versions))
	}
}

func TestSnapshotAsync(t *testing.T) {
	tmpDir := "./test_snapshots_async"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	// Создаем и заполняем дерево
	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 5000; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Асинхронное создание снапшота
	t.Log("Creating snapshot asynchronously...")
	resultChan := mgr.CreateSnapshotAsync()

	// Можем продолжать работу, пока снапшот создается
	t.Log("Continue working while snapshot is being created...")

	// Получаем результат
	result := <-resultChan
	if result.Error != nil {
		t.Fatalf("Async snapshot failed: %v", result.Error)
	}

	t.Logf("Async snapshot completed in %v", result.Duration)
	t.Logf("Version: %x", result.Version[:8])
}

func TestSnapshotMetrics(t *testing.T) {
	tmpDir := "./test_snapshots_metrics"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	// Создаем большое дерево
	tree, _ := CreateTree[*Account](mgr, "accounts")
	const itemCount = 10000
	for i := uint64(0); i < itemCount; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Создаем снапшот
	start := time.Now()
	_, err = mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	totalTime := time.Since(start)

	// Получаем метрики
	metrics := mgr.GetSnapshotMetrics()
	t.Logf("Snapshot metrics for %d items:", itemCount)
	t.Logf("  Capture:   %v", time.Duration(metrics.CaptureTimeNs))
	t.Logf("  Serialize: %v", time.Duration(metrics.SerializeTimeNs))
	t.Logf("  Write:     %v", time.Duration(metrics.WriteTimeNs))
	t.Logf("  Total:     %v (measured: %v)", time.Duration(metrics.TotalTimeNs), totalTime)

	// Проверяем, что capture быстрый (< 1ms для 10K элементов)
	if metrics.CaptureTimeNs > 1_000_000 { // 1ms
		t.Logf("Warning: Capture took %v (expected < 1ms)", time.Duration(metrics.CaptureTimeNs))
	}

	// Статистика хранилища
	stats := mgr.GetSnapshotStats()
	t.Logf("Storage stats:")
	t.Logf("  Written: %d bytes", stats.WrittenBytes)
	t.Logf("  Writes:  %d", stats.WriteCount)
	t.Logf("  Cache hit rate: %.2f%%", stats.CacheHitRate)
}

func TestSnapshotMultipleVersions(t *testing.T) {
	tmpDir := "./test_snapshots_versions"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")

	// Создаем несколько версий снапшотов
	versions := make([][32]byte, 5)
	for v := 0; v < 5; v++ {
		// Добавляем 100 элементов
		for i := 0; i < 100; i++ {
			uid := uint64(v*100 + i)
			tree.Insert(NewAccountDeterministic(uid, StatusUser))
		}

		// Создаем снапшот
		version, err := mgr.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", v, err)
		}
		versions[v] = version
		t.Logf("Version %d: %x (%d items)", v, version[:8], (v+1)*100)
	}

	// Проверяем список версий
	listedVersions, err := mgr.ListSnapshotVersions()
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}

	if len(listedVersions) != 5 {
		t.Errorf("Expected 5 versions, got %d", len(listedVersions))
	}

	// Все версии должны быть уникальны
	for i := 0; i < len(versions)-1; i++ {
		if versions[i] == versions[i+1] {
			t.Errorf("Versions %d and %d are identical", i, i+1)
		}
	}
}

func TestSnapshotDelete(t *testing.T) {
	tmpDir := "./test_snapshots_delete"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Создаем снапшот
	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Проверяем, что он есть
	versions, _ := mgr.ListSnapshotVersions()
	if len(versions) != 1 {
		t.Errorf("Expected 1 version, got %d", len(versions))
	}

	// Удаляем
	err = mgr.DeleteSnapshot(version)
	if err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}

	// Проверяем, что удалился
	versions, _ = mgr.ListSnapshotVersions()
	if len(versions) != 0 {
		t.Errorf("Expected 0 versions after delete, got %d", len(versions))
	}
}

func TestSnapshotStorage(t *testing.T) {
	tmpDir := "./test_snapshots_storage"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 1000; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Создаем снапшот
	_, err = mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Flush
	err = mgr.FlushSnapshots()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Compact
	err = mgr.CompactSnapshots()
	if err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	stats := mgr.GetSnapshotStats()
	t.Logf("After compact - Cache hit rate: %.2f%%", stats.CacheHitRate)
}

// ============================================
// Бенчмарки снапшотов
// ============================================

func BenchmarkSnapshotCreate(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			benchmarkSnapshotCreate(b, size)
		})
	}
}

func benchmarkSnapshotCreate(b *testing.B, itemCount int) {
	tmpDir := fmt.Sprintf("./bench_snapshot_%d", itemCount)
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		b.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	// Заполняем дерево
	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < uint64(itemCount); i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := mgr.CreateSnapshot()
		if err != nil {
			b.Fatalf("Failed to create snapshot: %v", err)
		}
	}

	metrics := mgr.GetSnapshotMetrics()
	b.ReportMetric(float64(metrics.CaptureTimeNs)/1000, "capture_us")
	b.ReportMetric(float64(metrics.SerializeTimeNs)/1000, "serialize_us")
	b.ReportMetric(float64(metrics.WriteTimeNs)/1000, "write_us")
}

func BenchmarkSnapshotCreateMultipleTrees(b *testing.B) {
	tmpDir := "./bench_snapshot_multi"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		b.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	// Создаем 5 деревьев по 10K элементов каждое
	numTrees := 5
	itemsPerTree := 10000

	for treeIdx := 0; treeIdx < numTrees; treeIdx++ {
		treeName := fmt.Sprintf("accounts_%d", treeIdx)
		tree, _ := CreateTree[*Account](mgr, treeName)

		for i := 0; i < itemsPerTree; i++ {
			uid := uint64(treeIdx*itemsPerTree + i)
			tree.Insert(NewAccountDeterministic(uid, StatusUser))
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := mgr.CreateSnapshot()
		if err != nil {
			b.Fatalf("Failed to create snapshot: %v", err)
		}
	}

	metrics := mgr.GetSnapshotMetrics()
	b.Logf("Metrics: %s", metrics.String())
}

func BenchmarkSnapshotAsync(b *testing.B) {
	tmpDir := "./bench_snapshot_async"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(DefaultConfig(), tmpDir)
	if err != nil {
		b.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 50000; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resultChan := mgr.CreateSnapshotAsync()
		result := <-resultChan
		if result.Error != nil {
			b.Fatalf("Async snapshot failed: %v", result.Error)
		}
	}
}

// ============================================
// Стресс-тесты
// ============================================

func TestSnapshotStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	tmpDir := "./test_snapshots_stress"
	defer os.RemoveAll(tmpDir)

	cfg := DefaultConfig()
	cfg.TopN = 10

	mgr, err := NewUniversalManagerWithSnapshot(cfg, tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	// Создаем 3 дерева
	tree1, _ := CreateTree[*Account](mgr, "accounts")
	tree2, _ := CreateTree[*Balance](mgr, "balances")
	tree3, _ := CreateTree[*Account](mgr, "admins")

	t.Log("Phase 1: Filling trees...")
	start := time.Now()

	// Заполняем
	for i := uint64(0); i < 50000; i++ {
		tree1.Insert(NewAccountDeterministic(i, StatusUser))
		tree2.Insert(NewBalance(i, 1, 1000_000000, 0))
		tree3.Insert(NewAccountDeterministic(i+100000, StatusMM))
	}

	t.Logf("Filled in %v", time.Since(start))

	// Создаем снапшоты
	t.Log("Phase 2: Creating snapshots...")
	versions := make([][32]byte, 10)

	for i := 0; i < 10; i++ {
		// Добавляем еще данных
		for j := 0; j < 1000; j++ {
			uid := uint64(50000 + i*1000 + j)
			tree1.Insert(NewAccountDeterministic(uid, StatusUser))
		}

		start := time.Now()
		version, err := mgr.CreateSnapshot()
		if err != nil {
			t.Fatalf("Snapshot %d failed: %v", i, err)
		}
		versions[i] = version
		duration := time.Since(start)

		t.Logf("Snapshot %d: %x in %v", i, version[:8], duration)

		metrics := mgr.GetSnapshotMetrics()
		if duration > 100*time.Millisecond {
			t.Logf("  Warning: Slow snapshot! %s", metrics.String())
		}
	}

	// Проверяем метаданные
	metadata, _ := mgr.GetSnapshotMetadata()
	t.Logf("Total snapshots: %d", metadata.Count)
	t.Logf("Total size: %d bytes", metadata.TotalSize)

	// Удаляем половину
	t.Log("Phase 3: Deleting half of snapshots...")
	for i := 0; i < 5; i++ {
		err := mgr.DeleteSnapshot(versions[i])
		if err != nil {
			t.Fatalf("Failed to delete snapshot %d: %v", i, err)
		}
	}

	// Компактим
	t.Log("Phase 4: Compacting...")
	start = time.Now()
	err = mgr.CompactSnapshots()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}
	t.Logf("Compacted in %v", time.Since(start))

	// Финальная статистика
	stats := mgr.GetSnapshotStats()
	t.Logf("Final stats:")
	t.Logf("  Writes: %d (%d bytes)", stats.WriteCount, stats.WrittenBytes)
	t.Logf("  Reads:  %d (%d bytes)", stats.ReadCount, stats.ReadBytes)
	t.Logf("  Cache hit rate: %.2f%%", stats.CacheHitRate)
	t.Logf("  Memtable size: %d bytes", stats.MemtableSize)
	t.Logf("  WAL size: %d bytes", stats.WALSize)
}

func TestSnapshotConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent test in short mode")
	}

	tmpDir := "./test_snapshots_concurrent"
	defer os.RemoveAll(tmpDir)

	mgr, err := NewUniversalManagerWithSnapshot(MediumConfig(), tmpDir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")

	// Заполняем базу
	for i := uint64(0); i < 10000; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	t.Log("Creating snapshots concurrently...")

	// Создаем снапшоты параллельно (не должно быть гонок)
	const numSnapshots = 5
	results := make(chan SnapshotResult, numSnapshots)

	for i := 0; i < numSnapshots; i++ {
		// Добавляем немного данных для каждого снапшота
		for j := 0; j < 100; j++ {
			uid := uint64(10000 + i*100 + j)
			tree.Insert(NewAccountDeterministic(uid, StatusUser))
		}

		// Асинхронный снапшот
		go func(idx int) {
			resultChan := mgr.CreateSnapshotAsync()
			result := <-resultChan
			t.Logf("Snapshot %d: completed in %v", idx, result.Duration)
			results <- result
		}(i)

		// Небольшая задержка между запусками
		time.Sleep(10 * time.Millisecond)
	}

	// Собираем результаты
	for i := 0; i < numSnapshots; i++ {
		result := <-results
		if result.Error != nil {
			t.Errorf("Snapshot failed: %v", result.Error)
		}
	}

	// Проверяем количество снапшотов
	versions, _ := mgr.ListSnapshotVersions()
	if len(versions) != numSnapshots {
		t.Errorf("Expected %d snapshots, got %d", numSnapshots, len(versions))
	}
}
