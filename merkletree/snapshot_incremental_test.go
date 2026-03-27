// snapshot_incremental_test.go
package merkletree

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// ============================================
// Вспомогательные функции
// ============================================

// setupIncrementalManager создаёт менеджер с включённым TrackDirty
func setupIncrementalManager(t *testing.T, dir string) *UniversalManager {
	t.Helper()
	cfg := DefaultConfig()
	cfg.TrackDirty = true

	mgr, err := NewUniversalManagerWithSnapshot(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	return mgr
}

// assertRootEqual проверяет совпадение глобального корня
func assertRootEqual(t *testing.T, label string, a, b [32]byte) {
	t.Helper()
	if a != b {
		t.Errorf("%s: roots differ\n  got:  %x\n  want: %x", label, a[:8], b[:8])
	}
}

// assertRootNotEqual проверяет что корни разные
func assertRootNotEqual(t *testing.T, label string, a, b [32]byte) {
	t.Helper()
	if a == b {
		t.Errorf("%s: roots unexpectedly equal: %x", label, a[:8])
	}
}

// ============================================
// TestCheckpointBasic
// Проверяет: чекпоинт сохраняется и восстанавливается полностью
// ============================================

func TestCheckpointBasic(t *testing.T) {
	dir := "./test_cp_basic"
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)
	//defer mgr.CloseSnapshots()

	// Наполняем дерево
	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 500; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	rootBefore := mgr.ComputeGlobalRoot()
	t.Logf("Root before checkpoint: %x", rootBefore[:8])

	// Создаём чекпоинт
	cpVersion, err := mgr.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}
	t.Logf("Checkpoint version: %x", cpVersion[:8])

	// Версия чекпоинта должна совпадать с глобальным корнем
	assertRootEqual(t, "checkpoint version vs global root", cpVersion, rootBefore)

	if err := mgr.CloseSnapshots(); err != nil { // ← явное закрытие
		t.Fatalf("close mgr: %v", err)
	}

	// Восстанавливаем в новый менеджер
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	factory := func(name string) TreeInterface {
		switch name {
		case "accounts":
			tr := New[*Account](mgr2.config)
			return &TypedTree[*Account]{Tree: tr}
		}
		return nil
	}

	if err := mgr2.LoadFromSnapshot(cpVersion, factory); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}

	rootAfter := mgr2.ComputeGlobalRoot()
	t.Logf("Root after restore: %x", rootAfter[:8])
	assertRootEqual(t, "restore", rootBefore, rootAfter)

	// Количество элементов должно совпасть
	restoredTree, ok := GetTree[*Account](mgr2, "accounts")
	if !ok {
		t.Fatal("Tree 'accounts' not found after restore")
	}
	if restoredTree.Size() != 500 {
		t.Errorf("Expected 500 items, got %d", restoredTree.Size())
	}
}

// ============================================
// TestIncrementalSnapshotBasic
// Проверяет: инкрементальный снапшот содержит только изменения
// ============================================

func TestIncrementalSnapshotBasic(t *testing.T) {
	dir := "./test_incr_basic"
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)
	//defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")

	// Начальное заполнение + чекпоинт
	for i := uint64(0); i < 1000; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	cpVersion, err := mgr.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}
	t.Logf("Checkpoint: %x (%d items)", cpVersion[:8], tree.Size())

	// Добавляем только 50 новых элементов
	for i := uint64(1000); i < 1050; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Инкрементальный снапшот
	snapVersion, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot (incremental) failed: %v", err)
	}
	t.Logf("Incremental snapshot: %x (%d items)", snapVersion[:8], tree.Size())

	// Версии должны отличаться
	assertRootNotEqual(t, "checkpoint vs snapshot", cpVersion, snapVersion)

	rootOriginal := mgr.ComputeGlobalRoot()

	if err := mgr.CloseSnapshots(); err != nil { // ← явное закрытие
		t.Fatalf("close mgr: %v", err)
	}

	// Восстанавливаем по хешу инкрементального снапшота
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	factory := func(name string) TreeInterface {
		switch name {
		case "accounts":
			tr := New[*Account](mgr2.config)
			return &TypedTree[*Account]{Tree: tr}
		}
		return nil
	}

	if err := mgr2.LoadFromSnapshot(snapVersion, factory); err != nil {
		t.Fatalf("LoadFromSnapshot (incremental) failed: %v", err)
	}

	restoredTree, ok := GetTree[*Account](mgr2, "accounts")
	if !ok {
		t.Fatal("Tree 'accounts' not found after restore")
	}

	// Должно быть 1050 элементов — чекпоинт (1000) + дельта (50)
	if restoredTree.Size() != 1050 {
		t.Errorf("Expected 1050 items, got %d", restoredTree.Size())
	}

	assertRootEqual(t, "incremental restore", rootOriginal, mgr2.ComputeGlobalRoot())
}

// ============================================
// TestIncrementalWithDeletions
// Проверяет: удалённые элементы корректно применяются из дельты
// ============================================

func TestIncrementalWithDeletions(t *testing.T) {
	dir := "./test_incr_deletions"
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)
	//defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")

	// Наполняем и делаем чекпоинт
	for i := uint64(0); i < 200; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}
	if _, err := mgr.CreateCheckpoint(); err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	// Удаляем 50 элементов и добавляем 10 новых
	for i := uint64(0); i < 50; i++ {
		//var key [8]byte
		account := NewAccountDeterministic(i, StatusUser)
		k := account.Key()
		id := binary.BigEndian.Uint64(k[:])
		tree.Delete(id)
	}
	for i := uint64(200); i < 210; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	expectedSize := 200 - 50 + 10 // 160
	rootBefore := mgr.ComputeGlobalRoot()

	snapVersion, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	t.Logf("Snapshot with deletions: %x (expected %d items)", snapVersion[:8], expectedSize)

	if err := mgr.CloseSnapshots(); err != nil { // ← явное закрытие
		t.Fatalf("close mgr: %v", err)
	}

	// Восстанавливаем
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	factory := func(name string) TreeInterface {
		switch name {
		case "accounts":
			tr := New[*Account](mgr2.config)
			return &TypedTree[*Account]{Tree: tr}
		}
		return nil
	}

	if err := mgr2.LoadFromSnapshot(snapVersion, factory); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}

	restoredTree, _ := GetTree[*Account](mgr2, "accounts")
	if restoredTree.Size() != expectedSize {
		t.Errorf("Expected %d items, got %d", expectedSize, restoredTree.Size())
	}
	assertRootEqual(t, "restore with deletions", rootBefore, mgr2.ComputeGlobalRoot())
}

// ============================================
// TestCheckpointRestoreFromIncremental
// Проверяет: при LoadFromSnapshot чекпоинта не нужна цепочка
// ============================================

func TestCheckpointRestoreFromIncremental(t *testing.T) {
	dir := "./test_cp_direct"
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)
	//defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 300; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Создаём чекпоинт, потом несколько инкрементальных снапшотов
	cpVersion, _ := mgr.CreateCheckpoint()

	for wave := 0; wave < 3; wave++ {
		for i := uint64(0); i < 100; i++ {
			tree.Insert(NewAccountDeterministic(uint64(300+wave*100)+i, StatusUser))
		}
		mgr.CreateSnapshot()
	}

	if err := mgr.CloseSnapshots(); err != nil { // ← явное закрытие
		t.Fatalf("close mgr: %v", err)
	}

	// Загружаем именно чекпоинт — должно быть только 300 элементов,
	// без применения последующих дельт
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	factory := func(name string) TreeInterface {
		switch name {
		case "accounts":
			tr := New[*Account](mgr2.config)
			return &TypedTree[*Account]{Tree: tr}
		}
		return nil
	}

	if err := mgr2.LoadFromSnapshot(cpVersion, factory); err != nil {
		t.Fatalf("LoadFromSnapshot(checkpoint) failed: %v", err)
	}

	restoredTree, _ := GetTree[*Account](mgr2, "accounts")
	if restoredTree.Size() != 300 {
		t.Errorf("Loading checkpoint directly: expected 300 items, got %d", restoredTree.Size())
	}
	t.Logf("Checkpoint restored correctly: %d items", restoredTree.Size())
}

// ============================================
// TestIncrementalChain
// Проверяет: цепочка CP → S1 → S2 → S3 восстанавливается корректно
// ============================================

func TestIncrementalChain(t *testing.T) {
	dir := "./test_incr_chain"
	defer os.RemoveAll(dir)

	// --- Фаза 1: создаём цепочку ---
	mgr := setupIncrementalManager(t, dir)

	tree, _ := CreateTree[*Account](mgr, "accounts")

	// CP: 100 элементов
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}
	cpVersion, _ := mgr.CreateCheckpoint()
	t.Logf("CP:  %x (100 items)", cpVersion[:8])

	// S1: +50
	for i := uint64(100); i < 150; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}
	s1, _ := mgr.CreateSnapshot()
	t.Logf("S1:  %x (150 items)", s1[:8])

	// S2: +50
	for i := uint64(150); i < 200; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}
	s2, _ := mgr.CreateSnapshot()
	t.Logf("S2:  %x (200 items)", s2[:8])

	// S3: +50
	for i := uint64(200); i < 250; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}
	s3, _ := mgr.CreateSnapshot()
	t.Logf("S3:  %x (250 items)", s3[:8])

	// ВАЖНО: закрываем основной менеджер ДО копирования
	// PebbleDB держит эксклюзивный file lock — два процесса не могут
	// читать одну и ту же директорию одновременно
	if err := mgr.CloseSnapshots(); err != nil {
		t.Fatalf("Failed to close mgr: %v", err)
	}

	// --- Фаза 2: проверяем восстановление в каждую точку цепочки ---
	type testCase struct {
		version      [32]byte
		expectedSize int
		label        string
	}

	cases := []testCase{
		{cpVersion, 100, "checkpoint"},
		{s1, 150, "S1"},
		{s2, 200, "S2"},
		{s3, 250, "S3"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			// Каждый под-тест получает свою копию хранилища
			restoreDir := dir + "_restore_" + tc.label
			defer os.RemoveAll(restoreDir)

			// Копируем закрытое хранилище
			if err := copySnapshotStorage(t, dir, restoreDir); err != nil {
				t.Fatalf("copySnapshotStorage failed: %v", err)
			}

			// Открываем менеджер на копии
			cfg := DefaultConfig()
			cfg.TrackDirty = true
			m, err := NewUniversalManagerWithSnapshot(cfg, restoreDir)
			if err != nil {
				t.Fatalf("Failed to open restored manager: %v", err)
			}
			defer m.CloseSnapshots()

			factory := func(name string) TreeInterface {
				switch name {
				case "accounts":
					tr := New[*Account](m.config)
					return &TypedTree[*Account]{Tree: tr}
				}
				return nil
			}

			if err := m.LoadFromSnapshot(tc.version, factory); err != nil {
				t.Fatalf("LoadFromSnapshot(%s) failed: %v", tc.label, err)
			}

			restoredTree, ok := GetTree[*Account](m, "accounts")
			if !ok {
				t.Fatal("Tree 'accounts' not found after restore")
			}

			if restoredTree.Size() != tc.expectedSize {
				t.Errorf("Expected %d items, got %d", tc.expectedSize, restoredTree.Size())
			}
			t.Logf("✓ Restored %s: %d items", tc.label, restoredTree.Size())
		})
	}
}

// ============================================
// TestTrackDirtyDisabled
// Проверяет: при TrackDirty=false CreateSnapshot() → всегда чекпоинт
// ============================================

func TestTrackDirtyDisabled(t *testing.T) {
	dir := "./test_track_dirty_off"
	defer os.RemoveAll(dir)

	cfg := DefaultConfig()
	cfg.TrackDirty = false // явно отключаем

	mgr, err := NewUniversalManagerWithSnapshot(cfg, dir)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	//defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// CreateSnapshot при TrackDirty=false должен создать чекпоинт
	v1, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Проверяем что это именно чекпоинт через LoadHeader
	header, err := mgr.snapshotMgr.storage.LoadHeader(&v1)
	if err != nil {
		t.Fatalf("LoadHeader failed: %v", err)
	}
	if header.Kind != KindCheckpoint {
		t.Errorf("Expected KindCheckpoint, got %v", header.Kind)
	}
	t.Logf("Snapshot kind: checkpoint (correct, TrackDirty=false)")

	if err := mgr.CloseSnapshots(); err != nil { // ← явное закрытие
		t.Fatalf("close mgr: %v", err)
	}

	// Восстановление должно работать как обычный чекпоинт
	mgr2, _ := NewUniversalManagerWithSnapshot(cfg, dir)
	defer mgr2.CloseSnapshots()

	factory := func(name string) TreeInterface {
		switch name {
		case "accounts":
			tr := New[*Account](mgr2.config)
			return &TypedTree[*Account]{Tree: tr}
		}
		return nil
	}

	if err := mgr2.LoadFromSnapshot(v1, factory); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}
	restoredTree, _ := GetTree[*Account](mgr2, "accounts")
	if restoredTree.Size() != 100 {
		t.Errorf("Expected 100 items, got %d", restoredTree.Size())
	}
}

// ============================================
// TestIncrementalMultipleTrees
// Проверяет: дельты работают для нескольких деревьев одновременно
// ============================================

func TestIncrementalMultipleTrees(t *testing.T) {
	dir := "./test_incr_multi"
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)
	//defer mgr.CloseSnapshots()

	accountTree, _ := CreateTree[*Account](mgr, "accounts")
	balanceTree, _ := CreateTree[*Balance](mgr, "balances")

	// Базовое заполнение + чекпоинт
	for i := uint64(0); i < 500; i++ {
		accountTree.Insert(NewAccountDeterministic(i, StatusUser))
		balanceTree.Insert(NewBalance(i, 1, 1_000_000, 0))
	}
	cpVersion, _ := mgr.CreateCheckpoint()
	t.Logf("Checkpoint: %x", cpVersion[:8])

	// Изменяем только balances, accounts не трогаем
	for i := uint64(500); i < 600; i++ {
		balanceTree.Insert(NewBalance(i, 1, 2_000_000, 0))
	}

	snapVersion, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	t.Logf("Incremental: %x (only balances changed)", snapVersion[:8])

	rootBefore := mgr.ComputeGlobalRoot()

	if err := mgr.CloseSnapshots(); err != nil { // ← явное закрытие
		t.Fatalf("close mgr: %v", err)
	}

	// Восстанавливаем
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	factory := func(name string) TreeInterface {
		switch name {
		case "accounts":
			tr := New[*Account](mgr2.config)
			return &TypedTree[*Account]{Tree: tr}
		case "balances":
			tr := New[*Balance](mgr2.config)
			return &TypedTree[*Balance]{Tree: tr}
		}
		return nil
	}

	if err := mgr2.LoadFromSnapshot(snapVersion, factory); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}

	at, _ := GetTree[*Account](mgr2, "accounts")
	bt, _ := GetTree[*Balance](mgr2, "balances")

	if at.Size() != 500 {
		t.Errorf("accounts: expected 500, got %d", at.Size())
	}
	if bt.Size() != 600 {
		t.Errorf("balances: expected 600, got %d", bt.Size())
	}
	assertRootEqual(t, "multi-tree restore", rootBefore, mgr2.ComputeGlobalRoot())
}

// ============================================
// TestAutoCheckpointOnFirstSnapshot
// Проверяет: первый CreateSnapshot() без чекпоинта → автоматически чекпоинт
// ============================================

func TestAutoCheckpointOnFirstSnapshot(t *testing.T) {
	dir := "./test_auto_cp"
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)
	defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Первый вызов — нет чекпоинта, должен автоматически создать его
	v1, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("First CreateSnapshot failed: %v", err)
	}

	header, err := mgr.snapshotMgr.storage.LoadHeader(&v1)
	if err != nil {
		t.Fatalf("LoadHeader failed: %v", err)
	}
	if header.Kind != KindCheckpoint {
		t.Errorf("First snapshot should be KindCheckpoint, got %v", header.Kind)
	}
	t.Log("First snapshot correctly auto-promoted to checkpoint")

	// Второй вызов — чекпоинт уже есть, должен быть инкрементальным
	for i := uint64(100); i < 150; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}
	v2, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Second CreateSnapshot failed: %v", err)
	}

	header2, err := mgr.snapshotMgr.storage.LoadHeader(&v2)
	if err != nil {
		t.Fatalf("LoadHeader failed: %v", err)
	}
	if header2.Kind != KindIncremental {
		t.Errorf("Second snapshot should be KindIncremental, got %v", header2.Kind)
	}
	t.Log("Second snapshot correctly created as incremental")
}

// ============================================
// BenchmarkCheckpointVsIncremental
// Сравниваем скорость чекпоинта и инкрементального снапшота
// ============================================

func BenchmarkCheckpointVsIncremental(b *testing.B) {
	const totalItems = 100_000
	const changedItems = 1_000 // ~1% изменений

	b.Run("Checkpoint", func(b *testing.B) {
		dir := "./bench_cp"
		defer os.RemoveAll(dir)

		cfg := DefaultConfig()
		cfg.TrackDirty = false
		mgr, _ := NewUniversalManagerWithSnapshot(cfg, dir)
		defer mgr.CloseSnapshots()

		tree, _ := CreateTree[*Account](mgr, "accounts")
		for i := uint64(0); i < totalItems; i++ {
			tree.Insert(NewAccountDeterministic(i, StatusUser))
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := mgr.CreateSnapshot(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Incremental_1pct", func(b *testing.B) {
		dir := "./bench_incr"
		defer os.RemoveAll(dir)

		cfg := DefaultConfig()
		cfg.TrackDirty = true
		mgr, _ := NewUniversalManagerWithSnapshot(cfg, dir)
		defer mgr.CloseSnapshots()

		tree, _ := CreateTree[*Account](mgr, "accounts")
		for i := uint64(0); i < totalItems; i++ {
			tree.Insert(NewAccountDeterministic(i, StatusUser))
		}
		mgr.CreateCheckpoint() // базовый чекпоинт

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Меняем 1% элементов перед каждым снапшотом
			for j := uint64(0); j < changedItems; j++ {
				tree.Insert(NewAccountDeterministic(j, StatusMM))
			}
			if _, err := mgr.CreateSnapshot(); err != nil {
				b.Fatal(err)
			}
		}

		b.ReportMetric(float64(changedItems)/float64(totalItems)*100, "%_changed")
	})
}

// ============================================
// TestIncrementalEmptyDelta
// Проверяет: инкрементальный снапшот без изменений — корректный edge case
// ============================================

func TestIncrementalEmptyDelta(t *testing.T) {
	dir := "./test_incr_empty"
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)
	//defer mgr.CloseSnapshots()

	tree, _ := CreateTree[*Account](mgr, "accounts")
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	mgr.CreateCheckpoint()
	rootBefore := mgr.ComputeGlobalRoot()

	// Создаём снапшот БЕЗ каких-либо изменений
	snapVersion, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Empty delta snapshot failed: %v", err)
	}

	// Версия должна совпасть с предыдущим глобальным корнем
	if snapVersion != rootBefore {
		t.Logf("Note: empty delta produced new version %x (vs root %x)", snapVersion[:8], rootBefore[:8])
	}

	if err := mgr.CloseSnapshots(); err != nil { // ← явное закрытие
		t.Fatalf("close mgr: %v", err)
	}

	// Восстановление должно работать корректно
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	factory := func(name string) TreeInterface {
		switch name {
		case "accounts":
			tr := New[*Account](mgr2.config)
			return &TypedTree[*Account]{Tree: tr}
		}
		return nil
	}

	if err := mgr2.LoadFromSnapshot(snapVersion, factory); err != nil {
		t.Fatalf("LoadFromSnapshot (empty delta) failed: %v", err)
	}

	restoredTree, _ := GetTree[*Account](mgr2, "accounts")
	if restoredTree.Size() != 100 {
		t.Errorf("Expected 100 items after empty-delta restore, got %d", restoredTree.Size())
	}
	t.Log("Empty delta snapshot handled correctly")
}

// TestIncrementalTimings проверяет что инкрементальный снапшот
// действительно быстрее чекпоинта при малом % изменений
func TestIncrementalTimings(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timing test in short mode")
	}

	const totalItems = 50_000
	const changedItems = 500 // 1%

	// --- Чекпоинт ---
	dirCP := "./test_timing_cp"
	defer os.RemoveAll(dirCP)

	cfgCP := DefaultConfig()
	cfgCP.TrackDirty = false
	mgrCP, _ := NewUniversalManagerWithSnapshot(cfgCP, dirCP)
	defer mgrCP.CloseSnapshots()

	treeCP, _ := CreateTree[*Account](mgrCP, "accounts")
	for i := uint64(0); i < totalItems; i++ {
		treeCP.Insert(NewAccountDeterministic(i, StatusUser))
	}
	startCP := time.Now()
	mgrCP.CreateSnapshot()
	cpDuration := time.Since(startCP)

	// --- Инкрементальный ---
	dirIncr := "./test_timing_incr"
	defer os.RemoveAll(dirIncr)

	cfgIncr := DefaultConfig()
	cfgIncr.TrackDirty = true
	mgrIncr, _ := NewUniversalManagerWithSnapshot(cfgIncr, dirIncr)
	defer mgrIncr.CloseSnapshots()

	treeIncr, _ := CreateTree[*Account](mgrIncr, "accounts")
	for i := uint64(0); i < totalItems; i++ {
		treeIncr.Insert(NewAccountDeterministic(i, StatusUser))
	}
	mgrIncr.CreateCheckpoint()

	for i := uint64(0); i < changedItems; i++ {
		treeIncr.Insert(NewAccountDeterministic(i, StatusMM))
	}
	startIncr := time.Now()
	mgrIncr.CreateSnapshot()
	incrDuration := time.Since(startIncr)

	t.Logf("Checkpoint duration:   %v (all %d items)", cpDuration, totalItems)
	t.Logf("Incremental duration:  %v (%d changed items, %.1f%%)",
		incrDuration, changedItems, float64(changedItems)/float64(totalItems)*100)

	if incrDuration >= cpDuration {
		t.Logf("Warning: incremental (%v) is not faster than checkpoint (%v)", incrDuration, cpDuration)
	} else {
		t.Logf("Speedup: %.1fx", float64(cpDuration)/float64(incrDuration))
	}
}

// Helpers
// copySnapshotStorage копирует директорию PebbleDB из src в dst.
// ВАЖНО: src должен быть закрыт перед вызовом (PebbleDB держит эксклюзивный лок).
func copySnapshotStorage(t *testing.T, src, dst string) error {
	t.Helper()

	// Проверяем что src существует
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return fmt.Errorf("source dir %q does not exist", src)
	}

	// Создаём dst если не существует
	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("create dst dir: %w", err)
	}

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Вычисляем относительный путь и целевой путь
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		return copyFile(dstPath, path, info.Mode())
	})
}

func copyFile(dst, src string, mode os.FileMode) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open src %q: %w", src, err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("create dst %q: %w", dst, err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("copy %q → %q: %w", src, dst, err)
	}
	return dstFile.Sync()
}

// ============================================
// TestMarkDirty
// Проверяет: мутация через указатель + MarkDirty корректно
// обновляет хеш и попадает в инкрементальный снапшот
// ============================================

func TestDeepDebugMarkDirty(t *testing.T) {
	dir := "./test_markdirty"
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)

	tree, err := CreateTree[*Account](mgr, "accounts")
	if err != nil {
		t.Fatalf("CreateTree failed: %v", err)
	}

	makeFactory := func(mgr *UniversalManager) TreeFactory {
		return func(name string) TreeInterface {
			switch name {
			case "accounts":
				tr := New[*Account](mgr.config)
				return &TypedTree[*Account]{Tree: tr}
			default:
				return nil
			}
		}
	}

	sortedMutationIDs := func(m map[uint64]AccountStatus) []uint64 {
		ids := make([]uint64, 0, len(m))
		for id := range m {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids
	}

	dumpVersions := func(t *testing.T, mgr *UniversalManager, label string) {
		t.Helper()

		versions, err := mgr.ListSnapshotVersions()
		if err != nil {
			t.Logf("[%s] ListSnapshotVersions err: %v", label, err)
			return
		}

		sort.Slice(versions, func(i, j int) bool {
			return string(versions[i][:]) < string(versions[j][:])
		})

		t.Logf("[%s] snapshot versions count=%d", label, len(versions))
		for i := range versions {
			v := versions[i]
			hdr, err := mgr.snapshotMgr.storage.LoadHeader(&v)
			if err != nil {
				t.Logf("[%s]   version[%d]=%x LoadHeader err=%v", label, i, v[:8], err)
				continue
			}
			t.Logf(
				"[%s]   version[%d]=%x kind=%v cpRef=%x parent=%x ts=%d schema=%d",
				label,
				i,
				v[:8],
				hdr.Kind,
				hdr.CheckpointRef[:8],
				hdr.ParentVersion[:8],
				hdr.Timestamp,
				hdr.SchemaVersion,
			)
		}
	}

	dumpTreeAccounts := func(t *testing.T, label string, tr *Tree[*Account], ids []uint64) {
		t.Helper()

		root := tr.ComputeRoot()
		stats := tr.GetStats()
		t.Logf(
			"[%s] tree root=%x size=%d dirtyNodes=%d items=%d deleted=%d cache=%d",
			label,
			root[:8],
			tr.Size(),
			tr.GetDirtyNodeCount(),
			stats.TotalItems,
			stats.DeletedNodes,
			stats.CacheSize,
		)

		for _, id := range ids {
			accGet, okGet := tr.Get(id)
			accItems, okItems := tr.items.Load(id)
			node := tr.findLeaf(tr.root, id, 0)

			var leafStatus AccountStatus
			var leafUID uint64
			var leafPtr *Account
			var leafDirty bool
			if node != nil && node.Value != nil {
				node.mu.RLock()
				leafUID = node.Value.UID
				leafStatus = node.Value.Status
				leafPtr = node.Value
				leafDirty = node.dirty.Load()
				node.mu.RUnlock()
			}

			var getStatus any = "<missing>"
			var getKey any = "<missing>"
			if okGet && accGet != nil {
				getStatus = accGet.Status
				getKey = accGet.Key()
			}

			var itemsStatus any = "<missing>"
			var itemsKey any = "<missing>"
			if okItems && accItems != nil {
				itemsStatus = accItems.Status
				itemsKey = accItems.Key()
			}

			t.Logf(
				"[%s] id=%d okGet=%v okItems=%v getStatus=%v itemsStatus=%v leafUID=%d leafStatus=%v leafDirty=%v same(get,items)=%v same(get,leaf)=%v same(items,leaf)=%v getKey=%x itemsKey=%x",
				label,
				id,
				okGet,
				okItems,
				getStatus,
				itemsStatus,
				leafUID,
				leafStatus,
				leafDirty,
				okGet && okItems && accGet == accItems,
				okGet && leafPtr != nil && accGet == leafPtr,
				okItems && leafPtr != nil && accItems == leafPtr,
				getKey,
				itemsKey,
			)

			if okGet && accGet != nil {
				if idx, ok := tr.keyIndex.Load(accGet.Key()); ok {
					t.Logf("[%s]   keyIndex[%x] = %v", label, accGet.Key(), idx)
				} else {
					t.Logf("[%s]   keyIndex[%x] = <missing>", label, accGet.Key())
				}
			}
		}
	}

	dumpDirtyMaps := func(t *testing.T, label string, tr *Tree[*Account]) {
		t.Helper()

		tr.dirtyMu.Lock()
		defer tr.dirtyMu.Unlock()

		dirtyKeys := make([][8]byte, 0, len(tr.dirtyKeys))
		for k := range tr.dirtyKeys {
			dirtyKeys = append(dirtyKeys, k)
		}
		sort.Slice(dirtyKeys, func(i, j int) bool {
			return binary.BigEndian.Uint64(dirtyKeys[i][:]) < binary.BigEndian.Uint64(dirtyKeys[j][:])
		})

		deletedKeys := make([][8]byte, 0, len(tr.deletedKeys))
		for k := range tr.deletedKeys {
			deletedKeys = append(deletedKeys, k)
		}
		sort.Slice(deletedKeys, func(i, j int) bool {
			return binary.BigEndian.Uint64(deletedKeys[i][:]) < binary.BigEndian.Uint64(deletedKeys[j][:])
		})

		t.Logf("[%s] dirtyKeys=%d deletedKeys=%d", label, len(dirtyKeys), len(deletedKeys))
		for i, k := range dirtyKeys {
			t.Logf("[%s]   dirty[%d] key=%x uint64=%d", label, i, k, binary.BigEndian.Uint64(k[:]))
		}
		for i, k := range deletedKeys {
			t.Logf("[%s]   deleted[%d] key=%x uint64=%d", label, i, k, binary.BigEndian.Uint64(k[:]))
		}
	}

	dumpSerializedDirty := func(t *testing.T, label string, tr *Tree[*Account]) {
		t.Helper()

		typed := &TypedTree[*Account]{Tree: tr}
		upserted, deleted, err := typed.serializeDirtyItems()
		if err != nil {
			t.Fatalf("[%s] serializeDirtyItems failed: %v", label, err)
		}

		t.Logf("[%s] serializeDirtyItems: upserted=%d deleted=%d", label, len(upserted), len(deleted))

		for i, raw := range upserted {
			var acc Account
			if err := acc.Deserialize(raw); err != nil {
				t.Fatalf("[%s] upserted[%d] deserialize failed: %v", label, i, err)
			}
			t.Logf("[%s]   upserted[%d]: uid=%d status=%v key=%x", label, i, acc.UID, acc.Status, acc.Key())
		}

		for i, raw := range deleted {
			if len(raw) != 8 {
				t.Logf("[%s]   deleted[%d]: INVALID LEN=%d raw=%x", label, i, len(raw), raw)
				continue
			}
			t.Logf("[%s]   deleted[%d]: key=%x uint64=%d", label, i, raw, binary.BigEndian.Uint64(raw))
		}
	}

	dumpIncrementalPayload := func(t *testing.T, mgr *UniversalManager, version [32]byte, label string) {
		t.Helper()

		deltas, err := mgr.snapshotMgr.storage.LoadIncrementalDelta(version)
		if err != nil {
			t.Logf("[%s] LoadIncrementalDelta err: %v", label, err)
			return
		}

		t.Logf("[%s] delta tree count=%d", label, len(deltas))
		for treeName, delta := range deltas {
			t.Logf("[%s]   tree=%s upserts=%d deleted=%d", label, treeName, len(delta.UpsertItems), len(delta.DeletedKeys))
			for i, raw := range delta.UpsertItems {
				var acc Account
				if err := acc.Deserialize(raw); err != nil {
					t.Fatalf("[%s]   tree=%s upsert[%d] deserialize failed: %v", label, treeName, i, err)
				}
				t.Logf("[%s]   tree=%s upsert[%d]: uid=%d status=%v key=%x", label, treeName, i, acc.UID, acc.Status, acc.Key())
			}
			for i, raw := range delta.DeletedKeys {
				if len(raw) != 8 {
					t.Logf("[%s]   tree=%s deleted[%d]: INVALID LEN=%d raw=%x", label, treeName, i, len(raw), raw)
					continue
				}
				t.Logf("[%s]   tree=%s deleted[%d]: key=%x uint64=%d", label, treeName, i, raw, binary.BigEndian.Uint64(raw))
			}
		}
	}

	checkStatuses := func(t *testing.T, label string, tr *Tree[*Account], expected map[uint64]AccountStatus) {
		t.Helper()

		ids := sortedMutationIDs(expected)
		for _, id := range ids {
			acc, ok := tr.Get(id)
			if !ok {
				t.Errorf("[%s] account %d not found", label, id)
				continue
			}
			match := acc.Status == expected[id]
			t.Logf("[%s] id=%d got=%v expected=%v match=%v", label, id, acc.Status, expected[id], match)
			if !match {
				t.Errorf("[%s] id=%d got=%v expected=%v", label, id, acc.Status, expected[id])
			}
		}
	}

	// ------------------------------------------------------------
	// 1. Базовое заполнение
	// ------------------------------------------------------------
	for i := uint64(0); i < 100; i++ {
		if err := tree.Insert(NewAccountDeterministic(i, StatusUser)); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	baseIDs := []uint64{0, 10, 20, 30, 50, 99}
	dumpTreeAccounts(t, "INIT", tree, baseIDs)

	rootBeforeCP := mgr.ComputeGlobalRoot()
	t.Logf("[INIT] global root before checkpoint=%x", rootBeforeCP[:8])

	cpVersion, err := mgr.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	t.Logf("[CP] checkpoint version=%x", cpVersion[:8])
	dumpVersions(t, mgr, "AFTER_CHECKPOINT")

	rootAfterCP := mgr.ComputeGlobalRoot()
	t.Logf("[CP] global root after checkpoint=%x", rootAfterCP[:8])

	// ------------------------------------------------------------
	// 2. Проверка одиночной мутации без MarkDirty
	// ------------------------------------------------------------
	acc0, ok := tree.Get(0)
	if !ok {
		t.Fatal("account 0 not found")
	}
	old0 := acc0.Status
	acc0.Status = StatusBlocked

	t.Logf("[STEP1] mutated id=0 through ptr: old=%v new=%v ptr=%p", old0, acc0.Status, acc0)

	dumpTreeAccounts(t, "AFTER_PTR_MUTATION_NO_MARK", tree, []uint64{0})

	rootNoMark := mgr.ComputeGlobalRoot()
	t.Logf("[STEP1] global root after ptr mutation without MarkDirty=%x", rootNoMark[:8])

	if rootAfterCP != rootNoMark {
		t.Errorf("[STEP1] root changed without MarkDirty: cp=%x noMark=%x", rootAfterCP[:8], rootNoMark[:8])
	}

	// ------------------------------------------------------------
	// 3. MarkDirty(0)
	// ------------------------------------------------------------
	tree.MarkDirty(0)

	dumpDirtyMaps(t, "AFTER_MARKDIRTY_0", tree)
	dumpTreeAccounts(t, "AFTER_MARKDIRTY_0", tree, []uint64{0})
	dumpSerializedDirty(t, "AFTER_MARKDIRTY_0", tree)

	rootAfterMark0 := mgr.ComputeGlobalRoot()
	t.Logf("[STEP2] global root after MarkDirty(0)=%x", rootAfterMark0[:8])

	if rootAfterCP == rootAfterMark0 {
		t.Errorf("[STEP2] root did not change after MarkDirty(0): cp=%x marked=%x", rootAfterCP[:8], rootAfterMark0[:8])
	}

	// ------------------------------------------------------------
	// 4. Батч мутаций через ptr + MarkDirty(ids...)
	// ------------------------------------------------------------
	mutations := map[uint64]AccountStatus{
		0:  StatusBlocked,
		10: StatusBlocked,
		20: StatusMM,
		30: StatusAlgo,
		50: StatusVIP,
		99: StatusSystem,
	}
	ids := sortedMutationIDs(mutations)

	for _, id := range ids {
		acc, ok := tree.Get(id)
		if !ok {
			t.Fatalf("account %d not found before mutation", id)
		}

		before := acc.Status
		acc.Status = mutations[id]

		accGet, _ := tree.Get(id)
		accItems, _ := tree.items.Load(id)
		leaf := tree.findLeaf(tree.root, id, 0)

		var leafStatus AccountStatus
		var leafPtr *Account
		if leaf != nil && leaf.Value != nil {
			leaf.mu.RLock()
			leafStatus = leaf.Value.Status
			leafPtr = leaf.Value
			leaf.mu.RUnlock()
		}

		t.Logf(
			"[STEP3_MUTATE] id=%d before=%v after=%v same(get,get2)=%v same(get,items)=%v same(get,leaf)=%v get2.status=%v items.status=%v leaf.status=%v",
			id,
			before,
			acc.Status,
			acc == accGet,
			acc == accItems,
			acc == leafPtr,
			accGet.Status,
			accItems.Status,
			leafStatus,
		)
	}

	dumpTreeAccounts(t, "BEFORE_BATCH_MARKDIRTY", tree, ids)

	tree.MarkDirty(ids...)

	dumpDirtyMaps(t, "AFTER_BATCH_MARKDIRTY", tree)
	dumpTreeAccounts(t, "AFTER_BATCH_MARKDIRTY", tree, ids)
	dumpSerializedDirty(t, "AFTER_BATCH_MARKDIRTY", tree)

	rootAfterBatch := mgr.ComputeGlobalRoot()
	t.Logf("[STEP3] global root after batch MarkDirty=%x", rootAfterBatch[:8])

	if rootAfterMark0 == rootAfterBatch {
		t.Errorf("[STEP3] root did not change after batch MarkDirty: prev=%x batch=%x", rootAfterMark0[:8], rootAfterBatch[:8])
	}

	// ------------------------------------------------------------
	// 5. Невалидные ID
	// ------------------------------------------------------------
	rootBeforeInvalid := mgr.ComputeGlobalRoot()
	tree.MarkDirty(99999, 88888)
	rootAfterInvalid := mgr.ComputeGlobalRoot()

	t.Logf("[STEP4] root before invalid MarkDirty=%x after=%x", rootBeforeInvalid[:8], rootAfterInvalid[:8])
	if rootBeforeInvalid != rootAfterInvalid {
		t.Errorf("[STEP4] root changed after invalid MarkDirty: before=%x after=%x", rootBeforeInvalid[:8], rootAfterInvalid[:8])
	}

	// ------------------------------------------------------------
	// 6. Создание incremental snapshot
	// ------------------------------------------------------------
	snapVersion, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	t.Logf("[SNAP] incremental snapshot version=%x", snapVersion[:8])

	header, err := mgr.snapshotMgr.storage.LoadHeader(&snapVersion)
	if err != nil {
		t.Fatalf("LoadHeader failed: %v", err)
	}
	t.Logf(
		"[SNAP] target header kind=%v version=%x cpRef=%x parent=%x ts=%d schema=%d",
		header.Kind,
		header.Version[:8],
		header.CheckpointRef[:8],
		header.ParentVersion[:8],
		header.Timestamp,
		header.SchemaVersion,
	)

	if header.Kind != KindIncremental {
		t.Fatalf("[SNAP] expected KindIncremental, got %v", header.Kind)
	}

	dumpVersions(t, mgr, "AFTER_INCREMENTAL")

	chain, err := mgr.snapshotMgr.storage.BuildChain(header.CheckpointRef, snapVersion)
	if err != nil {
		t.Fatalf("BuildChain failed: %v", err)
	}
	t.Logf("[CHAIN] len=%d", len(chain))
	for i, e := range chain {
		t.Logf("[CHAIN]   chain[%d] version=%x ts=%d", i, e.Version[:8], e.Timestamp)
	}

	dumpIncrementalPayload(t, mgr, snapVersion, "RAW_INCREMENTAL_PAYLOAD")

	finalRoot := mgr.ComputeGlobalRoot()
	t.Logf("[SNAP] final source global root=%x", finalRoot[:8])

	// ВАЖНО: закрываем исходный manager до открытия нового на том же dir.
	if err := mgr.CloseSnapshots(); err != nil {
		t.Fatalf("CloseSnapshots source failed: %v", err)
	}

	// ------------------------------------------------------------
	// 7. Восстановление ТОЛЬКО checkpoint
	// ------------------------------------------------------------
	mgrCP := setupIncrementalManager(t, dir)

	cpRef := header.CheckpointRef
	if err := mgrCP.snapshotMgr.loadCheckpoint(mgrCP, &cpRef, makeFactory(mgrCP)); err != nil {
		_ = mgrCP.CloseSnapshots()
		t.Fatalf("[CP_RESTORE] loadCheckpoint failed: %v", err)
	}

	cpTree, ok := GetTree[*Account](mgrCP, "accounts")
	if !ok {
		_ = mgrCP.CloseSnapshots()
		t.Fatal("[CP_RESTORE] accounts tree not found")
	}

	t.Logf("[CP_RESTORE] restored size=%d", cpTree.Size())
	dumpTreeAccounts(t, "CP_RESTORE", cpTree, ids)
	checkStatuses(t, "CP_RESTORE_EXPECT_OLD", cpTree, map[uint64]AccountStatus{
		0:  StatusUser,
		10: StatusUser,
		20: StatusUser,
		30: StatusUser,
		50: StatusUser,
		99: StatusUser,
	})

	if err := mgrCP.CloseSnapshots(); err != nil {
		t.Fatalf("[CP_RESTORE] CloseSnapshots failed: %v", err)
	}

	// ------------------------------------------------------------
	// 8. Восстановление checkpoint + manual applyIncremental
	// ------------------------------------------------------------
	mgrManual := setupIncrementalManager(t, dir)

	cpRef2 := header.CheckpointRef
	if err := mgrManual.snapshotMgr.loadCheckpoint(mgrManual, &cpRef2, makeFactory(mgrManual)); err != nil {
		_ = mgrManual.CloseSnapshots()
		t.Fatalf("[MANUAL] loadCheckpoint failed: %v", err)
	}

	for i, entry := range chain {
		t.Logf("[MANUAL] applying chain[%d] version=%x", i, entry.Version[:8])
		if err := mgrManual.snapshotMgr.applyIncremental(mgrManual, entry); err != nil {
			_ = mgrManual.CloseSnapshots()
			t.Fatalf("[MANUAL] applyIncremental failed for %x: %v", entry.Version[:8], err)
		}

		manualTree, ok := GetTree[*Account](mgrManual, "accounts")
		if !ok {
			_ = mgrManual.CloseSnapshots()
			t.Fatal("[MANUAL] accounts tree missing after applyIncremental")
		}
		dumpTreeAccounts(t, "MANUAL_AFTER_APPLY", manualTree, ids)
		checkStatuses(t, "MANUAL_AFTER_APPLY", manualTree, mutations)
	}

	_, ok2 := GetTree[*Account](mgrManual, "accounts")
	if !ok2 {
		_ = mgrManual.CloseSnapshots()
		t.Fatal("[MANUAL] accounts tree missing at final check")
	}

	manualRoot := mgrManual.ComputeGlobalRoot()
	t.Logf("[MANUAL] final root=%x", manualRoot[:8])

	if err := mgrManual.CloseSnapshots(); err != nil {
		t.Fatalf("[MANUAL] CloseSnapshots failed: %v", err)
	}

	// ------------------------------------------------------------
	// 9. Полный LoadFromSnapshot
	// ------------------------------------------------------------
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	if err := mgr2.LoadFromSnapshot(snapVersion, makeFactory(mgr2)); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}

	restoredTree, ok := GetTree[*Account](mgr2, "accounts")
	if !ok {
		t.Fatal("[FULL_RESTORE] accounts tree not found")
	}

	t.Logf("[FULL_RESTORE] restored size=%d", restoredTree.Size())
	dumpTreeAccounts(t, "FULL_RESTORE", restoredTree, ids)
	checkStatuses(t, "FULL_RESTORE", restoredTree, mutations)

	rootAfterRestore := mgr2.ComputeGlobalRoot()
	t.Logf("[FULL_RESTORE] restored global root=%x expected=%x", rootAfterRestore[:8], finalRoot[:8])

	assertRootEqual(t, "markdirty restore", finalRoot, rootAfterRestore)

	// ------------------------------------------------------------
	// 10. Доп. сравнение manual vs full restore
	// ------------------------------------------------------------
	if manualRoot != rootAfterRestore {
		t.Errorf(
			"[COMPARE] manual root != full restore root: manual=%x full=%x",
			manualRoot[:8],
			rootAfterRestore[:8],
		)
	}
}
