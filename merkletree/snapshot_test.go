// snapshot_test.go
package merkletree

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	
	"github.com/zeebo/blake3"
)

// ============================================
// Test Models
// ============================================

// TestAccount - тестовая модель для снапшотов
type TestAccount struct {
	AccountID uint64
	Balance   uint64
	Nonce     uint64
}

func (a *TestAccount) Hash() [32]byte {
	hasher := blake3.New()
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], a.AccountID)
	binary.BigEndian.PutUint64(buf[8:16], a.Balance)
	binary.BigEndian.PutUint64(buf[16:24], a.Nonce)
	hasher.Write(buf)
	var hash [32]byte
	copy(hash[:], hasher.Sum(nil))
	return hash
}

func (a *TestAccount) Key() [8]byte {
	return EncodeKey(a.AccountID)
}

func (a *TestAccount) ID() uint64 {
	return a.AccountID
}

// ============================================
// Helper Functions
// ============================================

// createTestManager создает TreeManager для тестов
func createTestManager(t *testing.T, snapshotPath string) *TreeManager[*TestAccount] {
	cfg := &Config{
		MaxDepth:    3,
		CacheSize:   10000,
		CacheShards: 8,
		TopN:        10,
	}
	
	mgr, err := NewManagerWithSnapshot[*TestAccount](cfg, snapshotPath)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	
	return mgr
}

// generateTestAccounts генерирует тестовые аккаунты
func generateTestAccounts(start, count int) []*TestAccount {
	accounts := make([]*TestAccount, count)
	for i := 0; i < count; i++ {
		accounts[i] = &TestAccount{
			AccountID: uint64(start + i),
			Balance:   uint64(1000 + i),
			Nonce:     uint64(i),
		}
	}
	return accounts
}

// tempDir создает временную директорию для тестов
func tempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "snapshot-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// cleanup удаляет временную директорию
func cleanup(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Logf("Warning: failed to cleanup %s: %v", dir, err)
	}
}

// ============================================
// Basic Functionality Tests
// ============================================

func TestSnapshotBasic(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем дерево и добавляем данные
	tree := mgr.GetOrCreateTree("accounts")
	accounts := generateTestAccounts(0, 100)
	tree.InsertBatch(accounts)
	
	// Вычисляем хеш до снапшота
	rootBefore := tree.ComputeRoot()
	globalRootBefore := mgr.ComputeGlobalRoot()
	
	// Создаем снапшот
	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	
	t.Logf("Snapshot created: %x", version[:8])
	
	// Проверяем, что version совпадает с global root
	if version != globalRootBefore {
		t.Errorf("Version mismatch: got %x, want %x", version, globalRootBefore)
	}
	
	// Изменяем данные
	tree.Insert(&TestAccount{AccountID: 9999, Balance: 9999, Nonce: 9999})
	rootAfterChange := tree.ComputeRoot()
	
	if rootAfterChange == rootBefore {
		t.Error("Root should change after insert")
	}
	
	// Восстанавливаем из снапшота
	if err := mgr.LoadFromSnapshot(&version); err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}
	
	// Проверяем восстановление
	tree, _ = mgr.GetTree("accounts")
	rootAfterRestore := tree.ComputeRoot()
	
	if rootAfterRestore != rootBefore {
		t.Errorf("Root mismatch after restore: got %x, want %x", rootAfterRestore, rootBefore)
	}
	
	// Проверяем количество элементов
	if tree.Size() != 100 {
		t.Errorf("Size mismatch: got %d, want 100", tree.Size())
	}
	
	// Проверяем элемент 9999 не существует
	if tree.Exists(9999) {
		t.Error("Item 9999 should not exist after restore")
	}
}

func TestSnapshotEmptyManager(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем снапшот пустого менеджера
	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	
	t.Logf("Empty snapshot created: %x", version[:8])
	
	// Добавляем данные
	tree := mgr.GetOrCreateTree("test")
	tree.Insert(&TestAccount{AccountID: 1, Balance: 100, Nonce: 1})
	
	// Восстанавливаем пустой снапшот
	if err := mgr.LoadFromSnapshot(&version); err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}
	
	// Проверяем, что деревья пусты
	trees := mgr.ListTrees()
	if len(trees) != 0 {
		t.Errorf("Expected 0 trees, got %d", len(trees))
	}
}

func TestSnapshotMultipleTrees(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем несколько деревьев
	treeNames := []string{"accounts", "orders", "balances"}
	expectedRoots := make(map[string][32]byte)
	
	for i, name := range treeNames {
		tree := mgr.GetOrCreateTree(name)
		accounts := generateTestAccounts(i*1000, 50)
		tree.InsertBatch(accounts)
		expectedRoots[name] = tree.ComputeRoot()
	}
	
	// Создаем снапшот
	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	
	// Очищаем все деревья
	mgr.ClearAll()
	
	// Восстанавливаем
	if err := mgr.LoadFromSnapshot(&version); err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}
	
	// Проверяем все деревья
	restoredTrees := mgr.ListTrees()
	if len(restoredTrees) != len(treeNames) {
		t.Errorf("Tree count mismatch: got %d, want %d", len(restoredTrees), len(treeNames))
	}
	
	for _, name := range treeNames {
		tree, exists := mgr.GetTree(name)
		if !exists {
			t.Errorf("Tree %s not found after restore", name)
			continue
		}
		
		root := tree.ComputeRoot()
		if root != expectedRoots[name] {
			t.Errorf("Root mismatch for tree %s", name)
		}
		
		if tree.Size() != 50 {
			t.Errorf("Size mismatch for tree %s: got %d, want 50", name, tree.Size())
		}
	}
}

// ============================================
// Restore Tests
// ============================================

func TestSnapshotRestoreLatest(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем несколько снапшотов
	versions := make([][32]byte, 3)
	
	for i := 0; i < 3; i++ {
		tree := mgr.GetOrCreateTree("test")
		tree.Insert(&TestAccount{
			AccountID: uint64(i),
			Balance:   uint64(i * 1000),
			Nonce:     uint64(i),
		})
		
		version, err := mgr.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", i, err)
		}
		versions[i] = version
		
		time.Sleep(10 * time.Millisecond) // Чтобы timestamp отличался
	}
	
	// Очищаем
	mgr.ClearAll()
	
	// Загружаем последний (nil = latest)
	if err := mgr.LoadFromSnapshot(nil); err != nil {
		t.Fatalf("Failed to load latest snapshot: %v", err)
	}
	
	// Проверяем, что загрузился последний
	tree, _ := mgr.GetTree("test")
	if tree.Size() != 3 {
		t.Errorf("Latest snapshot should have 3 items, got %d", tree.Size())
	}
}

func TestSnapshotRestoreSpecificVersion(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем снапшот с 1 элементом
	tree := mgr.GetOrCreateTree("test")
	tree.Insert(&TestAccount{AccountID: 1, Balance: 100, Nonce: 1})
	
	version1, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot 1: %v", err)
	}
	
	time.Sleep(10 * time.Millisecond)
	
	// Добавляем еще элементы и создаем второй снапшот
	tree.Insert(&TestAccount{AccountID: 2, Balance: 200, Nonce: 2})
	tree.Insert(&TestAccount{AccountID: 3, Balance: 300, Nonce: 3})
	
	version2, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot 2: %v", err)
	}
	
	// Загружаем первый снапшот
	if err := mgr.LoadFromSnapshot(&version1); err != nil {
		t.Fatalf("Failed to load snapshot 1: %v", err)
	}
	
	tree, _ = mgr.GetTree("test")
	if tree.Size() != 1 {
		t.Errorf("Version 1 should have 1 item, got %d", tree.Size())
	}
	
	// Загружаем второй снапшот
	if err := mgr.LoadFromSnapshot(&version2); err != nil {
		t.Fatalf("Failed to load snapshot 2: %v", err)
	}
	
	tree, _ = mgr.GetTree("test")
	if tree.Size() != 3 {
		t.Errorf("Version 2 should have 3 items, got %d", tree.Size())
	}
}

func TestSnapshotRestoreNonExistent(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Пытаемся загрузить несуществующий снапшот
	fakeVersion := [32]byte{1, 2, 3, 4, 5, 6, 7, 8}
	err := mgr.LoadFromSnapshot(&fakeVersion)
	
	if err == nil {
		t.Error("Expected error when loading non-existent snapshot")
	}
}

// ============================================
// Concurrent Tests
// ============================================

func TestSnapshotConcurrentWithInserts(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	tree := mgr.GetOrCreateTree("accounts")
	accounts := generateTestAccounts(0, 1000)
	tree.InsertBatch(accounts)
	
	// Запускаем параллельные вставки
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			tree.Insert(&TestAccount{
				AccountID: uint64(10000 + i),
				Balance:   uint64(i),
				Nonce:     uint64(i),
			})
			time.Sleep(1 * time.Millisecond)
		}
		done <- true
	}()
	
	// Создаем снапшот во время вставок
	time.Sleep(10 * time.Millisecond)
	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot during inserts: %v", err)
	}
	
	<-done
	
	// Проверяем, что снапшот валиден
	if err := mgr.LoadFromSnapshot(&version); err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}
	
	t.Logf("Snapshot created successfully during concurrent inserts")
}

func TestSnapshotMultipleSnapshots(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем 10 снапшотов
	versions := make([][32]byte, 10)
	for i := 0; i < 10; i++ {
		tree := mgr.GetOrCreateTree(fmt.Sprintf("tree_%d", i))
		accounts := generateTestAccounts(i*100, 10)
		tree.InsertBatch(accounts)
		
		version, err := mgr.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", i, err)
		}
		versions[i] = version
		
		time.Sleep(5 * time.Millisecond)
	}
	
	// Проверяем список версий
	listedVersions, err := mgr.ListSnapshotVersions()
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}
	
	if len(listedVersions) != 10 {
		t.Errorf("Expected 10 versions, got %d", len(listedVersions))
	}
	
	// Загружаем случайный снапшот (5-й)
	if err := mgr.LoadFromSnapshot(&versions[5]); err != nil {
		t.Fatalf("Failed to load snapshot 5: %v", err)
	}
	
	trees := mgr.ListTrees()
	if len(trees) != 6 { // 0..5
		t.Errorf("Expected 6 trees for snapshot 5, got %d", len(trees))
	}
}

// ============================================
// Async Tests
// ============================================

func TestSnapshotAsync(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем дерево с данными
	tree := mgr.GetOrCreateTree("accounts")
	accounts := generateTestAccounts(0, 5000)
	tree.InsertBatch(accounts)
	
	resultChan := mgr.CreateSnapshotAsync()
	
	// Можем продолжать работу
	tree.Insert(&TestAccount{AccountID: 9999, Balance: 9999, Nonce: 9999})
	
	// Ждем результат
	result := <-resultChan
	
	if result.Error != nil {
		t.Fatalf("Async snapshot failed: %v", result.Error)
	}
	
	t.Logf("Async snapshot created: %x (took %v)", result.Version[:8], result.Duration)
	
	if result.Duration == 0 {
		t.Error("Duration should not be zero")
	}
}

func TestSnapshotAsyncMultiple(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	tree := mgr.GetOrCreateTree("test")
	
	// Запускаем несколько асинхронных снапшотов
	channels := make([]<-chan SnapshotResult, 5)
	
	for i := 0; i < 5; i++ {
		tree.Insert(&TestAccount{
			AccountID: uint64(i),
			Balance:   uint64(i * 100),
			Nonce:     uint64(i),
		})
		
		channels[i] = mgr.CreateSnapshotAsync()
		time.Sleep(10 * time.Millisecond)
	}
	
	// Собираем результаты
	successCount := 0
	for i, ch := range channels {
		result := <-ch
		if result.Error != nil {
			t.Logf("Snapshot %d failed: %v", i, result.Error)
		} else {
			successCount++
			t.Logf("Snapshot %d: %x (%v)", i, result.Version[:8], result.Duration)
		}
	}
	
	if successCount != 5 {
		t.Errorf("Expected 5 successful snapshots, got %d", successCount)
	}
}

func TestSnapshotLargeData(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем дерево с большим количеством данных
	tree := mgr.GetOrCreateTree("accounts")
	accounts := generateTestAccounts(0, 10000)
	tree.InsertBatch(accounts)

	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	
	// Получаем метаданные
	meta, err := mgr.GetSnapshotMetadata()
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	
	t.Logf("Snapshot size: %d bytes (with built-in Snappy compression)", meta.TotalSize)
	
	// Восстанавливаем
	if err := mgr.LoadFromSnapshot(&version); err != nil {
		t.Fatalf("Failed to load snapshot: %v", err)
	}
	
	tree, _ = mgr.GetTree("accounts")
	if tree.Size() != 10000 {
		t.Errorf("Size mismatch after restore: got %d, want 10000", tree.Size())
	}
}

// ============================================
// Metadata Tests
// ============================================

func TestSnapshotMetadata(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Пустой менеджер
	meta, err := mgr.GetSnapshotMetadata()
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	
	if meta.Count != 0 {
		t.Errorf("Initial count should be 0, got %d", meta.Count)
	}
	
	// Создаем несколько снапшотов
	versions := make([][32]byte, 3)
	for i := 0; i < 3; i++ {
		tree := mgr.GetOrCreateTree(fmt.Sprintf("tree_%d", i))
		tree.Insert(&TestAccount{AccountID: uint64(i), Balance: uint64(i), Nonce: uint64(i)})
		
		version, err := mgr.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", i, err)
		}
		versions[i] = version
		time.Sleep(5 * time.Millisecond)
	}
	
	// Проверяем метаданные
	meta, err = mgr.GetSnapshotMetadata()
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	
	if meta.Count != 3 {
		t.Errorf("Count should be 3, got %d", meta.Count)
	}
	
	if meta.FirstVersion != versions[0] {
		t.Errorf("First version mismatch")
	}
	
	if meta.LastVersion != versions[2] {
		t.Errorf("Last version mismatch")
	}
	
	if meta.TotalSize == 0 {
		t.Error("Total size should not be zero")
	}
	
	t.Logf("Metadata: count=%d, first=%x, last=%x, size=%d",
		meta.Count, meta.FirstVersion[:4], meta.LastVersion[:4], meta.TotalSize)
}

func TestSnapshotListVersions(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем несколько снапшотов
	expectedVersions := make(map[[32]byte]bool)
	
	for i := 0; i < 5; i++ {
		tree := mgr.GetOrCreateTree("test")
		tree.Insert(&TestAccount{AccountID: uint64(i), Balance: uint64(i), Nonce: uint64(i)})
		
		version, err := mgr.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", i, err)
		}
		expectedVersions[version] = true
		time.Sleep(5 * time.Millisecond)
	}
	
	// Получаем список
	versions, err := mgr.ListSnapshotVersions()
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}
	
	if len(versions) != 5 {
		t.Errorf("Expected 5 versions, got %d", len(versions))
	}
	
	// Проверяем, что все версии присутствуют
	for _, version := range versions {
		if !expectedVersions[version] {
			t.Errorf("Unexpected version: %x", version[:8])
		}
	}
}

func TestSnapshotDelete(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем 3 снапшота
	versions := make([][32]byte, 3)
	for i := 0; i < 3; i++ {
		tree := mgr.GetOrCreateTree("test")
		tree.Insert(&TestAccount{AccountID: uint64(i), Balance: uint64(i), Nonce: uint64(i)})
		
		version, err := mgr.CreateSnapshot()
		if err != nil {
			t.Fatalf("Failed to create snapshot %d: %v", i, err)
		}
		versions[i] = version
		time.Sleep(5 * time.Millisecond)
	}
	
	// Удаляем средний
	if err := mgr.DeleteSnapshot(versions[1]); err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}
	
	// Проверяем список
	listedVersions, err := mgr.ListSnapshotVersions()
	if err != nil {
		t.Fatalf("Failed to list versions: %v", err)
	}
	
	if len(listedVersions) != 2 {
		t.Errorf("Expected 2 versions after delete, got %d", len(listedVersions))
	}
	
	// Проверяем, что удаленный не загружается
	err = mgr.LoadFromSnapshot(&versions[1])
	if err == nil {
		t.Error("Expected error when loading deleted snapshot")
	}
	
	// Проверяем, что другие загружаются
	if err := mgr.LoadFromSnapshot(&versions[0]); err != nil {
		t.Errorf("Failed to load snapshot 0: %v", err)
	}
	
	if err := mgr.LoadFromSnapshot(&versions[2]); err != nil {
		t.Errorf("Failed to load snapshot 2: %v", err)
	}
}

// ============================================
// Edge Cases Tests
// ============================================

func TestSnapshotLargeTree(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large tree test in short mode")
	}
	
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем большое дерево
	tree := mgr.GetOrCreateTree("large")
	accounts := generateTestAccounts(0, 50000)
	
	start := time.Now()
	tree.InsertBatch(accounts)
	insertDuration := time.Since(start)
	
	t.Logf("Inserted 50K accounts in %v", insertDuration)
	
	// Создаем снапшот
	start = time.Now()
	version, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create large snapshot: %v", err)
	}
	snapshotDuration := time.Since(start)
	
	t.Logf("Created snapshot in %v", snapshotDuration)
	
	// Очищаем
	mgr.ClearAll()
	
	// Восстанавливаем
	start = time.Now()
	if err := mgr.LoadFromSnapshot(&version); err != nil {
		t.Fatalf("Failed to load large snapshot: %v", err)
	}
	restoreDuration := time.Since(start)
	
	t.Logf("Restored snapshot in %v", restoreDuration)
	
	// Проверяем
	tree, _ = mgr.GetTree("large")
	if tree.Size() != 50000 {
		t.Errorf("Size mismatch: got %d, want 50000", tree.Size())
	}
}

func TestSnapshotWithoutInit(t *testing.T) {
	// Тест создания менеджера БЕЗ snapshot поддержки
	cfg := DefaultConfig()
	mgr := NewManager[*TestAccount](cfg)
	
	// Проверяем, что snapshot операции возвращают ошибки
	_, err := mgr.CreateSnapshot()
	if err == nil {
		t.Error("Expected error when creating snapshot without initialization")
	}
	
	err = mgr.LoadFromSnapshot(nil)
	if err == nil {
		t.Error("Expected error when loading snapshot without initialization")
	}
	
	_, err = mgr.GetSnapshotMetadata()
	if err == nil {
		t.Error("Expected error when getting metadata without initialization")
	}
}

func TestSnapshotVersionConflict(t *testing.T) {
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(t, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем снапшот
	tree := mgr.GetOrCreateTree("test")
	tree.Insert(&TestAccount{AccountID: 1, Balance: 100, Nonce: 1})
	
	version1, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot 1: %v", err)
	}
	
	// Изменяем данные так, чтобы global root был тот же (маловероятно, но проверяем)
	// В реальности это практически невозможно из-за криптографических свойств хеша
	
	// Просто создаем еще один снапшот
	tree.Insert(&TestAccount{AccountID: 2, Balance: 200, Nonce: 2})
	version2, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot 2: %v", err)
	}
	
	// Версии должны отличаться
	if version1 == version2 {
		t.Error("Different states should produce different versions")
	}
}

// ============================================
// Benchmark Tests
// ============================================

func BenchmarkSnapshotCreate(b *testing.B) {
	dir := tempDir(&testing.T{})
	defer cleanup(&testing.T{}, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(&testing.T{}, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Подготавливаем данные
	tree := mgr.GetOrCreateTree("accounts")
	accounts := generateTestAccounts(0, 10000)
	tree.InsertBatch(accounts)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := mgr.CreateSnapshot()
		if err != nil {
			b.Fatalf("Failed to create snapshot: %v", err)
		}
	}
}

func BenchmarkSnapshotRestore(b *testing.B) {
	dir := tempDir(&testing.T{})
	defer cleanup(&testing.T{}, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(&testing.T{}, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Создаем снапшот
	tree := mgr.GetOrCreateTree("accounts")
	accounts := generateTestAccounts(0, 10000)
	tree.InsertBatch(accounts)
	
	version, err := mgr.CreateSnapshot()
	if err != nil {
		b.Fatalf("Failed to create snapshot: %v", err)
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		if err := mgr.LoadFromSnapshot(&version); err != nil {
			b.Fatalf("Failed to load snapshot: %v", err)
		}
	}
}

func BenchmarkSnapshotCreateAsync(b *testing.B) {
	dir := tempDir(&testing.T{})
	defer cleanup(&testing.T{}, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(&testing.T{}, snapshotPath)
	defer mgr.CloseSnapshots()
	
	tree := mgr.GetOrCreateTree("accounts")
	accounts := generateTestAccounts(0, 10000)
	tree.InsertBatch(accounts)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		resultChan := mgr.CreateSnapshotAsync()
		result := <-resultChan
		if result.Error != nil {
			b.Fatalf("Failed to create async snapshot: %v", result.Error)
		}
	}
}

func BenchmarkSnapshotSerialization(b *testing.B) {
	dir := tempDir(&testing.T{})
	defer cleanup(&testing.T{}, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	mgr := createTestManager(&testing.T{}, snapshotPath)
	defer mgr.CloseSnapshots()
	
	// Различные размеры данных
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			tree := mgr.GetOrCreateTree(fmt.Sprintf("bench_%d", size))
			accounts := generateTestAccounts(0, size)
			tree.InsertBatch(accounts)
			
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				_, err := mgr.CreateSnapshot()
				if err != nil {
					b.Fatalf("Failed to create snapshot: %v", err)
				}
			}
		})
	}
}

// ============================================
// Integration Tests
// ============================================

func TestSnapshotIntegrationScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	
	dir := tempDir(t)
	defer cleanup(t, dir)
	
	snapshotPath := filepath.Join(dir, "snapshots.db")
	
	// Сценарий 1: Создание и заполнение
	t.Log("Phase 1: Creating manager and populating trees")
	mgr1, err := NewManagerWithSnapshot[*TestAccount](DefaultConfig(), snapshotPath)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	
	for i := 0; i < 5; i++ {
		tree := mgr1.GetOrCreateTree(fmt.Sprintf("tree_%d", i))
		accounts := generateTestAccounts(i*1000, 100)
		tree.InsertBatch(accounts)
	}
	
	version1, err := mgr1.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot 1: %v", err)
	}
	t.Logf("Snapshot 1 created: %x", version1[:8])
	
	// Модификации
	tree0, _ := mgr1.GetTree("tree_0")
	tree0.Insert(&TestAccount{AccountID: 9999, Balance: 9999, Nonce: 9999})
	
	version2, err := mgr1.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot 2: %v", err)
	}
	t.Logf("Snapshot 2 created: %x", version2[:8])
	
	mgr1.CloseSnapshots()
	
	// Сценарий 2: Новый менеджер загружает снапшот
	t.Log("Phase 2: New manager loading snapshot")
	mgr2, err := NewManagerWithSnapshot[*TestAccount](DefaultConfig(), snapshotPath)
	if err != nil {
		t.Fatalf("Failed to create manager 2: %v", err)
	}
	defer mgr2.CloseSnapshots()
	
	// Загружаем первый снапшот
	if err := mgr2.LoadFromSnapshot(&version1); err != nil {
		t.Fatalf("Failed to load snapshot 1: %v", err)
	}
	
	// Проверяем состояние
	trees := mgr2.ListTrees()
	if len(trees) != 5 {
		t.Errorf("Expected 5 trees, got %d", len(trees))
	}
	
	tree0, _ = mgr2.GetTree("tree_0")
	if tree0.Exists(9999) {
		t.Error("Item 9999 should not exist in snapshot 1")
	}
	
	// Загружаем второй снапшот
	if err := mgr2.LoadFromSnapshot(&version2); err != nil {
		t.Fatalf("Failed to load snapshot 2: %v", err)
	}
	
	tree0, _ = mgr2.GetTree("tree_0")
	if !tree0.Exists(9999) {
		t.Error("Item 9999 should exist in snapshot 2")
	}
	
	t.Log("Integration test passed")
}
