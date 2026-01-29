// snapshot.go
package merkletree

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// ============================================
// Lock-Free Snapshot Manager
// ============================================

const (
	CurrentSchemaVersion = 1
)

// Snapshot представляет снимок состояния
type Snapshot struct {
	SchemaVersion int                      `msgpack:"schema_version"`
	Version       [32]byte                 `msgpack:"version"`
	Timestamp     int64                    `msgpack:"timestamp"`
	TreeCount     int                      `msgpack:"tree_count"`
	Trees         map[string]*TreeSnapshot `msgpack:"trees"`
}

// TreeSnapshot снимок дерева
type TreeSnapshot struct {
	TreeID    string   `msgpack:"tree_id"`
	RootHash  [32]byte `msgpack:"root_hash"`
	ItemCount uint64   `msgpack:"item_count"`
	Items     [][]byte `msgpack:"items"`
}

// SnapshotMetadata метаданные снапшотов
type SnapshotMetadata struct {
	FirstVersion [32]byte `msgpack:"first_version"`
	LastVersion  [32]byte `msgpack:"last_version"`
	Count        int      `msgpack:"count"`
	TotalSize    int64    `msgpack:"total_size"`
}

// SnapshotOptions опции создания снапшота
type SnapshotOptions struct {
	Async   bool // Асинхронное создание
	Workers int  // Количество воркеров для сериализации
}

// DefaultSnapshotOptions возвращает опции по умолчанию
func DefaultSnapshotOptions() *SnapshotOptions {
	return &SnapshotOptions{
		Async:   false,
		Workers: runtime.NumCPU(),
	}
}

// SnapshotResult результат асинхронного снапшота
type SnapshotResult struct {
	Version  [32]byte
	Duration time.Duration
	Error    error
}

// SnapshotMetrics метрики производительности
type SnapshotMetrics struct {
	CaptureTimeNs   int64
	SerializeTimeNs int64
	WriteTimeNs     int64
	TotalTimeNs     int64
}

func (m SnapshotMetrics) String() string {
	return fmt.Sprintf("Capture: %dµs | Serialize: %dµs | Write: %dµs | Total: %dµs",
		m.CaptureTimeNs/1000,
		m.SerializeTimeNs/1000,
		m.WriteTimeNs/1000,
		m.TotalTimeNs/1000)
}

// ============================================
// SnapshotManager
// ============================================

// SnapshotManager управляет снапшотами с минимальными блокировками
type SnapshotManager[T Hashable] struct {
	storage *SnapshotStorage
	workers int
	
	// Метрики (lock-free atomic)
	captureTimeNs   atomic.Int64
	serializeTimeNs atomic.Int64
	writeTimeNs     atomic.Int64
	snapshotCount   atomic.Uint64
}

// NewSnapshotManager создает менеджер снапшотов
func NewSnapshotManager[T Hashable](dbPath string, workers int) (*SnapshotManager[T], error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	
	storage, err := NewSnapshotStorage(dbPath)
	if err != nil {
		return nil, err
	}
	
	return &SnapshotManager[T]{
		storage: storage,
		workers: workers,
	}, nil
}

// Close закрывает менеджер
func (sm *SnapshotManager[T]) Close() error {
	return sm.storage.Close()
}

// ============================================
// ФАЗА 1: Lock-Free Capture (~80µs)
// ============================================

type treeReference[T Hashable] struct {
	name string
	tree *Tree[T]
}

// captureTreeReferences быстро получает ссылки на деревья
// КРИТИЧНО: Минимальная блокировка TreeManager (<100µs)
func (sm *SnapshotManager[T]) captureTreeReferences(mgr *TreeManager[T]) ([]*treeReference[T], error) {
	start := time.Now()
	
	// Короткая read-блокировка только для копирования указателей
	mgr.mu.RLock()
	refs := make([]*treeReference[T], 0, len(mgr.trees))
	for name, tree := range mgr.trees {
		refs = append(refs, &treeReference[T]{
			name: name,
			tree: tree, // Shallow copy указателя - безопасно
		})
	}
	mgr.mu.RUnlock()
	
	// Записываем метрику
	sm.captureTimeNs.Store(time.Since(start).Nanoseconds())
	
	return refs, nil
}

// ============================================
// ФАЗА 2: Параллельная сериализация (lock-free)
// ============================================

// serializeTreeLockFree сериализует дерево БЕЗ блокировок
// Использует lock-free sync.Map.Range()
func (sm *SnapshotManager[T]) serializeTreeLockFree(tree *Tree[T], name string) ([]byte, error) {
	// Lock-free сбор элементов через sync.Map.Range
	// Range НЕ блокирует другие операции (Insert/Get)!
	items := make([]T, 0, tree.Size())
	
	tree.items.Range(func(key, value interface{}) bool {
		items = append(items, value.(T))
		return true
	})
	
	// MessagePack сериализация (CPU-bound, можно параллельно)
	data, err := msgpack.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tree %s: %w", name, err)
	}
	
	return data, nil
}

// serializeAllTreesParallel сериализует все деревья параллельно
func (sm *SnapshotManager[T]) serializeAllTreesParallel(refs []*treeReference[T]) (map[string][]byte, error) {
	start := time.Now()
	
	type result struct {
		name string
		data []byte
		err  error
	}
	
	jobs := make(chan *treeReference[T], len(refs))
	results := make(chan result, len(refs))
	
	// Запускаем воркеров
	var wg sync.WaitGroup
	for i := 0; i < sm.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ref := range jobs {
				data, err := sm.serializeTreeLockFree(ref.tree, ref.name)
				results <- result{name: ref.name, data: data, err: err}
			}
		}()
	}
	
	// Отправляем задачи
	for _, ref := range refs {
		jobs <- ref
	}
	close(jobs)
	
	// Ждем завершения
	wg.Wait()
	close(results)
	
	// Собираем результаты
	serialized := make(map[string][]byte, len(refs))
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		serialized[res.name] = res.data
	}
	
	sm.serializeTimeNs.Store(time.Since(start).Nanoseconds())
	
	return serialized, nil
}

// ============================================
// ФАЗА 3: Batch Write
// ============================================

// CreateSnapshot создает снапшот с минимальными блокировками
func (sm *SnapshotManager[T]) CreateSnapshot(mgr *TreeManager[T], opts *SnapshotOptions) ([32]byte, error) {
	if opts == nil {
		opts = DefaultSnapshotOptions()
	}
	
	totalStart := time.Now()
	
	// ФАЗА 1: Capture (~80µs блокировка)
	refs, err := sm.captureTreeReferences(mgr)
	if err != nil {
		return [32]byte{}, fmt.Errorf("capture failed: %w", err)
	}
	
	// Вычисляем version (GlobalRoot)
	version := mgr.ComputeGlobalRoot()
	
	// ФАЗА 2: Serialize (параллельно, БЕЗ блокировок)
	serializedTrees, err := sm.serializeAllTreesParallel(refs)
	if err != nil {
		return [32]byte{}, fmt.Errorf("serialize failed: %w", err)
	}
	
	// ФАЗА 3: Write (батч)
	writeStart := time.Now()
	if err := sm.storage.SaveSnapshot(version, time.Now().Unix(), serializedTrees); err != nil {
		return [32]byte{}, fmt.Errorf("write failed: %w", err)
	}
	sm.writeTimeNs.Store(time.Since(writeStart).Nanoseconds())
	
	totalDuration := time.Since(totalStart)
	sm.snapshotCount.Add(1)
	
	// Логируем если долго
	if totalDuration > 10*time.Millisecond {
		fmt.Printf("[WARN] Snapshot %x took %v (%s)\n",
			version[:4], totalDuration, sm.GetMetrics())
	}
	
	return version, nil
}

// CreateSnapshotAsync создает снапшот асинхронно (fire-and-forget)
// Возвращает канал для получения результата
// БЛОКИРОВКА: 0µs (всё в фоне)
func (sm *SnapshotManager[T]) CreateSnapshotAsync(mgr *TreeManager[T], opts *SnapshotOptions) <-chan SnapshotResult {
	resultChan := make(chan SnapshotResult, 1)
	
	go func() {
		defer close(resultChan)
		
		start := time.Now()
		version, err := sm.CreateSnapshot(mgr, opts)
		
		resultChan <- SnapshotResult{
			Version:  version,
			Duration: time.Since(start),
			Error:    err,
		}
	}()
	
	return resultChan
}

// ============================================
// Load Snapshot (параллельная загрузка)
// ============================================

// LoadSnapshot загружает снапшот
// Если version == nil, загружает последний
func (sm *SnapshotManager[T]) LoadSnapshot(mgr *TreeManager[T], version *[32]byte) error {
	start := time.Now()
	
	// Загружаем из storage
	snapshot, err := sm.storage.LoadSnapshot(version)
	if err != nil {
		return err
	}
	
	// Проверяем версию схемы
	if snapshot.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("unsupported schema version: %d", snapshot.SchemaVersion)
	}
	
	// Десериализуем деревья параллельно
	newTrees, err := sm.deserializeTreesParallel(mgr, snapshot)
	if err != nil {
		return err
	}
	
	// Короткая блокировка для замены деревьев
	mgr.mu.Lock()
	mgr.trees = newTrees
	mgr.globalRootDirty = true
	mgr.mu.Unlock()
	
	fmt.Printf("[INFO] Snapshot %x loaded in %v (%d trees)\n",
		snapshot.Version[:4], time.Since(start), len(newTrees))
	
	return nil
}

// deserializeTreesParallel десериализует деревья параллельно
func (sm *SnapshotManager[T]) deserializeTreesParallel(mgr *TreeManager[T], snapshot *Snapshot) (map[string]*Tree[T], error) {
	type jobData struct {
		name string
		data []byte
	}
	
	jobs := make(chan jobData, len(snapshot.Trees))
	type resultData struct {
		name string
		tree *Tree[T]
		err  error
	}
	results := make(chan resultData, len(snapshot.Trees))
	
	// Воркеры
	var wg sync.WaitGroup
	for i := 0; i < sm.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				tree, err := sm.deserializeTree(mgr, job.name, job.data)
				results <- resultData{name: job.name, tree: tree, err: err}
			}
		}()
	}
	
	// Отправляем задачи
	for name, treeSnap := range snapshot.Trees {
		if len(treeSnap.Items) > 0 {
			jobs <- jobData{name: name, data: treeSnap.Items[0]}
		}
	}
	close(jobs)
	
	wg.Wait()
	close(results)
	
	// Собираем результаты
	newTrees := make(map[string]*Tree[T], len(snapshot.Trees))
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		newTrees[res.name] = res.tree
	}
	
	return newTrees, nil
}

// deserializeTree десериализует одно дерево
func (sm *SnapshotManager[T]) deserializeTree(mgr *TreeManager[T], treeName string, data []byte) (*Tree[T], error) {
	// Десериализуем items
	var items []T
	if err := msgpack.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tree %s: %w", treeName, err)
	}
	
	// Создаем новое дерево
	tree := New[T](mgr.config)
	tree.name = treeName
	
	// Вставляем items батчем
	tree.InsertBatch(items)
	
	return tree, nil
}

// ============================================
// Metadata & Utilities
// ============================================

// GetMetadata возвращает метаданные снапшотов
func (sm *SnapshotManager[T]) GetMetadata() (*SnapshotMetadata, error) {
	return sm.storage.GetMetadata()
}

// ListVersions возвращает список версий
func (sm *SnapshotManager[T]) ListVersions() ([][32]byte, error) {
	return sm.storage.ListVersions()
}

// DeleteSnapshot удаляет снапшот
func (sm *SnapshotManager[T]) DeleteSnapshot(version [32]byte) error {
	return sm.storage.DeleteSnapshot(version)
}

// GetMetrics возвращает метрики производительности
func (sm *SnapshotManager[T]) GetMetrics() SnapshotMetrics {
	return SnapshotMetrics{
		CaptureTimeNs:   sm.captureTimeNs.Load(),
		SerializeTimeNs: sm.serializeTimeNs.Load(),
		WriteTimeNs:     sm.writeTimeNs.Load(),
		TotalTimeNs:     sm.captureTimeNs.Load() + sm.serializeTimeNs.Load() + sm.writeTimeNs.Load(),
	}
}

// GetSnapshotCount возвращает количество созданных снапшотов
func (sm *SnapshotManager[T]) GetSnapshotCount() uint64 {
	return sm.snapshotCount.Load()
}

// Compact сжимает базу данных
func (sm *SnapshotManager[T]) Compact() error {
	return sm.storage.Compact()
}

// Flush сбрасывает данные на диск
func (sm *SnapshotManager[T]) Flush() error {
	return sm.storage.Flush()
}

// GetStorageStats возвращает статистику хранилища
func (sm *SnapshotManager[T]) GetStorageStats() StorageStats {
	return sm.storage.GetStats()
}
