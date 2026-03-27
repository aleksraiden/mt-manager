package merkletree

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	//"github.com/vmihailenco/msgpack/v5"
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
// Теперь НЕ параметризован типом - работает с UniversalManager
type SnapshotManager struct {
	storage *SnapshotStorage
	workers int

	// Инкрементальные снапшоты
	lastCheckpoint atomic.Pointer[[32]byte]
	lastVersion    atomic.Pointer[[32]byte]

	// Метрики (lock-free atomic)
	captureTimeNs   atomic.Int64
	serializeTimeNs atomic.Int64
	writeTimeNs     atomic.Int64
	snapshotCount   atomic.Uint64
}

// NewSnapshotManager создает менеджер снапшотов
func NewSnapshotManager(dbPath string) (*SnapshotManager, error) {
	workers := runtime.NumCPU()
	if workers > 16 {
		workers = 16
	}

	storage, err := NewSnapshotStorage(dbPath)
	if err != nil {
		return nil, err
	}

	return &SnapshotManager{
		storage: storage,
		workers: workers,
	}, nil
}

// Close закрывает менеджер
func (sm *SnapshotManager) Close() error {
	return sm.storage.Close()
}

// ============================================
// ФАЗА 1: Lock-Free Capture (~80µs)
// ============================================

type treeReference struct {
	name string
	tree TreeInterface
}

// captureTreeReferences быстро получает ссылки на деревья
// КРИТИЧНО: Минимальная блокировка UniversalManager (<100µs)
func (sm *SnapshotManager) captureTreeReferences(mgr *UniversalManager) ([]*treeReference, error) {
	start := time.Now()

	// Короткая read-блокировка только для копирования указателей
	mgr.mu.RLock()
	refs := make([]*treeReference, 0, len(mgr.trees))
	for name, tree := range mgr.trees {
		if tree.isStateExcluded() {
			continue
		}
		refs = append(refs, &treeReference{
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
func (sm *SnapshotManager) serializeTreeLockFree(tree TreeInterface, name string) ([]byte, error) {
	itemBytes, err := tree.serializeItems()
	if err != nil {
		return nil, err // уже содержит имя дерева и тип
	}

	// Кодируем [][]byte в один блоб для хранения:
	// [count uint32][len0 uint32][data0...][len1 uint32][data1...]...
	return encodeItemsBlob(itemBytes), nil
}

// serializeAllTreesParallel сериализует все деревья параллельно
func (sm *SnapshotManager) serializeAllTreesParallel(refs []*treeReference) (map[string][]byte, error) {
	start := time.Now()

	type result struct {
		name string
		data []byte
		err  error
	}

	jobs := make(chan *treeReference, len(refs))
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

func (sm *SnapshotManager) CreateSnapshot(mgr *UniversalManager) ([32]byte, error) {
	// Если хотя бы одно дерево не трекает dirty — делаем полный чекпоинт.
	// Смешивать нельзя: инкрементальный снапшот должен быть либо для всех, либо ни для кого.
	if !mgr.allTreesTrackDirty() {
		return sm.CreateCheckpoint(mgr)
	}

	// Есть ли уже хотя бы один чекпоинт?
	lastCP := sm.lastCheckpoint.Load()
	if lastCP == nil {
		// Первый раз — обязательно полный
		return sm.CreateCheckpoint(mgr)
	}

	// Всё готово — делаем инкрементальный
	return sm.createIncremental(mgr)
}

func (sm *SnapshotManager) CreateCheckpoint(mgr *UniversalManager) ([32]byte, error) {
	refs, _ := sm.captureTreeReferences(mgr)
	version := mgr.ComputeGlobalRoot()

	serializedTrees, err := sm.serializeAllTreesParallel(refs)
	if err != nil {
		return [32]byte{}, err
	}

	// Получаем предыдущую версию как parent
	parentVersion := [32]byte{}
	if prev := sm.lastVersion.Load(); prev != nil {
		parentVersion = *prev
	}

	//добавлен parentVersion
	if err := sm.storage.SaveCheckpoint(version, parentVersion, time.Now().Unix(), serializedTrees); err != nil {
		return [32]byte{}, err
	}

	mgr.mu.RLock()
	for _, tree := range mgr.trees {
		tree.resetDirtyTracking()
	}
	mgr.mu.RUnlock()

	sm.lastCheckpoint.Store(&version)
	sm.lastVersion.Store(&version)
	return version, nil
}

func (sm *SnapshotManager) createIncremental(mgr *UniversalManager) ([32]byte, error) {
	version := mgr.ComputeGlobalRoot()

	lastCP := sm.lastCheckpoint.Load() // гарантированно не nil — проверено в CreateSnapshot

	parentVersion := [32]byte{}
	if prev := sm.lastVersion.Load(); prev != nil {
		parentVersion = *prev
	}

	mgr.mu.RLock()
	deltas := make(map[string]*IncrementalTreeSnapshot, len(mgr.trees))
	for name, tree := range mgr.trees {
		if tree.isStateExcluded() {
			continue
		}
		upserted, deleted, err := tree.serializeDirtyItems()
		if err != nil {
			mgr.mu.RUnlock()
			return [32]byte{}, fmt.Errorf("tree %q: %w", name, err)
		}
		deltas[name] = &IncrementalTreeSnapshot{
			TreeID:      name,
			RootHash:    tree.ComputeRoot(),
			UpsertItems: upserted,
			DeletedKeys: deleted,
		}
	}
	mgr.mu.RUnlock()

	if err := sm.storage.SaveIncremental(version, parentVersion, *lastCP, time.Now().Unix(), deltas); err != nil {
		return [32]byte{}, err
	}

	// Сбрасываем dirty только после успешной записи
	mgr.mu.RLock()
	for _, tree := range mgr.trees {
		tree.resetDirtyTracking()
	}
	mgr.mu.RUnlock()

	sm.lastVersion.Store(&version)
	return version, nil
}

// ============================================
// Load Snapshot (параллельная загрузка)
// ============================================

// LoadSnapshot загружает снапшот
// Если version == nil, загружает последний
func (sm *SnapshotManager) LoadSnapshot(mgr *UniversalManager, version *[32]byte, factory TreeFactory) error {
	header, err := sm.storage.LoadHeader(version)
	if err != nil {
		return err
	}

	switch header.Kind {
	case KindCheckpoint:
		// Простая полная загрузка — как сейчас
		return sm.loadCheckpoint(mgr, version, factory)

	case KindIncremental:
		// 1. Загружаем ближайший чекпоинт
		if err := sm.loadCheckpoint(mgr, &header.CheckpointRef, factory); err != nil {
			return fmt.Errorf("load base checkpoint %x: %w", header.CheckpointRef[:4], err)
		}

		// 2. Строим цепочку снапшотов от чекпоинта до target
		chain, err := sm.storage.BuildChain(header.CheckpointRef, *version)
		if err != nil {
			return fmt.Errorf("build chain: %w", err)
		}

		// 3. Применяем дельты последовательно
		for i, snap := range chain {
			if err := sm.applyIncremental(mgr, snap); err != nil {
				return fmt.Errorf("apply snapshot %d/%d (%x): %w",
					i+1, len(chain), snap.Version[:4], err)
			}
		}

		return nil
	}

	return fmt.Errorf("unknown snapshot kind: %d", header.Kind)
}

func (sm *SnapshotManager) loadCheckpoint(mgr *UniversalManager, version *[32]byte, factory TreeFactory) error {
	snapshot, err := sm.storage.LoadCheckpoint(version)
	if err != nil {
		return err
	}

	newTrees := make(map[string]TreeInterface, len(snapshot.Trees))
	for name, treeSnap := range snapshot.Trees {
		tree := factory(name)
		if tree == nil {
			return fmt.Errorf("factory returned nil for tree %q", name)
		}
		tree.SetName(name)

		if len(treeSnap.Items) == 0 {
			newTrees[name] = tree
			continue
		}

		itemBytes, err := decodeItemsBlob(treeSnap.Items[0])
		if err != nil {
			return fmt.Errorf("tree %q: decode blob: %w", name, err)
		}
		if err := tree.deserializeAndInsert(itemBytes); err != nil {
			return fmt.Errorf("tree %q: restore: %w", name, err)
		}
		newTrees[name] = tree
	}

	mgr.mu.Lock()
	mgr.trees = newTrees
	mgr.treeRootCache = make(map[string][32]byte)
	mgr.globalRootDirty = true
	mgr.mu.Unlock()

	return nil
}

func (sm *SnapshotManager) applyIncremental(mgr *UniversalManager, entry ChainEntry) error {
	deltas, err := sm.storage.LoadIncrementalDelta(entry.Version)
	if err != nil {
		return err
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for name, delta := range deltas {
		tree, exists := mgr.trees[name]
		if !exists {
			// Дерево появилось в дельте, но не в менеджере — пропускаем
			// (если нужно строгое поведение — возвращать ошибку)
			continue
		}
		if err := tree.applyDelta(delta.UpsertItems, delta.DeletedKeys); err != nil {
			return fmt.Errorf("tree %q apply delta: %w", name, err)
		}
		delete(mgr.treeRootCache, name)
	}

	mgr.globalRootDirty = true
	return nil
}

// ============================================
// Metadata & Utilities
// ============================================

// GetMetadata возвращает метаданные снапшотов
func (sm *SnapshotManager) GetMetadata() (*SnapshotMetadata, error) {
	return sm.storage.GetMetadata()
}

// ListVersions возвращает список версий
func (sm *SnapshotManager) ListVersions() ([][32]byte, error) {
	return sm.storage.ListVersions()
}

// DeleteSnapshot удаляет снапшот
func (sm *SnapshotManager) DeleteSnapshot(version [32]byte) error {
	return sm.storage.DeleteSnapshot(version)
}

// GetMetrics возвращает метрики производительности
func (sm *SnapshotManager) GetMetrics() SnapshotMetrics {
	return SnapshotMetrics{
		CaptureTimeNs:   sm.captureTimeNs.Load(),
		SerializeTimeNs: sm.serializeTimeNs.Load(),
		WriteTimeNs:     sm.writeTimeNs.Load(),
		TotalTimeNs:     sm.captureTimeNs.Load() + sm.serializeTimeNs.Load() + sm.writeTimeNs.Load(),
	}
}

// GetSnapshotCount возвращает количество созданных снапшотов
func (sm *SnapshotManager) GetSnapshotCount() uint64 {
	return sm.snapshotCount.Load()
}

// Compact сжимает базу данных
func (sm *SnapshotManager) Compact() error {
	return sm.storage.Compact()
}

// Flush сбрасывает данные на диск
func (sm *SnapshotManager) Flush() error {
	return sm.storage.Flush()
}

// GetStorageStats возвращает статистику хранилища
func (sm *SnapshotManager) GetStorageStats() StorageStats {
	return sm.storage.GetStats()
}

// Storage helpers
// encodeItemsBlob упаковывает [][]byte в один []byte:
// [N uint32] [len_0 uint32][bytes_0] ... [len_N-1 uint32][bytes_N-1]
func encodeItemsBlob(items [][]byte) []byte {
	totalSize := 4 // uint32 count
	for _, item := range items {
		totalSize += 4 + len(item)
	}

	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(items)))
	offset := 4
	for _, item := range items {
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(item)))
		offset += 4
		copy(buf[offset:], item)
		offset += len(item)
	}
	return buf
}

func decodeItemsBlob(data []byte) ([][]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("blob too short")
	}
	count := binary.BigEndian.Uint32(data[0:4])
	items := make([][]byte, 0, count)
	offset := 4
	for i := uint32(0); i < count; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("blob truncated at item %d", i)
		}
		itemLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if offset+itemLen > len(data) {
			return nil, fmt.Errorf("blob truncated at item %d data", i)
		}
		items = append(items, data[offset:offset+itemLen])
		offset += itemLen
	}
	return items, nil
}
