// snapshot_storage.go
package merkletree

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"context"
	"github.com/cockroachdb/pebble/v2"
	//"github.com/cockroachdb/pebble/v2/bloom"
)

// ============================================
// Lock-Free Snapshot Storage
// Оптимизировано для high-frequency updates
// ============================================

const (
	// Префиксы ключей
	prefixSnapshotMeta = "snap:meta:"  // snap:meta:{version} → metadata
	prefixSnapshotTree = "snap:tree:"  // snap:tree:{version}:{tree_name} → tree data
	prefixGlobalMeta   = "global:"     // global:last, global:first, global:count
)

// SnapshotStorage хранилище снапшотов с оптимизациями для PebbleDB
type SnapshotStorage struct {
	db *pebble.DB
	
	// Метрики (lock-free)
	writtenBytes atomic.Uint64
	readBytes    atomic.Uint64
	writeCount   atomic.Uint64
	readCount    atomic.Uint64
}

// NewSnapshotStorage создает оптимизированное хранилище
func NewSnapshotStorage(dbPath string) (*SnapshotStorage, error) {
	opts := &pebble.Options{
		// Большой cache для горячих снапшотов
		Cache: pebble.NewCache(256 << 20), // 256MB
		
		// Большой write buffer для батчинга
		MemTableSize: 128 << 20, // 128MB
		
		// Агрессивный compaction для меньшей фрагментации
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 12,
		LBaseMaxBytes:         128 << 20,
		
		// Параллельный compaction
		// v2+ pebble not included
		/**
		MaxConcurrentCompactions = func() int { return 4 },
		// Bloom filters для быстрого поиска
		Levels: []pebble.LevelOptions{{
			BlockSize:      64 << 10, // 64KB блоки
			IndexBlockSize: 128 << 10,
			FilterPolicy:   bloom.FilterPolicy(10), // 10 bits = ~1% false positive
			FilterType:     pebble.TableFilter,
			Compression:    pebble.SnappyCompression, // Встроенное сжатие
		}},
		**/
		// WAL оптимизации
		WALBytesPerSync: 1 << 20, // 1MB - реже fsync
		
		// Файловые дескрипторы
		MaxOpenFiles:       2000,
		FormatMajorVersion: pebble.FormatNewest,
		
		// Sync настройки (можно отключить для скорости)
		DisableWAL: false, // Рекомендуется false для надежности
		BytesPerSync: 512 << 10, // 512KB
	}
	
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}
	
	return &SnapshotStorage{db: db}, nil
}

// Close закрывает хранилище
func (s *SnapshotStorage) Close() error {
	// Принудительный flush перед закрытием
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}
	return s.db.Close()
}

// ============================================
// Batch Write (атомарная запись снапшота)
// ============================================

// SaveSnapshot сохраняет снапшот одним батчем
// trees: map[treeName]serializedData
func (s *SnapshotStorage) SaveSnapshot(version [32]byte, timestamp int64, trees map[string][]byte) error {
	batch := s.db.NewBatch()
	defer batch.Close()
	
	// 1. Метаданные снапшота
	metaKey := makeSnapshotMetaKey(version)
	metaValue := encodeSnapshotMeta(version, timestamp, len(trees))
	if err := batch.Set(metaKey, metaValue, pebble.NoSync); err != nil {
		return fmt.Errorf("failed to set metadata: %w", err)
	}
	
	// 2. Данные деревьев
	totalSize := uint64(0)
	for treeName, treeData := range trees {
		treeKey := makeSnapshotTreeKey(version, treeName)
		if err := batch.Set(treeKey, treeData, pebble.NoSync); err != nil {
			return fmt.Errorf("failed to set tree %s: %w", treeName, err)
		}
		totalSize += uint64(len(treeData))
	}
	
	// 3. Обновляем глобальные метаданные
	if err := s.updateGlobalMeta(batch, version); err != nil {
		return fmt.Errorf("failed to update global meta: %w", err)
	}
	
	// 4. Коммитим батч (NoSync для скорости, fsync в фоне)
	// Используйте pebble.Sync если критична надежность
	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	
	// Обновляем метрики
	s.writtenBytes.Add(totalSize)
	s.writeCount.Add(1)
	
	return nil
}

// ============================================
// Load Snapshot (параллельная загрузка)
// ============================================

// LoadSnapshot загружает снапшот
// Если version == nil, загружает последний
func (s *SnapshotStorage) LoadSnapshot(version *[32]byte) (*Snapshot, error) {
	// Определяем версию
	targetVersion := version
	if targetVersion == nil {
		lastVer, err := s.getLastVersion()
		if err != nil {
			return nil, fmt.Errorf("no snapshots found: %w", err)
		}
		targetVersion = lastVer
	}
	
	// Читаем метаданные
	metaKey := makeSnapshotMetaKey(*targetVersion)
	metaData, closer, err := s.db.Get(metaKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, fmt.Errorf("snapshot not found: %x", targetVersion)
		}
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	
	ver, timestamp, treeCount := decodeSnapshotMeta(metaData)
	closer.Close()
	
	// Получаем список деревьев
	treeNames, err := s.listSnapshotTrees(*targetVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to list trees: %w", err)
	}
	
	// Загружаем деревья параллельно
	trees := make(map[string]*TreeSnapshot, treeCount)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, len(treeNames))
	
	for _, treeName := range treeNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			
			treeKey := makeSnapshotTreeKey(*targetVersion, name)
			treeData, closer, err := s.db.Get(treeKey)
			if err != nil {
				errChan <- fmt.Errorf("failed to read tree %s: %w", name, err)
				return
			}
			
			// Копируем данные
			dataCopy := make([]byte, len(treeData))
			copy(dataCopy, treeData)
			closer.Close()
			
			// Создаем TreeSnapshot (данные уже сериализованы)
			treeSnapshot := &TreeSnapshot{
				TreeID: name,
				Items:  [][]byte{dataCopy}, // Данные в сыром виде
			}
			
			mu.Lock()
			trees[name] = treeSnapshot
			mu.Unlock()
			
			s.readBytes.Add(uint64(len(dataCopy)))
		}(treeName)
	}
	
	wg.Wait()
	close(errChan)
	
	// Проверяем ошибки
	if err := <-errChan; err != nil {
		return nil, err
	}
	
	s.readCount.Add(1)
	
	return &Snapshot{
		SchemaVersion: CurrentSchemaVersion,
		Version:       ver,
		Timestamp:     timestamp,
		TreeCount:     treeCount,
		Trees:         trees,
	}, nil
}

// ============================================
// Metadata Operations
// ============================================

// GetMetadata возвращает метаданные снапшотов
func (s *SnapshotStorage) GetMetadata() (*SnapshotMetadata, error) {
	metadata := &SnapshotMetadata{}
	
	// First version
	firstData, closer, err := s.db.Get([]byte(prefixGlobalMeta + "first"))
	if err == nil {
		copy(metadata.FirstVersion[:], firstData)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return nil, err
	}
	
	// Last version
	lastData, closer, err := s.db.Get([]byte(prefixGlobalMeta + "last"))
	if err == nil {
		copy(metadata.LastVersion[:], lastData)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return nil, err
	}
	
	// Count
	countData, closer, err := s.db.Get([]byte(prefixGlobalMeta + "count"))
	if err == nil {
		metadata.Count = int(binary.BigEndian.Uint32(countData))
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return nil, err
	}
	
	// Total size (итерируем)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixSnapshotTree),
		UpperBound: []byte(prefixSnapshotTree + "\xff"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	
	for iter.First(); iter.Valid(); iter.Next() {
		metadata.TotalSize += int64(len(iter.Value()))
	}
	
	return metadata, iter.Error()
}

// ListVersions возвращает список всех версий
func (s *SnapshotStorage) ListVersions() ([][32]byte, error) {
	var versions [][32]byte
	
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixSnapshotMeta),
		UpperBound: []byte(prefixSnapshotMeta + "\xff"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < len(prefixSnapshotMeta)+32 {
			continue
		}
		
		var version [32]byte
		copy(version[:], key[len(prefixSnapshotMeta):])
		versions = append(versions, version)
	}
	
	return versions, iter.Error()
}

// DeleteSnapshot удаляет снапшот
func (s *SnapshotStorage) DeleteSnapshot(version [32]byte) error {
	// Получаем список деревьев
	treeNames, err := s.listSnapshotTrees(version)
	if err != nil {
		return err
	}
	
	batch := s.db.NewBatch()
	defer batch.Close()
	
	// Удаляем метаданные
	metaKey := makeSnapshotMetaKey(version)
	if err := batch.Delete(metaKey, pebble.NoSync); err != nil {
		return err
	}
	
	// Удаляем деревья
	for _, treeName := range treeNames {
		treeKey := makeSnapshotTreeKey(version, treeName)
		if err := batch.Delete(treeKey, pebble.NoSync); err != nil {
			return err
		}
	}
	
	// Обновляем count
	if err := s.decrementCount(batch); err != nil {
		return err
	}
	
	return batch.Commit(pebble.Sync)
}

// ============================================
// Utilities
// ============================================

// Compact принудительно сжимает базу
func (s *SnapshotStorage) Compact() error {
	//v2 not support without context
	start := []byte(prefixSnapshotTree)
	end := []byte(prefixSnapshotTree + "\xff")
	return s.db.Compact(context.Background(), start, end, true)
	
	//return true
}

// Flush сбрасывает memtable на диск
func (s *SnapshotStorage) Flush() error {
	return s.db.Flush()
}

// GetStats возвращает статистику
func (s *SnapshotStorage) GetStats() StorageStats {
	metrics := s.db.Metrics()
	
	hits := metrics.BlockCache.Hits
	misses := metrics.BlockCache.Misses
	hitRate := 0.0
	if hits+misses > 0 {
		hitRate = float64(hits) / float64(hits+misses) * 100
	}
	
	return StorageStats{
		WrittenBytes:    s.writtenBytes.Load(),
		ReadBytes:       s.readBytes.Load(),
		WriteCount:      s.writeCount.Load(),
		ReadCount:       s.readCount.Load(),
		CacheHitRate:    hitRate,
		CompactionCount: metrics.Compact.Count,
		MemtableSize:    metrics.MemTable.Size,
		WALSize:         metrics.WAL.Size,
	}
}

type StorageStats struct {
	WrittenBytes    uint64
	ReadBytes       uint64
	WriteCount      uint64
	ReadCount       uint64
	CacheHitRate    float64
	CompactionCount int64
	MemtableSize    uint64
	WALSize         uint64
}

// ============================================
// Internal helpers
// ============================================

func makeSnapshotMetaKey(version [32]byte) []byte {
	key := make([]byte, len(prefixSnapshotMeta)+32)
	copy(key, prefixSnapshotMeta)
	copy(key[len(prefixSnapshotMeta):], version[:])
	return key
}

func makeSnapshotTreeKey(version [32]byte, treeName string) []byte {
	return []byte(fmt.Sprintf("%s%x:%s", prefixSnapshotTree, version, treeName))
}

func encodeSnapshotMeta(version [32]byte, timestamp int64, treeCount int) []byte {
	buf := make([]byte, 32+8+4)
	copy(buf[0:32], version[:])
	binary.BigEndian.PutUint64(buf[32:40], uint64(timestamp))
	binary.BigEndian.PutUint32(buf[40:44], uint32(treeCount))
	return buf
}

func decodeSnapshotMeta(data []byte) ([32]byte, int64, int) {
	var version [32]byte
	copy(version[:], data[0:32])
	timestamp := int64(binary.BigEndian.Uint64(data[32:40]))
	treeCount := int(binary.BigEndian.Uint32(data[40:44]))
	return version, timestamp, treeCount
}

func (s *SnapshotStorage) listSnapshotTrees(version [32]byte) ([]string, error) {
	prefix := fmt.Sprintf("%s%x:", prefixSnapshotTree, version)
	
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	
	var names []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		treeName := key[len(prefix):]
		names = append(names, treeName)
	}
	
	return names, iter.Error()
}

func (s *SnapshotStorage) getLastVersion() (*[32]byte, error) {
	data, closer, err := s.db.Get([]byte(prefixGlobalMeta + "last"))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	
	var version [32]byte
	copy(version[:], data)
	return &version, nil
}

func (s *SnapshotStorage) updateGlobalMeta(batch *pebble.Batch, version [32]byte) error {
	// Check if first
	_, closer, err := s.db.Get([]byte(prefixGlobalMeta + "first"))
	isFirst := err == pebble.ErrNotFound
	if closer != nil {
		closer.Close()
	}
	
	if isFirst {
		batch.Set([]byte(prefixGlobalMeta+"first"), version[:], pebble.NoSync)
	}
	
	// Always update last
	batch.Set([]byte(prefixGlobalMeta+"last"), version[:], pebble.NoSync)
	
	// Increment count
	return s.incrementCount(batch)
}

func (s *SnapshotStorage) incrementCount(batch *pebble.Batch) error {
	count := uint32(0)
	
	data, closer, err := s.db.Get([]byte(prefixGlobalMeta + "count"))
	if err == nil {
		count = binary.BigEndian.Uint32(data)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return err
	}
	
	count++
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, count)
	
	return batch.Set([]byte(prefixGlobalMeta+"count"), buf, pebble.NoSync)
}

func (s *SnapshotStorage) decrementCount(batch *pebble.Batch) error {
	count := uint32(0)
	
	data, closer, err := s.db.Get([]byte(prefixGlobalMeta + "count"))
	if err == nil {
		count = binary.BigEndian.Uint32(data)
		closer.Close()
	} else if err != pebble.ErrNotFound {
		return err
	}
	
	if count > 0 {
		count--
	}
	
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, count)
	
	return batch.Set([]byte(prefixGlobalMeta+"count"), buf, pebble.NoSync)
}
