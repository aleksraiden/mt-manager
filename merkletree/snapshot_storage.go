// snapshot_storage.go
package merkletree

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"context"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"golang.org/x/sync/errgroup"
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
	// Block cache — самая важная настройка для read performance.
    // Правило: ~30% от RAM, доступной процессу.
    // Один объект Cache можно шарить между несколькими DB.
    blockCache := pebble.NewCache(512 << 20) // 512MB
    defer blockCache.Unref()                 // DB держит свой ref, этот освобождаем
	//без этого cache не освобождается при db.Close()
	
	opts := &pebble.Options{
		// Большой cache для горячих снапшотов
		Cache: blockCache, //pebble.NewCache(256 << 20), // 256MB
		
		// Большой write buffer для батчинга
		MemTableSize: 128 << 20, // 128MB
		
		// Сколько memtable может существовать одновременно до stop-writes.
        // 2 = пока один flush'ится, второй принимает записи.
        // При 3+ — больше буферизации, но больше RAM.
        MemTableStopWritesThreshold: 4,
		
		// Агрессивный compaction для меньшей фрагментации
		L0CompactionThreshold: 2,
		
        // Полная остановка записей (hard limit). Даём время compaction догнать.
        L0StopWritesThreshold: 36,
		
		// Максимальный размер L1. Каждый следующий уровень = LBase * 10.
        // L1=256MB → L2=2.5GB → L3=25GB → L4=250GB
        LBaseMaxBytes: 		256 << 20, // 256MB

		// Параллелизм compaction: (min, max) горутин
        CompactionConcurrencyRange: func() (int, int) { return 1, 4 },
					
		// Буферизуем WAL-записи, fsync каждые 1MB вместо каждой записи.
        // Снижает IOPS при batch-записях в 5–10x.
        WALBytesPerSync: 1 << 20, // 1MB

        // Аналогично для SSTable файлов.
        BytesPerSync: 4 << 20, // 4MB
				
		// Файловые дескрипторы
		MaxOpenFiles:       2000,
		FormatMajorVersion: pebble.FormatNewest,
		
		// Sync настройки (можно отключить для скорости)
		DisableWAL: false, // Рекомендуется false для надежности
	}
	
	// DBCompressionFastest = FastestCompression на всех уровнях
    //   (аналог SnappyCompression из v1, фактически LZ4/Snappy).
    // DBCompressionBalanced = Snappy на L0-L5, Zstd на L6.
    // DBCompressionGood     = Snappy на L0-L5, Zstd(лучше) на L6.
    opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
        return pebble.DBCompressionBalanced
    })
	
	// Устанавливаем одинаковые параметры для всех 7 уровней.
	for i := range opts.Levels {
        opts.Levels[i].BlockSize      = 32 << 10  // 32KB
        opts.Levels[i].IndexBlockSize = 256 << 10 // 256KB
		opts.Levels[i].FilterPolicy   = bloom.FilterPolicy(10)
        opts.Levels[i].FilterType    = pebble.TableFilter
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

    treeNames, err := s.listSnapshotTrees(*targetVersion)
    if err != nil {
        return nil, fmt.Errorf("failed to list trees: %w", err)
    }

    // trees — общая карта результатов.
    // Горутины пишут в разные ключи, но map не thread-safe,
    // поэтому защищаем мьютексом.
    trees := make(map[string]*TreeSnapshot, treeCount)
    var mu sync.Mutex

    // errgroup.WithContext создаёт группу + контекст.
    // Контекст отменяется автоматически, как только
    // ЛЮБАЯ горутина вернёт ненулевую ошибку.
    // Это ключевое отличие от ручного WaitGroup+errChan:
    // не нужно собирать ошибки вручную, первая ошибка
    // останавливает всю группу.
    g, ctx := errgroup.WithContext(context.Background())

    for _, name := range treeNames {
        // Важно: захватываем переменную в локальную копию
        // до передачи в горутину. Без этого все горутины
        // увидят последнее значение name из range.
        name := name

        g.Go(func() error {
            // Проверяем контекст в начале горутины.
            // Если другая горутина уже вернула ошибку,
            // ctx будет отменён и мы не начнём лишнюю работу.
            select {
            case <-ctx.Done():
                return ctx.Err()
            default:
            }

            treeKey := makeSnapshotTreeKey(*targetVersion, name)
            treeData, closer, err := s.db.Get(treeKey)
            if err != nil {
                // Возвращаем ошибку — errgroup сохранит её
                // и отменит ctx для остальных горутин.
                return fmt.Errorf("failed to read tree %s: %w", name, err)
            }

            // Копируем данные ДО закрытия closer,
            // потому что pebble освобождает буфер при Close.
            dataCopy := make([]byte, len(treeData))
            copy(dataCopy, treeData)
            closer.Close()

            treeSnapshot := &TreeSnapshot{
                TreeID: name,
                Items:  [][]byte{dataCopy},
            }

            // Единственное место конкуренции — запись в map.
            // Лок берётся только здесь, не на весь I/O.
            mu.Lock()
            trees[name] = treeSnapshot
            mu.Unlock()

            s.readBytes.Add(uint64(len(dataCopy)))
            return nil
        })
    }

    // g.Wait() ждёт ВСЕ горутины И возвращает
    // первую ненулевую ошибку (остальные отбрасываются).
    // Если все горутины вернули nil — возвращает nil.
    if err := g.Wait(); err != nil {
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
