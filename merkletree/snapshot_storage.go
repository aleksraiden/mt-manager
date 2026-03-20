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

/**
const (
	// Префиксы ключей
	prefixSnapshotMeta = "snap:meta:"  // snap:meta:{version} → metadata
	prefixSnapshotTree = "snap:tree:"  // snap:tree:{version}:{tree_name} → tree data
	prefixGlobalMeta   = "global:"     // global:last, global:first, global:count
) **/
const (
    // Существующие
    prefixSnapshotMeta = "snap:meta:"
    prefixSnapshotTree = "snap:tree:"
    prefixGlobalMeta   = "global:"

    // Новые
    prefixCheckpointMeta = "cp:meta:"   // cp:meta:{version} → header чекпоинта
    prefixCheckpointTree = "cp:tree:"   // cp:tree:{version}:{tree_name} → полные данные
    prefixIncrMeta       = "inc:meta:"  // inc:meta:{version} → header инкрементального
    prefixIncrTree       = "inc:tree:"  // inc:tree:{version}:{tree_name} → дельта
    prefixChainIndex     = "chain:"     // chain:{cpVersion} → список инкрементальных версий
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

/**
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
} **/
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

    // Count = количество cp:meta: + inc:meta: записей
    count := 0
    for _, prefix := range []string{prefixCheckpointMeta, prefixIncrMeta} {
        iter, err := s.db.NewIter(&pebble.IterOptions{
            LowerBound: []byte(prefix),
            UpperBound: []byte(prefix + "\xff"),
        })
        if err != nil {
            return nil, err
        }
        for iter.First(); iter.Valid(); iter.Next() {
            count++
        }
        if err := iter.Error(); err != nil {
            iter.Close()
            return nil, err
        }
        iter.Close()
    }
    metadata.Count = count

    // TotalSize — сумма по обоим tree-префиксам
    for _, prefix := range []string{prefixCheckpointTree, prefixIncrTree} {
        iter, err := s.db.NewIter(&pebble.IterOptions{
            LowerBound: []byte(prefix),
            UpperBound: []byte(prefix + "\xff"),
        })
        if err != nil {
            return nil, err
        }
        for iter.First(); iter.Valid(); iter.Next() {
            metadata.TotalSize += int64(len(iter.Value()))
        }
        if err := iter.Error(); err != nil {
            iter.Close()
            return nil, err
        }
        iter.Close()
    }

    return metadata, nil
}

// ListVersions возвращает список всех версий
// ListVersions возвращает список всех версий (чекпоинты + инкрементальные)
func (s *SnapshotStorage) ListVersions() ([][32]byte, error) {
    var versions [][32]byte

    // Вспомогательная функция для обхода одного префикса
    scanPrefix := func(prefix string) error {
        iter, err := s.db.NewIter(&pebble.IterOptions{
            LowerBound: []byte(prefix),
            UpperBound: []byte(prefix + "\xff"),
        })
        if err != nil {
            return err
        }
        defer iter.Close()

        prefixLen := len(prefix)
        for iter.First(); iter.Valid(); iter.Next() {
            key := iter.Key()
            if len(key) < prefixLen+32 {
                continue
            }
            var version [32]byte
            copy(version[:], key[prefixLen:prefixLen+32])
            versions = append(versions, version)
        }
        return iter.Error()
    }

    if err := scanPrefix(prefixCheckpointMeta); err != nil {
        return nil, err
    }
    if err := scanPrefix(prefixIncrMeta); err != nil {
        return nil, err
    }
    return versions, nil
}

/**
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
}**/
func (s *SnapshotStorage) DeleteSnapshot(version [32]byte) error {
    // Определяем тип снапшота по наличию заголовка
    header, err := s.LoadHeader(&version)
    if err != nil {
        return fmt.Errorf("cannot determine snapshot type: %w", err)
    }

    batch := s.db.NewBatch()
    defer batch.Close()

    switch header.Kind {
    case KindCheckpoint:
        // Удаляем заголовок
        batch.Delete(makeCheckpointMetaKey(version), pebble.NoSync)

        // Удаляем данные деревьев
        treeNames, err := s.listCheckpointTrees(version)
        if err != nil {
            return err
        }
        for _, name := range treeNames {
            batch.Delete(makeCheckpointTreeKey(version, name), pebble.NoSync)
        }

        // Удаляем chain index этого чекпоинта
        batch.Delete(makeChainIndexKey(version), pebble.NoSync)

    case KindIncremental:
        // Удаляем заголовок
        batch.Delete(makeIncrMetaKey(version), pebble.NoSync)

        // Удаляем дельты деревьев
        treeNames, err := s.listIncrementalTrees(version)
        if err != nil {
            return err
        }
        for _, name := range treeNames {
            batch.Delete(makeIncrTreeKey(version, name), pebble.NoSync)
        }

        // Удаляем запись из chain index родительского чекпоинта
        if err := s.removeFromChainIndex(batch, header.CheckpointRef, version); err != nil {
            return err
        }

    default:
        return fmt.Errorf("unknown snapshot kind: %d", header.Kind)
    }

    if err := s.decrementCount(batch); err != nil {
        return err
    }

    return batch.Commit(pebble.Sync)
}

// removeFromChainIndex удаляет конкретную версию из chain index чекпоинта
func (s *SnapshotStorage) removeFromChainIndex(batch *pebble.Batch, cpVersion [32]byte, removeVersion [32]byte) error {
    chainKey := makeChainIndexKey(cpVersion)

    data, closer, err := s.db.Get(chainKey)
    if err == pebble.ErrNotFound {
        return nil // индекса нет — ничего делать не нужно
    }
    if err != nil {
        return err
    }
    entries, err := decodeChainIndex(data)
    closer.Close()
    if err != nil {
        return err
    }

    // Фильтруем удаляемую версию
    filtered := entries[:0]
    for _, e := range entries {
        if e.Version != removeVersion {
            filtered = append(filtered, e)
        }
    }

    return batch.Set(chainKey, encodeChainIndex(filtered), pebble.NoSync)
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


// SaveCheckpoint сохраняет полный снапшот (чекпоинт)
// Аналог существующего SaveSnapshot, но с заголовком KindCheckpoint
func (s *SnapshotStorage) SaveCheckpoint(version [32]byte, parentVersion [32]byte, timestamp int64, trees map[string][]byte) error {
    batch := s.db.NewBatch()
    defer batch.Close()

    // 1. Заголовок чекпоинта
    header := SnapshotHeader{
        Kind:          KindCheckpoint,
        Version:       version,
        ParentVersion: parentVersion,
        CheckpointRef: version, // чекпоинт сам себе является ref
        Timestamp:     timestamp,
        SchemaVersion: CurrentSchemaVersion,
    }
    cpMetaKey := makeCheckpointMetaKey(version)
    if err := batch.Set(cpMetaKey, encodeHeader(header), pebble.NoSync); err != nil {
        return fmt.Errorf("set checkpoint header: %w", err)
    }

    // 2. Данные деревьев
    totalSize := uint64(0)
    for treeName, treeData := range trees {
        key := makeCheckpointTreeKey(version, treeName)
        if err := batch.Set(key, treeData, pebble.NoSync); err != nil {
            return fmt.Errorf("set checkpoint tree %s: %w", treeName, err)
        }
        totalSize += uint64(len(treeData))
    }

    // 3. Инициализируем пустой chain index для этого чекпоинта
    chainKey := makeChainIndexKey(version)
    if err := batch.Set(chainKey, encodeChainIndex(nil), pebble.NoSync); err != nil {
        return fmt.Errorf("init chain index: %w", err)
    }

    // 4. Глобальные метаданные (переиспользуем существующий updateGlobalMeta)
    if err := s.updateGlobalMeta(batch, version); err != nil {
        return fmt.Errorf("update global meta: %w", err)
    }

    if err := batch.Commit(pebble.NoSync); err != nil {
        return fmt.Errorf("commit checkpoint: %w", err)
    }

    s.writtenBytes.Add(totalSize)
    s.writeCount.Add(1)
    return nil
}

// SaveIncremental сохраняет инкрементальный снапшот (только дельта)
func (s *SnapshotStorage) SaveIncremental(
    version [32]byte,
    parentVersion [32]byte,
    checkpointRef [32]byte,
    timestamp int64,
    deltas map[string]*IncrementalTreeSnapshot,
) error {
    batch := s.db.NewBatch()
    defer batch.Close()

    // 1. Заголовок
    header := SnapshotHeader{
        Kind:          KindIncremental,
        Version:       version,
        ParentVersion: parentVersion,
        CheckpointRef: checkpointRef,
        Timestamp:     timestamp,
        SchemaVersion: CurrentSchemaVersion,
    }
    incMetaKey := makeIncrMetaKey(version)
    if err := batch.Set(incMetaKey, encodeHeader(header), pebble.NoSync); err != nil {
        return fmt.Errorf("set incremental header: %w", err)
    }

    // 2. Дельты деревьев
    totalSize := uint64(0)
    for treeName, delta := range deltas {
        data, err := encodeIncrementalTree(delta)
        if err != nil {
            return fmt.Errorf("encode delta for tree %s: %w", treeName, err)
        }
        key := makeIncrTreeKey(version, treeName)
        if err := batch.Set(key, data, pebble.NoSync); err != nil {
            return fmt.Errorf("set incremental tree %s: %w", treeName, err)
        }
        totalSize += uint64(len(data))
    }

    // 3. Обновляем chain index чекпоинта — добавляем эту версию в цепочку
    if err := s.appendToChainIndex(batch, checkpointRef, version, timestamp); err != nil {
        return fmt.Errorf("update chain index: %w", err)
    }

    // 4. Глобальные метаданные
    if err := s.updateGlobalMeta(batch, version); err != nil {
        return fmt.Errorf("update global meta: %w", err)
    }

    if err := batch.Commit(pebble.NoSync); err != nil {
        return fmt.Errorf("commit incremental: %w", err)
    }

    s.writtenBytes.Add(totalSize)
    s.writeCount.Add(1)
    return nil
}

// LoadHeader читает только заголовок снапшота — дёшево, без данных деревьев
func (s *SnapshotStorage) LoadHeader(version *[32]byte) (SnapshotHeader, error) {
    targetVersion := version
    if targetVersion == nil {
        v, err := s.getLastVersion()
        if err != nil {
            return SnapshotHeader{}, fmt.Errorf("no snapshots: %w", err)
        }
        targetVersion = v
    }

    // Пробуем сначала как чекпоинт
    data, closer, err := s.db.Get(makeCheckpointMetaKey(*targetVersion))
    if err == nil {
        defer closer.Close()
        return decodeHeader(data)
    }

    // Потом как инкрементальный
    data, closer, err = s.db.Get(makeIncrMetaKey(*targetVersion))
    if err == nil {
        defer closer.Close()
        return decodeHeader(data)
    }

    return SnapshotHeader{}, fmt.Errorf("snapshot %x not found", (*targetVersion)[:4])
}

// LoadCheckpoint загружает полный чекпоинт — используется как база для восстановления
func (s *SnapshotStorage) LoadCheckpoint(version *[32]byte) (*Snapshot, error) {
    targetVersion := version
    if targetVersion == nil {
        v, err := s.getLastVersion()
        if err != nil {
            return nil, err
        }
        targetVersion = v
    }

    // Читаем заголовок
    metaData, closer, err := s.db.Get(makeCheckpointMetaKey(*targetVersion))
    if err != nil {
        return nil, fmt.Errorf("checkpoint %x not found: %w", (*targetVersion)[:4], err)
    }
    header, err := decodeHeader(metaData)
    closer.Close()
    if err != nil {
        return nil, err
    }

    // Читаем деревья параллельно (переиспользуем логику из LoadSnapshot)
    treeNames, err := s.listCheckpointTrees(*targetVersion)
    if err != nil {
        return nil, err
    }

    trees := make(map[string]*TreeSnapshot, len(treeNames))
    var mu sync.Mutex
    g, ctx := errgroup.WithContext(context.Background())

    for _, name := range treeNames {
        name := name
        g.Go(func() error {
            select {
            case <-ctx.Done():
                return ctx.Err()
            default:
            }
            treeData, closer, err := s.db.Get(makeCheckpointTreeKey(*targetVersion, name))
            if err != nil {
                return fmt.Errorf("read checkpoint tree %s: %w", name, err)
            }
            dataCopy := make([]byte, len(treeData))
            copy(dataCopy, treeData)
            closer.Close()

            mu.Lock()
            trees[name] = &TreeSnapshot{TreeID: name, Items: [][]byte{dataCopy}}
            mu.Unlock()
            s.readBytes.Add(uint64(len(dataCopy)))
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        return nil, err
    }

    s.readCount.Add(1)
    return &Snapshot{
        SchemaVersion: header.SchemaVersion,
        Version:       header.Version,
        Timestamp:     header.Timestamp,
        TreeCount:     len(trees),
        Trees:         trees,
    }, nil
}

// ChainEntry одна запись в цепочке инкрементальных снапшотов
type ChainEntry struct {
    Version   [32]byte
    Timestamp int64
}

// BuildChain возвращает отсортированный список инкрементальных снапшотов
// между чекпоинтом (не включительно) и targetVersion (включительно)
func (s *SnapshotStorage) BuildChain(checkpointRef [32]byte, targetVersion [32]byte) ([]ChainEntry, error) {
    // Читаем chain index — список всех инкрементальных снапшотов после этого CP
    chainKey := makeChainIndexKey(checkpointRef)
    data, closer, err := s.db.Get(chainKey)
    if err != nil {
        if err == pebble.ErrNotFound {
            return nil, fmt.Errorf("chain index not found for checkpoint %x", checkpointRef[:4])
        }
        return nil, err
    }
    allEntries, err := decodeChainIndex(data)
    closer.Close()
    if err != nil {
        return nil, err
    }

    // Фильтруем: берём только те, что ≤ targetVersion по timestamp
    // Chain index уже отсортирован по времени добавления
    var chain []ChainEntry
    for _, entry := range allEntries {
        chain = append(chain, entry)
        if entry.Version == targetVersion {
            break // дошли до нужной точки
        }
    }

    if len(chain) == 0 || chain[len(chain)-1].Version != targetVersion {
        return nil, fmt.Errorf("target version %x not found in chain of checkpoint %x",
            targetVersion[:4], checkpointRef[:4])
    }

    return chain, nil
}

// LoadIncrementalDelta загружает дельту одного инкрементального снапшота
func (s *SnapshotStorage) LoadIncrementalDelta(version [32]byte) (map[string]*IncrementalTreeSnapshot, error) {
    treeNames, err := s.listIncrementalTrees(version)
    if err != nil {
        return nil, err
    }

    result := make(map[string]*IncrementalTreeSnapshot, len(treeNames))
    var mu sync.Mutex
    g, ctx := errgroup.WithContext(context.Background())

    for _, name := range treeNames {
        name := name
        g.Go(func() error {
            select {
            case <-ctx.Done():
                return ctx.Err()
            default:
            }
            data, closer, err := s.db.Get(makeIncrTreeKey(version, name))
            if err != nil {
                return fmt.Errorf("read delta tree %s: %w", name, err)
            }
            dataCopy := make([]byte, len(data))
            copy(dataCopy, data)
            closer.Close()

            delta, err := decodeIncrementalTree(dataCopy)
            if err != nil {
                return fmt.Errorf("decode delta tree %s: %w", name, err)
            }

            mu.Lock()
            result[name] = delta
            mu.Unlock()
            return nil
        })
    }

    return result, g.Wait()
}

func makeCheckpointMetaKey(v [32]byte) []byte {
    key := make([]byte, len(prefixCheckpointMeta)+32)
    copy(key, prefixCheckpointMeta)
    copy(key[len(prefixCheckpointMeta):], v[:])
    return key
}

func makeCheckpointTreeKey(v [32]byte, treeName string) []byte {
    return []byte(fmt.Sprintf("%s%x:%s", prefixCheckpointTree, v, treeName))
}

func makeIncrMetaKey(v [32]byte) []byte {
    key := make([]byte, len(prefixIncrMeta)+32)
    copy(key, prefixIncrMeta)
    copy(key[len(prefixIncrMeta):], v[:])
    return key
}

func makeIncrTreeKey(v [32]byte, treeName string) []byte {
    return []byte(fmt.Sprintf("%s%x:%s", prefixIncrTree, v, treeName))
}

func makeChainIndexKey(cpVersion [32]byte) []byte {
    return []byte(fmt.Sprintf("%s%x", prefixChainIndex, cpVersion))
}

func (s *SnapshotStorage) listCheckpointTrees(v [32]byte) ([]string, error) {
    return s.listTreesWithPrefix(fmt.Sprintf("%s%x:", prefixCheckpointTree, v))
}

func (s *SnapshotStorage) listIncrementalTrees(v [32]byte) ([]string, error) {
    return s.listTreesWithPrefix(fmt.Sprintf("%s%x:", prefixIncrTree, v))
}

// listTreesWithPrefix — общий итератор по ключам с префиксом
func (s *SnapshotStorage) listTreesWithPrefix(prefix string) ([]string, error) {
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
        names = append(names, key[len(prefix):])
    }
    return names, iter.Error()
}

// Chain index: [count uint32]([version 32][timestamp 8]...)
func encodeChainIndex(entries []ChainEntry) []byte {
    buf := make([]byte, 4+len(entries)*40)
    binary.BigEndian.PutUint32(buf[0:4], uint32(len(entries)))
    for i, e := range entries {
        off := 4 + i*40
        copy(buf[off:off+32], e.Version[:])
        binary.BigEndian.PutUint64(buf[off+32:off+40], uint64(e.Timestamp))
    }
    return buf
}

func decodeChainIndex(data []byte) ([]ChainEntry, error) {
    if len(data) < 4 {
        return nil, fmt.Errorf("chain index too short")
    }
    count := int(binary.BigEndian.Uint32(data[0:4]))
    entries := make([]ChainEntry, count)
    for i := 0; i < count; i++ {
        off := 4 + i*40
        if off+40 > len(data) {
            return nil, fmt.Errorf("chain index truncated at entry %d", i)
        }
        copy(entries[i].Version[:], data[off:off+32])
        entries[i].Timestamp = int64(binary.BigEndian.Uint64(data[off+32 : off+40]))
    }
    return entries, nil
}

// appendToChainIndex читает текущий индекс, добавляет запись, записывает обратно
func (s *SnapshotStorage) appendToChainIndex(batch *pebble.Batch, cpVersion [32]byte, newVersion [32]byte, timestamp int64) error {
    chainKey := makeChainIndexKey(cpVersion)

    var existing []ChainEntry
    data, closer, err := s.db.Get(chainKey)
    if err == nil {
        existing, err = decodeChainIndex(data)
        closer.Close()
        if err != nil {
            return err
        }
    } else if err != pebble.ErrNotFound {
        return err
    }

    existing = append(existing, ChainEntry{Version: newVersion, Timestamp: timestamp})
    return batch.Set(chainKey, encodeChainIndex(existing), pebble.NoSync)
}

// Кодирование дельты дерева
// Layout: [upsertCount uint32]([len uint32][bytes]...) [deleteCount uint32]([key 8]...)
func encodeIncrementalTree(delta *IncrementalTreeSnapshot) ([]byte, error) {
    upsertBlob := encodeItemsBlob(delta.UpsertItems) // уже есть в snapshot.go
    deleteBlob := encodeItemsBlob(delta.DeletedKeys)
    result := make([]byte, len(upsertBlob)+len(deleteBlob))
    copy(result, upsertBlob)
    copy(result[len(upsertBlob):], deleteBlob)
    return result, nil
}

func decodeIncrementalTree(data []byte) (*IncrementalTreeSnapshot, error) {
    // Читаем upsert blob — он начинается с count uint32
    if len(data) < 4 {
        return nil, fmt.Errorf("incremental tree data too short")
    }
    upsertCount := int(binary.BigEndian.Uint32(data[0:4]))
    upsertSize := 4
    for i := 0; i < upsertCount; i++ {
        if upsertSize+4 > len(data) {
            return nil, fmt.Errorf("upsert blob truncated")
        }
        itemLen := int(binary.BigEndian.Uint32(data[upsertSize : upsertSize+4]))
        upsertSize += 4 + itemLen
    }

    upserted, err := decodeItemsBlob(data[:upsertSize])
    if err != nil {
        return nil, fmt.Errorf("decode upserts: %w", err)
    }

    deleted, err := decodeItemsBlob(data[upsertSize:])
    if err != nil {
        return nil, fmt.Errorf("decode deletes: %w", err)
    }

    return &IncrementalTreeSnapshot{
        UpsertItems: upserted,
        DeletedKeys: deleted,
    }, nil
}



// encodeHeader кодирует SnapshotHeader в байты
// Layout: [Kind 1][Version 32][ParentVersion 32][CheckpointRef 32][Timestamp 8][SchemaVersion 4] = 109 bytes
func encodeHeader(h SnapshotHeader) []byte {
    buf := make([]byte, 109)
    buf[0] = byte(h.Kind)
    copy(buf[1:33], h.Version[:])
    copy(buf[33:65], h.ParentVersion[:])
    copy(buf[65:97], h.CheckpointRef[:])
    binary.BigEndian.PutUint64(buf[97:105], uint64(h.Timestamp))
    binary.BigEndian.PutUint32(buf[105:109], uint32(h.SchemaVersion))
    return buf
}

func decodeHeader(data []byte) (SnapshotHeader, error) {
    if len(data) < 109 {
        return SnapshotHeader{}, fmt.Errorf("header too short: %d bytes", len(data))
    }
    var h SnapshotHeader
    h.Kind = SnapshotKind(data[0])
    copy(h.Version[:], data[1:33])
    copy(h.ParentVersion[:], data[33:65])
    copy(h.CheckpointRef[:], data[65:97])
    h.Timestamp = int64(binary.BigEndian.Uint64(data[97:105]))
    h.SchemaVersion = int(binary.BigEndian.Uint32(data[105:109]))
    return h, nil
}
