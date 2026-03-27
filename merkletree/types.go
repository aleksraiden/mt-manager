package merkletree

import (
	"encoding/binary"
	"fmt"
)

// Hashable - интерфейс для любых объектов, которые можно хранить в дереве
// Любая структура должна уметь возвращать свой хеш и ключ для индексации
type Hashable interface {
	// Hash возвращает криптографический хеш объекта
	Hash() [32]byte

	// Key возвращает ключ для индексации в дереве
	Key() [8]byte

	// ID возвращает уникальный идентификатор объекта
	ID() uint64
}

// Serializable - опциональный интерфейс для элементов дерева.
// Если T реализует Serializable, снапшот будет использовать
// эти методы вместо msgpack.
// Если T НЕ реализует Serializable, сохранение снапшота вернёт ошибку.
type Serializable interface {
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

type KeyOrder uint8

const (
	KeyOrderMSB KeyOrder = iota // BigEndian, default — range queries работают
	KeyOrderLSB                 // LittleEndian — быстрее для numeric keys, без range
)

// Хелперы для использования в Key() реализации
func KeyMSB(v uint64) [8]byte {
	return EncodeKey(v) // уже существует, просто алиас
}

func KeyLSB(v uint64) [8]byte {
	return [8]byte{
		byte(v),
		byte(v >> 8),
		byte(v >> 16),
		byte(v >> 24),
		byte(v >> 32),
		byte(v >> 40),
		byte(v >> 48),
		byte(v >> 56),
	}
}

type SnapshotKind uint8

const (
	KindCheckpoint  SnapshotKind = 1 // полный срез
	KindIncremental SnapshotKind = 2 // только дельта
)

// SnapshotHeader — метаданные любого снапшота
type SnapshotHeader struct {
	Kind          SnapshotKind
	Version       [32]byte // хеш этого снапшота (GlobalRoot)
	ParentVersion [32]byte // хеш предыдущего снапшота/чекпоинта
	CheckpointRef [32]byte // хеш ближайшего чекпоинта (для быстрого поиска)
	Timestamp     int64
	SchemaVersion int
	TreeCount     int
}

// IncrementalTreeSnapshot — дельта одного дерева
type IncrementalTreeSnapshot struct {
	TreeID   string
	RootHash [32]byte
	// Изменённые/добавленные элементы
	UpsertItems [][]byte
	// Удалённые ключи (8 bytes each)
	DeletedKeys [][]byte
}

// Config содержит параметры конфигурации дерева
type Config struct {
	MaxDepth    int  // Максимальная глубина дерева
	CacheSize   int  // Размер кеша
	CacheShards uint // Количество шардов для кеша (2^n)

	KeyEncoding KeyOrder

	TopN       int // Для хранения топ-левел кеша
	UseTopNMax bool
	UseTopNMin bool

	// Включить отслеживание изменений для инкрементальных снапшотов.
	// Если false — любой вызов CreateSnapshot() автоматически создаёт чекпоинт.
	TrackDirty bool

	ExcludeState bool // если true — дерево не входит в GlobalRoot и снапшоты
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		MaxDepth:     8,
		CacheSize:    65_536,
		CacheShards:  8,
		TopN:         0,
		UseTopNMax:   false,
		UseTopNMin:   false,
		TrackDirty:   false, // по умолчанию только чекпоинты
		ExcludeState: false,
	}
}

// SmallConfig для небольших деревьев (<100K элементов)
func SmallConfig() *Config {
	return &Config{
		MaxDepth:     8,
		CacheSize:    16_384, // 16K
		CacheShards:  6,      // 64 шарда
		TrackDirty:   false,  // по умолчанию только чекпоинты
		ExcludeState: false,
	}
}

// MediumConfig для средних деревьев (100K-1M элементов)
func MediumConfig() *Config {
	return &Config{
		MaxDepth:     8,
		CacheSize:    131_072, // 128K
		CacheShards:  8,       // 256 шардов
		TrackDirty:   false,   // по умолчанию только чекпоинты
		ExcludeState: false,
	}
}

// LargeConfig для больших деревьев (1M-10M элементов)
func LargeConfig() *Config {
	return &Config{
		MaxDepth:     8,
		CacheSize:    1_024_000, // 1M
		CacheShards:  10,        // 1024 шарда
		TrackDirty:   false,     // по умолчанию только чекпоинты
		ExcludeState: false,
	}
}

// HugeConfig для огромных деревьев (>10M элементов)
func HugeConfig() *Config {
	return &Config{
		MaxDepth:     8,
		CacheSize:    1_024_000, // 1M
		CacheShards:  12,        // 4096 шардов
		TrackDirty:   false,     // по умолчанию только чекпоинты
		ExcludeState: false,
	}
}

// NoCacheConfig без кеша (для экономии памяти)
func NoCacheConfig() *Config {
	return &Config{
		MaxDepth:     8,
		CacheSize:    0, // Без кеша
		CacheShards:  0,
		TrackDirty:   false, // по умолчанию только чекпоинты
		ExcludeState: false,
	}
}

// CustomConfig создает конфигурацию с указанными параметрами
func CustomConfig(maxDepth int, cacheSize int, cacheShards uint) *Config {
	return &Config{
		MaxDepth:     maxDepth,
		CacheSize:    cacheSize,
		CacheShards:  cacheShards,
		TrackDirty:   false, // по умолчанию только чекпоинты
		ExcludeState: false,
	}
}

// EncodeKey кодирует uint64 в [8]byte BigEndian
func EncodeKey(id uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], id)
	return key
}

// CollisionError возникает когда два разных элемента
// претендуют на один лист дерева
type CollisionError struct {
	Slot       byte   // байт-индекс слота (key[depth])
	Depth      int    // глубина коллизии
	ExistingID uint64 // кто уже занял слот
	NewID      uint64 // кто пытается вставиться
}

func (e *CollisionError) Error() string {
	return fmt.Sprintf(
		"tree collision: depth=%d slot=0x%02X existing_id=%d new_id=%d",
		e.Depth, e.Slot, e.ExistingID, e.NewID,
	)
}
