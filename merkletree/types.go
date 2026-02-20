package merkletree

import (
	"encoding/binary"
)

// Hashable - интерфейс для любых объектов, которые можно хранить в дереве
// Любая структура должна уметь возвращать свой хеш и ключ для индексации
type Hashable interface {
	// Hash возвращает криптографический хеш объекта
	Hash() [32]byte

	// Key возвращает ключ для индексации в дереве (8 байт BigEndian)
	Key() [8]byte

	// ID возвращает уникальный идентификатор объекта
	ID() uint64
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


// Config содержит параметры конфигурации дерева
type Config struct {
	MaxDepth    int  // Максимальная глубина дерева
	CacheSize   int  // Размер кеша
	CacheShards uint // Количество шардов для кеша (2^n)
	
	KeyEncoding KeyOrder
	
	TopN        int  // Для хранения топ-левел кеша
	UseTopNMax 	bool
	UseTopNMin 	bool
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		MaxDepth:    8,
		CacheSize:   65_536,
		CacheShards: 8,
		TopN:        0,
		UseTopNMax:	 false,
		UseTopNMin:	 false,
	}
}

// SmallConfig для небольших деревьев (<100K элементов)
func SmallConfig() *Config {
	return &Config{
		MaxDepth:    8,
		CacheSize:   16_384, 	// 16K
		CacheShards: 6,      	// 64 шарда
	}
}

// MediumConfig для средних деревьев (100K-1M элементов)
func MediumConfig() *Config {
	return &Config{
		MaxDepth:    8,
		CacheSize:   131_072, 	// 128K
		CacheShards: 8,       	// 256 шардов
	}
}

// LargeConfig для больших деревьев (1M-10M элементов)
func LargeConfig() *Config {
	return &Config{
		MaxDepth:    8,
		CacheSize:   1_024_000, 	// 1M
		CacheShards: 10,        // 1024 шарда
	}
}

// HugeConfig для огромных деревьев (>10M элементов)
func HugeConfig() *Config {
	return &Config{
		MaxDepth:    8,
		CacheSize:   1_024_000, 	 // 1M
		CacheShards: 12,         // 4096 шардов
	}
}

// NoCacheConfig без кеша (для экономии памяти)
func NoCacheConfig() *Config {
	return &Config{
		MaxDepth:    8,
		CacheSize:   0, // Без кеша
		CacheShards: 0,
	}
}

// CustomConfig создает конфигурацию с указанными параметрами
func CustomConfig(maxDepth int, cacheSize int, cacheShards uint) *Config {
	return &Config{
		MaxDepth:    maxDepth,
		CacheSize:   cacheSize,
		CacheShards: cacheShards,
	}
}

// EncodeKey кодирует uint64 в [8]byte BigEndian
func EncodeKey(id uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], id)
	return key
}
