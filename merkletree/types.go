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

// Config содержит параметры конфигурации дерева
type Config struct {
	MaxDepth    int  // Максимальная глубина дерева
	CacheSize   int  // Размер кеша
	CacheShards uint // Количество шардов для кеша (2^n)
	TopN        int // Для хранения топ-левел кеша
	UseAppLookup	bool //специализированный лукап
	UseUidLookup	bool 
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		MaxDepth:    3,
		CacheSize:   100000,
		CacheShards: 8,
		TopN:        0,
		UseAppLookup:	false,
		UseUidLookup:	false,
	}
}

// EncodeKey кодирует uint64 в [8]byte BigEndian
func EncodeKey(id uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], id)
	return key
}
