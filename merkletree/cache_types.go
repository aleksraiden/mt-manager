package merkletree

// SmallConfig для небольших деревьев (<100K элементов)
func SmallConfig() *Config {
	return &Config{
		MaxDepth:    3,
		CacheSize:   10_000, // 10K
		CacheShards: 6,      // 64 шарда
	}
}

// MediumConfig для средних деревьев (100K-1M элементов)
func MediumConfig() *Config {
	return &Config{
		MaxDepth:    3,
		CacheSize:   100_000, // 100K
		CacheShards: 8,       // 256 шардов
	}
}

// LargeConfig для больших деревьев (1M-10M элементов)
func LargeConfig() *Config {
	return &Config{
		MaxDepth:    4,
		CacheSize:   1_000_000, // 1M
		CacheShards: 10,        // 1024 шарда
	}
}

// HugeConfig для огромных деревьев (>10M элементов)
func HugeConfig() *Config {
	return &Config{
		MaxDepth:    5,
		CacheSize:   10_000_000, // 10M
		CacheShards: 12,         // 4096 шардов
	}
}

// NoCacheConfig без кеша (для экономии памяти)
func NoCacheConfig() *Config {
	return &Config{
		MaxDepth:    3,
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
