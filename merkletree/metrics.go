package merkletree

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Metrics собирает метрики производительности
type Metrics struct {
	InsertCount      atomic.Uint64
	GetCount         atomic.Uint64
	CacheHits        atomic.Uint64
	CacheMisses      atomic.Uint64
	ComputeRootCount atomic.Uint64
	TotalInsertTime  atomic.Uint64 // наносекунды
	TotalGetTime     atomic.Uint64
	TotalComputeTime atomic.Uint64
	startTime        time.Time
}

// NewMetrics создает новые метрики
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// RecordInsert записывает вставку
func (m *Metrics) RecordInsert(duration time.Duration) {
	m.InsertCount.Add(1)
	m.TotalInsertTime.Add(uint64(duration.Nanoseconds()))
}

// RecordGet записывает чтение
func (m *Metrics) RecordGet(duration time.Duration, cacheHit bool) {
	m.GetCount.Add(1)
	m.TotalGetTime.Add(uint64(duration.Nanoseconds()))

	if cacheHit {
		m.CacheHits.Add(1)
	} else {
		m.CacheMisses.Add(1)
	}
}

// RecordComputeRoot записывает вычисление корня
func (m *Metrics) RecordComputeRoot(duration time.Duration) {
	m.ComputeRootCount.Add(1)
	m.TotalComputeTime.Add(uint64(duration.Nanoseconds()))
}

// GetStats возвращает статистику
func (m *Metrics) GetStats() MetricsStats {
	inserts := m.InsertCount.Load()
	gets := m.GetCount.Load()
	hits := m.CacheHits.Load()
	misses := m.CacheMisses.Load()
	computes := m.ComputeRootCount.Load()

	var avgInsert, avgGet, avgCompute time.Duration
	var cacheHitRate float64

	if inserts > 0 {
		avgInsert = time.Duration(m.TotalInsertTime.Load() / inserts)
	}

	if gets > 0 {
		avgGet = time.Duration(m.TotalGetTime.Load() / gets)
	}

	if computes > 0 {
		avgCompute = time.Duration(m.TotalComputeTime.Load() / computes)
	}

	if hits+misses > 0 {
		cacheHitRate = float64(hits) / float64(hits+misses) * 100
	}

	return MetricsStats{
		Inserts:        inserts,
		Gets:           gets,
		CacheHits:      hits,
		CacheMisses:    misses,
		CacheHitRate:   cacheHitRate,
		ComputeRoots:   computes,
		AvgInsertTime:  avgInsert,
		AvgGetTime:     avgGet,
		AvgComputeTime: avgCompute,
		Uptime:         time.Since(m.startTime),
	}
}

// MetricsStats статистика метрик
type MetricsStats struct {
	Inserts        uint64
	Gets           uint64
	CacheHits      uint64
	CacheMisses    uint64
	CacheHitRate   float64
	ComputeRoots   uint64
	AvgInsertTime  time.Duration
	AvgGetTime     time.Duration
	AvgComputeTime time.Duration
	Uptime         time.Duration
}

// String форматированный вывод
func (s MetricsStats) String() string {
	return fmt.Sprintf(`Метрики производительности:
  Операции:
    Вставок: %d (avg: %v)
    Чтений: %d (avg: %v)
    Вычислений корня: %d (avg: %v)
  Кеш:
    Попаданий: %d
    Промахов: %d
    Hit Rate: %.2f%%
  Uptime: %v`,
		s.Inserts, s.AvgInsertTime,
		s.Gets, s.AvgGetTime,
		s.ComputeRoots, s.AvgComputeTime,
		s.CacheHits,
		s.CacheMisses,
		s.CacheHitRate,
		s.Uptime)
}
