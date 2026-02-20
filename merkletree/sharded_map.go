package merkletree

import (
	"sync"
)

const itemShardCount = 64  // степень двойки

type itemShard[T Hashable] struct {
    mu    sync.RWMutex
    items map[uint64]T
    // cache line padding: RWMutex=24b + map ptr=8b → нужно 32b до 64
    _     [32]byte
}

type ShardedItemMap[T Hashable] struct {
    shards [itemShardCount]itemShard[T]
}

func NewShardedItemMap[T Hashable]() *ShardedItemMap[T] {
    m := &ShardedItemMap[T]{}
    for i := range m.shards {
        m.shards[i].items = make(map[uint64]T)
    }
    return m
}

func (m *ShardedItemMap[T]) shard(id uint64) *itemShard[T] {
    return &m.shards[(id*0x9e3779b97f4a7c15)>>(64-6)]
}

func (m *ShardedItemMap[T]) Store(id uint64, item T) {
    s := m.shard(id)
    s.mu.Lock()
    s.items[id] = item
    s.mu.Unlock()
}

func (m *ShardedItemMap[T]) Load(id uint64) (T, bool) {
    s := m.shard(id)
    s.mu.RLock()
    item, ok := s.items[id]
    s.mu.RUnlock()
    return item, ok
}

func (m *ShardedItemMap[T]) Delete(id uint64) {
    s := m.shard(id)
    s.mu.Lock()
    delete(s.items, id)
    s.mu.Unlock()
}

func (m *ShardedItemMap[T]) Range(fn func(_ uint64, item T) bool) {
    for i := range m.shards {
        s := &m.shards[i]
        s.mu.RLock()
        for id, item := range s.items {
            s.mu.RUnlock()
            if !fn(id, item) {
                return
            }
            s.mu.RLock()
        }
        s.mu.RUnlock()
    }
}

func (m *ShardedItemMap[T]) Clear() {
    for i := range m.shards {
        s := &m.shards[i]
        s.mu.Lock()
        s.items = make(map[uint64]T)
        s.mu.Unlock()
    }
}

func (m *ShardedItemMap[T]) Len() int {
    total := 0
    for i := range m.shards {
        s := &m.shards[i]
        s.mu.RLock()
        total += len(s.items)
        s.mu.RUnlock()
    }
    return total
}