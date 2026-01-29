package merkletree

// ShardedCache представляет шардированный LRU кеш
type ShardedCache[T Hashable] struct {
	shards    []*lruCache[T]
	shardMask uint64
}

// newShardedCache создает новый шардированный кеш
func newShardedCache[T Hashable](totalCapacity int, power uint) *ShardedCache[T] {
	numShards := 1 << power
	shardCapacity := totalCapacity / numShards
	if shardCapacity < 1 {
		shardCapacity = 1
	}

	shards := make([]*lruCache[T], numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = newLRUCache[T](shardCapacity)
	}

	return &ShardedCache[T]{
		shards:    shards,
		shardMask: uint64(numShards - 1),
	}
}

// get возвращает значение из соответствующего шарда
func (s *ShardedCache[T]) get(key uint64) (T, bool) {
	return s.shards[key&s.shardMask].get(key)
}

func (sc *ShardedCache[T]) tryGet(id uint64) (T, bool) {
    shard := sc.shards[id%uint64(len(sc.shards))]
    return shard.tryGet(id)
}

// put добавляет значение в соответствующий шард
func (s *ShardedCache[T]) put(key uint64, value T) {
	s.shards[key&s.shardMask].put(key, value)
}

// size возвращает общий размер всех шардов
func (s *ShardedCache[T]) size() int {
	total := 0
	for _, shard := range s.shards {
		total += shard.size()
	}
	return total
}

// Resize для ShardedCache
func (s *ShardedCache[T]) Resize(newTotalCapacity int) {
	newShardCapacity := newTotalCapacity / len(s.shards)
	if newShardCapacity < 1 {
		newShardCapacity = 1
	}

	for _, shard := range s.shards {
		shard.Resize(newShardCapacity)
	}
}

// delete удаляет элемент из кеша
func (s *ShardedCache[T]) delete(id uint64) {
	s.getShard(id).delete(id)
}

// clear очищает все шарды кеша
func (s *ShardedCache[T]) clear() {
	for _, shard := range s.shards {
		shard.clear()
	}
}

// getShard возвращает шард по ключу
func (s *ShardedCache[T]) getShard(key uint64) *lruCache[T] {
	return s.shards[key&s.shardMask]
}
