package merkletree

import (
    "sync"
)

type lruCache[T Hashable] struct {
    cache    map[uint64]*lruNode[T]  // offset 0,  8 bytes
    head     *lruNode[T]             // offset 8,  8 bytes
    tail     *lruNode[T]             // offset 16, 8 bytes
    capacity int                     // offset 24, 8 bytes
    mu       sync.RWMutex            // offset 32, 24 bytes
    nodePool *sync.Pool              // offset 56, 8 bytes ← указатель, не объект!
}                                    // Total: 64 bytes = 1 cache line

type lruNode[T Hashable] struct {
    id    uint64
    value T
    prev  *lruNode[T]
    next  *lruNode[T]
}

func newLRUCache[T Hashable](capacity int) *lruCache[T] {
    c := &lruCache[T]{
        cache:    make(map[uint64]*lruNode[T], capacity),
        capacity: capacity,
    }
    // Pool аллоцируется отдельно — НЕ в struct
    c.nodePool = &sync.Pool{
        New: func() any { return new(lruNode[T]) },
    }
    return c
}

func (c *lruCache[T]) put(id uint64, item T) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if node, exists := c.cache[id]; exists {
        node.value = item
        c.moveToHead(node)
        return
    }

    // ← Берём из pool вместо new(lruNode[T])
    node := c.nodePool.Get().(*lruNode[T])
    node.id = id
    node.value = item
    node.prev = nil
    node.next = nil

    c.cache[id] = node
    c.addToHead(node)

    if len(c.cache) > c.capacity {
        c.removeTail()
    }
}

func (c *lruCache[T]) get(id uint64) (T, bool) {
    c.mu.RLock()
    node, exists := c.cache[id]
    c.mu.RUnlock()

    if !exists {
        var zero T
        return zero, false
    }

    c.mu.Lock()
    c.moveToHead(node)
    c.mu.Unlock()

    return node.value, true
}

func (c *lruCache[T]) tryGet(id uint64) (T, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    if node, exists := c.cache[id]; exists {
        return node.value, true
    }
    var zero T
    return zero, false
}

func (c *lruCache[T]) delete(id uint64) {
    c.mu.Lock()
    defer c.mu.Unlock()

    node, exists := c.cache[id]
    if !exists {
        return
    }

    delete(c.cache, id)
    c.removeNode(node)
    c.returnToPool(node) // ← возвращаем в pool
}

// returnToPool очищает узел и возвращает в pool (вызывается под lock)
func (c *lruCache[T]) returnToPool(node *lruNode[T]) {
    var zero T
    node.value = zero // обнуляем, чтобы не держать ссылку на T
    node.prev = nil
    node.next = nil
    node.id = 0
    c.nodePool.Put(node)
}

func (c *lruCache[T]) removeTail() {
    if c.tail == nil {
        return
    }
    tail := c.tail
    delete(c.cache, tail.id)
    c.removeNode(tail)
    c.returnToPool(tail) // ← возвращаем в pool вместо GC
}

func (c *lruCache[T]) clear() {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Возвращаем все узлы в pool
    for node := c.head; node != nil; {
        next := node.next
        c.returnToPool(node)
        node = next
    }

    c.cache = make(map[uint64]*lruNode[T], c.capacity)
    c.head = nil
    c.tail = nil
}

func (c *lruCache[T]) Resize(newCapacity int) {
    c.mu.Lock()
    defer c.mu.Unlock()

    for len(c.cache) > newCapacity {
        c.removeTail()
    }
    c.capacity = newCapacity
}

func (c *lruCache[T]) size() int {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return len(c.cache)
}

// addToHead, moveToHead, removeNode — без изменений
func (c *lruCache[T]) addToHead(node *lruNode[T]) {
    node.next = c.head
    node.prev = nil
    if c.head != nil {
        c.head.prev = node
    }
    c.head = node
    if c.tail == nil {
        c.tail = node
    }
}

func (c *lruCache[T]) moveToHead(node *lruNode[T]) {
    if node == c.head {
        return
    }
    c.removeNode(node)
    c.addToHead(node)
}

func (c *lruCache[T]) removeNode(node *lruNode[T]) {
    if node.prev != nil {
        node.prev.next = node.next
    } else {
        c.head = node.next
    }
    if node.next != nil {
        node.next.prev = node.prev
    } else {
        c.tail = node.prev
    }
}
