package merkletree

import (
	"sync"
	"sync/atomic"
)

const DefaultArenaBlockSize 	= 8192

// ConcurrentArena arena с минимальными блокировками
type ConcurrentArena[T Hashable] struct {
	blocks       [][]Node[T]
	activeBlock  []Node[T]
	activeIndex  atomic.Uint32
	blockSize    int
	mu           sync.Mutex
	allocCounter atomic.Uint64
}

// newConcurrentArena создает concurrent arena
func newConcurrentArena[T Hashable](blockSize int) *ConcurrentArena[T] {
	if blockSize <= 0 {
        blockSize = DefaultArenaBlockSize
    }
	
	arena := &ConcurrentArena[T]{
		blocks:    make([][]Node[T], 0, 16),
		blockSize: blockSize, //NodeBlockSize,
	}

	// Предаллоцируем первый блок
	arena.allocNewBlock()

	return arena
}

// alloc выделяет узел (lock-free в большинстве случаев)
func (a *ConcurrentArena[T]) alloc() *Node[T] {
	// Пытаемся взять из текущего блока без блокировки
	idx := a.activeIndex.Add(1) - 1

	if int(idx) < a.blockSize {
		a.allocCounter.Add(1)
		return &a.activeBlock[idx]
	}

	// Нужен новый блок - берем блокировку
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check (другой поток мог выделить)
	idx = a.activeIndex.Load()
	if int(idx) < a.blockSize {
		newIdx := a.activeIndex.Add(1) - 1
		a.allocCounter.Add(1)
		return &a.activeBlock[newIdx]
	}

	// Выделяем новый блок
	a.allocNewBlock()
	a.allocCounter.Add(1)
	return &a.activeBlock[0]
}

// allocNewBlock выделяет новый блок (под блокировкой)
func (a *ConcurrentArena[T]) allocNewBlock() {
	newBlock := make([]Node[T], a.blockSize)
	a.blocks = append(a.blocks, newBlock)
	a.activeBlock = newBlock
	a.activeIndex.Store(1) // Первый элемент уже используем
}

// allocated возвращает количество аллоцированных узлов
func (a *ConcurrentArena[T]) allocated() int {
	return int(a.allocCounter.Load())
}

// reset сбрасывает arena
func (a *ConcurrentArena[T]) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.blocks = make([][]Node[T], 0, 16)
	a.allocNewBlock()
	a.allocCounter.Store(0)
}
