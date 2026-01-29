package merkletree

import (
	"sync"
	"time"
)

// WriteBuffer буфер с автоматическим flush
type WriteBuffer[T Hashable] struct {
	tree          *Tree[T]
	buffer        []T
	bufferSize    int
	flushInterval time.Duration
	mu            sync.Mutex
	stopChan      chan struct{}
	flushChan     chan struct{}
}

// NewWriteBuffer создает write buffer
func NewWriteBuffer[T Hashable](tree *Tree[T], bufferSize int, flushInterval time.Duration) *WriteBuffer[T] {
	wb := &WriteBuffer[T]{
		tree:          tree,
		buffer:        make([]T, 0, bufferSize),
		bufferSize:    bufferSize,
		flushInterval: flushInterval,
		stopChan:      make(chan struct{}),
		flushChan:     make(chan struct{}, 1),
	}

	// Запускаем фоновый flusher
	go wb.autoFlusher()

	return wb
}

// Insert добавление с автоматическим flush
func (wb *WriteBuffer[T]) Insert(item T) error {
	wb.mu.Lock()
	wb.buffer = append(wb.buffer, item)
	shouldFlush := len(wb.buffer) >= wb.bufferSize
	wb.mu.Unlock()

	if shouldFlush {
		wb.Flush()
	}

	return nil
}

// Flush принудительный flush
func (wb *WriteBuffer[T]) Flush() error {
	wb.mu.Lock()
	if len(wb.buffer) == 0 {
		wb.mu.Unlock()
		return nil
	}

	items := make([]T, len(wb.buffer))
	copy(items, wb.buffer)
	wb.buffer = wb.buffer[:0] // Очищаем, но сохраняем capacity
	wb.mu.Unlock()

	// Батчевая вставка
	wb.tree.InsertBatch(items)

	return nil
}

// autoFlusher фоновый flush по таймеру
func (wb *WriteBuffer[T]) autoFlusher() {
	ticker := time.NewTicker(wb.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wb.Flush()
		case <-wb.flushChan:
			wb.Flush()
		case <-wb.stopChan:
			wb.Flush() // Финальный flush
			return
		}
	}
}

// Close закрывает буфер
func (wb *WriteBuffer[T]) Close() error {
	close(wb.stopChan)
	return nil
}

// Size текущий размер буфера
func (wb *WriteBuffer[T]) Size() int {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return len(wb.buffer)
}
