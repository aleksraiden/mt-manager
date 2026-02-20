// cache_topn.go
package merkletree

import (
	"encoding/binary"
	"sort"
	"sync"
)

// ============================================
// TopNCache - thread-safe кеш для топ-N элементов
// ============================================

// TopNCache хранит top-N элементов с поддержкой O(1) доступа
// Thread-safe: все операции защищены RWMutex
type TopNCache[T Hashable] struct {
	items     []T          // Отсортированный слайс (защищен mu)
	capacity  int          // Максимальный размер (readonly после инициализации)
	ascending bool         // Порядок сортировки (readonly после инициализации)
	mu        sync.RWMutex // RWMutex для read/write разделения
	enabled   bool         // Флаг активации (readonly после инициализации)
}

// NewTopNCache создает новый TopN кеш
// capacity - максимальное количество элементов (0 = отключен)
// ascending - true для min-heap (меньшие первыми), false для max-heap
func NewTopNCache[T Hashable](capacity int, ascending bool) *TopNCache[T] {
	if capacity <= 0 {
		// Неактивный кеш - не потребляет ресурсов
		return &TopNCache[T]{
			enabled: false,
		}
	}
	
	return &TopNCache[T]{
		items:     make([]T, 0, capacity),
		capacity:  capacity,
		ascending: ascending,
		enabled:   true,
	}
}

// IsEnabled проверяет, активен ли кеш
func (tc *TopNCache[T]) IsEnabled() bool {
	return tc.enabled
}

// ============================================
// Write операции (требуют Write Lock)
// ============================================

// TryInsert пытается добавить элемент в top-N
// Возвращает true если элемент был добавлен
// Thread-safe: использует Write Lock
func (tc *TopNCache[T]) TryInsert(item T) bool {
	if !tc.enabled {
		return false
	}
	
	//Do NOT add 0-ID item into cache 
	if item.ID() == 0 {
		return false 
	}
	
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	// Все операции под одним Lock - гарантия атомарности
	count := len(tc.items)
	key := keyToUint64(item.Key())
	
	// Кеш не заполнен - просто добавляем
	if count < tc.capacity {
		tc.items = append(tc.items, item)
		tc.resortUnderLock()
		return true
	}
	
	// Кеш заполнен - проверяем, попадает ли элемент в top-N
	lastKey := keyToUint64(tc.items[count-1].Key())
	
	shouldInsert := false
	if tc.ascending {
		// Min-heap: новый элемент < последнего
		shouldInsert = key < lastKey
	} else {
		// Max-heap: новый элемент > последнего
		shouldInsert = key > lastKey
	}
	
	if shouldInsert {
		// Заменяем последний элемент и пересортировываем
		tc.items[count-1] = item
		tc.resortUnderLock()
		return true
	}
	
	return false
}

// Remove удаляет элемент из top-N по ID
// Возвращает true если элемент был найден и удален
// Thread-safe: использует Write Lock
func (tc *TopNCache[T]) Remove(item T) bool {
	if !tc.enabled {
		return false
	}
	
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	count := len(tc.items)
	id := item.ID()
	
	//Do NOT deleting 0-ID element 
	if id == 0 {
		return false
	}
	
	// Линейный поиск элемента (O(n), но n <= capacity обычно мало)
	for i := 0; i < count; i++ {
		if tc.items[i].ID() == id {
			// Удаляем элемент сдвигом слайса
			tc.items = append(tc.items[:i], tc.items[i+1:]...)
			return true
		}
	}
	
	return false
}

// Clear очищает кеш
// Thread-safe: использует Write Lock
func (tc *TopNCache[T]) Clear() {
	if !tc.enabled {
		return
	}
	
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	// Очищаем без deallocate для переиспользования capacity
	tc.items = tc.items[:0]
}

// ============================================
// Read операции (требуют Read Lock)
// ============================================

// GetTop возвращает top-N элементов (или меньше если кеш не заполнен)
// Возвращает копию слайса для thread-safety
// Thread-safe: использует Read Lock (позволяет параллельное чтение)
func (tc *TopNCache[T]) GetTop(n int) []T {
	if !tc.enabled {
		return nil
	}
	
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	count := len(tc.items)
	if n > count {
		n = count
	}
	
	if n == 0 {
		return nil
	}
	
	// Копируем для thread-safety (caller может модифицировать результат)
	result := make([]T, n)
	copy(result, tc.items[:n])
	return result
}

// GetFirst возвращает первый элемент
// Для ascending cache - минимальный элемент
// Для descending cache - максимальный элемент
// Thread-safe: использует Read Lock
func (tc *TopNCache[T]) GetFirst() (T, bool) {
	if !tc.enabled {
		var zero T
		return zero, false
	}
	
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	if len(tc.items) == 0 {
		var zero T
		return zero, false
	}
	
	return tc.items[0], true
}

// Count возвращает текущее количество элементов в кеше
// Thread-safe: использует Read Lock
func (tc *TopNCache[T]) Count() int {
	if !tc.enabled {
		return 0
	}
	
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	return len(tc.items)
}

// ============================================
// Iterator API
// ============================================

// GetIteratorMin возвращает итератор для элементов в порядке возрастания
// Создает атомарный snapshot под Read Lock
// Thread-safe: snapshot независим от дальнейших модификаций
func (tc *TopNCache[T]) GetIteratorMin() *TopNIterator[T] {
	if !tc.enabled {
		return NewTopNIterator[T](nil)
	}
	
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	
	if len(tc.items) == 0 {
		return NewTopNIterator[T](nil)
	}
	
	// Копируем весь слайс для создания независимого snapshot
	snapshot := make([]T, len(tc.items))
	copy(snapshot, tc.items)
	
	return NewTopNIterator(snapshot)
}

// GetIteratorMax возвращает итератор для элементов в порядке убывания
// Создает атомарный snapshot под Read Lock
// Thread-safe: snapshot независим от дальнейших модификаций
func (tc *TopNCache[T]) GetIteratorMax() *TopNIterator[T] {
    if !tc.enabled {
        return NewTopNIterator[T](nil)
    }

    tc.mu.RLock()
    defer tc.mu.RUnlock()

    if len(tc.items) == 0 {
        return NewTopNIterator[T](nil)
    }

    snapshot := make([]T, len(tc.items))
    copy(snapshot, tc.items)

    // Если кеш ascending (min-heap) — разворачиваем для max-итерации
    if tc.ascending {
        for i, j := 0, len(snapshot)-1; i < j; i, j = i+1, j-1 {
            snapshot[i], snapshot[j] = snapshot[j], snapshot[i]
        }
    }

    return NewTopNIterator(snapshot)
}
// ============================================
// Internal helpers (вызываются только под Lock)
// ============================================

// resortUnderLock сортирует элементы
// ВАЖНО: должен вызываться ТОЛЬКО под tc.mu.Lock()
func (tc *TopNCache[T]) resortUnderLock() {
	count := len(tc.items)
	if count <= 1 {
		return
	}
	
	// Сортировка O(n log n) выполняется под Write Lock
	sort.Slice(tc.items, func(i, j int) bool {
		keyI := keyToUint64(tc.items[i].Key())
		keyJ := keyToUint64(tc.items[j].Key())
		if tc.ascending {
			return keyI < keyJ
		}
		return keyI > keyJ
	})
}

// ============================================
// TopNIterator - итератор для обхода элементов
// ============================================

// TopNIterator итератор для обхода TopN элементов
// Работает со snapshot'ом данных и не зависит от изменений в исходном кеше
type TopNIterator[T Hashable] struct {
	items   []T // Snapshot элементов
	current int // Текущая позиция
}

// NewTopNIterator создает новый итератор из snapshot'а элементов
// Копирует слайс для полной независимости от источника
func NewTopNIterator[T Hashable](items []T) *TopNIterator[T] {
	// Копируем слайс для safety (даже если items уже копия)
	snapshot := make([]T, len(items))
	copy(snapshot, items)
	
	return &TopNIterator[T]{
		items:   snapshot,
		current: 0,
	}
}

// HasNext проверяет, есть ли еще элементы
func (it *TopNIterator[T]) HasNext() bool {
	return it.current < len(it.items)
}

// Next возвращает следующий элемент и сдвигает позицию
// Возвращает (item, true) если элемент есть, (zero, false) если достигнут конец
func (it *TopNIterator[T]) Next() (T, bool) {
	if !it.HasNext() {
		var zero T
		return zero, false
	}
	
	item := it.items[it.current]
	it.current++
	return item, true
}

// Peek возвращает следующий элемент БЕЗ сдвига позиции
// Полезно для проверки элемента перед его извлечением
func (it *TopNIterator[T]) Peek() (T, bool) {
	if !it.HasNext() {
		var zero T
		return zero, false
	}
	
	return it.items[it.current], true
}

// Remaining возвращает количество оставшихся элементов
func (it *TopNIterator[T]) Remaining() int {
	return len(it.items) - it.current
}

// Reset сбрасывает итератор в начало
// Позволяет повторно обойти те же элементы
func (it *TopNIterator[T]) Reset() {
	it.current = 0
}

// ToSlice возвращает оставшиеся элементы как слайс
// Возвращает копию оставшихся элементов
func (it *TopNIterator[T]) ToSlice() []T {
	if !it.HasNext() {
		return nil
	}
	
	result := make([]T, it.Remaining())
	copy(result, it.items[it.current:])
	return result
}

// ForEach применяет функцию к каждому оставшемуся элементу
// Исчерпывает итератор после выполнения
func (it *TopNIterator[T]) ForEach(fn func(T)) {
	for it.HasNext() {
		item, _ := it.Next()
		fn(item)
	}
}

// TakeWhile берет элементы пока предикат возвращает true
// Останавливается на первом элементе, не удовлетворяющем условию
// Итератор остается на позиции первого не прошедшего элемента
func (it *TopNIterator[T]) TakeWhile(predicate func(T) bool) []T {
	result := make([]T, 0, it.Remaining())
	
	for it.HasNext() {
		item, _ := it.Peek()
		if !predicate(item) {
			break
		}
		it.Next()
		result = append(result, item)
	}
	
	return result
}

// ============================================
// Utility functions
// ============================================

// keyToUint64 конвертирует [8]byte ключ в uint64 (BigEndian)
// Используется для сравнения ключей при сортировке
func keyToUint64(key [8]byte) uint64 {
	return binary.BigEndian.Uint64(key[:])
}
