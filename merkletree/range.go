package merkletree

import (
	"sort"
	"sync"
	"runtime"
)

// RangeQuery возвращает все элементы в диапазоне [startKey, endKey)
// includeStart - включать ли startKey
// includeEnd - включать ли endKey
func (t *Tree[T]) RangeQuery(startKey, endKey []byte, includeStart, includeEnd bool) []T {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil
	}
	
	result := make([]T, 0)
	t.rangeQueryNode(t.root, startKey, endKey, includeStart, includeEnd, 0, &result)
	
	// Сортируем результат по ID для детерминизма
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID() < result[j].ID()
	})
	
	return result
}

// RangeQueryParallel параллельная версия для больших диапазонов
// Рекомендуется для диапазонов > 1000 элементов
func (t *Tree[T]) RangeQueryParallel(startKey, endKey []byte, includeStart, includeEnd bool) []T {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil
	}
	
	t.root.mu.RLock()
	numChildren := len(t.root.Children)
	children := make([]*Node[T], numChildren)
	copy(children, t.root.Children)
	t.root.mu.RUnlock()
	
	if numChildren == 0 {
		return nil
	}
	
	// Канал для сбора результатов из каждой ветки
	resultChan := make(chan []T, numChildren)
	var wg sync.WaitGroup
	
	// Запускаем goroutine для каждой ветки первого уровня
	for i := 0; i < numChildren; i++ {
		wg.Add(1)
		go func(child *Node[T]) {
			defer wg.Done()
			
			localResult := make([]T, 0)
			t.rangeQueryNode(child, startKey, endKey, includeStart, includeEnd, 1, &localResult)
			
			if len(localResult) > 0 {
				resultChan <- localResult
			}
		}(children[i])
	}
	
	// Закрываем канал после завершения всех worker'ов
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	
	// Собираем результаты
	allResults := make([]T, 0)
	for batch := range resultChan {
		allResults = append(allResults, batch...)
	}
	
	// Сортируем по ID
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].ID() < allResults[j].ID()
	})
	
	return allResults
}

// RangeQueryWorkerPool использует пул worker'ов для обхода
func (t *Tree[T]) RangeQueryWorkerPool(startKey, endKey []byte, includeStart, includeEnd bool, numWorkers int) []T {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil
	}
	
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	
	t.root.mu.RLock()
	children := make([]*Node[T], len(t.root.Children))
	copy(children, t.root.Children)
	t.root.mu.RUnlock()
	
	// Канал задач
	tasks := make(chan *Node[T], len(children))
	for _, child := range children {
		tasks <- child
	}
	close(tasks)
	
	// Канал результатов
	resultChan := make(chan []T, len(children))
	var wg sync.WaitGroup
	
	// Запускаем worker'ы
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for child := range tasks {
				localResult := make([]T, 0)
				t.rangeQueryNode(child, startKey, endKey, includeStart, includeEnd, 1, &localResult)
				if len(localResult) > 0 {
					resultChan <- localResult
				}
			}
		}()
	}
	
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	
	// Собираем результаты
	allResults := make([]T, 0)
	for batch := range resultChan {
		allResults = append(allResults, batch...)
	}
	
	// Сортируем
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].ID() < allResults[j].ID()
	})
	
	return allResults
}

// rangeQueryNode рекурсивный обход для range-запроса
func (t *Tree[T]) rangeQueryNode(node *Node[T], startKey, endKey []byte, includeStart, includeEnd bool, depth int, result *[]T) {
	if node == nil {
		return
	}
	
	node.mu.RLock()
	defer node.mu.RUnlock()
	
	// Если это лист - проверяем вхождение в диапазон
	if node.IsLeaf {
		// Проверяем, что это не удаленный узел
		if node.Hash == DeletedNodeHash {
			return
		}
		
		itemKey := node.Value.Key()
		itemKeySlice := itemKey[:]
		
		// Сравниваем с границами
		cmpStart := compareKeys(itemKeySlice, startKey)
		cmpEnd := compareKeys(itemKeySlice, endKey)
		
		inRange := false
		if includeStart && includeEnd {
			inRange = cmpStart >= 0 && cmpEnd <= 0
		} else if includeStart && !includeEnd {
			inRange = cmpStart >= 0 && cmpEnd < 0
		} else if !includeStart && includeEnd {
			inRange = cmpStart > 0 && cmpEnd <= 0
		} else {
			inRange = cmpStart > 0 && cmpEnd < 0
		}
		
		if inRange {
			*result = append(*result, node.Value)
		}
		return
	}
	
	// Промежуточный узел - обходим все ветки
	for i := 0; i < len(node.Keys); i++ {
		t.rangeQueryNode(node.Children[i], startKey, endKey, includeStart, includeEnd, depth+1, result)
	}
}

// compareKeys сравнивает два ключа лексикографически
// Возвращает: -1 если a < b, 0 если a == b, 1 если a > b
func compareKeys(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	
	// Если все байты равны, сравниваем длины
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// RangeQueryByID - более удобный вариант для uint64 ID
func (t *Tree[T]) RangeQueryByID(startID, endID uint64, includeStart, includeEnd bool) []T {
	startKey := idToKey(startID)
	endKey := idToKey(endID)
	return t.RangeQuery(startKey, endKey, includeStart, includeEnd)
}

// RangeQueryByIDParallel параллельная версия для больших диапазонов
func (t *Tree[T]) RangeQueryByIDParallel(startID, endID uint64, includeStart, includeEnd bool) []T {
	startKey := idToKey(startID)
	endKey := idToKey(endID)
	return t.RangeQueryParallel(startKey, endKey, includeStart, includeEnd)
}

// RangeQueryAuto автоматически выбирает последовательную или параллельную версию
// в зависимости от размера ожидаемого результата
func (t *Tree[T]) RangeQueryAuto(startKey, endKey []byte, includeStart, includeEnd bool) []T {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil
	}
	
	// Эвристика: для больших диапазонов используем параллельную версию
	startID := keyToID(startKey)
	endID := keyToID(endKey)
	rangeSize := endID - startID
	
	// Параллелизм выгоден для диапазонов > 1000 элементов
	if rangeSize > 1000 {
		return t.RangeQueryParallel(startKey, endKey, includeStart, includeEnd)
	}
	
	return t.RangeQuery(startKey, endKey, includeStart, includeEnd)
}

// idToKey конвертирует uint64 в []byte (big-endian)
func idToKey(id uint64) []byte {
	key := make([]byte, 8)
	key[0] = byte(id >> 56)
	key[1] = byte(id >> 48)
	key[2] = byte(id >> 40)
	key[3] = byte(id >> 32)
	key[4] = byte(id >> 24)
	key[5] = byte(id >> 16)
	key[6] = byte(id >> 8)
	key[7] = byte(id)
	return key
}

// keyToID конвертирует []byte в uint64
func keyToID(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}
	return uint64(key[0])<<56 |
		uint64(key[1])<<48 |
		uint64(key[2])<<40 |
		uint64(key[3])<<32 |
		uint64(key[4])<<24 |
		uint64(key[5])<<16 |
		uint64(key[6])<<8 |
		uint64(key[7])
}
