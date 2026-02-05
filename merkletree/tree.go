package merkletree

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/zeebo/blake3"
)

const (
	NodeBlockSize = 1024 * 1024

	// Агрессивные пороги для больших систем
	SmallBatchThreshold    = 30  // Было 50
	ParallelBatchThreshold = 150 // Было 200
)

var (
	// Константный хеш для удаленных узлов
	DeletedNodeHash = blake3.Sum256([]byte("__DELETED_NODE__"))
)

// Tree - убираем избыточный padding, оставляем только критичный
type Tree[T Hashable] struct {
	root       *Node[T]
	items      sync.Map
	itemCount  atomic.Uint64
	arena      *ConcurrentArena[T]
	cache      *ShardedCache[T]
	maxDepth   int
	mu         sync.RWMutex
	
	topMinCache   *TopNCache[T]  // Минимальные элементы (ascending)
	topMaxCache   *TopNCache[T]  // Максимальные элементы (descending)
	
	useAppLookup	bool		//флаг ипользования (автоматически) лукап-таблицы 
	appLookup	  	sync.Map	//специализированный лукап 
	
	name            string  // Имя дерева (для снапшотов)

	// Lazy hashing (компактно)
	dirtyNodes     atomic.Uint64
	cachedRoot     atomic.Value		//[32]byte
	rootCacheValid atomic.Bool

	// Метрики
	insertCount        atomic.Uint64
	batchInsertCount   atomic.Uint64
	deleteCount        atomic.Uint64
	deletedNodeCount   atomic.Uint64
	getCount           atomic.Uint64
	cacheHits          atomic.Uint64
	cacheMisses        atomic.Uint64
	computeCount       atomic.Uint64
	batchCount         atomic.Uint64
	simpleBatchCount   atomic.Uint64
	parallelBatchCount atomic.Uint64
}

// Node - минимальный padding только для mutex
type Node[T Hashable] struct {
	Hash     [32]byte
	Children []*Node[T]
	Keys     []byte
	Value    T
	IsLeaf   bool
	dirty    atomic.Bool

	_padding [7]byte // Выравнивание до 8 байт для mutex

	mu sync.RWMutex // В отдельной cache line от данных
}

func New[T Hashable](cfg *Config) *Tree[T] {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	arena := newConcurrentArena[T]()
	root := arena.alloc()

	t := &Tree[T]{
		root:     root,
		arena:    arena,
		cache:    newShardedCache[T](cfg.CacheSize, cfg.CacheShards),
		maxDepth: cfg.MaxDepth,
		
		// Инициализация TopN кешей
		topMinCache: NewTopNCache[T](cfg.TopN, true),   // ascending = min-heap
		topMaxCache: NewTopNCache[T](cfg.TopN, false),  // descending = max-heap
		
		useAppLookup: cfg.UseAppLoockup,
	}
	
	t.cachedRoot.Store([32]byte{}) // zero value
	
	return t
}

//Одиночная вставка с блокировкой 
func (t *Tree[T]) Insert(item T) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	t.items.Store(item.ID(), item)
	t.itemCount.Add(1)
	t.cache.put(item.ID(), item)
	t.insertNode(t.root, item, 0)
	t.rootCacheValid.Store(false)
	
	// Обновляем TopN кеши
	t.topMinCache.TryInsert(item)
	t.topMaxCache.TryInsert(item)
	
	t.insertCount.Add(1)
}

// InsertBatch вставляет батч элементов (автоматический выбор стратегии)
func (t *Tree[T]) InsertBatch(items []T) {
	if len(items) == 0 {
		return
	}

	// Автоматический выбор стратегии в зависимости от размера батча
	if len(items) < SmallBatchThreshold {
		// < 30 элементов: глобальная блокировка быстрее
		t.insertBatchSimple(items)
	} else if len(items) < ParallelBatchThreshold {
		// 30-150 элементов: последовательно с per-node блокировками
		t.insertBatchSequential(items)
	} else if len(items) < 5000 {
		// 150-5000 элементов: параллельная вставка
		t.insertBatchParallel(items)
	} else {
		// > 5000 элементов: мега-параллельная с группировкой
		t.insertBatchMegaParallel(items)
	}
	
	t.batchInsertCount.Add(1)
}

// insertBatchSimple для маленьких батчей (с глобальной блокировкой)
func (t *Tree[T]) insertBatchSimple(items []T) {
	// Берем ОДНУ глобальную блокировку на весь батч
	// TODO: можно оптимизировать, вынеся из-под блокировки часть операций, но будет два обхода цикла 
	t.mu.Lock()
	defer t.mu.Unlock()
	
	for _, item := range items {
		t.items.Store(item.ID(), item)
		t.cache.put(item.ID(), item)
		
		// Обновляем TopN
		t.topMinCache.TryInsert(item)
		t.topMaxCache.TryInsert(item)
		
		t.insertNodeUnderGlobalLock(t.root, item, 0)
	}
	
	t.itemCount.Add(uint64(len(items)))

	//for _, item := range items {
	//	t.insertNodeUnderGlobalLock(t.root, item, 0)
	//}
	//t.mu.Unlock()

	t.rootCacheValid.Store(false)
}

// insertBatchSequential последовательная вставка (per-node locking)
func (t *Tree[T]) insertBatchSequential(items []T) {
	for _, item := range items {
		t.items.Store(item.ID(), item)
		t.cache.put(item.ID(), item)
		
		t.topMinCache.TryInsert(item)
		t.topMaxCache.TryInsert(item)
	}
	t.itemCount.Add(uint64(len(items)))

	for _, item := range items {
		t.insertNode(t.root, item, 0)
	}

	t.rootCacheValid.Store(false)
}

// insertBatchParallel параллельная вставка
func (t *Tree[T]) insertBatchParallel(items []T) {
	if len(items) == 0 {
		return
	}

	numWorkers := runtime.NumCPU()
	
	if numWorkers >= 32 {
		numWorkers = numWorkers * 3 / 2
	}
	
	if numWorkers > 64 {
		numWorkers = 64
	}
	
	if numWorkers > len(items) {
		numWorkers = len(items)
	}

	chunkSize := (len(items) + numWorkers - 1) / numWorkers
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(items) {
			break
		}
		
		end := start + chunkSize
		if end > len(items) {
			end = len(items)
		}

		wg.Add(1)
		go func(chunk []T) {
			defer wg.Done()
			for _, item := range chunk {
				t.items.Store(item.ID(), item)
				t.cache.put(item.ID(), item)
				
				// TopN обновление (thread-safe)
				t.topMinCache.TryInsert(item)
				t.topMaxCache.TryInsert(item)
			}
		}(items[start:end])
	}

	wg.Wait()
	t.itemCount.Add(uint64(len(items)))

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(items) {
			break
		}
		
		end := start + chunkSize
		if end > len(items) {
			end = len(items)
		}

		wg.Add(1)
		go func(chunk []T) {
			defer wg.Done()
			for _, item := range chunk {
				t.insertNode(t.root, item, 0)
			}
		}(items[start:end])
	}

	wg.Wait()
	t.rootCacheValid.Store(false)
}

// insertBatchMegaParallel для больших батчей
func (t *Tree[T]) insertBatchMegaParallel(items []T) {
	if len(items) == 0 {
		return
	}

	numCPU := runtime.NumCPU()
	numWorkers := numCPU * 3
	if numWorkers > 144 {
		numWorkers = 144
	}
	if numWorkers > len(items) {
		numWorkers = len(items)
	}

	chunkSize := (len(items) + numWorkers - 1) / numWorkers
	var wg sync.WaitGroup

	// Фаза 1: Maps
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(items) {
			break
		}
		
		end := start + chunkSize
		if end > len(items) {
			end = len(items)
		}

		wg.Add(1)
		go func(chunk []T) {
			defer wg.Done()
			for _, item := range chunk {
				t.items.Store(item.ID(), item)
				t.cache.put(item.ID(), item)
				
				// TopN обновление (thread-safe)
				t.topMinCache.TryInsert(item)
				t.topMaxCache.TryInsert(item)
			}
		}(items[start:end])
	}

	wg.Wait()
	t.itemCount.Add(uint64(len(items)))

	// Фаза 2: Группировка
	groupSize := 256
	if len(items) > 10000 {
		groupSize = 4096
	}
	
	groups := make([][]T, groupSize)
	for _, item := range items {
		key := item.Key()
		var groupKey int
		if groupSize == 256 {
			groupKey = int(key[0])
		} else {
			groupKey = (int(key[0]) << 8) | int(key[1])
		}
		groups[groupKey] = append(groups[groupKey], item)
	}

	groupChan := make(chan []T, groupSize)
	for _, group := range groups {
		if len(group) > 0 {
			groupChan <- group
		}
	}
	close(groupChan)

	numGroupWorkers := numCPU * 2
	if numGroupWorkers > 96 {
		numGroupWorkers = 96
	}
	
	for i := 0; i < numGroupWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for group := range groupChan {
				for _, item := range group {
					t.insertNode(t.root, item, 0)
				}
			}
		}()
	}

	wg.Wait()
	t.rootCacheValid.Store(false)
}

// insertNodeUnderGlobalLock - вставка БЕЗ per-node блокировок (вызывается под t.mu.Lock)
func (t *Tree[T]) insertNodeUnderGlobalLock(node *Node[T], item T, depth int) {
	key := item.Key()
	
	if depth >= t.maxDepth-1 {
		idx := key[len(key)-1]
		
		for i, k := range node.Keys {
			if k == idx {
				child := node.Children[i]
				child.Value = item
				// помечаем как грязный
				child.dirty.Store(true)
				node.dirty.Store(true)
				t.dirtyNodes.Add(1)
				return
			}
		}
		
		// Новый лист
		child := t.arena.alloc()
		child.IsLeaf = true
		child.Value = item
		//помечаем как грязный
		child.dirty.Store(true)
		node.Keys = append(node.Keys, idx)
		node.Children = append(node.Children, child)
		node.dirty.Store(true)
		t.dirtyNodes.Add(1)
		return
	}
	
	// Промежуточный узел - без изменений
	idx := key[depth]
	for i, k := range node.Keys {
		if k == idx {
			t.insertNodeUnderGlobalLock(node.Children[i], item, depth+1)
			node.dirty.Store(true)
			t.dirtyNodes.Add(1)
			return
		}
	}
	
	child := t.arena.alloc()
	node.Keys = append(node.Keys, idx)
	node.Children = append(node.Children, child)
	node.dirty.Store(true)
	t.dirtyNodes.Add(1)
	t.insertNodeUnderGlobalLock(child, item, depth+1)
}

// insertNode - ЕДИНЫЙ метод с per-node блокировками
func (t *Tree[T]) insertNode(node *Node[T], item T, depth int) {
	key := item.Key()
	node.mu.Lock()
	
	if depth >= t.maxDepth-1 {
		idx := key[len(key)-1]
		
		for i, k := range node.Keys {
			if k == idx {
				child := node.Children[i]
				node.mu.Unlock()
				
				child.mu.Lock()
				child.Value = item
				// НЕ вычисляем хеш! Только помечаем как грязный
				child.dirty.Store(true)
				child.mu.Unlock()
				
				node.dirty.Store(true)
				t.dirtyNodes.Add(1)
				return
			}
		}
		
		// Новый лист
		child := t.arena.alloc()
		child.IsLeaf = true
		child.Value = item
		// НЕ вычисляем хеш! Только помечаем как грязный
		child.dirty.Store(true)
		node.Keys = append(node.Keys, idx)
		node.Children = append(node.Children, child)
		node.dirty.Store(true)
		t.dirtyNodes.Add(1)
		node.mu.Unlock()
		return
	}
	
	// Промежуточный узел - без изменений
	idx := key[depth]
	for i, k := range node.Keys {
		if k == idx {
			child := node.Children[i]
			node.mu.Unlock()
			t.insertNode(child, item, depth+1)
			node.dirty.Store(true)
			t.dirtyNodes.Add(1)
			return
		}
	}
	
	child := t.arena.alloc()
	node.Keys = append(node.Keys, idx)
	node.Children = append(node.Children, child)
	node.dirty.Store(true)
	t.dirtyNodes.Add(1)
	node.mu.Unlock()
	t.insertNode(child, item, depth+1)
}

func (t *Tree[T]) Get(id uint64) (T, bool) {
    t.getCount.Add(1)
    
    // Быстрая проверка без обновления LRU
    if item, ok := t.cache.tryGet(id); ok {
        t.cacheHits.Add(1)
        
        // Опционально: периодически обновляем LRU (каждый N-й доступ)
        if t.getCount.Load() % 100 == 0 {
            t.cache.put(id, item)  // Обновляем позицию
        }
        
        return item, true
    }

    // Обычная логика для cache miss
    if val, ok := t.items.Load(id); ok {
        item := val.(T)
        t.cache.put(id, item)
        t.cacheMisses.Add(1)
        return item, true
    }

    var zero T
    t.cacheMisses.Add(1)
    return zero, false
}

// ComputeRoot вычисляет корневой хеш (с автоматическим выбором стратегии)
func (t *Tree[T]) ComputeRoot() [32]byte {
    // Быстрое чтение (lock-free)
    if t.rootCacheValid.Load() {
        if val := t.cachedRoot.Load(); val != nil {
            return val.([32]byte)
        }
    }
	
	t.mu.RLock()
		// Вычисляем новый root (дорого, делаем редко)
		newRoot := t.computeNodeHash(t.root, 0, true)
	t.mu.RUnlock()
	
    // CAS-loop: пытаемся установить, если dirty не изменился
    for attempts := 0; attempts < 16; attempts++ { // лимит попыток, чтобы не loop forever
        currentDirty := t.dirtyNodes.Load()

        // Устанавливаем новый root
        t.cachedRoot.Store(newRoot)
        t.rootCacheValid.Store(true)

        // Если dirty не изменился — успех (другие потоки не модифицировали дерево)
        if t.dirtyNodes.CompareAndSwap(currentDirty, 0) {
            return newRoot
        }
		
		t.mu.RLock()
			// Dirty изменился → кто-то вставил/удалил → пересчитываем
			newRoot = t.computeNodeHash(t.root, 0, true)
		t.mu.RUnlock()
    }

    // Редкий fallback: если CAS не удался много раз (очень высокая contention)
    // — просто берём lock и делаем финальную запись
    t.mu.Lock()
    defer t.mu.Unlock()
    
	newRoot = t.computeNodeHash(t.root, 0, true)
    t.cachedRoot.Store(newRoot)
    t.rootCacheValid.Store(true)
    t.dirtyNodes.Store(0)
    
	return newRoot
}

// computeNodeHash - ЕДИНЫЙ метод с автоматическим параллелизмом
func (t *Tree[T]) computeNodeHash(node *Node[T], depth int, allowParallel bool) [32]byte {
	if node == nil {
		return [32]byte{}
	}
	
	node.mu.RLock()
	
	// Если узел не грязный - возвращаем кеш
	if !node.dirty.Load() && node.Hash != [32]byte{} {
		hash := node.Hash
		node.mu.RUnlock()
		return hash
	}
	
	// Для листьев - вычисляем хеш
	if node.IsLeaf {
		value := node.Value
		node.mu.RUnlock()
		
		// Вычисляем хеш элемента
		hash := value.Hash()
		
		// Обновляем узел
		node.mu.Lock()
		node.Hash = hash
		node.dirty.Store(false)
		node.mu.Unlock()
		
		return hash
	}
	
	count := len(node.Keys)
	if count == 0 {
		node.mu.RUnlock()
		return [32]byte{}
	}
	
	// Проверяем, стоит ли использовать batch-хеширование листьев
	// Условия:
	// 1. Разрешен параллелизм
	// 2. Глубина близка к листьям (depth >= maxDepth - 3)
	// 3. Достаточно детей для эффективности (>= 8)
	useBatchLeafHashing := allowParallel && 
		depth >= t.maxDepth-4 && // Было -3, делаем -4 (глубже в дерево)
		count >= 6 &&             // Было 8, уменьшаем порог
		runtime.NumCPU() > 8      // Было > 1, теперь только для серверов
	
	indices := make([]int, count)
	for i := range indices {
		indices[i] = i
	}
	
	keys := make([]byte, count)
	copy(keys, node.Keys)
	children := make([]*Node[T], count)
	copy(children, node.Children)
	node.mu.RUnlock()
	
	// Сортировка индексов
	for i := 0; i < count-1; i++ {
		for j := i + 1; j < count; j++ {
			if keys[indices[i]] > keys[indices[j]] {
				indices[i], indices[j] = indices[j], indices[i]
			}
		}
	}
	
	// Batch-хеширование листьев
	var leafHashes map[*Node[T]][32]byte
	if useBatchLeafHashing {
		// Собираем все листья на следующем уровне
		leaves := make([]*Node[T], 0, count*4) // Оценка: ~4 листа на ребенка
		
		for _, idx := range indices {
			child := children[idx]
			if child != nil {
				// Если ребенок сам лист - добавляем его
				child.mu.RLock()
				isLeaf := child.IsLeaf
				child.mu.RUnlock()
				
				if isLeaf {
					leaves = append(leaves, child)
				} else {
					// Если промежуточный узел - собираем его листья
					t.collectLeaves(child, &leaves)
				}
			}
		}
		
		// Хешируем все листья параллельно, если их достаточно
		if len(leaves) >= 16 {
			leafHashes = t.computeLeafHashesBatch(leaves)
		}
	}
	
	// Решаем, использовать ли параллелизм для промежуточных узлов
	useParallel := allowParallel && depth < 2 && count >= 4 && runtime.NumCPU() > 1
	
	// Вычисляем хеши детей
	childHashes := make([][32]byte, count)
	
	if useParallel {
		// Параллельное вычисление промежуточных узлов
		var wg sync.WaitGroup
		for i, idx := range indices {
			wg.Add(1)
			go func(i, idx int) {
				defer wg.Done()
				childHashes[i] = t.computeNodeHashWithCache(children[idx], depth+1, true, leafHashes)
			}(i, idx)
		}
		wg.Wait()
	} else {
		// Последовательное вычисление
		for i, idx := range indices {
			childHashes[i] = t.computeNodeHashWithCache(children[idx], depth+1, false, leafHashes)
		}
	}
	
	// Хешируем результат
	hasher := blake3.New()
	for i, idx := range indices {
		hasher.Write([]byte{keys[idx]})
		hasher.Write(childHashes[i][:])
	}
	
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	
	node.mu.Lock()
	node.Hash = result
	node.dirty.Store(false)
	node.mu.Unlock()
	
	return result
}

// computeNodeHashWithCache - вспомогательный метод, который использует кеш листьев
func (t *Tree[T]) computeNodeHashWithCache(node *Node[T], depth int, allowParallel bool, leafCache map[*Node[T]][32]byte) [32]byte {
	if node == nil {
		return [32]byte{}
	}
	
	// Проверяем кеш листьев
	if leafCache != nil {
		if hash, exists := leafCache[node]; exists {
			return hash
		}
	}
	
	// Иначе используем обычную логику
	return t.computeNodeHash(node, depth, allowParallel)
}

// collectLeaves рекурсивно собирает все листья из поддерева
func (t *Tree[T]) collectLeaves(node *Node[T], leaves *[]*Node[T]) {
	if node == nil {
		return
	}
	
	node.mu.RLock()
	isLeaf := node.IsLeaf
	children := node.Children
	node.mu.RUnlock()
	
	if isLeaf {
		*leaves = append(*leaves, node)
		return
	}
	
	// Рекурсивно собираем листья из детей
	for _, child := range children {
		if child != nil {
			child.mu.RLock()
			childIsLeaf := child.IsLeaf
			child.mu.RUnlock()
			
			if childIsLeaf {
				*leaves = append(*leaves, child)
			} else {
				t.collectLeaves(child, leaves)
			}
		}
	}
}

// computeLeafHashesBatch вычисляет хеши нескольких листьев параллельно
// Возвращает map[*Node]hash для быстрого lookup
func (t *Tree[T]) computeLeafHashesBatch(leaves []*Node[T]) map[*Node[T]][32]byte {
	if len(leaves) == 0 {
		return nil
	}
	
	results := make(map[*Node[T]][32]byte, len(leaves))
	var resultMu sync.Mutex
	
	// Определяем количество воркеров
	numWorkers := runtime.NumCPU()
	if numWorkers > 32 {
		numWorkers = 32  // Ограничение для очень больших CPU
	}
	
	// Минимум 2 листа на воркер (было 4)
	if len(leaves) < numWorkers*2 {
		numWorkers = (len(leaves) + 1) / 2
	}
	
	chunkSize := (len(leaves) + numWorkers - 1) / numWorkers
	var wg sync.WaitGroup
	
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(leaves) {
			break
		}
		
		end := start + chunkSize
		if end > len(leaves) {
			end = len(leaves)
		}
		
		wg.Add(1)
		go func(chunk []*Node[T]) {
			defer wg.Done()
			
			localResults := make(map[*Node[T]][32]byte, len(chunk))
			
			for _, leaf := range chunk {
				if leaf == nil {
					continue
				}
				
				leaf.mu.RLock()
				isDirty := leaf.dirty.Load()
				value := leaf.Value
				currentHash := leaf.Hash
				leaf.mu.RUnlock()
				
				var hash [32]byte
				
				// Если грязный - вычисляем, иначе используем кеш
				if isDirty || currentHash == [32]byte{} {
					hash = value.Hash()
					
					// Обновляем узел
					leaf.mu.Lock()
					leaf.Hash = hash
					leaf.dirty.Store(false)
					leaf.mu.Unlock()
				} else {
					hash = currentHash
				}
				
				localResults[leaf] = hash
			}
			
			// Мержим результаты
			resultMu.Lock()
			for node, hash := range localResults {
				results[node] = hash
			}
			resultMu.Unlock()
		}(leaves[start:end])
	}
	
	wg.Wait()
	return results
}

func (t *Tree[T]) Size() int {
	return int(t.itemCount.Load())
}

func (t *Tree[T]) GetAllItems() []T {
	items := make([]T, 0, t.itemCount.Load())

	t.items.Range(func(key, value interface{}) bool {
		items = append(items, value.(T))
		return true
	})

	return items
}

func (t *Tree[T]) GetDirtyNodeCount() uint64 {
	return t.dirtyNodes.Load()
}

func (t *Tree[T]) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.items = sync.Map{}
	t.itemCount.Store(0)
	t.arena.reset()
	t.root = t.arena.alloc()
	t.cache.clear()
	t.rootCacheValid.Store(false)
	
	// Очищаем TopN кеши
	t.topMinCache.Clear()
	t.topMaxCache.Clear()
	
	t.dirtyNodes.Store(0)
	t.deletedNodeCount.Store(0)
}

type Stats struct {
	TotalItems        int
	DeletedNodes      int    
	AllocatedNodes    int
	CacheSize         int
	DirtyNodes        uint64
	InsertCount       uint64
	BatchInsertCount  uint64
	DeleteCount       uint64 
	GetCount          uint64
	CacheHits         uint64
	CacheMisses       uint64
	CacheHitRate      float64
	ComputeCount      uint64
	BatchCount        uint64
	SimpleBatches     uint64
	ParallelBatches   uint64
	SequentialBatches uint64
}

// GetStats возвращает полную статистику дерева
func (t *Tree[T]) GetStats() Stats {
	hits := t.cacheHits.Load()
	misses := t.cacheMisses.Load()
	hitRate := 0.0
	if hits+misses > 0 {
		hitRate = float64(hits) / float64(hits+misses) * 100
	}

	batchCount := t.batchCount.Load()
	simpleBatches := t.simpleBatchCount.Load()
	parallelBatches := t.parallelBatchCount.Load()
	sequentialBatches := batchCount - simpleBatches - parallelBatches

	return Stats{
		TotalItems:        int(t.itemCount.Load()),
		DeletedNodes:      int(t.deletedNodeCount.Load()), 
		AllocatedNodes:    t.arena.allocated(),
		CacheSize:         t.cache.size(),
		DirtyNodes:        t.dirtyNodes.Load(),
		InsertCount:       t.insertCount.Load(),
		BatchInsertCount: t.batchInsertCount.Load(),
		DeleteCount:       t.deleteCount.Load(),
		GetCount:          t.getCount.Load(),
		CacheHits:         hits,
		CacheMisses:       misses,
		CacheHitRate:      hitRate,
		ComputeCount:      t.computeCount.Load(),
		BatchCount:        batchCount,
		SimpleBatches:     simpleBatches,
		ParallelBatches:   parallelBatches,
		SequentialBatches: sequentialBatches,
	}
}

type CacheStats struct {
	Size     int
	Capacity int
	Usage    float64
}

func (t *Tree[T]) CacheStats() CacheStats {
	capacity := t.getCacheCapacity()
	size := t.cache.size()
	usage := 0.0
	if capacity > 0 {
		usage = float64(size) / float64(capacity) * 100
	}

	return CacheStats{
		Size:     size,
		Capacity: capacity,
		Usage:    usage,
	}
}

func (t *Tree[T]) getCacheCapacity() int {
	capacity := 0
	for _, shard := range t.cache.shards {
		shard.mu.Lock()
		capacity += shard.capacity
		shard.mu.Unlock()
	}
	return capacity
}

// Delete удаляет элемент по ID
func (t *Tree[T]) Delete(id uint64) bool {
	// Проверяем существование
	val, exists := t.items.Load(id)
	if !exists {
		return false
	}
	
	item := val.(T)
	
	// Удаляем из sync.Map
	t.items.Delete(id)
	t.itemCount.Add(^uint64(0)) // Декремент (атомарно вычитаем 1)
	
	// Удаляем из cache
	t.cache.delete(id)
	
	// Удаляем из TopN кешей
	t.topMinCache.Remove(item)
	t.topMaxCache.Remove(item)
	
	// Помечаем элемент в дереве как удаленный
	t.deleteNode(t.root, item, 0)
	
	// Инвалидируем корневой хеш
	t.rootCacheValid.Store(false)
	
	return true
}

// DeleteBatch удаляет несколько элементов
func (t *Tree[T]) DeleteBatch(ids []uint64) int {
	if len(ids) == 0 {
		return 0
	}
	
	deleted := 0
	items := make([]T, 0, len(ids))
	
	// Собираем элементы для удаления
	for _, id := range ids {
		val, exists := t.items.Load(id)
		if !exists {
			continue
		}
		
		item := val.(T)
		items = append(items, item)
		
		// Удаляем из maps
		t.items.Delete(id)
		t.cache.delete(id)
		
		// Удаляем из TopN
		t.topMinCache.Remove(item)
		t.topMaxCache.Remove(item)
		
		deleted++
	}
	
	if deleted == 0 {
		return 0
	}
	
	t.itemCount.Add(^uint64(deleted-1)) // Атомарно вычитаем deleted
	
	// Удаляем из структуры дерева
	if len(items) < SmallBatchThreshold {
		// Простая блокировка для малых батчей
		t.mu.Lock()
		for _, item := range items {
			t.deleteNode(t.root, item, 0)
		}
		t.mu.Unlock()
	} else {
		// Параллельное удаление для больших батчей
		numWorkers := runtime.NumCPU()
		if numWorkers > 16 {
			numWorkers = 16
		}
		
		// ВАЖНО: не больше воркеров, чем элементов!
		if numWorkers > len(items) {
			numWorkers = len(items)
		}
		
		chunkSize := (len(items) + numWorkers - 1) / numWorkers
		var wg sync.WaitGroup
		
		for i := 0; i < numWorkers; i++ {
			start := i * chunkSize
			if start >= len(items) {
				break // Не запускаем лишние горутины
			}
			
			end := start + chunkSize
			if end > len(items) {
				end = len(items)
			}
			
			wg.Add(1)
			go func(chunk []T) {
				defer wg.Done()
				for _, item := range chunk {
					t.deleteNode(t.root, item, 0)
				}
			}(items[start:end])
		}
		
		wg.Wait()
	}
	
	t.rootCacheValid.Store(false)
	t.deleteCount.Add(uint64(deleted))
	return deleted
}

// deleteNode - ЕДИНЫЙ метод удаления с per-node блокировками
func (t *Tree[T]) deleteNode(node *Node[T], item T, depth int) {
	key := item.Key()
	
	node.mu.Lock()
	
	if depth >= t.maxDepth-1 {
		// Листовой уровень
		idx := key[len(key)-1]
		
		for i, k := range node.Keys {
			if k == idx {
				child := node.Children[i]
				node.mu.Unlock()
				
				child.mu.Lock()
				if child.IsLeaf {
					// Обнуляем значение и ставим deleted hash
					var zero T
					child.Value = zero
					child.Hash = DeletedNodeHash
					child.dirty.Store(false)
					t.deletedNodeCount.Add(1)
				}
				child.mu.Unlock()
				
				node.dirty.Store(true)
				t.dirtyNodes.Add(1)
				return
			}
		}
		
		node.mu.Unlock()
		return
	}
	
	// Промежуточный узел
	idx := key[depth]
	for i, k := range node.Keys {
		if k == idx {
			child := node.Children[i]
			node.mu.Unlock()
			t.deleteNode(child, item, depth+1)
			node.dirty.Store(true)
			t.dirtyNodes.Add(1)
			return
		}
	}
	
	node.mu.Unlock()
}

// deleteNodeUnderGlobalLock - БЕЗ per-node блокировок
func (t *Tree[T]) deleteNodeUnderGlobalLock(node *Node[T], item T, depth int) {
	key := item.Key()
	
	if depth >= t.maxDepth-1 {
		idx := key[len(key)-1]
		
		for i, k := range node.Keys {
			if k == idx {
				child := node.Children[i]
				if child.IsLeaf {
					var zero T
					child.Value = zero
					child.Hash = DeletedNodeHash
					child.dirty.Store(false)
					node.dirty.Store(true)
					t.dirtyNodes.Add(1)
					t.deletedNodeCount.Add(1)
					return
				}
			}
		}
		return
	}
	
	idx := key[depth]
	for i, k := range node.Keys {
		if k == idx {
			t.deleteNodeUnderGlobalLock(node.Children[i], item, depth+1)
			node.dirty.Store(true)
			t.dirtyNodes.Add(1)
			return
		}
	}
}



// Exists проверяет существование элемента
func (t *Tree[T]) Exists(id uint64) bool {
	_, exists := t.items.Load(id)
	return exists
}

// GetDeletedCount возвращает количество удаленных узлов в дереве
func (t *Tree[T]) GetDeletedCount() int {
	return int(t.deletedNodeCount.Load())
}

// countDeletedNodes подсчитывает удаленные узлы рекурсивно
func (t *Tree[T]) countDeletedNodes(node *Node[T]) int {
	if node == nil {
		return 0
	}
	
	node.mu.RLock()
	defer node.mu.RUnlock()
	
	if node.IsLeaf {
		// Проверяем ID вместо прямого сравнения
		if node.Value.ID() == 0 {
			return 1
		}
		return 0
	}
	
	count := 0
	for _, child := range node.Children {
		count += t.countDeletedNodes(child)
	}
	
	return count
}

// ============================================
// Ordered Access API (TopN)
// ============================================

// GetMin возвращает минимальный элемент O(1)
func (t *Tree[T]) GetMin() (T, bool) {
	return t.topMinCache.GetFirst()
}

// GetMax возвращает максимальный элемент O(1)
func (t *Tree[T]) GetMax() (T, bool) {
	return t.topMaxCache.GetFirst()
}

// GetTopMin возвращает top-N минимальных элементов O(1)
// Элементы отсортированы по возрастанию ключа
func (t *Tree[T]) GetTopMin(n int) []T {
	return t.topMinCache.GetTop(n)
}

// GetTopMax возвращает top-N максимальных элементов O(1)
// Элементы отсортированы по убыванию ключа
func (t *Tree[T]) GetTopMax(n int) []T {
	return t.topMaxCache.GetTop(n)
}

// GetMinKey возвращает минимальный ключ O(1)
func (t *Tree[T]) GetMinKey() (uint64, bool) {
	item, ok := t.GetMin()
	if !ok {
		return 0, false
	}
	return keyToUint64(item.Key()), true
}

// GetMaxKey возвращает максимальный ключ O(1)
func (t *Tree[T]) GetMaxKey() (uint64, bool) {
	item, ok := t.GetMax()
	if !ok {
		return 0, false
	}
	return keyToUint64(item.Key()), true
}

// IsTopNEnabled проверяет, активен ли TopN кеш
func (t *Tree[T]) IsTopNEnabled() bool {
	return t.topMinCache.IsEnabled()
}

// GetTopNCapacity возвращает размер TopN кеша
func (t *Tree[T]) GetTopNCapacity() int {
	if !t.topMinCache.IsEnabled() {
		return 0
	}
	return t.topMinCache.capacity
}

// ClearTopN очищает TopN кеши (полезно при rebuild)
func (t *Tree[T]) ClearTopN() {
	t.topMinCache.Clear()
	t.topMaxCache.Clear()
}

// IterTopMin возвращает итератор для минимальных элементов
// Итератор обходит элементы в порядке возрастания ключа
func (t *Tree[T]) IterTopMin() *TopNIterator[T] {
	return t.topMinCache.GetIteratorMin()
}

// IterTopMax возвращает итератор для максимальных элементов
// Итератор обходит элементы в порядке убывания ключа
func (t *Tree[T]) IterTopMax() *TopNIterator[T] {
	return t.topMaxCache.GetIteratorMax()
}

//Работа с лукап-таблицей 
func (t *Tree[T]) LookupAdd(key [16]byte, val uint64) {
	if t.useAppLookup == false {
		return
	}
	
	t.appLookup.Store(key, val)
}

func (t *Tree[T]) LookupGet(key [16]byte) (uint64, bool) {
	if t.useAppLookup == false {
		return 0, false
	}
	
	val, ok := t.appLookup.Load(key)
	
	if val == nil {
		return uint64(0), false
	}
	
	return val.(uint64), ok
}

func (t *Tree[T]) LookupDel(key [16]byte) {
	if t.useAppLookup == false {
		return
	}
	
	t.appLookup.Delete(key)
}

func (t *Tree[T]) LookupClear() {
	if t.useAppLookup == false {
		return
	}
	
	t.appLookup.Clear()
}