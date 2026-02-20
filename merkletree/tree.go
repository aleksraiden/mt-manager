package merkletree

import (
	"runtime"
	"sync"
	"slices"
	"cmp"
	"sync/atomic"

	"github.com/zeebo/blake3"
)

const (
	NodeBlockSize 			= 8192 // 8k nodes per block, ~1-2MB depending on T
	
	// Агрессивные пороги для больших систем
	SmallBatchThreshold    = 32  
	ParallelBatchThreshold = 512   // 32..512 → sequential
    MegaParallelThreshold  = 8196  // 512..8196 → parallel, >8196 → mega
)

var (
	// Константный хеш для удаленных узлов
	DeletedNodeHash = blake3.Sum256([]byte("__DELETED_NODE__"))
)

var blake3HasherPool = sync.Pool{
    New: func() any { return blake3.New() },
}

var childHashSlicePool = sync.Pool{
    New: func() any {
        s := make([][32]byte, 0, 256)
        return &s
    },
}

// Tree - убираем избыточный padding, оставляем только критичный
type Tree[T Hashable] struct {
	root       *Node[T]
	items      *ShardedItemMap[T] //sync.Map
	itemCount  atomic.Uint64
	arena      *ConcurrentArena[T]
	cache      *ShardedCache[T]
	maxDepth   int
	keyOrder   KeyOrder
	
	mu         sync.RWMutex
	
	topNCache   	*TopNCache[T]	//Универсальный кеш (только один может быть)
	
	name            string  // Имя дерева (для снапшотов)

	// Lazy hashing (компактно)
	dirtyNodes     atomic.Uint64
	cachedRoot     atomic.Value		//[32]byte
	rootCacheValid atomic.Bool
	
	// Метрики
	insertCount        atomic.Uint64
	deleteCount        atomic.Uint64
	deletedNodeCount   atomic.Uint64
	getCount           atomic.Uint64
	cacheHits          atomic.Uint64
	cacheMisses        atomic.Uint64
	computeCount       atomic.Uint64
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

	arena := newConcurrentArena[T](DefaultArenaBlockSize)
	root := arena.alloc()
	
	if cfg.MaxDepth < 1 || cfg.MaxDepth > 8 {
        panic("merkletree: MaxDepth must be between 1 and 8 (key length)")
    }

	t := &Tree[T]{
		root:     root,
		arena:    arena,
		cache:    newShardedCache[T](cfg.CacheSize, cfg.CacheShards),
		maxDepth: cfg.MaxDepth,
		keyOrder: cfg.KeyEncoding,
		items:	  NewShardedItemMap[T](),
	}
	
	// СТАЛО: TopN > 0 всегда создаёт кеш, флаги уточняют режим
	if cfg.TopN > 0 {
		if cfg.UseTopNMax && !cfg.UseTopNMin {
			t.topNCache = NewTopNCache[T](cfg.TopN, false) // явный max-heap
		} else {
			t.topNCache = NewTopNCache[T](cfg.TopN, true)  // min-heap (дефолт)
		}
	}
	
	
	
	t.cachedRoot.Store([32]byte{}) // zero value
	
	return t
}

// normalizeKey применяет нужный порядок байт.
// При KeyOrderMSB — no-op (нулевой оверхед).
// При KeyOrderLSB — reverses bytes.
func (t *Tree[T]) normalizeKey(key [8]byte) [8]byte {
    if t.keyOrder == KeyOrderLSB {
        return [8]byte{key[7], key[6], key[5], key[4],
                       key[3], key[2], key[1], key[0]}
    }
    return key
}

// encodeID конвертирует uint64 в ключ с учётом keyOrder.
// Используется в RangeQueryByID.
func (t *Tree[T]) encodeID(id uint64) [8]byte {
    if t.keyOrder == KeyOrderLSB {
        return KeyLSB(id)
    }
    return KeyMSB(id)
}

//Одиночная вставка с блокировкой 
func (t *Tree[T]) Insert(item T) {
    t.items.Store(item.ID(), item)
    t.itemCount.Add(1)
    t.cache.put(item.ID(), item)
    t.insertNode(t.root, item, 0)   // per-node locking внутри
    t.rootCacheValid.Store(false)

    if t.topNCache != nil {
        t.topNCache.TryInsert(item)
    }

    t.insertCount.Add(1)
}

// InsertBatch вставляет батч элементов (автоматический выбор стратегии)
func (t *Tree[T]) InsertBatch(items []T) {
    if len(items) == 0 {
        return
    }
    switch {
    case len(items) < SmallBatchThreshold:
        t.insertBatchSimple(items)
    case len(items) < ParallelBatchThreshold:
        t.insertBatchSequential(items)
    case len(items) < MegaParallelThreshold:
        t.insertBatchParallel(items)  
    default:
        t.insertBatchMegaParallel(items)
    }
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
		if t.topNCache != nil {
			t.topNCache.TryInsert(item)
		}
		
		t.insertNodeUnderGlobalLock(t.root, item, 0)
	}
	
	t.itemCount.Add(uint64(len(items)))
	t.insertCount.Add(uint64(len(items)))

	t.rootCacheValid.Store(false)
}

// insertBatchSequential последовательная вставка (per-node locking)
func (t *Tree[T]) insertBatchSequential(items []T) {
	for _, item := range items {
		t.items.Store(item.ID(), item)
		t.cache.put(item.ID(), item)
		
		t.insertNode(t.root, item, 0)
		
		if t.topNCache != nil {
			t.topNCache.TryInsert(item)
		}
	}
	t.itemCount.Add(uint64(len(items)))
	t.insertCount.Add(uint64(len(items)))

	t.rootCacheValid.Store(false)
}

// insertBatchParallel параллельная вставка
func (t *Tree[T]) insertBatchParallel(items []T) {
    if len(items) == 0 {
        return
    }

    // Нельзя партиционировать при maxDepth=1 — root сам является листом
    if t.maxDepth <= 1 {
        t.insertBatchSequential(items)
        return
    }

    // Фаза 1: items + cache (без contention на дереве)
    numWorkers := runtime.NumCPU()
    if numWorkers > len(items) {
        numWorkers = len(items)
    }
    chunkSize := (len(items) + numWorkers - 1) / numWorkers

    type chunk struct{ start, end int }
    chunks := make([]chunk, 0, numWorkers)
    for i := 0; i < numWorkers; i++ {
        s := i * chunkSize
        if s >= len(items) {
            break
        }
        e := s + chunkSize
        if e > len(items) {
            e = len(items)
        }
        chunks = append(chunks, chunk{s, e})
    }

    var wg sync.WaitGroup
    for _, c := range chunks {
        wg.Add(1)
        go func(slice []T) {
            defer wg.Done()
            for _, item := range slice {
                t.items.Store(item.ID(), item)
                t.cache.put(item.ID(), item)
                if t.topNCache != nil {
                    t.topNCache.TryInsert(item)
                }
            }
        }(items[c.start:c.end])
    }
    wg.Wait()
    t.itemCount.Add(uint64(len(items)))
    t.insertCount.Add(uint64(len(items)))

    // Фаза 2: группируем по key[0] — тот же байт, что использует insertNode на depth=0
    // Все items с одинаковым key[0] гарантированно попадут в одно поддерево
    type bucket struct {
        keyByte byte
        items   []T
    }
    bucketMap := make(map[byte]*bucket, 256)
    for _, item := range items {
        kb := t.normalizeKey(item.Key())[0]
        b, ok := bucketMap[kb]
        if !ok {
            b = &bucket{keyByte: kb}
            bucketMap[kb] = b
        }
        b.items = append(b.items, item)
    }

    // Фаза 3: под одним root.Lock() находим/создаём всех нужных детей
    // Один lock вместо N конкурентных обращений к root
    type subtreeWork struct {
        child *Node[T]
        items []T
    }
    work := make([]subtreeWork, 0, len(bucketMap))

    t.root.mu.Lock()
    for _, b := range bucketMap {
        // Ищем существующего ребёнка — в точности как insertNode при depth=0
        childIdx := -1
        for i, k := range t.root.Keys {
            if k == b.keyByte {
                childIdx = i
                break
            }
        }

        var child *Node[T]
        if childIdx >= 0 {
            child = t.root.Children[childIdx]
        } else {
            // Создаём нового ребёнка (промежуточный, не лист)
            child = t.arena.alloc()
            t.root.Keys = append(t.root.Keys, b.keyByte)
            t.root.Children = append(t.root.Children, child)
            t.root.dirty.Store(true)
            t.dirtyNodes.Add(1)
        }
        work = append(work, subtreeWork{child, b.items})
    }
    t.root.mu.Unlock()

    // Фаза 4: каждая горутина работает с эксклюзивным поддеревом — нет contention!
    for _, w := range work {
        wg.Add(1)
        go func(child *Node[T], slice []T) {
            defer wg.Done()
            for _, item := range slice {
                // depth=1: root уже обработан, начинаем с его детей
                t.insertNode(child, item, 1)
            }
        }(w.child, w.items)
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
				if t.topNCache != nil {
					t.topNCache.TryInsert(item)
				}
			}
		}(items[start:end])
	}

	wg.Wait()
	t.itemCount.Add(uint64(len(items)))
	t.insertCount.Add(uint64(len(items)))

	// Фаза 2: Группировка
	groupSize := 256
	if len(items) > 10000 {
		groupSize = 4096
	}
	
	groups := make([][]T, groupSize)
	for _, item := range items {
		key := t.normalizeKey(item.Key())
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
	key := t.normalizeKey(item.Key())
	
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
	key := t.normalizeKey(item.Key())
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
    count := t.getCount.Add(1)

    if item, ok := t.cache.tryGet(id); ok {
        t.cacheHits.Add(1)
        if count%100 == 0 {     // один вызов на каждое кратное 100
            t.cache.put(id, item)
        }
        return item, true
    }

    if item, ok := t.items.Load(id); ok {
        //item := val.(T)
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
    if t.rootCacheValid.Load() {
        if val := t.cachedRoot.Load(); val != nil {
            return val.([32]byte)
        }
    }

    t.mu.RLock()
    newRoot := t.computeNodeHash(t.root, 0, true)
    t.mu.RUnlock()

    for attempts := 0; attempts < 16; attempts++ {
        currentDirty := t.dirtyNodes.Load()

        // CAS первый — только если успех, публикуем
        if t.dirtyNodes.CompareAndSwap(currentDirty, 0) {
            t.cachedRoot.Store(newRoot)
            t.rootCacheValid.Store(true)
            return newRoot
        }

        // CAS провалился → кто-то изменил дерево → пересчитываем
        t.mu.RLock()
        newRoot = t.computeNodeHash(t.root, 0, true)
        t.mu.RUnlock()
    }

    // Fallback под полным локом
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
	slices.SortFunc(indices, func(a, b int) int {
		return cmp.Compare(keys[a], keys[b])
	})
	
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
	//childHashes := make([][32]byte, count)
	chPtr := childHashSlicePool.Get().(*[][32]byte)
	var childHashes [][32]byte
	if cap(*chPtr) < count {
		childHashes = make([][32]byte, count)
		*chPtr = childHashes
	} else {
		childHashes = (*chPtr)[:count]
	}
	
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
	hasher := blake3HasherPool.Get().(*blake3.Hasher)
	hasher.Reset()
	for i, idx := range indices {
		hasher.Write([]byte{keys[idx]})
		hasher.Write(childHashes[i][:])
	}
	var result [32]byte
	copy(result[:], hasher.Sum(nil))
	blake3HasherPool.Put(hasher)
	
	*chPtr = (*chPtr)[:0]                    // сбрасываем len, cap сохраняем
	childHashSlicePool.Put(chPtr)
	
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


// collectLeaves итеративно собирает все листья из поддерева.
// Explicit stack вместо рекурсии: нет frame allocations, нет риска stack overflow.
func (t *Tree[T]) collectLeaves(root *Node[T], leaves *[]*Node[T]) {
    if root == nil {
        return
    }

    // Pre-allocate stack: maxDepth=8, типичный branching factor 16-64,
    // 64 слотов достаточно для большинства случаев без realloc.
    stack := make([]*Node[T], 0, 64)
    stack = append(stack, root)

    for len(stack) > 0 {
        // Pop: берём с конца (не с начала — дешевле, O(1) vs O(n))
        top := len(stack) - 1
        node := stack[top]
        stack = stack[:top]

        node.mu.RLock()
        isLeaf := node.IsLeaf
        children := node.Children
        node.mu.RUnlock()

        if isLeaf {
            *leaves = append(*leaves, node)
            continue
        }

        // Push детей в обратном порядке, чтобы left-first traversal
        // (первый ребёнок обрабатывается первым)
        for i := len(children) - 1; i >= 0; i-- {
            if children[i] != nil {
                stack = append(stack, children[i])
            }
        }
    }
}

func (t *Tree[T]) computeLeafHashesBatch(leaves []*Node[T]) map[*Node[T]][32]byte {
    if len(leaves) == 0 {
        return nil
    }

    numWorkers := runtime.NumCPU()
    if numWorkers > 32 {
        numWorkers = 32
    }
    if len(leaves) < numWorkers*2 {
        numWorkers = (len(leaves) + 1) / 2
    }

    // результаты в плоском slice,
    // каждый воркер пишет в свой эксклюзивный диапазон — нет мьютекса!
	// нулевой contention во время параллельной фазы. Финальный merge — O(N) в одном потоке без блокировок.
    resultHashes := make([][32]byte, len(leaves))

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
        // Передаём subslice — горутин владеет только своим диапазоном
        go func(chunk []*Node[T], out [][32]byte) {
            defer wg.Done()
            for i, leaf := range chunk {
                if leaf == nil {
                    continue
                }

                leaf.mu.RLock()
                isDirty := leaf.dirty.Load()
                value := leaf.Value
                currentHash := leaf.Hash
                leaf.mu.RUnlock()

                var hash [32]byte
                if isDirty || currentHash == ([32]byte{}) {
                    hash = value.Hash()
                    leaf.mu.Lock()
                    leaf.Hash = hash
                    leaf.dirty.Store(false)
                    leaf.mu.Unlock()
                } else {
                    hash = currentHash
                }

                out[i] = hash // пишем в эксклюзивный диапазон, без lock
            }
        }(leaves[start:end], resultHashes[start:end])
    }

    wg.Wait()

    // Строим map однократно в одном потоке, после завершения всех горутин
    results := make(map[*Node[T]][32]byte, len(leaves))
    for i, leaf := range leaves {
        if leaf != nil {
            results[leaf] = resultHashes[i]
        }
    }
    return results
}

func (t *Tree[T]) Size() int {
	return int(t.itemCount.Load())
}

func (t *Tree[T]) GetAllItems() []T {
	items := make([]T, 0, t.itemCount.Load())

	t.items.Range(func(_ uint64, value T) bool {
		items = append(items, value)
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

	t.items.Clear()
	t.itemCount.Store(0)
	t.arena.reset()
	t.root = t.arena.alloc()
	t.cache.clear()
	t.rootCacheValid.Store(false)
	
	// Очищаем TopN кеши
	if t.topNCache != nil {
		t.topNCache.Clear()
	}
	
	t.dirtyNodes.Store(0)
	t.deletedNodeCount.Store(0)
}

//Удаление и пересборка дерева, чтобы GC мог очистить удаленные элементы 
func (t *Tree[T]) Compact() {
    t.mu.Lock()
    defer t.mu.Unlock()
	
    t.arena.reset()
    t.root = t.arena.alloc()
    t.deletedNodeCount.Store(0)
    t.items.Range(func(_ uint64, item T) bool {
        t.insertNodeUnderGlobalLock(t.root, item, 0)
        return true
    })
    t.rootCacheValid.Store(false)
}

type Stats struct {
	TotalItems        int
	DeletedNodes      int    
	AllocatedNodes    int
	CacheSize         int
	DirtyNodes        uint64
	InsertCount       uint64
	DeleteCount       uint64 
	GetCount          uint64
	CacheHits         uint64
	CacheMisses       uint64
	CacheHitRate      float64
	ComputeCount      uint64
}

// GetStats возвращает полную статистику дерева
func (t *Tree[T]) GetStats() Stats {
	hits := t.cacheHits.Load()
	misses := t.cacheMisses.Load()
	hitRate := 0.0
	if hits+misses > 0 {
		hitRate = float64(hits) / float64(hits+misses) * 100
	}

	return Stats{
		TotalItems:        int(t.itemCount.Load()),
		DeletedNodes:      int(t.deletedNodeCount.Load()), 
		AllocatedNodes:    t.arena.allocated(),
		CacheSize:         t.cache.size(),
		DirtyNodes:        t.dirtyNodes.Load(),
		InsertCount:       t.insertCount.Load(),
		DeleteCount:       t.deleteCount.Load(),
		GetCount:          t.getCount.Load(),
		CacheHits:         hits,
		CacheMisses:       misses,
		CacheHitRate:      hitRate,
		ComputeCount:      t.computeCount.Load(),
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
		shard.mu.RLock()
		capacity += shard.capacity
		shard.mu.RUnlock()
	}
	return capacity
}

// Delete удаляет элемент по ID
func (t *Tree[T]) Delete(id uint64) bool {
	// Проверяем существование
	item, exists := t.items.Load(id)
	if !exists {
		return false
	}
	
	//item := val.(T)
	
	// Удаляем из sync.Map
	t.items.Delete(id)
	t.itemCount.Add(^uint64(0)) // Декремент (атомарно вычитаем 1)
	
	// Удаляем из cache
	t.cache.delete(id)
	
	// Удаляем из TopN кешей
	if t.topNCache != nil {
		t.topNCache.Remove(item)
	}
	
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
		item, exists := t.items.Load(id)
		if !exists {
			continue
		}
		
		//item := val.(T)
		items = append(items, item)
		
		// Удаляем из maps
		t.items.Delete(id)
		t.cache.delete(id)
		
		// Удаляем из TopN
		if t.topNCache != nil {
			t.topNCache.Remove(item)
		}
		
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
	key := t.normalizeKey(item.Key())
	
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

//INFO: В обоих случаях GetTop - так как кеш один и порядок задан в конфиге 

//В зависимости от типа кеша Min или Max
func (t *Tree[T]) GetTop() T {
	var zero T
	
	if t.topNCache == nil {
		return zero //[]T{}
	}
	
	v, err := t.topNCache.GetFirst()
	
	if err == true {
		return v 
	}
	
	return zero
}

// GetMin возвращает минимальный элемент O(1)
func (t *Tree[T]) GetMin() (T, bool) {
	if t.topNCache == nil {
		var zero T
		return zero, false
	}
	
	return t.topNCache.GetFirst()
}

// GetMax возвращает максимальный элемент O(1)
func (t *Tree[T]) GetMax() (T, bool) {
	if t.topNCache == nil {
		var zero T
		return zero, false
	}
	
	return t.topNCache.GetFirst()
}

// GetTopMin возвращает top-N минимальных элементов O(1)
// Элементы отсортированы по возрастанию ключа
func (t *Tree[T]) GetTopMin(n int) []T {
	if t.topNCache == nil {
		return nil //[]T{}
	}
	
	return t.topNCache.GetTop(n)
}

// GetTopMax возвращает top-N максимальных элементов O(1)
// Элементы отсортированы по убыванию ключа
func (t *Tree[T]) GetTopMax(n int) []T {
	if t.topNCache == nil {
		return nil //[]T{}
	}
	
	return t.topNCache.GetTop(n)
}

// GetMinKey возвращает минимальный ключ O(1)
func (t *Tree[T]) GetMinKey() (uint64, bool) {
	item, ok := t.GetMin()
	if !ok {
		return 0, false
	}
	return keyToUint64(t.normalizeKey(item.Key())), true
}

// GetMaxKey возвращает максимальный ключ O(1)
func (t *Tree[T]) GetMaxKey() (uint64, bool) {
	item, ok := t.GetMax()
	if !ok {
		return 0, false
	}
	return keyToUint64(t.normalizeKey(item.Key())), true
}

// IsTopNEnabled проверяет, активен ли TopN кеш
func (t *Tree[T]) IsTopNEnabled() bool {
	if t.topNCache != nil {
		return t.topNCache.IsEnabled()
	}
	
	return false
}

// GetTopNCapacity возвращает размер TopN кеша
func (t *Tree[T]) GetTopNCapacity() int {
	if t.topNCache == nil {
		return 0
	} 
	
	if !t.topNCache.IsEnabled() {
		return 0
	}
	return t.topNCache.capacity
}

// ClearTopN очищает TopN кеши (полезно при rebuild)
func (t *Tree[T]) ClearTopN() {
	if t.topNCache != nil {
		t.topNCache.Clear()
	}
}

// IterTopMin возвращает итератор для минимальных элементов
// Итератор обходит элементы в порядке возрастания ключа
func (t *Tree[T]) IterTopMin() *TopNIterator[T] {
    if t.topNCache != nil {
        return t.topNCache.GetIteratorMin()
    }
    return NewTopNIterator[T](nil)  // HasNext() → false, Next() → zero,false
}

// IterTopMax возвращает итератор для максимальных элементов
// Итератор обходит элементы в порядке убывания ключа
func (t *Tree[T]) IterTopMax() *TopNIterator[T] {
    if t.topNCache != nil {
        return t.topNCache.GetIteratorMax()
    }
    return NewTopNIterator[T](nil)  // безопасный пустой итератор
}
