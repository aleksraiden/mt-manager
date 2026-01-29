package merkletree

import (
	"fmt"
	"sync"
	"sort"
	"runtime"
	"github.com/zeebo/blake3"
)

// TreeManager управляет несколькими Merkle-деревьями
// и вычисляет общий корневой хеш из всех деревьев
type TreeManager[T Hashable] struct {
	trees map[string]*Tree[T]
	config *Config
	mu     sync.RWMutex
	
	// Поля для встроенного мета-дерева
    globalRootCache [32]byte      // Кешированный глобальный корень
    globalRootDirty bool           // Флаг "грязного" состояния кеша
    treeRootCache   map[string][32]byte  // Кеш корней отдельных деревьев
	
	snapshotMgr     *SnapshotManager[T]
}

// NewManager создает новый менеджер деревьев
func NewManager[T Hashable](cfg *Config) *TreeManager[T] {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return &TreeManager[T]{
		trees:           make(map[string]*Tree[T]),  // Tree
		config:          cfg,
		treeRootCache:   make(map[string][32]byte),  // Инициализация кеша
		globalRootDirty: true,                        // Начально грязный
	}
}

// NewManagerWithSnapshot создает TreeManager с поддержкой снапшотов
// snapshotPath - путь к директории для PebbleDB (если пустой, снапшоты отключены)
func NewManagerWithSnapshot[T Hashable](cfg *Config, snapshotPath string) (*TreeManager[T], error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	
	tm := &TreeManager[T]{
		trees:           make(map[string]*Tree[T]),
		config:          cfg,
		treeRootCache:   make(map[string][32]byte),
		globalRootDirty: true,
	}
	
	if snapshotPath != "" {
		// Используем все CPU для параллелизма
		workers := runtime.NumCPU()
		if workers > 16 {
			workers = 16 // Cap для разумного использования
		}
		
		snapshotMgr, err := NewSnapshotManager[T](snapshotPath, workers)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize snapshot manager: %w", err)
		}
		tm.snapshotMgr = snapshotMgr
	}
	
	return tm, nil
}

// CreateTree создает новое дерево с указанным именем
func (m *TreeManager[T]) CreateTree(name string) (*Tree[T], error) { 
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.trees[name]; exists {
		return nil, fmt.Errorf("дерево с именем '%s' уже существует", name)
	}

	tree := New[T](m.config) 
	tree.name = name
	m.trees[name] = tree

	// Инвалидируем глобальный корень
	m.globalRootDirty = true

	return tree, nil
}

// GetTree возвращает дерево по имени
func (m *TreeManager[T]) GetTree(name string) (*Tree[T], bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tree, exists := m.trees[name]
	return tree, exists
}

// GetOrCreateTree получает существующее дерево или создает новое
func (m *TreeManager[T]) GetOrCreateTree(name string) *Tree[T] {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tree, exists := m.trees[name]; exists {
		return tree
	}

	tree := New[T](m.config)
	tree.name = name
	m.trees[name] = tree
	
	// Инвалидируем глобальный корень
	m.globalRootDirty = true
	
	return tree
}

// RemoveTree удаляет дерево по имени
func (m *TreeManager[T]) RemoveTree(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.trees[name]; !exists {
		return false
	}

	delete(m.trees, name)
	delete(m.treeRootCache, name)

	// Инвалидируем глобальный корень
	m.globalRootDirty = true

	return true
}

// ListTrees возвращает список имен всех деревьев
func (m *TreeManager[T]) ListTrees() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.trees))
	for name := range m.trees {
		names = append(names, name)
	}

	return names
}

func (m *TreeManager[T]) ComputeGlobalRoot() [32]byte {
	m.mu.RLock()

	// Проверяем, нужен ли пересчет
	if !m.globalRootDirty {
		// Проверяем, не изменились ли деревья напрямую
		// Быстрая проверка: сравниваем кешированные корни с реальными
		needsUpdate := false
		for name, tree := range m.trees {
			cachedRoot, exists := m.treeRootCache[name]
			if !exists {
				needsUpdate = true
				break
			}
			// Вычисляем реальный корень и сравниваем
			actualRoot := tree.ComputeRoot()
			if actualRoot != cachedRoot {
				needsUpdate = true
				break
			}
		}
		
		if !needsUpdate {
			defer m.mu.RUnlock()
			return m.globalRootCache
		}
	}
	m.mu.RUnlock()

	// Нужен пересчет - берем write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check после получения write lock
	if !m.globalRootDirty {
		// Повторная проверка актуальности кешей
		needsUpdate := false
		for name, tree := range m.trees {
			cachedRoot, exists := m.treeRootCache[name]
			if !exists {
				needsUpdate = true
				break
			}
			actualRoot := tree.ComputeRoot()
			if actualRoot != cachedRoot {
				needsUpdate = true
				m.treeRootCache[name] = actualRoot // Обновляем кеш
			}
		}
		
		if !needsUpdate {
			return m.globalRootCache
		}
	}

	if len(m.trees) == 0 {
		m.globalRootCache = [32]byte{}
		m.globalRootDirty = false
		return m.globalRootCache
	}

	// Получаем отсортированные имена деревьев для детерминизма
	names := make([]string, 0, len(m.trees))
	for name := range m.trees {
		names = append(names, name)
	}
	sort.Strings(names)

	// Собираем корни всех деревьев (всегда пересчитываем для надежности)
	roots := make([][32]byte, len(names))
	for i, name := range names {
		roots[i] = m.trees[name].ComputeRoot()
		m.treeRootCache[name] = roots[i] // Обновляем кеш
	}

	// Вычисляем глобальный корень через итеративное хеширование
	m.globalRootCache = m.computeMerkleRoot(roots)
	m.globalRootDirty = false

	return m.globalRootCache
}

// computeMerkleRoot итеративно вычисляет корень Меркла из листьев
// Не создает промежуточные структуры данных
func (m *TreeManager[T]) computeMerkleRoot(hashes [][32]byte) [32]byte {
    if len(hashes) == 0 {
        return [32]byte{}
    }
    
    if len(hashes) == 1 {
        return hashes[0]
    }
    
    // Итеративное построение снизу вверх без рекурсии
    currentLevel := hashes
    
    for len(currentLevel) > 1 {
        nextLevel := make([][32]byte, 0, (len(currentLevel)+1)/2)
        
        for i := 0; i < len(currentLevel); i += 2 {
            hasher := blake3.New()
            
            // Пишем левый хеш
            hasher.Write(currentLevel[i][:])
            
            // Пишем правый хеш (если есть, иначе используем нулевой хеш)
            if i+1 < len(currentLevel) {
                hasher.Write(currentLevel[i+1][:])
            } else {
                // Вместо дублирования используем нулевой хеш
                var zero [32]byte
                hasher.Write(zero[:])
            }
            
            var parentHash [32]byte
            copy(parentHash[:], hasher.Sum(nil))
            nextLevel = append(nextLevel, parentHash)
        }
        
        currentLevel = nextLevel
    }
    
    return currentLevel[0]
}

// InvalidateGlobalRoot помечает глобальный корень как требующий пересчета
// Вызывается при любых изменениях в деревьях
func (m *TreeManager[T]) InvalidateGlobalRoot() {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.globalRootDirty = true
}

// InvalidateTreeRoot инвалидирует кеш корня конкретного дерева
func (m *TreeManager[T]) InvalidateTreeRoot(treeName string) {
    m.mu.Lock()
    defer m.mu.Unlock()
    delete(m.treeRootCache, treeName)
    m.globalRootDirty = true
}

// GetMerkleProof возвращает Merkle proof для конкретного дерева
// Интегрированная версия без создания отдельной структуры MetaTree
func (m *TreeManager[T]) GetMerkleProof(treeName string) (*MerkleProof, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    
    if _, exists := m.trees[treeName]; !exists {
        return nil, fmt.Errorf("дерево '%s' не найдено", treeName)
    }
    
    // Получаем отсортированный список деревьев
    names := make([]string, 0, len(m.trees))
    for name := range m.trees {
        names = append(names, name)
    }
    sort.Strings(names)
    
    // Находим индекс целевого дерева
    targetIndex := -1
    for i, name := range names {
        if name == treeName {
            targetIndex = i
            break
        }
    }
    
    if targetIndex == -1 {
        return nil, fmt.Errorf("дерево '%s' не найдено в индексе", treeName)
    }
    
    // Собираем корни всех деревьев
    roots := make([][32]byte, len(names))
    for i, name := range names {
        roots[i] = m.trees[name].ComputeRoot()
    }
    
    // Вычисляем proof path
    proofPath, isLeft := m.computeProofPath(roots, targetIndex)
    
    return &MerkleProof{
        TreeName:   treeName,
        TreeRoot:   roots[targetIndex],
        ProofPath:  proofPath,
        IsLeft:     isLeft,
        GlobalRoot: m.ComputeGlobalRoot(),
    }, nil
}

// computeProofPath вычисляет proof path для элемента по индексу
// Идиоматичная версия без указателей на слайсы
func (m *TreeManager[T]) computeProofPath(hashes [][32]byte, targetIndex int) ([][32]byte, []bool) {
    if len(hashes) <= 1 {
        return nil, nil
    }
    
    proofPath := make([][32]byte, 0)
    isLeft := make([]bool, 0)
    
    currentLevel := hashes
    currentIndex := targetIndex
    
    // Идем снизу вверх по уровням
    for len(currentLevel) > 1 {
        nextLevel := make([][32]byte, 0, (len(currentLevel)+1)/2)
        nextIndex := currentIndex / 2
        
        for i := 0; i < len(currentLevel); i += 2 {
            hasher := blake3.New()
            
            var leftHash, rightHash [32]byte
            leftHash = currentLevel[i]
            
            if i+1 < len(currentLevel) {
                rightHash = currentLevel[i+1]
            }
            
            // Если текущий индекс в этой паре
            if i == (currentIndex &^1) { // currentIndex & ^1 обнуляет младший бит
                if currentIndex%2 == 0 {
                    // Целевой элемент слева - добавляем правый хеш
                    proofPath = append(proofPath, rightHash)
                    isLeft = append(isLeft, true)
                } else {
                    // Целевой элемент справа - добавляем левый хеш
                    proofPath = append(proofPath, leftHash)
                    isLeft = append(isLeft, false)
                }
            }
            
            hasher.Write(leftHash[:])
            if i+1 < len(currentLevel) {
                hasher.Write(rightHash[:])
            } else {
                var zero [32]byte
                hasher.Write(zero[:])
            }
            
            var parentHash [32]byte
            copy(parentHash[:], hasher.Sum(nil))
            nextLevel = append(nextLevel, parentHash)
        }
        
        currentLevel = nextLevel
        currentIndex = nextIndex
    }
    
    return proofPath, isLeft
}

// ComputeAllRoots вычисляет корневые хеши всех деревьев параллельно
// Возвращает map[имя_дерева]корневой_хеш
func (m *TreeManager[T]) ComputeAllRoots() map[string][32]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make(map[string][32]byte, len(m.trees))
	var wg sync.WaitGroup
	var resultMu sync.Mutex

	for name, tree := range m.trees {
		wg.Add(1)
		go func(n string, t *Tree[T]) {
			defer wg.Done()
			root := t.ComputeRoot()

			resultMu.Lock()
			results[n] = root
			resultMu.Unlock()
		}(name, tree)
	}

	wg.Wait()
	return results
}

// GetTotalStats возвращает суммарную статистику по всем деревьям
func (m *TreeManager[T]) GetTotalStats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := ManagerStats{
		TreeCount: len(m.trees),
		TreeStats: make(map[string]Stats),
	}

	for name, tree := range m.trees {
		treeStats := tree.GetStats()
		stats.TreeStats[name] = treeStats
		stats.TotalItems += treeStats.TotalItems
		stats.TotalNodes += treeStats.AllocatedNodes
		stats.TotalCacheSize += treeStats.CacheSize
	}

	return stats
}

// ManagerStats содержит статистику менеджера
type ManagerStats struct {
	TreeCount      int              // Количество деревьев
	TotalItems     int              // Всего элементов во всех деревьях
	TotalNodes     int              // Всего узлов
	TotalCacheSize int              // Общий размер кешей
	TreeStats      map[string]Stats // Статистика по каждому дереву
}

// InsertToTree вставляет элемент в указанное дерево
func (m *TreeManager[T]) InsertToTree(treeName string, item T) error {
    tree, exists := m.GetTree(treeName)
    if !exists {
        return fmt.Errorf("дерево '%s' не найдено", treeName)
    }
    
    tree.Insert(item)
    
    // Инвалидируем кеш
    m.InvalidateTreeRoot(treeName)
    
    return nil
}

// BatchInsertToTree вставляет батч элементов в указанное дерево
func (m *TreeManager[T]) BatchInsertToTree(treeName string, items []T) error {
    tree, exists := m.GetTree(treeName)
    if !exists {
        return fmt.Errorf("дерево '%s' не найдено", treeName)
    }
    
    tree.InsertBatch(items)
    
    // Инвалидируем кеш
    m.InvalidateTreeRoot(treeName)
    
    return nil
}

// ClearAll очищает все деревья
func (m *TreeManager[T]) ClearAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, tree := range m.trees {
		tree.Clear()
	}
}

// RemoveAll удаляет все деревья
func (m *TreeManager[T]) RemoveAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.trees = make(map[string]*Tree[T])
}

// VerifyTreeInclusion проверяет, что дерево включено в глобальный корень
func (m *TreeManager[T]) VerifyTreeInclusion(treeName string) (bool, error) {
	proof, err := m.GetMerkleProof(treeName) 
	if err != nil {
		return false, err
	}

	return proof.Verify(), nil
}

// RenameTree переименовывает дерево
func (m *TreeManager[T]) RenameTree(oldName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tree, exists := m.trees[oldName]
	if !exists {
		return fmt.Errorf("дерево '%s' не найдено", oldName)
	}

	if _, exists := m.trees[newName]; exists {
		return fmt.Errorf("дерево '%s' уже существует", newName)
	}

	m.trees[newName] = tree
	delete(m.trees, oldName)

	return nil
}

// TreeExists проверяет существование дерева
func (m *TreeManager[T]) TreeExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.trees[name]
	return exists
}

// GetTreeSize возвращает размер дерева по имени
func (m *TreeManager[T]) GetTreeSize(name string) (int, error) {
	tree, exists := m.GetTree(name)
	if !exists {
		return 0, fmt.Errorf("дерево '%s' не найдено", name)
	}

	return tree.Size(), nil
}

// GetTreeStats возвращает статистику конкретного дерева
func (m *TreeManager[T]) GetTreeStats(name string) (Stats, error) {
	tree, exists := m.GetTree(name)
	if !exists {
		return Stats{}, fmt.Errorf("дерево '%s' не найдено", name)
	}

	return tree.GetStats(), nil
}

// GetTreeRoot возвращает корень конкретного дерева
func (m *TreeManager[T]) GetTreeRoot(name string) ([32]byte, error) {
	tree, exists := m.GetTree(name)
	if !exists {
		return [32]byte{}, fmt.Errorf("дерево '%s' не найдено", name)
	}

	return tree.ComputeRoot(), nil
}

// GetFromTree получает элемент из конкретного дерева
func (m *TreeManager[T]) GetFromTree(treeName string, id uint64) (T, error) {
	tree, exists := m.GetTree(treeName)
	if !exists {
		var zero T
		return zero, fmt.Errorf("дерево '%s' не найдено", treeName)
	}

	item, ok := tree.Get(id)
	if !ok {
		var zero T
		return zero, fmt.Errorf("элемент %d не найден в дереве '%s'", id, treeName)
	}

	return item, nil
}

// ClearTree очищает конкретное дерево
func (m *TreeManager[T]) ClearTree(name string) error {
	tree, exists := m.GetTree(name)
	if !exists {
		return fmt.Errorf("дерево '%s' не найдено", name)
	}

	tree.Clear()
	return nil
}

// CloneTree создает копию дерева с новым именем
func (m *TreeManager[T]) CloneTree(sourceName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sourceTree, exists := m.trees[sourceName]
	if !exists {
		return fmt.Errorf("дерево '%s' не найдено", sourceName)
	}

	if _, exists := m.trees[newName]; exists {
		return fmt.Errorf("дерево '%s' уже существует", newName)
	}

	// Создаем новое дерево
	newTree := New[T](m.config)

	// Копируем все элементы
	items := sourceTree.GetAllItems()
	newTree.InsertBatch(items)

	m.trees[newName] = newTree

	return nil
}

// GetTreeInfo возвращает детальную информацию о дереве
func (m *TreeManager[T]) GetTreeInfo(name string) (TreeInfo, error) {
	tree, exists := m.GetTree(name)
	if !exists {
		return TreeInfo{}, fmt.Errorf("дерево '%s' не найдено", name)
	}

	stats := tree.GetStats()
	root := tree.ComputeRoot()

	return TreeInfo{
		Name:      name,
		Size:      stats.TotalItems,
		Nodes:     stats.AllocatedNodes,
		CacheSize: stats.CacheSize,
		Root:      root,
	}, nil
}

// TreeInfo информация о дереве
type TreeInfo struct {
	Name      string
	Size      int
	Nodes     int
	CacheSize int
	Root      [32]byte
}

// String форматированный вывод информации о дереве
func (ti TreeInfo) String() string {
	return fmt.Sprintf("Дерево '%s':\n  Элементов: %d\n  Узлов: %d\n  Кеш: %d\n  Корень: %x",
		ti.Name, ti.Size, ti.Nodes, ti.CacheSize, ti.Root[:16])
}

// GetAllTreesInfo возвращает информацию обо всех деревьях
func (m *TreeManager[T]) GetAllTreesInfo() []TreeInfo {
	names := m.ListTrees()
	infos := make([]TreeInfo, 0, len(names))

	for _, name := range names {
		if info, err := m.GetTreeInfo(name); err == nil {
			infos = append(infos, info)
		}
	}

	return infos
}

// CreateTreeWithConfig создает дерево с кастомной конфигурацией
func (m *TreeManager[T]) CreateTreeWithConfig(name string, cfg *Config) (*Tree[T], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.trees[name]; exists {
		return nil, fmt.Errorf("дерево '%s' уже существует", name)
	}

	tree := New[T](cfg)
	m.trees[name] = tree
	return tree, nil
}

// SetDefaultConfig устанавливает конфигурацию по умолчанию для новых деревьев
func (m *TreeManager[T]) SetDefaultConfig(cfg *Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = cfg
}

// GetStats возвращает общую статистику менеджера
func (tm *TreeManager[T]) GetStats() ManagerStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	stats := ManagerStats{
		TreeCount: len(tm.trees),
	}

	for _, tree := range tm.trees {
		treeStats := tree.GetStats()
		stats.TotalItems += treeStats.TotalItems
		stats.TotalNodes += treeStats.AllocatedNodes
		stats.TotalCacheSize += treeStats.CacheSize
	}

	return stats
}

// ============================================
// Snapshot API (Lock-Free)
// ============================================

// CreateSnapshot создает снапшот синхронно
// Блокировка: ~80µs
func (m *TreeManager[T]) CreateSnapshot() ([32]byte, error) {
	if m.snapshotMgr == nil {
		return [32]byte{}, fmt.Errorf("snapshot manager not initialized")
	}
	return m.snapshotMgr.CreateSnapshot(m, DefaultSnapshotOptions())
}

// CreateSnapshotAsync создает снапшот асинхронно (рекомендуется!)
// Блокировка: 0µs (всё в фоне)
// Возвращает канал для получения результата
func (m *TreeManager[T]) CreateSnapshotAsync() <-chan SnapshotResult {
	if m.snapshotMgr == nil {
		ch := make(chan SnapshotResult, 1)
		ch <- SnapshotResult{Error: fmt.Errorf("snapshot manager not initialized")}
		close(ch)
		return ch
	}
	
	opts := DefaultSnapshotOptions()
	opts.Async = true
	return m.snapshotMgr.CreateSnapshotAsync(m, opts)
}

// LoadFromSnapshot загружает снапшот
// version == nil загружает последний
func (m *TreeManager[T]) LoadFromSnapshot(version *[32]byte) error {
	if m.snapshotMgr == nil {
		return fmt.Errorf("snapshot manager not initialized")
	}
	return m.snapshotMgr.LoadSnapshot(m, version)
}

// GetSnapshotMetadata возвращает метаданные
func (m *TreeManager[T]) GetSnapshotMetadata() (*SnapshotMetadata, error) {
	if m.snapshotMgr == nil {
		return nil, fmt.Errorf("snapshot manager not initialized")
	}
	return m.snapshotMgr.GetMetadata()
}

// ListSnapshotVersions возвращает список версий
func (m *TreeManager[T]) ListSnapshotVersions() ([][32]byte, error) {
	if m.snapshotMgr == nil {
		return nil, fmt.Errorf("snapshot manager not initialized")
	}
	return m.snapshotMgr.ListVersions()
}

// DeleteSnapshot удаляет снапшот
func (m *TreeManager[T]) DeleteSnapshot(version [32]byte) error {
	if m.snapshotMgr == nil {
		return fmt.Errorf("snapshot manager not initialized")
	}
	return m.snapshotMgr.DeleteSnapshot(version)
}

// GetSnapshotMetrics возвращает метрики производительности
func (m *TreeManager[T]) GetSnapshotMetrics() SnapshotMetrics {
	if m.snapshotMgr == nil {
		return SnapshotMetrics{}
	}
	return m.snapshotMgr.GetMetrics()
}

// GetSnapshotStats возвращает статистику хранилища
func (m *TreeManager[T]) GetSnapshotStats() StorageStats {
	if m.snapshotMgr == nil {
		return StorageStats{}
	}
	return m.snapshotMgr.GetStorageStats()
}

// CompactSnapshots сжимает базу снапшотов
func (m *TreeManager[T]) CompactSnapshots() error {
	if m.snapshotMgr == nil {
		return fmt.Errorf("snapshot manager not initialized")
	}
	return m.snapshotMgr.Compact()
}

// FlushSnapshots сбрасывает данные на диск
func (m *TreeManager[T]) FlushSnapshots() error {
	if m.snapshotMgr == nil {
		return fmt.Errorf("snapshot manager not initialized")
	}
	return m.snapshotMgr.Flush()
}

// CloseSnapshots закрывает snapshot manager
func (m *TreeManager[T]) CloseSnapshots() error {
	if m.snapshotMgr == nil {
		return nil
	}
	return m.snapshotMgr.Close()
}

// IsSnapshotEnabled проверяет доступность снапшотов
func (m *TreeManager[T]) IsSnapshotEnabled() bool {
	return m.snapshotMgr != nil
}