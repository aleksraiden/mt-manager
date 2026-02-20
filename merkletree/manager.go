package merkletree

import (
	"fmt"
	"sort"
	"sync"

	"github.com/zeebo/blake3"
)

// UniversalManager управляет несколькими Merkle-деревьями разных типов
// и вычисляет общий корневой хеш из всех деревьев
type UniversalManager struct {
	trees           map[string]TreeInterface
	config          *Config
	mu              sync.RWMutex
	globalRootCache [32]byte
	globalRootDirty bool
	treeRootCache   map[string][32]byte
	snapshotMgr     *SnapshotManager
}

// TreeOptions - опции для создания конкретного дерева
type TreeOptions struct {
	// TopN - количество топ элементов для этого дерева (0 = отключено)
	// Если не указано, используется значение из Config
	TopN *int
	
	// CacheSize - размер кеша для этого дерева (0 = отключено)
	// Если не указано, используется значение из Config
	CacheSize *int
	
	// CacheShards - количество шардов кеша для этого дерева
	// Если не указано, используется значение из Config
	CacheShards *uint
}

// NewUniversalManager создает новый универсальный менеджер деревьев
func NewUniversalManager(cfg *Config) *UniversalManager {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return &UniversalManager{
		trees:           make(map[string]TreeInterface),
		config:          cfg,
		treeRootCache:   make(map[string][32]byte),
		globalRootDirty: true,
	}
}

// NewUniversalManagerWithSnapshot создает менеджер с поддержкой снапшотов
func NewUniversalManagerWithSnapshot(cfg *Config, snapshotPath string) (*UniversalManager, error) {
	mgr := NewUniversalManager(cfg)
	
	if snapshotPath != "" {
		snapshotMgr, err := NewSnapshotManager(snapshotPath)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize snapshot manager: %w", err)
		}
		mgr.snapshotMgr = snapshotMgr
	}
	
	return mgr, nil
}

// CreateTree создает новое дерево с указанным типом и именем
func CreateTree[T Hashable](m *UniversalManager, name string) (*Tree[T], error) {
	return CreateTreeWithConfig[T](m, name, m.config)
}

// CreateTreeWithConfig создает дерево с кастомной конфигурацией
func CreateTreeWithConfig[T Hashable](m *UniversalManager, name string, cfg *Config) (*Tree[T], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.trees[name]; exists {
		return nil, fmt.Errorf("дерево с именем '%s' уже существует", name)
	}

	tree := New[T](cfg)
	tree.name = name
	
	wrapped := &TypedTree[T]{Tree: tree}
	m.trees[name] = wrapped
	m.globalRootDirty = true

	return tree, nil
}


// GetTree возвращает дерево по имени с нужным типом
func GetTree[T Hashable](m *UniversalManager, name string) (*Tree[T], bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	treeInterface, exists := m.trees[name]
	if !exists {
		return nil, false
	}

	// Type assertion для получения типизированного дерева
	typedTree, ok := treeInterface.(*TypedTree[T])
	if !ok {
		return nil, false // Дерево существует, но с другим типом
	}

	return typedTree.Tree, true
}

// GetOrCreateTree получает существующее дерево или создает новое
func GetOrCreateTree[T Hashable](m *UniversalManager, name string) (*Tree[T], error) {
	// Сначала пытаемся получить
	if tree, exists := GetTree[T](m, name); exists {
		return tree, nil
	}

	// Создаем новое
	return CreateTree[T](m, name)
}

// RemoveTree удаляет дерево по имени
func (m *UniversalManager) RemoveTree(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.trees[name]; !exists {
		return false
	}

	delete(m.trees, name)
	delete(m.treeRootCache, name)
	m.globalRootDirty = true
	return true
}

// ListTrees возвращает список имен всех деревьев
func (m *UniversalManager) ListTrees() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.trees))
	for name := range m.trees {
		names = append(names, name)
	}
	return names
}

// ComputeGlobalRoot вычисляет глобальный корень из всех деревьев
func (m *UniversalManager) ComputeGlobalRoot() [32]byte {
	m.mu.RLock()

	// Проверяем, нужен ли пересчет
	if !m.globalRootDirty {
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
				m.treeRootCache[name] = actualRoot
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

	// Собираем корни всех деревьев
	roots := make([][32]byte, len(names))
	for i, name := range names {
		roots[i] = m.trees[name].ComputeRoot()
		m.treeRootCache[name] = roots[i]
	}

	// Вычисляем глобальный корень
	m.globalRootCache = m.computeMerkleRoot(roots)
	m.globalRootDirty = false
	return m.globalRootCache
}

// computeMerkleRoot итеративно вычисляет корень Меркла из листьев
func (m *UniversalManager) computeMerkleRoot(hashes [][32]byte) [32]byte {
	if len(hashes) == 0 {
		return [32]byte{}
	}

	if len(hashes) == 1 {
		return hashes[0]
	}

	currentLevel := hashes
	for len(currentLevel) > 1 {
		nextLevel := make([][32]byte, 0, (len(currentLevel)+1)/2)

		for i := 0; i < len(currentLevel); i += 2 {
			hasher := blake3HasherPool.Get().(*blake3.Hasher)
			hasher.Reset()

			hasher.Write(currentLevel[i][:])

			if i+1 < len(currentLevel) {
				hasher.Write(currentLevel[i+1][:])
			} else {
				var zero [32]byte
				hasher.Write(zero[:])
			}

			var parentHash [32]byte
			copy(parentHash[:], hasher.Sum(nil))
			
			blake3HasherPool.Put(hasher)
			
			nextLevel = append(nextLevel, parentHash)
		}

		currentLevel = nextLevel
	}

	return currentLevel[0]
}

// InvalidateGlobalRoot помечает глобальный корень как требующий пересчета
func (m *UniversalManager) InvalidateGlobalRoot() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.globalRootDirty = true
}

// InvalidateTreeRoot инвалидирует кеш корня конкретного дерева
func (m *UniversalManager) InvalidateTreeRoot(treeName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.treeRootCache, treeName)
	m.globalRootDirty = true
}

// GetMerkleProof возвращает Merkle proof для конкретного дерева
func (m *UniversalManager) GetMerkleProof(treeName string) (*MerkleProof, error) {
    // ComputeGlobalRoot сам управляет своими блокировками
    globalRoot := m.ComputeGlobalRoot()

    m.mu.RLock()
    defer m.mu.RUnlock()

    if _, exists := m.trees[treeName]; !exists {
        return nil, fmt.Errorf("дерево '%s' не найдено", treeName)
    }

    names := make([]string, 0, len(m.trees))
    for name := range m.trees {
        names = append(names, name)
    }
    sort.Strings(names)

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

    roots := make([][32]byte, len(names))
    for i, name := range names {
        roots[i] = m.trees[name].ComputeRoot()
    }

    proofPath, isLeft := m.computeProofPath(roots, targetIndex)

    return &MerkleProof{
        TreeName:   treeName,
        TreeRoot:   roots[targetIndex],
        ProofPath:  proofPath,
        IsLeft:     isLeft,
        GlobalRoot: globalRoot, // ← pre-computed, без рекурсивной блокировки
    }, nil
}

// computeProofPath вычисляет proof path для элемента по индексу
func (m *UniversalManager) computeProofPath(hashes [][32]byte, targetIndex int) ([][32]byte, []bool) {
	if len(hashes) <= 1 {
		return nil, nil
	}

	proofPath := make([][32]byte, 0)
	isLeft := make([]bool, 0)
	currentLevel := hashes
	currentIndex := targetIndex

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

			if i == (currentIndex &^ 1) {
				if currentIndex%2 == 0 {
					proofPath = append(proofPath, rightHash)
					isLeft = append(isLeft, true)
				} else {
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
func (m *UniversalManager) ComputeAllRoots() map[string][32]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make(map[string][32]byte, len(m.trees))
	var wg sync.WaitGroup
	var resultMu sync.Mutex

	for name, tree := range m.trees {
		wg.Add(1)
		go func(n string, t TreeInterface) {
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
func (m *UniversalManager) GetTotalStats() ManagerStats {
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
	TreeCount      int
	TotalItems     int
	TotalNodes     int
	TotalCacheSize int
	TreeStats      map[string]Stats
}

// InsertToTree вставляет элемент в указанное дерево
func InsertToTree[T Hashable](m *UniversalManager, treeName string, item T) error {
	tree, exists := GetTree[T](m, treeName)
	if !exists {
		return fmt.Errorf("дерево '%s' не найдено или имеет другой тип", treeName)
	}

	tree.Insert(item)
	m.InvalidateTreeRoot(treeName)
	return nil
}

// BatchInsertToTree вставляет батч элементов в указанное дерево
func BatchInsertToTree[T Hashable](m *UniversalManager, treeName string, items []T) error {
	tree, exists := GetTree[T](m, treeName)
	if !exists {
		return fmt.Errorf("дерево '%s' не найдено или имеет другой тип", treeName)
	}

	tree.InsertBatch(items)
	m.InvalidateTreeRoot(treeName)
	return nil
}

// ClearAll очищает все деревья
func (m *UniversalManager) ClearAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, tree := range m.trees {
		tree.Clear()
	}
	m.globalRootDirty = true
}

// RemoveAll удаляет все деревья
func (m *UniversalManager) RemoveAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trees = make(map[string]TreeInterface)
	m.treeRootCache = make(map[string][32]byte)
	m.globalRootDirty = true
}

// VerifyTreeInclusion проверяет, что дерево включено в глобальный корень
func (m *UniversalManager) VerifyTreeInclusion(treeName string) (bool, error) {
	proof, err := m.GetMerkleProof(treeName)
	if err != nil {
		return false, err
	}

	return proof.Verify(), nil
}

// TreeExists проверяет существование дерева
func (m *UniversalManager) TreeExists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.trees[name]
	return exists
}

// GetTreeSize возвращает размер дерева по имени
func (m *UniversalManager) GetTreeSize(name string) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tree, exists := m.trees[name]
	if !exists {
		return 0, fmt.Errorf("дерево '%s' не найдено", name)
	}

	return tree.Size(), nil
}

// GetTreeRoot возвращает корень конкретного дерева
func (m *UniversalManager) GetTreeRoot(name string) ([32]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tree, exists := m.trees[name]
	if !exists {
		return [32]byte{}, fmt.Errorf("дерево '%s' не найдено", name)
	}

	return tree.ComputeRoot(), nil
}

// ClearTree очищает конкретное дерево
func (m *UniversalManager) ClearTree(name string) error {
	m.mu.RLock()
	tree, exists := m.trees[name]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("дерево '%s' не найдено", name)
	}

	tree.Clear()
	m.InvalidateTreeRoot(name)
	return nil
}

// GetTreeInfo возвращает детальную информацию о дереве
func (m *UniversalManager) GetTreeInfo(name string) (TreeInfo, error) {
	m.mu.RLock()
	tree, exists := m.trees[name]
	m.mu.RUnlock()

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
func (m *UniversalManager) GetAllTreesInfo() []TreeInfo {
	m.mu.RLock()
    defer m.mu.RUnlock()

    infos := make([]TreeInfo, 0, len(m.trees))
    for name, tree := range m.trees {
        stats := tree.GetStats()
        root := tree.ComputeRoot() // ComputeRoot использует свои внутренние локи, не m.mu
        infos = append(infos, TreeInfo{
            Name:      name,
            Size:      stats.TotalItems,
            Nodes:     stats.AllocatedNodes,
            CacheSize: stats.CacheSize,
            Root:      root,
        })
    }
    return infos
}

// SetDefaultConfig устанавливает конфигурацию по умолчанию для новых деревьев
func (m *UniversalManager) SetDefaultConfig(cfg *Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config = cfg
}

// ============================================
// Snapshot API
// ============================================

// CreateSnapshot создает снапшот синхронно
func (m *UniversalManager) CreateSnapshot() ([32]byte, error) {
	if m.snapshotMgr == nil {
		return [32]byte{}, fmt.Errorf("snapshot manager not initialized")
	}

	return m.snapshotMgr.CreateSnapshot(m, DefaultSnapshotOptions())
}

// CreateSnapshotAsync создает снапшот асинхронно
func (m *UniversalManager) CreateSnapshotAsync() <-chan SnapshotResult {
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
func (m *UniversalManager) LoadFromSnapshot(version *[32]byte) error {
	if m.snapshotMgr == nil {
		return fmt.Errorf("snapshot manager not initialized")
	}

	return m.snapshotMgr.LoadSnapshot(m, version)
}

// GetSnapshotMetadata возвращает метаданные
func (m *UniversalManager) GetSnapshotMetadata() (*SnapshotMetadata, error) {
	if m.snapshotMgr == nil {
		return nil, fmt.Errorf("snapshot manager not initialized")
	}

	return m.snapshotMgr.GetMetadata()
}

// ListSnapshotVersions возвращает список версий
func (m *UniversalManager) ListSnapshotVersions() ([][32]byte, error) {
	if m.snapshotMgr == nil {
		return nil, fmt.Errorf("snapshot manager not initialized")
	}

	return m.snapshotMgr.ListVersions()
}

// DeleteSnapshot удаляет снапшот
func (m *UniversalManager) DeleteSnapshot(version [32]byte) error {
	if m.snapshotMgr == nil {
		return fmt.Errorf("snapshot manager not initialized")
	}

	return m.snapshotMgr.DeleteSnapshot(version)
}

// GetSnapshotMetrics возвращает метрики производительности
func (m *UniversalManager) GetSnapshotMetrics() SnapshotMetrics {
	if m.snapshotMgr == nil {
		return SnapshotMetrics{}
	}

	return m.snapshotMgr.GetMetrics()
}

// GetSnapshotStats возвращает статистику хранилища
func (m *UniversalManager) GetSnapshotStats() StorageStats {
	if m.snapshotMgr == nil {
		return StorageStats{}
	}

	return m.snapshotMgr.GetStorageStats()
}

// CompactSnapshots сжимает базу снапшотов
func (m *UniversalManager) CompactSnapshots() error {
	if m.snapshotMgr == nil {
		return fmt.Errorf("snapshot manager not initialized")
	}

	return m.snapshotMgr.Compact()
}

// FlushSnapshots сбрасывает данные на диск
func (m *UniversalManager) FlushSnapshots() error {
	if m.snapshotMgr == nil {
		return fmt.Errorf("snapshot manager not initialized")
	}

	return m.snapshotMgr.Flush()
}

// CloseSnapshots закрывает snapshot manager
func (m *UniversalManager) CloseSnapshots() error {
	if m.snapshotMgr == nil {
		return nil
	}

	return m.snapshotMgr.Close()
}

// IsSnapshotEnabled проверяет доступность снапшотов
func (m *UniversalManager) IsSnapshotEnabled() bool {
	return m.snapshotMgr != nil
}
