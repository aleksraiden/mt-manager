package merkletree

import (
	"testing"
)

func TestManagerCreateTree(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	tree, err := CreateTree[*Account](manager, "test")
	if err != nil {
		t.Fatalf("Не удалось создать дерево: %v", err)
	}

	if tree == nil {
		t.Fatal("Дерево не должно быть nil")
	}

	// Попытка создать дерево с тем же именем
	_, err = CreateTree[*Account](manager, "test")
	if err == nil {
		t.Error("Должна быть ошибка при создании дублирующегося дерева")
	}
}

func TestManagerGetTree(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	_, err := CreateTree[*Account](manager, "test")
	if err != nil {
		t.Fatalf("Не удалось создать дерево: %v", err)
	}

	tree, exists := GetTree[*Account](manager, "test")
	if !exists {
		t.Error("Дерево должно существовать")
	}

	if tree == nil {
		t.Error("Дерево не должно быть nil")
	}

	// Несуществующее дерево
	_, exists = GetTree[*Account](manager, "nonexistent")
	if exists {
		t.Error("Несуществующее дерево не должно быть найдено")
	}
}

func TestManagerGetOrCreateTree(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	tree1, err := GetOrCreateTree[*Account](manager, "test")
	if err != nil {
		t.Fatalf("Не удалось создать дерево: %v", err)
	}
	if tree1 == nil {
		t.Fatal("Дерево не должно быть nil")
	}

	tree2, err := GetOrCreateTree[*Account](manager, "test")
	if err != nil {
		t.Fatalf("Не удалось получить дерево: %v", err)
	}
	if tree1 != tree2 {
		t.Error("GetOrCreateTree должен возвращать то же дерево")
	}
}

func TestManagerRemoveTree(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	CreateTree[*Account](manager, "test")

	if !manager.RemoveTree("test") {
		t.Error("Удаление должно вернуть true")
	}

	if manager.RemoveTree("test") {
		t.Error("Повторное удаление должно вернуть false")
	}
}

func TestManagerListTrees(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	CreateTree[*Account](manager, "tree1")
	CreateTree[*Account](manager, "tree2")
	CreateTree[*Account](manager, "tree3")

	trees := manager.ListTrees()
	if len(trees) != 3 {
		t.Errorf("Ожидалось 3 дерева, получено %d", len(trees))
	}
}

func TestManagerComputeGlobalRoot(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	tree1, _ := GetOrCreateTree[*Account](manager, "users")
	tree2, _ := GetOrCreateTree[*Account](manager, "admins")

	for i := uint64(0); i < 100; i++ {
		tree1.Insert(NewAccount(i, StatusUser))
		tree2.Insert(NewAccount(i+1000, StatusSystem))
	}

	root := manager.ComputeGlobalRoot()
	// Проверяем детерминизм
	root2 := manager.ComputeGlobalRoot()
	if root != root2 {
		t.Error("Глобальный корень должен быть детерминированным")
	}

	// Проверяем, что корень не нулевой
	var zero [32]byte
	if root == zero {
		t.Error("Глобальный корень не должен быть нулевым")
	}
}

func TestManagerComputeAllRoots(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	tree1, _ := GetOrCreateTree[*Account](manager, "tree1")
	tree2, _ := GetOrCreateTree[*Account](manager, "tree2")

	for i := uint64(0); i < 100; i++ {
		tree1.Insert(NewAccount(i, StatusUser))
		tree2.Insert(NewAccount(i+1000, StatusMM))
	}

	roots := manager.ComputeAllRoots()
	if len(roots) != 2 {
		t.Errorf("Ожидалось 2 корня, получено %d", len(roots))
	}

	if _, ok := roots["tree1"]; !ok {
		t.Error("Должен быть корень для tree1")
	}

	if _, ok := roots["tree2"]; !ok {
		t.Error("Должен быть корень для tree2")
	}
}

func TestManagerGetTotalStats(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	tree1, _ := GetOrCreateTree[*Account](manager, "tree1")
	tree2, _ := GetOrCreateTree[*Account](manager, "tree2")

	for i := uint64(0); i < 500; i++ {
		tree1.Insert(NewAccount(i, StatusUser))
	}

	for i := uint64(500); i < 1000; i++ {
		tree2.Insert(NewAccount(i, StatusMM))
	}

	stats := manager.GetTotalStats()
	if stats.TreeCount != 2 {
		t.Errorf("Ожидалось 2 дерева, получено %d", stats.TreeCount)
	}

	if stats.TotalItems != 1000 {
		t.Errorf("Ожидалось 1000 элементов, получено %d", stats.TotalItems)
	}
}

func TestManagerInsertToTree(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	CreateTree[*Account](manager, "test")

	acc := NewAccount(123, StatusUser)
	err := InsertToTree[*Account](manager, "test", acc)
	if err != nil {
		t.Errorf("Не удалось вставить: %v", err)
	}

	// Вставка в несуществующее дерево
	err = InsertToTree[*Account](manager, "nonexistent", acc)
	if err == nil {
		t.Error("Должна быть ошибка при вставке в несуществующее дерево")
	}
}

func TestManagerBatchInsertToTree(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	CreateTree[*Account](manager, "test")

	accounts := make([]*Account, 100)
	for i := range accounts {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}

	err := BatchInsertToTree[*Account](manager, "test", accounts)
	if err != nil {
		t.Errorf("Не удалось вставить батч: %v", err)
	}

	tree, _ := GetTree[*Account](manager, "test")
	if tree.Size() != 100 {
		t.Errorf("Ожидалось 100 элементов, получено %d", tree.Size())
	}
}

func TestManagerClearAll(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	tree1, _ := GetOrCreateTree[*Account](manager, "tree1")
	tree2, _ := GetOrCreateTree[*Account](manager, "tree2")

	for i := uint64(0); i < 100; i++ {
		tree1.Insert(NewAccount(i, StatusUser))
		tree2.Insert(NewAccount(i+1000, StatusMM))
	}

	manager.ClearAll()

	if tree1.Size() != 0 {
		t.Error("tree1 должно быть пустым после ClearAll")
	}

	if tree2.Size() != 0 {
		t.Error("tree2 должно быть пустым после ClearAll")
	}
}

func TestManagerRemoveAll(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	CreateTree[*Account](manager, "tree1")
	CreateTree[*Account](manager, "tree2")

	manager.RemoveAll()

	trees := manager.ListTrees()
	if len(trees) != 0 {
		t.Errorf("После RemoveAll не должно быть деревьев, получено %d", len(trees))
	}
}

func TestManagerTreeNaming(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())

	// Создание с именем
	_, err := CreateTree[*Account](manager, "my_accounts")
	if err != nil {
		t.Fatalf("Не удалось создать дерево: %v", err)
	}

	// Проверка существования
	if !manager.TreeExists("my_accounts") {
		t.Error("Дерево должно существовать")
	}

	if manager.TreeExists("nonexistent") {
		t.Error("Несуществующее дерево")
	}
}

func TestManagerGetTreeInfo(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())
	tree, _ := CreateTree[*Account](manager, "test")

	for i := uint64(0); i < 50; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	info, err := manager.GetTreeInfo("test")
	if err != nil {
		t.Fatalf("Не удалось получить info: %v", err)
	}

	if info.Name != "test" {
		t.Errorf("Неверное имя: %s", info.Name)
	}

	if info.Size != 50 {
		t.Errorf("Неверный размер: %d", info.Size)
	}

	t.Logf("\n%s", info.String())
}

// Тест смешанных типов в одном менеджере
func TestManagerMixedTypes(t *testing.T) {
	manager := NewUniversalManager(DefaultConfig())

	// Создаем деревья разных типов
	accountTree, err := CreateTree[*Account](manager, "accounts")
	if err != nil {
		t.Fatalf("Не удалось создать дерево аккаунтов: %v", err)
	}

	balanceTree, err := CreateTree[*Balance](manager, "balances")
	if err != nil {
		t.Fatalf("Не удалось создать дерево балансов: %v", err)
	}

	// Заполняем аккаунты
	for i := uint64(0); i < 10; i++ {
		accountTree.Insert(NewAccount(i, StatusUser))
	}

	// Заполняем балансы
	for i := uint64(0); i < 10; i++ {
		balanceTree.Insert(NewBalance(i, 1, 1000_000000, 0))
	}

	// Проверяем глобальный корень
	globalRoot := manager.ComputeGlobalRoot()
	var zero [32]byte
	if globalRoot == zero {
		t.Error("Глобальный корень не должен быть нулевым")
	}

	// Проверяем статистику
	stats := manager.GetTotalStats()
	if stats.TreeCount != 2 {
		t.Errorf("Ожидалось 2 дерева, получено %d", stats.TreeCount)
	}

	if stats.TotalItems != 20 {
		t.Errorf("Ожидалось 20 элементов, получено %d", stats.TotalItems)
	}

	// Проверяем type safety
	_, exists := GetTree[*Account](manager, "accounts")
	if !exists {
		t.Error("Дерево accounts должно существовать как Account")
	}

	// Попытка получить с неправильным типом должна вернуть false
	_, exists = GetTree[*Balance](manager, "accounts")
	if exists {
		t.Error("Дерево accounts не должно получаться как Balance")
	}
}

// Бенчмарки менеджера
func BenchmarkManagerComputeGlobalRoot(b *testing.B) {
	manager := NewUniversalManager(DefaultConfig())

	for i := 0; i < 10; i++ {
		tree, _ := GetOrCreateTree[*Account](manager, string(rune('a'+i)))
		for j := uint64(0); j < 1000; j++ {
			tree.Insert(NewAccount(uint64(i)*1000+j, StatusUser))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.ComputeGlobalRoot()
	}
}

func BenchmarkManagerComputeAllRootsParallel(b *testing.B) {
	manager := NewUniversalManager(DefaultConfig())

	for i := 0; i < 10; i++ {
		tree, _ := GetOrCreateTree[*Account](manager, string(rune('a'+i)))
		for j := uint64(0); j < 10000; j++ {
			tree.Insert(NewAccount(uint64(i)*10000+j, StatusUser))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.ComputeAllRoots()
	}
}

func BenchmarkManagerMixedTypes(b *testing.B) {
	manager := NewUniversalManager(DefaultConfig())

	// Создаем деревья разных типов
	accountTree, _ := CreateTree[*Account](manager, "accounts")
	balanceTree, _ := CreateTree[*Balance](manager, "balances")

	for i := uint64(0); i < 1000; i++ {
		accountTree.Insert(NewAccount(i, StatusUser))
		balanceTree.Insert(NewBalance(i, 1, 1000_000000, 0))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.ComputeGlobalRoot()
	}
}
