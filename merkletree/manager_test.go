package merkletree

import (
	"testing"
)

func TestManagerCreateTree(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	tree, err := manager.CreateTree("test")
	if err != nil {
		t.Fatalf("Не удалось создать дерево: %v", err)
	}

	if tree == nil {
		t.Fatal("Дерево не должно быть nil")
	}

	// Попытка создать дерево с тем же именем
	_, err = manager.CreateTree("test")
	if err == nil {
		t.Error("Должна быть ошибка при создании дублирующегося дерева")
	}
}

func TestManagerGetTree(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	_, err := manager.CreateTree("test")
	if err != nil {
		t.Fatalf("Не удалось создать дерево: %v", err)
	}

	tree, exists := manager.GetTree("test")
	if !exists {
		t.Error("Дерево должно существовать")
	}

	if tree == nil {
		t.Error("Дерево не должно быть nil")
	}

	// Несуществующее дерево
	_, exists = manager.GetTree("nonexistent")
	if exists {
		t.Error("Несуществующее дерево не должно быть найдено")
	}
}

func TestManagerGetOrCreateTree(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	tree1 := manager.GetOrCreateTree("test")
	if tree1 == nil {
		t.Fatal("Дерево не должно быть nil")
	}

	tree2 := manager.GetOrCreateTree("test")
	if tree1 != tree2 {
		t.Error("GetOrCreateTree должен возвращать то же дерево")
	}
}

func TestManagerRemoveTree(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	manager.CreateTree("test")

	if !manager.RemoveTree("test") {
		t.Error("Удаление должно вернуть true")
	}

	if manager.RemoveTree("test") {
		t.Error("Повторное удаление должно вернуть false")
	}
}

func TestManagerListTrees(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	manager.CreateTree("tree1")
	manager.CreateTree("tree2")
	manager.CreateTree("tree3")

	trees := manager.ListTrees()
	if len(trees) != 3 {
		t.Errorf("Ожидалось 3 дерева, получено %d", len(trees))
	}
}

func TestManagerComputeGlobalRoot(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	tree1 := manager.GetOrCreateTree("users")
	tree2 := manager.GetOrCreateTree("admins")

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
	manager := NewManager[*Account](DefaultConfig())

	tree1 := manager.GetOrCreateTree("tree1")
	tree2 := manager.GetOrCreateTree("tree2")

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
	manager := NewManager[*Account](DefaultConfig())

	tree1 := manager.GetOrCreateTree("tree1")
	tree2 := manager.GetOrCreateTree("tree2")

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
	manager := NewManager[*Account](DefaultConfig())
	manager.CreateTree("test")

	acc := NewAccount(123, StatusUser)
	err := manager.InsertToTree("test", acc)
	if err != nil {
		t.Errorf("Не удалось вставить: %v", err)
	}

	// Вставка в несуществующее дерево
	err = manager.InsertToTree("nonexistent", acc)
	if err == nil {
		t.Error("Должна быть ошибка при вставке в несуществующее дерево")
	}
}

func TestManagerBatchInsertToTree(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())
	manager.CreateTree("test")

	accounts := make([]*Account, 100)
	for i := range accounts {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}

	err := manager.BatchInsertToTree("test", accounts)
	if err != nil {
		t.Errorf("Не удалось вставить батч: %v", err)
	}

	tree, _ := manager.GetTree("test")
	if tree.Size() != 100 {
		t.Errorf("Ожидалось 100 элементов, получено %d", tree.Size())
	}
}

func TestManagerClearAll(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	tree1 := manager.GetOrCreateTree("tree1")
	tree2 := manager.GetOrCreateTree("tree2")

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
	manager := NewManager[*Account](DefaultConfig())

	manager.CreateTree("tree1")
	manager.CreateTree("tree2")

	manager.RemoveAll()

	trees := manager.ListTrees()
	if len(trees) != 0 {
		t.Errorf("После RemoveAll не должно быть деревьев, получено %d", len(trees))
	}
}

// Бенчмарки менеджера
func BenchmarkManagerComputeGlobalRoot(b *testing.B) {
	manager := NewManager[*Account](DefaultConfig())

	for i := 0; i < 10; i++ {
		tree := manager.GetOrCreateTree(string(rune('a' + i)))
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
	manager := NewManager[*Account](DefaultConfig())

	for i := 0; i < 10; i++ {
		tree := manager.GetOrCreateTree(string(rune('a' + i)))
		for j := uint64(0); j < 10000; j++ {
			tree.Insert(NewAccount(uint64(i)*10000+j, StatusUser))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.ComputeAllRoots()
	}
}

func TestManagerTreeNaming(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	// Создание с именем
	_, err := manager.CreateTree("my_accounts")
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

func TestManagerRenameTree(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	manager.CreateTree("old_name")

	err := manager.RenameTree("old_name", "new_name")
	if err != nil {
		t.Fatalf("Не удалось переименовать: %v", err)
	}

	if manager.TreeExists("old_name") {
		t.Error("Старое имя не должно существовать")
	}

	if !manager.TreeExists("new_name") {
		t.Error("Новое имя должно существовать")
	}
}

func TestManagerCloneTree(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	tree, _ := manager.CreateTree("original")
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	err := manager.CloneTree("original", "clone")
	if err != nil {
		t.Fatalf("Не удалось клонировать: %v", err)
	}

	originalSize, _ := manager.GetTreeSize("original")
	cloneSize, _ := manager.GetTreeSize("clone")

	if originalSize != cloneSize {
		t.Errorf("Размеры должны совпадать: %d != %d", originalSize, cloneSize)
	}
}

func TestManagerGetTreeInfo(t *testing.T) {
	manager := NewManager[*Account](DefaultConfig())

	tree, _ := manager.CreateTree("test")
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
