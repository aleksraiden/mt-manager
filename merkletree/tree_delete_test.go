// merkletree/tree_delete_test.go
package merkletree

import (
	"testing"
)

func TestTreeDelete(t *testing.T) {
	tree := New[*Account](DefaultConfig())
	
	// Добавляем элементы
	items := make([]*Account, 1000)
	for i := range items {
		items[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(items)
	
	if tree.Size() != 1000 {
		t.Errorf("Expected 1000 items, got %d", tree.Size())
	}
	
	// Удаляем один элемент
	deleted := tree.Delete(500)
	if !deleted {
		t.Error("Failed to delete item 500")
	}
	
	if tree.Size() != 999 {
		t.Errorf("Expected 999 items, got %d", tree.Size())
	}
	
	// Проверяем, что элемент удален
	_, exists := tree.Get(500)
	if exists {
		t.Error("Item 500 should be deleted")
	}
	
	// Проверяем, что другие элементы остались
	_, exists = tree.Get(499)
	if !exists {
		t.Error("Item 499 should exist")
	}
	
	// Удаляем батчем
	idsToDelete := []uint64{100, 200, 300, 400}
	deletedCount := tree.DeleteBatch(idsToDelete)
	
	if deletedCount != 4 {
		t.Errorf("Expected to delete 4 items, deleted %d", deletedCount)
	}
	
	if tree.Size() != 995 {
		t.Errorf("Expected 995 items, got %d", tree.Size())
	}
	
	// Проверяем корневой хеш
	root1 := tree.ComputeRoot()
	
	// Удаляем еще элементы
	tree.Delete(50)
	root2 := tree.ComputeRoot()
	
	if root1 == root2 {
		t.Error("Root hash should change after deletion")
	}
}

func BenchmarkDelete(b *testing.B) {
	tree := New[*Account](DefaultConfig())
	
	// Подготовка
	items := make([]*Account, 100000)
	for i := range items {
		items[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(items)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		id := uint64(i % 100000)
		tree.Delete(id)
	}
}

func BenchmarkDeleteBatch(b *testing.B) {
	tree := New[*Account](DefaultConfig())
	
	items := make([]*Account, 100000)
	for i := range items {
		items[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(items)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		ids := make([]uint64, 100)
		for j := range ids {
			ids[j] = uint64((i*100 + j) % 100000)
		}
		tree.DeleteBatch(ids)
	}
}
