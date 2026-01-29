package merkletree

import "fmt"

// TreeInterface - общий интерфейс для работы с деревьями любых типов
type TreeInterface interface {
	// Основные операции
	ComputeRoot() [32]byte
	Size() int
	Clear()
	GetStats() Stats
	
	// Вспомогательные методы
	Name() string
	SetName(name string)
	
	// Методы для снапшотов (type-erased)
	getAllItemsErased() []interface{}
	insertBatchErased(items []interface{}) error
}

// TypedTree - обертка вокруг Tree[T], реализующая TreeInterface
type TypedTree[T Hashable] struct {
	*Tree[T]
}

// Реализация TreeInterface для TypedTree

func (t *TypedTree[T]) ComputeRoot() [32]byte {
	return t.Tree.ComputeRoot()
}

func (t *TypedTree[T]) Size() int {
	return t.Tree.Size()
}

func (t *TypedTree[T]) Clear() {
	t.Tree.Clear()
}

func (t *TypedTree[T]) GetStats() Stats {
	return t.Tree.GetStats()
}

func (t *TypedTree[T]) Name() string {
	return t.Tree.name
}

func (t *TypedTree[T]) SetName(name string) {
	t.Tree.name = name
}

// Type-erased методы для снапшотов
func (t *TypedTree[T]) getAllItemsErased() []interface{} {
	items := t.Tree.GetAllItems()
	result := make([]interface{}, len(items))
	for i, item := range items {
		result[i] = item
	}
	return result
}

func (t *TypedTree[T]) insertBatchErased(items []interface{}) error {
	typedItems := make([]T, len(items))
	for i, item := range items {
		typedItem, ok := item.(T)
		if !ok {
			return fmt.Errorf("invalid type in batch insert")
		}
		typedItems[i] = typedItem
	}
	t.Tree.InsertBatch(typedItems)
	return nil
}
