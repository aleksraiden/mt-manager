package merkletree

import (
	"fmt"
	"reflect"
)

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
	//getAllItemsErased() []interface{}
	//insertBatchErased(items []interface{}) error
	
	// Для снапшотов - работают через интерфейс Serializable
    // Возвращает ошибку если T не реализует Serializable
    serializeItems() ([][]byte, error)
    deserializeAndInsert(items [][]byte) error
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

/***
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
***/

func (t *TypedTree[T]) serializeItems() ([][]byte, error) {
    items := t.Tree.GetAllItems()
    if len(items) == 0 {
        return nil, nil
    }

    // Проверяем, реализует ли T интерфейс Serializable
    // Достаточно проверить первый элемент - все T одинаковые
    if _, ok := any(items[0]).(Serializable); !ok {
        return nil, fmt.Errorf(
            "tree %q: type %T does not implement Serializable - "+
                "add Serialize()/Deserialize() methods to enable snapshots",
            t.Tree.name, items[0],
        )
    }

    result := make([][]byte, len(items))
    for i, item := range items {
        data, err := any(item).(Serializable).Serialize()
        if err != nil {
            return nil, fmt.Errorf("tree %q: failed to serialize item %d: %w",
                t.Tree.name, i, err)
        }
        result[i] = data
    }
    return result, nil
}

func (t *TypedTree[T]) deserializeAndInsert(items [][]byte) error {
    if len(items) == 0 {
        return nil
    }

    // Проверяем поддержку через zero value типа T
    var zero T
    if _, ok := any(&zero).(Serializable); !ok {
        return fmt.Errorf(
            "tree %q: type %T does not implement Serializable",
            t.Tree.name, zero,
        )
    }

    batch := make([]T, 0, len(items))
    /*
	for i, data := range items {
        // Десериализуем через pointer на zero value
        var item T
        if err := any(&item).(Serializable).Deserialize(data); err != nil {
            return fmt.Errorf("tree %q: failed to deserialize item %d: %w",
                t.Tree.name, i, err)
        }
        batch = append(batch, item)
    }*/
	for i, data := range items {
        var item T
        // Для pointer types (T = *Foo): item == nil, нельзя вызывать методы напрямую.
        // Используем reflect для создания нового значения базового типа.
        rv := reflect.New(reflect.TypeOf(item).Elem()) // работает если T = *Foo
        s, ok := rv.Interface().(Serializable)
        if !ok {
            return fmt.Errorf("tree %q: type %T does not implement Serializable", t.Tree.name, item)
        }
        if err := s.Deserialize(data); err != nil {
            return fmt.Errorf("tree %q: item %d: %w", t.Tree.name, i, err)
        }
        item = rv.Interface().(T)
        batch = append(batch, item)
    }

    t.Tree.InsertBatch(batch)
    return nil
}