package merkletree

import (
	"encoding/binary"
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

	// Для снапшотов - работают через интерфейс Serializable
	// Возвращает ошибку если T не реализует Serializable
	serializeItems() ([][]byte, error)
	deserializeAndInsert(items [][]byte) error

	// Новые методы для инкрементальных снапшотов
	serializeDirtyItems() (upserted [][]byte, deletedKeys [][]byte, err error)
	applyDelta(upserted [][]byte, deletedKeys [][]byte) error
	resetDirtyTracking()
	enableDirtyTracking()
	isDirtyTrackingEnabled() bool
	isStateExcluded() bool
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

func (t *TypedTree[T]) isStateExcluded() bool {
	return t.Tree.excludeState
}

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

	// Проверяем Serializable через reflect — корректно для pointer types (T = *Foo)
	var zero T
	zeroType := reflect.TypeOf(zero)
	if zeroType == nil {
		return fmt.Errorf("tree %q: cannot determine type T", t.Tree.name)
	}
	if zeroType.Kind() != reflect.Ptr {
		return fmt.Errorf("tree %q: T must be a pointer type, got %s", t.Tree.name, zeroType)
	}
	// Создаём тестовый экземпляр *Foo и проверяем интерфейс
	if _, ok := reflect.New(zeroType.Elem()).Interface().(Serializable); !ok {
		return fmt.Errorf("tree %q: type %T does not implement Serializable - "+
			"add Serialize()/Deserialize() methods to enable snapshots",
			t.Tree.name, zero)
	}

	batch := make([]T, 0, len(items))
	for i, data := range items {
		var item T
		rv := reflect.New(reflect.TypeOf(item).Elem())
		s := rv.Interface().(Serializable)
		if err := s.Deserialize(data); err != nil {
			return fmt.Errorf("tree %q: item %d: %w", t.Tree.name, i, err)
		}
		item = rv.Interface().(T)
		batch = append(batch, item)
	}

	t.Tree.InsertBatch(batch)
	return nil
}

func (t *TypedTree[T]) serializeDirtyItems() ([][]byte, [][]byte, error) {
	t.Tree.dirtyMu.Lock()
	// Копируем множества под локом, чтобы не блокировать надолго
	dirtySnapshot := make(map[[8]byte]struct{}, len(t.Tree.dirtyKeys))
	for k := range t.Tree.dirtyKeys {
		dirtySnapshot[k] = struct{}{}
	}
	deletedSnapshot := make(map[[8]byte]struct{}, len(t.Tree.deletedKeys))
	for k := range t.Tree.deletedKeys {
		deletedSnapshot[k] = struct{}{}
	}
	t.Tree.dirtyMu.Unlock()

	// Сериализуем dirty элементы
	upserted := make([][]byte, 0, len(dirtySnapshot))
	for key := range dirtySnapshot {
		item, found := t.Tree.GetByKey(key) // нужен метод GetByKey в Tree
		if !found {
			continue
		}
		s, ok := any(item).(Serializable)
		if !ok {
			return nil, nil, fmt.Errorf("type %T does not implement Serializable", item)
		}
		data, err := s.Serialize()
		if err != nil {
			return nil, nil, err
		}
		upserted = append(upserted, data)
	}

	// Сериализуем удалённые ключи
	deleted := make([][]byte, 0, len(deletedSnapshot))
	for key := range deletedSnapshot {
		k := key // копия
		deleted = append(deleted, k[:])
	}

	return upserted, deleted, nil
}

func (t *TypedTree[T]) applyDelta(upserted [][]byte, deletedKeys [][]byte) error {
	// Вставляем изменённые элементы
	if err := t.deserializeAndInsert(upserted); err != nil {
		return err
	}
	// Удаляем удалённые
	for _, keyBytes := range deletedKeys {
		if len(keyBytes) != 8 {
			return fmt.Errorf("invalid key length: %d", len(keyBytes))
		}
		// Delete принимает uint64, конвертируем из [8]byte
		id := binary.BigEndian.Uint64(keyBytes)
		t.Tree.Delete(id)
	}
	return nil
}

func (t *TypedTree[T]) isDirtyTrackingEnabled() bool {
	return t.Tree.isDirtyTrackingEnabled()
}

func (t *TypedTree[T]) resetDirtyTracking()  { t.Tree.ResetDirtyTracking() }
func (t *TypedTree[T]) enableDirtyTracking() { t.Tree.EnableDirtyTracking() }
