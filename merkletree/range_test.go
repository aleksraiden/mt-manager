package merkletree

import (
	"testing"
)

func TestRangeQuery(t *testing.T) {
	// Создаем менеджер с одним деревом
	mgr := NewManager[*Account](SmallConfig())
	tree, err := mgr.CreateTree("test")
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}
	
	// Вставляем элементы с ID 10, 20, 30, 40, 50
	accounts := []*Account{
		NewAccount(10, StatusUser),
		NewAccount(20, StatusUser),
		NewAccount(30, StatusUser),
		NewAccount(40, StatusUser),
		NewAccount(50, StatusUser),
	}
	
	for _, acc := range accounts {
		tree.Insert(acc)
	}
	
	// Тест 1: [20, 40] включительно
	result := tree.RangeQueryByID(20, 40, true, true)
	if len(result) != 3 {
		t.Errorf("Test 1: Expected 3 items (20,30,40), got %d", len(result))
		for _, r := range result {
			t.Logf("  ID: %d", r.UID)
		}
	}
	
	// Тест 2: [20, 40) исключая конец
	result = tree.RangeQueryByID(20, 40, true, false)
	if len(result) != 2 {
		t.Errorf("Test 2: Expected 2 items (20,30), got %d", len(result))
		for _, r := range result {
			t.Logf("  ID: %d", r.UID)
		}
	}
	
	// Тест 3: (20, 40) исключая оба конца
	result = tree.RangeQueryByID(20, 40, false, false)
	if len(result) != 1 {
		t.Errorf("Test 3: Expected 1 item (30), got %d", len(result))
		for _, r := range result {
			t.Logf("  ID: %d", r.UID)
		}
	}
	
	// Тест 4: Весь диапазон [10, 50]
	result = tree.RangeQueryByID(10, 50, true, true)
	if len(result) != 5 {
		t.Errorf("Test 4: Expected 5 items, got %d", len(result))
		for _, r := range result {
			t.Logf("  ID: %d", r.UID)
		}
	}
	
	// Тест 5: Пустой диапазон
	result = tree.RangeQueryByID(100, 200, true, true)
	if len(result) != 0 {
		t.Errorf("Test 5: Expected 0 items, got %d", len(result))
	}
}

func TestRangeQueryWithDeletes(t *testing.T) {
	mgr := NewManager[*Account](SmallConfig())
	tree, err := mgr.CreateTree("test")
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}
	
	// Вставляем элементы
	accounts := []*Account{
		NewAccount(10, StatusUser),
		NewAccount(20, StatusUser),
		NewAccount(30, StatusUser),
		NewAccount(40, StatusUser),
		NewAccount(50, StatusUser),
	}
	
	tree.InsertBatch(accounts)
	
	// Удаляем элемент 30
	tree.DeleteBatch([]uint64{30})
	
	// Проверяем, что 30 не включается в результат
	result := tree.RangeQueryByID(20, 40, true, true)
	if len(result) != 2 {
		t.Errorf("Expected 2 items (20,40) after delete, got %d", len(result))
		for _, r := range result {
			t.Logf("  ID: %d", r.UID)
		}
	}
	
	// Проверяем, что элементы правильные
	found20 := false
	found40 := false
	for _, r := range result {
		if r.UID == 20 {
			found20 = true
		}
		if r.UID == 40 {
			found40 = true
		}
		if r.UID == 30 {
			t.Errorf("Found deleted item 30 in range query!")
		}
	}
	
	if !found20 || !found40 {
		t.Errorf("Missing expected items: found20=%v, found40=%v", found20, found40)
	}
}

func TestRangeQueryOrdering(t *testing.T) {
	mgr := NewManager[*Account](SmallConfig())
	tree, err := mgr.CreateTree("test")
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}
	
	// Вставляем в произвольном порядке
	accounts := []*Account{
		NewAccount(50, StatusUser),
		NewAccount(10, StatusUser),
		NewAccount(30, StatusUser),
		NewAccount(40, StatusUser),
		NewAccount(20, StatusUser),
	}
	
	tree.InsertBatch(accounts)
	
	// Проверяем, что результаты в правильном порядке
	result := tree.RangeQueryByID(10, 50, true, true)
	if len(result) != 5 {
		t.Errorf("Expected 5 items, got %d", len(result))
	}
	
	// Проверяем сортировку
	for i := 0; i < len(result)-1; i++ {
		if result[i].UID > result[i+1].UID {
			t.Errorf("Result not sorted: %d > %d at positions %d, %d",
				result[i].UID, result[i+1].UID, i, i+1)
		}
	}
}

func BenchmarkRangeQuery(b *testing.B) {
	mgr := NewManager[*Account](LargeConfig())
	tree, err := mgr.CreateTree("bench")
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	
	// Вставляем 100K элементов
	accounts := make([]*Account, 100000)
	for i := 0; i < 100000; i++ {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(accounts)
	
	b.ResetTimer()
	
	// Запрос 1000 элементов из середины диапазона
	for i := 0; i < b.N; i++ {
		result := tree.RangeQueryByID(40000, 41000, true, false)
		if len(result) == 0 {
			b.Fatal("Empty result")
		}
	}
}

func BenchmarkRangeQuerySmall(b *testing.B) {
	mgr := NewManager[*Account](LargeConfig())
	tree, err := mgr.CreateTree("bench")
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	
	accounts := make([]*Account, 100000)
	for i := 0; i < 100000; i++ {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(accounts)
	
	b.ResetTimer()
	
	// Запрос 10 элементов
	for i := 0; i < b.N; i++ {
		result := tree.RangeQueryByID(50000, 50010, true, false)
		if len(result) == 0 {
			b.Fatal("Empty result")
		}
	}
}

func BenchmarkRangeQueryLarge(b *testing.B) {
	mgr := NewManager[*Account](LargeConfig())
	tree, err := mgr.CreateTree("bench")
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	
	accounts := make([]*Account, 100000)
	for i := 0; i < 100000; i++ {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(accounts)
	
	b.ResetTimer()
	
	// Запрос 10000 элементов
	for i := 0; i < b.N; i++ {
		result := tree.RangeQueryByID(30000, 40000, true, false)
		if len(result) == 0 {
			b.Fatal("Empty result")
		}
	}
}

func BenchmarkRangeQueryWithDeletes(b *testing.B) {
	mgr := NewManager[*Account](LargeConfig())
	tree, err := mgr.CreateTree("bench")
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	
	// Вставляем 100K элементов
	accounts := make([]*Account, 100000)
	for i := 0; i < 100000; i++ {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}
	tree.InsertBatch(accounts)
	
	// Удаляем каждый 10-й элемент
	deleteIDs := make([]uint64, 0, 10000)
	for i := 0; i < 100000; i += 10 {
		deleteIDs = append(deleteIDs, uint64(i))
	}
	tree.DeleteBatch(deleteIDs)
	
	b.ResetTimer()
	
	// Запрос из диапазона с удаленными элементами
	for i := 0; i < b.N; i++ {
		result := tree.RangeQueryByID(40000, 41000, true, false)
		if len(result) == 0 {
			b.Fatal("Empty result")
		}
	}
}
