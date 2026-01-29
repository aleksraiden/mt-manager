package merkletree

import (
	"testing"
)

func TestTreeRangeQuery(t *testing.T) {
	mgr := NewUniversalManager(DefaultConfig())
	tree, _ := CreateTree[*Account](mgr, "test")

	// Вставляем элементы
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccount(i*10, StatusUser))
	}

	// Range query [200, 500)
	results := tree.RangeQueryByID(200, 500, true, false)

	expectedCount := 30 // IDs: 200, 210, 220, ..., 490
	if len(results) != expectedCount {
		t.Errorf("Expected %d results, got %d", expectedCount, len(results))
	}

	// Проверяем порядок
	for i := 0; i < len(results)-1; i++ {
		if results[i].ID() >= results[i+1].ID() {
			t.Error("Results should be sorted")
		}
	}

	// Проверяем границы
	if len(results) > 0 {
		first := results[0].ID()
		last := results[len(results)-1].ID()

		if first < 200 {
			t.Errorf("First ID %d should be >= 200", first)
		}
		if last >= 500 {
			t.Errorf("Last ID %d should be < 500", last)
		}
	}
}

func TestTreeRangeQueryEmpty(t *testing.T) {
	mgr := NewUniversalManager(DefaultConfig())
	tree, _ := CreateTree[*Account](mgr, "test")

	// Вставляем элементы с IDs: 0, 10, 20, ..., 990
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccount(i*10, StatusUser))
	}

	// Query вне диапазона
	results := tree.RangeQueryByID(1000, 2000, true, false)

	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestTreeRangeQueryBoundaries(t *testing.T) {
	mgr := NewUniversalManager(DefaultConfig())
	tree, _ := CreateTree[*Account](mgr, "test")

	// Вставляем: 100, 200, 300, 400, 500
	for _, id := range []uint64{100, 200, 300, 400, 500} {
		tree.Insert(NewAccount(id, StatusUser))
	}

	tests := []struct {
		name          string
		start         uint64
		end           uint64
		includeStart  bool
		includeEnd    bool
		expectedCount int
	}{
		{"[200, 400)", 200, 400, true, false, 2}, // 200, 300
		{"(200, 400]", 200, 400, false, true, 2}, // 300, 400
		{"[200, 400]", 200, 400, true, true, 3},  // 200, 300, 400
		{"(200, 400)", 200, 400, false, false, 1}, // 300
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := tree.RangeQueryByID(tt.start, tt.end, tt.includeStart, tt.includeEnd)
			if len(results) != tt.expectedCount {
				t.Errorf("Expected %d results, got %d", tt.expectedCount, len(results))
			}
		})
	}
}

func TestTreeRangeQueryLarge(t *testing.T) {
	mgr := NewUniversalManager(DefaultConfig())
	tree, _ := CreateTree[*Account](mgr, "test")

	// Вставляем 10000 элементов
	for i := uint64(0); i < 10000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	// Range query [3000, 7000)
	results := tree.RangeQueryByID(3000, 7000, true, false)

	if len(results) != 4000 {
		t.Errorf("Expected 4000 results, got %d", len(results))
	}

	// Проверяем все элементы
	for i, acc := range results {
		expectedID := uint64(3000 + i)
		if acc.ID() != expectedID {
			t.Errorf("At position %d: expected ID %d, got %d", i, expectedID, acc.ID())
		}
	}
}

func BenchmarkTreeRangeQuery(b *testing.B) {
	mgr := NewUniversalManager(DefaultConfig())
	tree, _ := CreateTree[*Account](mgr, "test")

	// Заполняем дерево
	for i := uint64(0); i < 100000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.RangeQueryByID(10000, 10100, true, false)
	}
}
