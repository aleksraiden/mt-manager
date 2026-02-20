package merkletree

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"runtime"
	"slices"
	"sync"
)

func (t *Tree[T]) RangeQuery(startKey, endKey []byte, includeStart, includeEnd bool) []T {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil
	}

	result := make([]T, 0)

	t.items.Range(func(_ uint64, item T) bool {
		itemKey := t.normalizeKey(item.Key())
		ik := itemKey[:]

		cmpStart := bytes.Compare(ik, startKey)
		cmpEnd := bytes.Compare(ik, endKey)

		var hit bool
		switch {
		case includeStart && includeEnd:
			hit = cmpStart >= 0 && cmpEnd <= 0
		case includeStart && !includeEnd:
			hit = cmpStart >= 0 && cmpEnd < 0
		case !includeStart && includeEnd:
			hit = cmpStart > 0 && cmpEnd <= 0
		default:
			hit = cmpStart > 0 && cmpEnd < 0
		}

		if hit {
			result = append(result, item)
		}
		return true
	})

	slices.SortFunc(result, func(a, b T) int {
		return cmp.Compare(a.ID(), b.ID())
	})

	return result
}

func (t *Tree[T]) RangeQueryParallel(startKey, endKey []byte, includeStart, includeEnd bool) []T {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil
	}

	allItems := t.GetAllItems()
	if len(allItems) == 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	if numWorkers > len(allItems) {
		numWorkers = len(allItems)
	}

	chunkSize := (len(allItems) + numWorkers - 1) / numWorkers
	partials := make([][]T, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(allItems) {
			break
		}
		end := start + chunkSize
		if end > len(allItems) {
			end = len(allItems)
		}

		wg.Add(1)
		go func(idx int, chunk []T) {
			defer wg.Done()
			local := make([]T, 0)
			for _, item := range chunk {
				itemKey := t.normalizeKey(item.Key())
				ik := itemKey[:]

				cmpStart := bytes.Compare(ik, startKey)
				cmpEnd := bytes.Compare(ik, endKey)

				var hit bool
				switch {
				case includeStart && includeEnd:
					hit = cmpStart >= 0 && cmpEnd <= 0
				case includeStart && !includeEnd:
					hit = cmpStart >= 0 && cmpEnd < 0
				case !includeStart && includeEnd:
					hit = cmpStart > 0 && cmpEnd <= 0
				default:
					hit = cmpStart > 0 && cmpEnd < 0
				}

				if hit {
					local = append(local, item)
				}
			}
			partials[idx] = local
		}(i, allItems[start:end])
	}

	wg.Wait()

	result := make([]T, 0)
	for _, p := range partials {
		result = append(result, p...)
	}

	slices.SortFunc(result, func(a, b T) int {
		return cmp.Compare(a.ID(), b.ID())
	})

	return result
}

func (t *Tree[T]) RangeQueryWorkerPool(startKey, endKey []byte, includeStart, includeEnd bool, numWorkers int) []T {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil
	}

	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	allItems := t.GetAllItems()
	if len(allItems) == 0 {
		return nil
	}

	if numWorkers > len(allItems) {
		numWorkers = len(allItems)
	}

	chunkSize := (len(allItems) + numWorkers - 1) / numWorkers
	partials := make([][]T, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(allItems) {
			break
		}
		end := start + chunkSize
		if end > len(allItems) {
			end = len(allItems)
		}

		wg.Add(1)
		go func(idx int, chunk []T) {
			defer wg.Done()
			local := make([]T, 0)
			for _, item := range chunk {
				itemKey := t.normalizeKey(item.Key())
				ik := itemKey[:]

				cmpStart := bytes.Compare(ik, startKey)
				cmpEnd := bytes.Compare(ik, endKey)

				var hit bool
				switch {
				case includeStart && includeEnd:
					hit = cmpStart >= 0 && cmpEnd <= 0
				case includeStart && !includeEnd:
					hit = cmpStart >= 0 && cmpEnd < 0
				case !includeStart && includeEnd:
					hit = cmpStart > 0 && cmpEnd <= 0
				default:
					hit = cmpStart > 0 && cmpEnd < 0
				}

				if hit {
					local = append(local, item)
				}
			}
			partials[idx] = local
		}(i, allItems[start:end])
	}

	wg.Wait()

	result := make([]T, 0)
	for _, p := range partials {
		result = append(result, p...)
	}

	slices.SortFunc(result, func(a, b T) int {
		return cmp.Compare(a.ID(), b.ID())
	})

	return result
}

// rangeQueryNode оставлен для внутреннего использования (computeNodeHash и т.п.)
func (t *Tree[T]) rangeQueryNode(node *Node[T], startKey, endKey []byte, includeStart, includeEnd bool, depth int, result *[]T) {
	if node == nil {
		return
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if node.IsLeaf {
		if node.Hash == DeletedNodeHash {
			return
		}

		itemKey := node.Value.Key()
		ik := itemKey[:]

		cmpStart := bytes.Compare(ik, startKey)
		cmpEnd := bytes.Compare(ik, endKey)

		var hit bool
		switch {
		case includeStart && includeEnd:
			hit = cmpStart >= 0 && cmpEnd <= 0
		case includeStart && !includeEnd:
			hit = cmpStart >= 0 && cmpEnd < 0
		case !includeStart && includeEnd:
			hit = cmpStart > 0 && cmpEnd <= 0
		default:
			hit = cmpStart > 0 && cmpEnd < 0
		}

		if hit {
			*result = append(*result, node.Value)
		}
		return
	}

	for i := range node.Keys {
		t.rangeQueryNode(node.Children[i], startKey, endKey, includeStart, includeEnd, depth+1, result)
	}
}

// compareKeys — обёртка над bytes.Compare (SIMD на x86/ARM).
func compareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (t *Tree[T]) RangeQueryByID(startID, endID uint64, includeStart, includeEnd bool) []T {
	sk := t.encodeID(startID) //insted of original idToKey
	ek := t.encodeID(endID)
	return t.RangeQuery(sk[:], ek[:], includeStart, includeEnd)
}

func (t *Tree[T]) RangeQueryByIDParallel(startID, endID uint64, includeStart, includeEnd bool) []T {
	sk := t.encodeID(startID)
	ek := t.encodeID(endID)
	return t.RangeQueryParallel(sk[:], ek[:], includeStart, includeEnd)
}

func (t *Tree[T]) RangeQueryAuto(startKey, endKey []byte, includeStart, includeEnd bool) []T {
	if len(startKey) == 0 || len(endKey) == 0 {
		return nil
	}

	startID := keyToID(startKey)
	endID := keyToID(endKey)
	rangeSize := endID - startID

	if rangeSize > 1000 {
		return t.RangeQueryParallel(startKey, endKey, includeStart, includeEnd)
	}

	return t.RangeQuery(startKey, endKey, includeStart, includeEnd)
}

// idToKey — возвращает [8]byte (на стеке, без heap-аллокации).
func idToKey(id uint64) [8]byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], id)
	return key
}

// keyToID конвертирует []byte в uint64.
func keyToID(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(key)
}
