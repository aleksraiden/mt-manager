package merkletree

import (
	"runtime"
	"testing"
	"time"
	"sync"
	"os"
	"errors"
	"math/rand"
)

func TestTreeBasicOperations(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	// Вставка аккаунтов
	for i := uint64(0); i < 1000; i++ {
		acc := NewAccount(i, StatusUser)
		if err := tree.Insert(acc); err != nil {
			t.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}

	// Проверка размера
	if tree.Size() != 1000 {
		t.Errorf("Ожидалось 1000 аккаунтов, получено %d", tree.Size())
	}

	// Проверка получения аккаунта
	acc, ok := tree.Get(500)
	if !ok {
		t.Error("Аккаунт 500 должен существовать")
	}
	if acc.UID != 500 {
		t.Errorf("Ожидался UID 500, получен %d", acc.UID)
	}

	// Проверка несуществующего аккаунта
	_, ok = tree.Get(10000)
	if ok {
		t.Error("Аккаунт 10000 не должен существовать")
	}
}

func TestTreeBatchInsert(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	// Подготовка пакета аккаунтов
	accounts := make([]*Account, 1000)
	for i := range accounts {
		accounts[i] = NewAccount(uint64(i), StatusUser)
	}

	// Пакетная вставка
	errs := tree.InsertBatch(accounts)
    if len(errs) > 0 {
        t.Fatalf("unexpected collisions in batch: %v", errs)
    }

	if tree.Size() != 1000 {
		t.Errorf("Ожидалось 1000 аккаунтов, получено %d", tree.Size())
	}
}

func TestTreeRootHash(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	// Вставка детерминированных аккаунтов
	for i := uint64(0); i < 100; i++ {
		acc := NewAccountDeterministic(i, StatusUser) // <-- Изменено
		if err := tree.Insert(acc); err != nil {
			t.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}

	root1 := tree.ComputeRoot()

	// Создаем второе дерево с теми же данными
	tree2 := New[*Account](DefaultConfig())
	for i := uint64(0); i < 100; i++ {
		acc := NewAccountDeterministic(i, StatusUser) // <-- Изменено
		tree2.Insert(acc)
	}

	root2 := tree2.ComputeRoot()

	// Корни должны совпадать
	if root1 != root2 {
		t.Errorf("Корневые хеши должны совпадать для идентичных деревьев\nRoot1: %x\nRoot2: %x", root1[:16], root2[:16])
	}
}

func TestCacheHit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CacheSize = 100
	tree := New[*Account](cfg)

	// Вставка аккаунтов
	for i := uint64(0); i < 1000; i++ {
		acc := NewAccount(i, StatusUser)
		if err := tree.Insert(acc); err != nil {
			t.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}

	// Первое чтение - кеш промах
	acc1, _ := tree.Get(100)

	// Второе чтение - должно быть из кеша
	acc2, _ := tree.Get(100)

	if acc1.UID != acc2.UID {
		t.Error("Кеш должен возвращать тот же объект")
	}
}

func TestTreeClear(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	if tree.Size() != 100 {
		t.Errorf("Ожидалось 100, получено %d", tree.Size())
	}

	tree.Clear()

	if tree.Size() != 0 {
		t.Errorf("После Clear ожидалось 0, получено %d", tree.Size())
	}
}

func TestTreeStats(t *testing.T) {
	tree := New[*Account](DefaultConfig())

	for i := uint64(0); i < 1000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	stats := tree.GetStats()

	if stats.TotalItems != 1000 {
		t.Errorf("Ожидалось 1000 элементов, получено %d", stats.TotalItems)
	}

	if stats.AllocatedNodes == 0 {
		t.Error("Должны быть аллоцированы узлы")
	}
}

// Бенчмарки
func BenchmarkTreeInsert(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acc := NewAccount(uint64(i), StatusUser)
		if err := tree.Insert(acc); err != nil {
			b.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}
}

func BenchmarkTreeInsertBatch(b *testing.B) {
	tree := New[*Account](DefaultConfig())
	batchSize := 1000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		accounts := make([]*Account, batchSize)
		for j := range accounts {
			accounts[j] = NewAccount(uint64(i*batchSize+j), StatusUser)
		}
		b.StartTimer()

		tree.InsertBatch(accounts)
	}
}

func BenchmarkTreeGet(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	// Подготовка данных
	for i := uint64(0); i < 1000000; i++ {
		acc := NewAccount(i, StatusUser)
		if err := tree.Insert(acc); err != nil {
			b.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(uint64(i % 1000000))
	}
}

func BenchmarkTreeGetCached(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	// Подготовка данных
	for i := uint64(0); i < 1000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	// Прогреваем кеш
	for i := uint64(0); i < 100; i++ {
		tree.Get(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(uint64(i % 100))
	}
}

func BenchmarkTreeComputeRoot(b *testing.B) {
	tree := New[*Account](DefaultConfig())

	// Подготовка данных
	for i := uint64(0); i < 10000; i++ {
		acc := NewAccount(i, StatusUser)
		if err := tree.Insert(acc); err != nil {
			b.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.ComputeRoot()
	}
}

// Интеграционный тест
func TestLargeScalePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Пропуск большого теста в short режиме")
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := DefaultConfig()
	cfg.CacheSize = 100000
	tree := New[*Account](cfg)

	t.Log("Заполнение 5M аккаунтов...")
	start := time.Now()

	for i := uint64(0); i < 5_000_000; i++ {
		acc := NewAccount(i, StatusUser)
		if err := tree.Insert(acc); err != nil {
			t.Fatalf("unexpected collision on insert ID=%d: %v", acc.UID, err)
		}

		if i > 0 && i%1_000_000 == 0 {
			t.Logf("Вставлено %dM аккаунтов", i/1_000_000)
		}
	}

	insertTime := time.Since(start)
	t.Logf("Время вставки: %v (%.0f ops/sec)", insertTime, 5_000_000/insertTime.Seconds())

	// Вычисление корня
	t.Log("Вычисление корневого хеша...")
	start = time.Now()
	root := tree.ComputeRoot()
	rootTime := time.Since(start)
	t.Logf("Root: %x | Время: %v", root[:16], rootTime)

	// Статистика памяти
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("Heap alloc: %v MB", m.HeapAlloc/1024/1024)

	stats := tree.GetStats()
	t.Logf("Статистика дерева: %+v", stats)
}

func TestTreeConcurrentGetAndRoot(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode")
	}

	tree := New[*Account](DefaultConfig())

	// Заполняем
	for i := uint64(0); i < 100_000; i++ {
		tree.Insert(NewAccount(i, StatusUser))
	}

	// Параллельные геты + периодические ComputeRoot
	const workers = 32
	const rounds = 1000

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for j := 0; j < rounds; j++ {
				uid := uint64(rng.Intn(100_000))
				_, _ = tree.Get(uid)

				if j%50 == 0 {
					_ = tree.ComputeRoot()
				}
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkConcurrentGetAndRootHighContention(b *testing.B) {
    tree := New[*Account](DefaultConfig())
    // Заполняем большим количеством элементов
    for i := uint64(0); i < 500_000; i++ {
        tree.Insert(NewAccount(i, StatusUser))
    }

    b.ResetTimer()
    b.ReportAllocs()

    b.RunParallel(func(pb *testing.PB) {
        rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(b.N)))
        for pb.Next() {
            uid := uint64(rng.Intn(500_000))
            _, _ = tree.Get(uid)

            // 1% шанс вызвать ComputeRoot (симулируем CometBFT finality)
            if rng.Intn(100) == 0 {
                _ = tree.ComputeRoot()
            }
        }
    })
}

func TestRootChangesAfterMutation(t *testing.T) {
    tree := New[*Account](DefaultConfig())
    acc1 := NewAccount(1, StatusUser)
    if err := tree.Insert(acc1); err != nil {
		t.Fatalf("unexpected collision on insert ID=%d: %v", acc1.UID, err)
	}

    root1 := tree.ComputeRoot()

    acc2 := NewAccount(2, StatusUser)
    if err := tree.Insert(acc2); err != nil {
		t.Fatalf("unexpected collision on insert ID=%d: %v", acc2.UID, err)
	}

    root2 := tree.ComputeRoot()

    if root1 == root2 {
        t.Error("Root hash должен измениться после вставки")
    }
}

// TestCollisionIsDetected проверяет что коллизия возвращает ошибку
// и НЕ перезаписывает существующий элемент
func TestCollisionIsDetected(t *testing.T) {
    // MaxDepth=3 + BigEndian: IDs 1 и 257 имеют одинаковый путь
    // key[0]=0x00, key[1]=0x00, key[7]=0x01 для обоих
    cfg := &Config{MaxDepth: 3, CacheSize: 1024, CacheShards: 4}
    tree := New[*Account](cfg)

    // Первая вставка — должна пройти
    acc1 := NewAccount(1, StatusUser)
    if err := tree.Insert(acc1); err != nil {
        t.Fatalf("первая вставка не должна давать ошибку: %v", err)
    }

    // Вторая вставка с коллизионным ID — должна вернуть CollisionError
    acc257 := NewAccount(257, StatusUser) // key[7] == 0x01, как у ID=1
    err := tree.Insert(acc257)
    if err == nil {
        t.Fatal("ожидалась CollisionError, получили nil")
    }

    var colErr *CollisionError
    if !errors.As(err, &colErr) {
        t.Fatalf("ожидался *CollisionError, получили %T: %v", err, err)
    }

    t.Logf("коллизия обнаружена корректно: %v", colErr)

    // Проверяем что оригинальный элемент НЕ перезаписан
    if tree.Size() != 1 {
        t.Errorf("размер должен быть 1, получено %d", tree.Size())
    }

    got, ok := tree.Get(1)
    if !ok {
        t.Fatal("ID=1 должен существовать")
    }
    if got.UID != 1 {
        t.Errorf("ID=1 должен быть нетронут, получен UID=%d", got.UID)
    }

    // ID=257 не должен быть в дереве
    _, ok = tree.Get(257)
    if ok {
        t.Error("ID=257 не должен быть в дереве после коллизии")
    }
}

// TestUpdateSameIDIsNotCollision проверяет что обновление того же ID
// не считается коллизией
func TestUpdateSameIDIsNotCollision(t *testing.T) {
    tree := New[*Account](DefaultConfig())

    acc := NewAccount(42, StatusUser)
    if err := tree.Insert(acc); err != nil {
        t.Fatalf("первая вставка: %v", err)
    }

    //StatusMM — любой статус отличный от StatusUser, чтобы убедиться что UPDATE прошёл
    accUpdated := NewAccount(42, StatusMM)
    if err := tree.Insert(accUpdated); err != nil {
        t.Fatalf("UPDATE того же ID не должен давать коллизию: %v", err)
    }

    got, _ := tree.Get(42)

    if got.Status != StatusMM {
        t.Errorf("UPDATE должен обновить значение, ожидался StatusMM, получен %v", got.Status)
    }
}

// TestBatchCollisionPartialSuccess проверяет что при коллизии в батче
// успешные элементы вставляются, коллизионные — нет
func TestBatchCollisionPartialSuccess(t *testing.T) {
    cfg := &Config{MaxDepth: 3, CacheSize: 1024, CacheShards: 4}
    tree := New[*Account](cfg)

    // ID=1 уже в дереве
    if err := tree.Insert(NewAccount(1, StatusUser)); err != nil {
        t.Fatal(err)
    }

    // Батч: ID=2 (чистый), ID=257 (коллизия с ID=1), ID=3 (чистый)
    batch := []*Account{
        NewAccount(2, StatusUser),   // чистый
        NewAccount(257, StatusUser), // коллизия: key[7]=0x01 как у ID=1
        NewAccount(3, StatusUser),   // чистый
    }

    errs := tree.InsertBatch(batch)

    // Ровно одна ошибка
    if len(errs) != 1 {
        t.Errorf("ожидалась 1 ошибка, получено %d: %v", len(errs), errs)
    }

    // Размер: 1 (существующий) + 2 (успешные из батча) = 3
    if tree.Size() != 3 {
        t.Errorf("ожидалось 3 элемента, получено %d", tree.Size())
    }

    // ID=2 и ID=3 должны быть в дереве
    if _, ok := tree.Get(2); !ok {
        t.Error("ID=2 должен быть вставлен")
    }
    if _, ok := tree.Get(3); !ok {
        t.Error("ID=3 должен быть вставлен")
    }

    // ID=257 не должен быть в дереве
    if _, ok := tree.Get(257); ok {
        t.Error("ID=257 не должен быть в дереве после коллизии")
    }
}

// ============================================
// TestMarkDirty
// Проверяет: мутация через указатель + MarkDirty корректно
// обновляет хеш и попадает в инкрементальный снапшот
// ============================================

/***original
func TestMarkDirty(t *testing.T) {
	dir := "./test_markdirty"
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)

	tree, _ := CreateTree[*Account](mgr, "accounts")

	// Базовое заполнение
	for i := uint64(0); i < 100; i++ {
		tree.Insert(NewAccountDeterministic(i, StatusUser))
	}

	// Чекпоинт
	cpVersion, err := mgr.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}
	t.Logf("Checkpoint: %x", cpVersion[:8])

	rootAfterCP := mgr.ComputeGlobalRoot()

	// --- Тест 1: хеш не меняется без MarkDirty ---
	acc0, _ := tree.Get(0)
	acc0.Status = StatusBlocked // мутируем через указатель
	rootNoMark := mgr.ComputeGlobalRoot()
	if rootAfterCP != rootNoMark {
		t.Error("Root should NOT change without MarkDirty")
	}

	// --- Тест 2: хеш меняется после MarkDirty ---
	tree.MarkDirty(0)
	rootAfterMark := mgr.ComputeGlobalRoot()
	if rootAfterCP == rootAfterMark {
		t.Error("Root SHOULD change after MarkDirty")
	}
	t.Logf("Root changed: %x → %x", rootAfterCP[:8], rootAfterMark[:8])

	// --- Тест 3: батч мутаций ---
	// Мутируем через указатель — items уже содержит тот же *Account
	mutations := map[uint64]AccountStatus{
		10: StatusBlocked,
		20: StatusMM,
		30: StatusAlgo,
		50: StatusVIP,
		99: StatusSystem,
	}
	ids := make([]uint64, 0, len(mutations))
	for id, status := range mutations {
		acc, ok := tree.Get(id)
		if !ok {
			t.Fatalf("Account %d not found", id)
		}
		acc.Status = status  // мутируем через указатель — items видит изменение
		ids = append(ids, id)
	}

	// Один вызов — инвалидирует хеши + регистрирует в dirtyKeys
	tree.MarkDirty(ids...)

	rootAfterBatch := mgr.ComputeGlobalRoot()
	if rootAfterMark == rootAfterBatch {
		t.Error("Root SHOULD change after batch MarkDirty")
	}
	t.Logf("Batch MarkDirty(%d items): %x → %x", len(ids), rootAfterMark[:8], rootAfterBatch[:8])

	// --- Тест 4: несуществующий ID не паникует и не меняет корень ---
	rootBeforeInvalid := mgr.ComputeGlobalRoot()
	tree.MarkDirty(99999, 88888)
	rootAfterInvalid := mgr.ComputeGlobalRoot()
	if rootBeforeInvalid != rootAfterInvalid {
		t.Error("Root should NOT change for non-existent IDs")
	}

	// --- Тест 5: мутации попадают в инкрементальный снапшот ---
	snapVersion, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	t.Logf("Incremental snapshot: %x", snapVersion[:8])

	// Проверяем что это инкрементальный, а не чекпоинт
	header, err := mgr.snapshotMgr.storage.LoadHeader(&snapVersion)
	if err != nil {
		t.Fatalf("LoadHeader failed: %v", err)
	}
	if header.Kind != KindIncremental {
		t.Errorf("Expected KindIncremental, got %v", header.Kind)
	}

	finalRoot := mgr.ComputeGlobalRoot()

	// Закрываем перед восстановлением
	if err := mgr.CloseSnapshots(); err != nil {
		t.Fatalf("CloseSnapshots: %v", err)
	}

	// --- Тест 6: восстановление — все мутации на месте ---
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	factory := func(name string) TreeInterface {
		switch name {
		case "accounts":
			tr := New[*Account](mgr2.config)
			return &TypedTree[*Account]{Tree: tr}
		}
		return nil
	}

	if err := mgr2.LoadFromSnapshot(snapVersion, factory); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}

	restoredTree, ok := GetTree[*Account](mgr2, "accounts")
	if !ok {
		t.Fatal("Tree 'accounts' not found after restore")
	}

	if restoredTree.Size() != 100 {
		t.Errorf("Expected 100 items, got %d", restoredTree.Size())
	}

	// Проверяем каждую мутацию
	for id, expectedStatus := range mutations {
		acc, found := restoredTree.Get(id)
		if !found {
			t.Errorf("Account %d not found after restore", id)
			continue
		}
		if acc.Status != expectedStatus {
			t.Errorf("Account %d: expected status %v, got %v",
				id, expectedStatus, acc.Status)
		}
	}

	// Проверяем acc0 (StatusBlocked, id=0)
	acc0restored, _ := restoredTree.Get(0)
	if acc0restored.Status != StatusBlocked {
		t.Errorf("Account 0: expected StatusBlocked, got %v", acc0restored.Status)
	}

	// Корень должен совпасть
	assertRootEqual(t, "markdirty restore", finalRoot, mgr2.ComputeGlobalRoot())
	t.Logf("✓ All %d mutations restored correctly", len(mutations)+1)
} **/

func TestMarkDirty(t *testing.T) {
    dir := "./test_markdirty"
    defer os.RemoveAll(dir)

    mgr := setupIncrementalManager(t, dir)

    tree, _ := CreateTree[*Account](mgr, "accounts")

    for i := uint64(0); i < 100; i++ {
        tree.Insert(NewAccountDeterministic(i, StatusUser))
    }

    cpVersion, err := mgr.CreateCheckpoint()
    if err != nil {
        t.Fatalf("CreateCheckpoint failed: %v", err)
    }
    t.Logf("Checkpoint: %x", cpVersion[:8])

    // --- ТОЧКА 1: мутируем через указатель ---
    mutations := map[uint64]AccountStatus{
        10: StatusBlocked,
        20: StatusMM,
        30: StatusAlgo,
        50: StatusVIP,
        99: StatusSystem,
    }
    ids := make([]uint64, 0, len(mutations))
    for id, status := range mutations {
        acc, ok := tree.Get(id)
        if !ok {
            t.Fatalf("Account %d not found", id)
        }
        acc.Status = status
        ids = append(ids, id)

        // Лог 1: проверяем что указатель в items тот же
        accFromItems, _ := tree.Get(id)
        t.Logf("[L1] id=%d: after mutation via ptr, items has status=%v (expected %v), same ptr=%v",
            id, accFromItems.Status, status, acc == accFromItems)
    }

    tree.MarkDirty(ids...)

    // --- ТОЧКА 2: что в dirtyKeys после MarkDirty ---
    tree.dirtyMu.Lock()
    t.Logf("[L2] dirtyKeys count: %d (expected %d)", len(tree.dirtyKeys), len(mutations))
    for k := range tree.dirtyKeys {
        t.Logf("[L2] dirtyKey: %x", k)
    }
    tree.dirtyMu.Unlock()

    // --- ТОЧКА 3: что serializeDirtyItems возвращает ---
    typedTree := &TypedTree[*Account]{Tree: tree}
    upserted, deleted, err := typedTree.serializeDirtyItems()
    if err != nil {
        t.Fatalf("serializeDirtyItems failed: %v", err)
    }
    t.Logf("[L3] upserted=%d deleted=%d (expected upserted=%d)", len(upserted), len(deleted), len(mutations))

    // Десериализуем и проверяем статусы в дельте
    for i, data := range upserted {
        var acc Account
        if err := acc.Deserialize(data); err != nil {
            t.Fatalf("Deserialize upserted[%d] failed: %v", i, err)
        }
        t.Logf("[L3] upserted[%d]: id=%d status=%v", i, acc.UID, acc.Status)
    }

    // --- ТОЧКА 4: создаём снапшот и проверяем его header ---
    snapVersion, err := mgr.CreateSnapshot()
    if err != nil {
        t.Fatalf("CreateSnapshot failed: %v", err)
    }
    header, _ := mgr.snapshotMgr.storage.LoadHeader(&snapVersion)
    t.Logf("[L4] snapshot kind=%v", header.Kind)

    finalRoot := mgr.ComputeGlobalRoot()

    if err := mgr.CloseSnapshots(); err != nil {
        t.Fatalf("CloseSnapshots: %v", err)
    }

    // --- ТОЧКА 5: восстановление ---
    mgr2 := setupIncrementalManager(t, dir)
    defer mgr2.CloseSnapshots()

    factory := func(name string) TreeInterface {
        switch name {
        case "accounts":
            tr := New[*Account](mgr2.config)
            return &TypedTree[*Account]{Tree: tr}
        }
        return nil
    }

    if err := mgr2.LoadFromSnapshot(snapVersion, factory); err != nil {
        t.Fatalf("LoadFromSnapshot failed: %v", err)
    }

    restoredTree, _ := GetTree[*Account](mgr2, "accounts")
    t.Logf("[L5] restored size=%d", restoredTree.Size())

    for id, expectedStatus := range mutations {
        acc, found := restoredTree.Get(id)
        if !found {
            t.Errorf("[L5] Account %d not found", id)
            continue
        }
        t.Logf("[L5] id=%d: got status=%v expected=%v match=%v",
            id, acc.Status, expectedStatus, acc.Status == expectedStatus)
    }

    assertRootEqual(t, "markdirty restore", finalRoot, mgr2.ComputeGlobalRoot())
}