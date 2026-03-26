package merkletree

import (
	"runtime"
	"testing"
	"time"
	"sync"
	"os"
	"sort"
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
} 

func TestMarkDirtyPathInvalidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TrackDirty = true

	tree := New[*Account](cfg)

	for i := uint64(0); i < 100; i++ {
		if err := tree.Insert(NewAccountDeterministic(i, StatusUser)); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	rootBefore := tree.ComputeRoot()
	if tree.GetDirtyNodeCount() != 0 {
		t.Fatalf("expected dirtyNodes=0 after initial ComputeRoot, got %d", tree.GetDirtyNodeCount())
	}

	acc, ok := tree.Get(20)
	if !ok {
		t.Fatal("account 20 not found")
	}

	oldStatus := acc.Status
	acc.Status = StatusMM

	rootAfterPtrMutation := tree.ComputeRoot()
	if rootBefore != rootAfterPtrMutation {
		t.Fatalf(
			"root changed without MarkDirty: before=%x afterPtr=%x",
			rootBefore[:8],
			rootAfterPtrMutation[:8],
		)
	}

	if tree.GetDirtyNodeCount() != 0 {
		t.Fatalf("expected dirtyNodes=0 before MarkDirty, got %d", tree.GetDirtyNodeCount())
	}

	tree.MarkDirty(20)

	if tree.GetDirtyNodeCount() == 0 {
		t.Fatalf("expected dirtyNodes > 0 after MarkDirty, got %d", tree.GetDirtyNodeCount())
	}

	tree.dirtyMu.Lock()
	_, dirtyTracked := tree.dirtyKeys[acc.Key()]
	deletedTracked := len(tree.deletedKeys)
	dirtyCount := len(tree.dirtyKeys)
	tree.dirtyMu.Unlock()

	if !dirtyTracked {
		t.Fatalf("expected key %x to be present in dirtyKeys", acc.Key())
	}

	if deletedTracked != 0 {
		t.Fatalf("expected deletedKeys to stay empty, got %d", deletedTracked)
	}

	rootAfterMarkDirty := tree.ComputeRoot()
	if rootBefore == rootAfterMarkDirty {
		t.Fatalf(
			"root did not change after MarkDirty: before=%x afterMark=%x",
			rootBefore[:8],
			rootAfterMarkDirty[:8],
		)
	}

	if tree.GetDirtyNodeCount() != 0 {
		t.Fatalf("expected dirtyNodes=0 after recompute, got %d", tree.GetDirtyNodeCount())
	}

	acc2, ok := tree.Get(20)
	if !ok {
		t.Fatal("account 20 not found after recompute")
	}

	if acc2.Status != StatusMM {
		t.Fatalf("expected status=%v after recompute, got %v", StatusMM, acc2.Status)
	}

	t.Logf(
		"path invalidation ok: id=%d status %v -> %v dirtyKeys=%d root %x -> %x",
		acc2.UID,
		oldStatus,
		acc2.Status,
		dirtyCount,
		rootBefore[:8],
		rootAfterMarkDirty[:8],
	)
}

func TestRestoreDoesNotIncreaseSizeOnUpdates(t *testing.T) {
	dir := "./test_restore_size_updates"
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	mgr := setupIncrementalManager(t, dir)

	tree, err := CreateTree[*Account](mgr, "accounts")
	if err != nil {
		t.Fatalf("CreateTree failed: %v", err)
	}

	makeFactory := func(mgr *UniversalManager) TreeFactory {
		return func(name string) TreeInterface {
			switch name {
			case "accounts":
				tr := New[*Account](mgr.config)
				return &TypedTree[*Account]{Tree: tr}
			default:
				return nil
			}
		}
	}

	sortedMutationIDs := func(m map[uint64]AccountStatus) []uint64 {
		ids := make([]uint64, 0, len(m))
		for id := range m {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return ids
	}

	assertTreeCardinality := func(t *testing.T, label string, tr *Tree[*Account], want int) {
		t.Helper()

		gotSize := tr.Size()
		stats := tr.GetStats()
		items := tr.GetAllItems()

		uniq := make(map[uint64]int, len(items))
		for _, acc := range items {
			if acc == nil {
				t.Fatalf("[%s] GetAllItems returned nil item", label)
			}
			uniq[acc.UID]++
		}

		t.Logf(
			"[%s] size=%d stats.TotalItems=%d len(GetAllItems)=%d uniqueIDs=%d",
			label,
			gotSize,
			stats.TotalItems,
			len(items),
			len(uniq),
		)

		if gotSize != want {
			t.Fatalf("[%s] Size()=%d, want=%d", label, gotSize, want)
		}
		if stats.TotalItems != want {
			t.Fatalf("[%s] GetStats().TotalItems=%d, want=%d", label, stats.TotalItems, want)
		}
		if len(items) != want {
			t.Fatalf("[%s] len(GetAllItems())=%d, want=%d", label, len(items), want)
		}
		if len(uniq) != want {
			t.Fatalf("[%s] unique IDs=%d, want=%d", label, len(uniq), want)
		}

		for id, count := range uniq {
			if count != 1 {
				t.Fatalf("[%s] id=%d appears %d times in GetAllItems()", label, id, count)
			}
		}
	}

	checkStatuses := func(t *testing.T, label string, tr *Tree[*Account], expected map[uint64]AccountStatus) {
		t.Helper()

		for _, id := range sortedMutationIDs(expected) {
			acc, ok := tr.Get(id)
			if !ok {
				t.Fatalf("[%s] account %d not found", label, id)
			}
			if acc.Status != expected[id] {
				t.Fatalf("[%s] account %d status=%v, want=%v", label, id, acc.Status, expected[id])
			}
		}
	}

	const initialCount = 100

	for i := uint64(0); i < initialCount; i++ {
		if err := tree.Insert(NewAccountDeterministic(i, StatusUser)); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	assertTreeCardinality(t, "SOURCE_BEFORE_CP", tree, initialCount)

	cpVersion, err := mgr.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}
	t.Logf("[CP] version=%x", cpVersion[:8])

	mutations := map[uint64]AccountStatus{
		0:  StatusBlocked,
		10: StatusBlocked,
		20: StatusMM,
		30: StatusAlgo,
		50: StatusVIP,
		99: StatusSystem,
	}

	ids := sortedMutationIDs(mutations)
	for _, id := range ids {
		acc, ok := tree.Get(id)
		if !ok {
			t.Fatalf("source account %d not found", id)
		}
		acc.Status = mutations[id]
	}
	tree.MarkDirty(ids...)

	snapVersion, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	t.Logf("[SNAP] version=%x", snapVersion[:8])

	sourceRoot := mgr.ComputeGlobalRoot()
	assertTreeCardinality(t, "SOURCE_AFTER_INCREMENTAL", tree, initialCount)
	checkStatuses(t, "SOURCE_AFTER_INCREMENTAL", tree, mutations)

	header, err := mgr.snapshotMgr.storage.LoadHeader(&snapVersion)
	if err != nil {
		t.Fatalf("LoadHeader failed: %v", err)
	}
	if header.Kind != KindIncremental {
		t.Fatalf("expected incremental snapshot, got kind=%v", header.Kind)
	}

	chain, err := mgr.snapshotMgr.storage.BuildChain(header.CheckpointRef, snapVersion)
	if err != nil {
		t.Fatalf("BuildChain failed: %v", err)
	}
	if len(chain) != 1 {
		t.Fatalf("expected chain len=1, got %d", len(chain))
	}

	if err := mgr.CloseSnapshots(); err != nil {
		t.Fatalf("CloseSnapshots source failed: %v", err)
	}

	// 1) Checkpoint-only restore: размер должен быть 100 и старые статусы.
	mgrCP := setupIncrementalManager(t, dir)

	cpRef := header.CheckpointRef
	if err := mgrCP.snapshotMgr.loadCheckpoint(mgrCP, &cpRef, makeFactory(mgrCP)); err != nil {
		_ = mgrCP.CloseSnapshots()
		t.Fatalf("[CP] loadCheckpoint failed: %v", err)
	}

	cpTree, ok := GetTree[*Account](mgrCP, "accounts")
	if !ok {
		_ = mgrCP.CloseSnapshots()
		t.Fatal("[CP] accounts tree not found")
	}

	assertTreeCardinality(t, "CP_ONLY_RESTORE", cpTree, initialCount)

	for _, id := range ids {
		acc, ok := cpTree.Get(id)
		if !ok {
			_ = mgrCP.CloseSnapshots()
			t.Fatalf("[CP] account %d not found", id)
		}
		if acc.Status != StatusUser {
			_ = mgrCP.CloseSnapshots()
			t.Fatalf("[CP] account %d status=%v, want=%v", id, acc.Status, StatusUser)
		}
	}

	if err := mgrCP.CloseSnapshots(); err != nil {
		t.Fatalf("[CP] CloseSnapshots failed: %v", err)
	}

	// 2) Manual restore: checkpoint + applyIncremental.
	mgrManual := setupIncrementalManager(t, dir)

	cpRef2 := header.CheckpointRef
	if err := mgrManual.snapshotMgr.loadCheckpoint(mgrManual, &cpRef2, makeFactory(mgrManual)); err != nil {
		_ = mgrManual.CloseSnapshots()
		t.Fatalf("[MANUAL] loadCheckpoint failed: %v", err)
	}

	for i, entry := range chain {
		t.Logf("[MANUAL] applying chain[%d]=%x", i, entry.Version[:8])
		if err := mgrManual.snapshotMgr.applyIncremental(mgrManual, entry); err != nil {
			_ = mgrManual.CloseSnapshots()
			t.Fatalf("[MANUAL] applyIncremental failed: %v", err)
		}
	}

	manualTree, ok := GetTree[*Account](mgrManual, "accounts")
	if !ok {
		_ = mgrManual.CloseSnapshots()
		t.Fatal("[MANUAL] accounts tree not found")
	}

	assertTreeCardinality(t, "MANUAL_RESTORE", manualTree, initialCount)
	checkStatuses(t, "MANUAL_RESTORE", manualTree, mutations)

	manualRoot := mgrManual.ComputeGlobalRoot()
	if manualRoot != sourceRoot {
		_ = mgrManual.CloseSnapshots()
		t.Fatalf("[MANUAL] root=%x, want sourceRoot=%x", manualRoot[:8], sourceRoot[:8])
	}

	if err := mgrManual.CloseSnapshots(); err != nil {
		t.Fatalf("[MANUAL] CloseSnapshots failed: %v", err)
	}

	// 3) Full restore through LoadFromSnapshot.
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	if err := mgr2.LoadFromSnapshot(snapVersion, makeFactory(mgr2)); err != nil {
		t.Fatalf("[FULL] LoadFromSnapshot failed: %v", err)
	}

	fullTree, ok := GetTree[*Account](mgr2, "accounts")
	if !ok {
		t.Fatal("[FULL] accounts tree not found")
	}

	assertTreeCardinality(t, "FULL_RESTORE", fullTree, initialCount)
	checkStatuses(t, "FULL_RESTORE", fullTree, mutations)

	fullRoot := mgr2.ComputeGlobalRoot()
	if fullRoot != sourceRoot {
		t.Fatalf("[FULL] root=%x, want sourceRoot=%x", fullRoot[:8], sourceRoot[:8])
	}
}
