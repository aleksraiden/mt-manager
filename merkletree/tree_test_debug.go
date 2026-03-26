package merkletree

import (
	"testing"
	"os"
	"sort"
	"encoding/binary"
)
// ============================================
// TestMarkDirty
// Проверяет: мутация через указатель + MarkDirty корректно
// обновляет хеш и попадает в инкрементальный снапшот
// ============================================

func TestDeepDebugMarkDirty(t *testing.T) {
	dir := "./test_markdirty"
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

	dumpVersions := func(t *testing.T, mgr *UniversalManager, label string) {
		t.Helper()

		versions, err := mgr.ListSnapshotVersions()
		if err != nil {
			t.Logf("[%s] ListSnapshotVersions err: %v", label, err)
			return
		}

		sort.Slice(versions, func(i, j int) bool {
			return string(versions[i][:]) < string(versions[j][:])
		})

		t.Logf("[%s] snapshot versions count=%d", label, len(versions))
		for i := range versions {
			v := versions[i]
			hdr, err := mgr.snapshotMgr.storage.LoadHeader(&v)
			if err != nil {
				t.Logf("[%s]   version[%d]=%x LoadHeader err=%v", label, i, v[:8], err)
				continue
			}
			t.Logf(
				"[%s]   version[%d]=%x kind=%v cpRef=%x parent=%x ts=%d schema=%d",
				label,
				i,
				v[:8],
				hdr.Kind,
				hdr.CheckpointRef[:8],
				hdr.ParentVersion[:8],
				hdr.Timestamp,
				hdr.SchemaVersion,
			)
		}
	}

	dumpTreeAccounts := func(t *testing.T, label string, tr *Tree[*Account], ids []uint64) {
		t.Helper()

		root := tr.ComputeRoot()
		stats := tr.GetStats()
		t.Logf(
			"[%s] tree root=%x size=%d dirtyNodes=%d items=%d deleted=%d cache=%d",
			label,
			root[:8],
			tr.Size(),
			tr.GetDirtyNodeCount(),
			stats.TotalItems,
			stats.DeletedNodes,
			stats.CacheSize,
		)

		for _, id := range ids {
			accGet, okGet := tr.Get(id)
			accItems, okItems := tr.items.Load(id)
			node := tr.findLeaf(tr.root, id, 0)

			var leafStatus AccountStatus
			var leafUID uint64
			var leafPtr *Account
			var leafDirty bool
			if node != nil && node.Value != nil {
				node.mu.RLock()
				leafUID = node.Value.UID
				leafStatus = node.Value.Status
				leafPtr = node.Value
				leafDirty = node.dirty.Load()
				node.mu.RUnlock()
			}

			var getStatus any = "<missing>"
			var getKey any = "<missing>"
			if okGet && accGet != nil {
				getStatus = accGet.Status
				getKey = accGet.Key()
			}

			var itemsStatus any = "<missing>"
			var itemsKey any = "<missing>"
			if okItems && accItems != nil {
				itemsStatus = accItems.Status
				itemsKey = accItems.Key()
			}

			t.Logf(
				"[%s] id=%d okGet=%v okItems=%v getStatus=%v itemsStatus=%v leafUID=%d leafStatus=%v leafDirty=%v same(get,items)=%v same(get,leaf)=%v same(items,leaf)=%v getKey=%x itemsKey=%x",
				label,
				id,
				okGet,
				okItems,
				getStatus,
				itemsStatus,
				leafUID,
				leafStatus,
				leafDirty,
				okGet && okItems && accGet == accItems,
				okGet && leafPtr != nil && accGet == leafPtr,
				okItems && leafPtr != nil && accItems == leafPtr,
				getKey,
				itemsKey,
			)

			if okGet && accGet != nil {
				if idx, ok := tr.keyIndex.Load(accGet.Key()); ok {
					t.Logf("[%s]   keyIndex[%x] = %v", label, accGet.Key(), idx)
				} else {
					t.Logf("[%s]   keyIndex[%x] = <missing>", label, accGet.Key())
				}
			}
		}
	}

	dumpDirtyMaps := func(t *testing.T, label string, tr *Tree[*Account]) {
		t.Helper()

		tr.dirtyMu.Lock()
		defer tr.dirtyMu.Unlock()

		dirtyKeys := make([][8]byte, 0, len(tr.dirtyKeys))
		for k := range tr.dirtyKeys {
			dirtyKeys = append(dirtyKeys, k)
		}
		sort.Slice(dirtyKeys, func(i, j int) bool {
			return binary.BigEndian.Uint64(dirtyKeys[i][:]) < binary.BigEndian.Uint64(dirtyKeys[j][:])
		})

		deletedKeys := make([][8]byte, 0, len(tr.deletedKeys))
		for k := range tr.deletedKeys {
			deletedKeys = append(deletedKeys, k)
		}
		sort.Slice(deletedKeys, func(i, j int) bool {
			return binary.BigEndian.Uint64(deletedKeys[i][:]) < binary.BigEndian.Uint64(deletedKeys[j][:])
		})

		t.Logf("[%s] dirtyKeys=%d deletedKeys=%d", label, len(dirtyKeys), len(deletedKeys))
		for i, k := range dirtyKeys {
			t.Logf("[%s]   dirty[%d] key=%x uint64=%d", label, i, k, binary.BigEndian.Uint64(k[:]))
		}
		for i, k := range deletedKeys {
			t.Logf("[%s]   deleted[%d] key=%x uint64=%d", label, i, k, binary.BigEndian.Uint64(k[:]))
		}
	}

	dumpSerializedDirty := func(t *testing.T, label string, tr *Tree[*Account]) {
		t.Helper()

		typed := &TypedTree[*Account]{Tree: tr}
		upserted, deleted, err := typed.serializeDirtyItems()
		if err != nil {
			t.Fatalf("[%s] serializeDirtyItems failed: %v", label, err)
		}

		t.Logf("[%s] serializeDirtyItems: upserted=%d deleted=%d", label, len(upserted), len(deleted))

		for i, raw := range upserted {
			var acc Account
			if err := acc.Deserialize(raw); err != nil {
				t.Fatalf("[%s] upserted[%d] deserialize failed: %v", label, i, err)
			}
			t.Logf("[%s]   upserted[%d]: uid=%d status=%v key=%x", label, i, acc.UID, acc.Status, acc.Key())
		}

		for i, raw := range deleted {
			if len(raw) != 8 {
				t.Logf("[%s]   deleted[%d]: INVALID LEN=%d raw=%x", label, i, len(raw), raw)
				continue
			}
			t.Logf("[%s]   deleted[%d]: key=%x uint64=%d", label, i, raw, binary.BigEndian.Uint64(raw))
		}
	}

	dumpIncrementalPayload := func(t *testing.T, mgr *UniversalManager, version [32]byte, label string) {
		t.Helper()

		deltas, err := mgr.snapshotMgr.storage.LoadIncrementalDelta(version)
		if err != nil {
			t.Logf("[%s] LoadIncrementalDelta err: %v", label, err)
			return
		}

		t.Logf("[%s] delta tree count=%d", label, len(deltas))
		for treeName, delta := range deltas {
			t.Logf("[%s]   tree=%s upserts=%d deleted=%d", label, treeName, len(delta.UpsertItems), len(delta.DeletedKeys))
			for i, raw := range delta.UpsertItems {
				var acc Account
				if err := acc.Deserialize(raw); err != nil {
					t.Fatalf("[%s]   tree=%s upsert[%d] deserialize failed: %v", label, treeName, i, err)
				}
				t.Logf("[%s]   tree=%s upsert[%d]: uid=%d status=%v key=%x", label, treeName, i, acc.UID, acc.Status, acc.Key())
			}
			for i, raw := range delta.DeletedKeys {
				if len(raw) != 8 {
					t.Logf("[%s]   tree=%s deleted[%d]: INVALID LEN=%d raw=%x", label, treeName, i, len(raw), raw)
					continue
				}
				t.Logf("[%s]   tree=%s deleted[%d]: key=%x uint64=%d", label, treeName, i, raw, binary.BigEndian.Uint64(raw))
			}
		}
	}

	checkStatuses := func(t *testing.T, label string, tr *Tree[*Account], expected map[uint64]AccountStatus) {
		t.Helper()

		ids := sortedMutationIDs(expected)
		for _, id := range ids {
			acc, ok := tr.Get(id)
			if !ok {
				t.Errorf("[%s] account %d not found", label, id)
				continue
			}
			match := acc.Status == expected[id]
			t.Logf("[%s] id=%d got=%v expected=%v match=%v", label, id, acc.Status, expected[id], match)
			if !match {
				t.Errorf("[%s] id=%d got=%v expected=%v", label, id, acc.Status, expected[id])
			}
		}
	}

	// ------------------------------------------------------------
	// 1. Базовое заполнение
	// ------------------------------------------------------------
	for i := uint64(0); i < 100; i++ {
		if err := tree.Insert(NewAccountDeterministic(i, StatusUser)); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	baseIDs := []uint64{0, 10, 20, 30, 50, 99}
	dumpTreeAccounts(t, "INIT", tree, baseIDs)

	rootBeforeCP := mgr.ComputeGlobalRoot()
	t.Logf("[INIT] global root before checkpoint=%x", rootBeforeCP[:8])

	cpVersion, err := mgr.CreateCheckpoint()
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	t.Logf("[CP] checkpoint version=%x", cpVersion[:8])
	dumpVersions(t, mgr, "AFTER_CHECKPOINT")

	rootAfterCP := mgr.ComputeGlobalRoot()
	t.Logf("[CP] global root after checkpoint=%x", rootAfterCP[:8])

	// ------------------------------------------------------------
	// 2. Проверка одиночной мутации без MarkDirty
	// ------------------------------------------------------------
	acc0, ok := tree.Get(0)
	if !ok {
		t.Fatal("account 0 not found")
	}
	old0 := acc0.Status
	acc0.Status = StatusBlocked

	t.Logf("[STEP1] mutated id=0 through ptr: old=%v new=%v ptr=%p", old0, acc0.Status, acc0)

	dumpTreeAccounts(t, "AFTER_PTR_MUTATION_NO_MARK", tree, []uint64{0})

	rootNoMark := mgr.ComputeGlobalRoot()
	t.Logf("[STEP1] global root after ptr mutation without MarkDirty=%x", rootNoMark[:8])

	if rootAfterCP != rootNoMark {
		t.Errorf("[STEP1] root changed without MarkDirty: cp=%x noMark=%x", rootAfterCP[:8], rootNoMark[:8])
	}

	// ------------------------------------------------------------
	// 3. MarkDirty(0)
	// ------------------------------------------------------------
	tree.MarkDirty(0)

	dumpDirtyMaps(t, "AFTER_MARKDIRTY_0", tree)
	dumpTreeAccounts(t, "AFTER_MARKDIRTY_0", tree, []uint64{0})
	dumpSerializedDirty(t, "AFTER_MARKDIRTY_0", tree)

	rootAfterMark0 := mgr.ComputeGlobalRoot()
	t.Logf("[STEP2] global root after MarkDirty(0)=%x", rootAfterMark0[:8])

	if rootAfterCP == rootAfterMark0 {
		t.Errorf("[STEP2] root did not change after MarkDirty(0): cp=%x marked=%x", rootAfterCP[:8], rootAfterMark0[:8])
	}

	// ------------------------------------------------------------
	// 4. Батч мутаций через ptr + MarkDirty(ids...)
	// ------------------------------------------------------------
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
			t.Fatalf("account %d not found before mutation", id)
		}

		before := acc.Status
		acc.Status = mutations[id]

		accGet, _ := tree.Get(id)
		accItems, _ := tree.items.Load(id)
		leaf := tree.findLeaf(tree.root, id, 0)

		var leafStatus AccountStatus
		var leafPtr *Account
		if leaf != nil && leaf.Value != nil {
			leaf.mu.RLock()
			leafStatus = leaf.Value.Status
			leafPtr = leaf.Value
			leaf.mu.RUnlock()
		}

		t.Logf(
			"[STEP3_MUTATE] id=%d before=%v after=%v same(get,get2)=%v same(get,items)=%v same(get,leaf)=%v get2.status=%v items.status=%v leaf.status=%v",
			id,
			before,
			acc.Status,
			acc == accGet,
			acc == accItems,
			acc == leafPtr,
			accGet.Status,
			accItems.Status,
			leafStatus,
		)
	}

	dumpTreeAccounts(t, "BEFORE_BATCH_MARKDIRTY", tree, ids)

	tree.MarkDirty(ids...)

	dumpDirtyMaps(t, "AFTER_BATCH_MARKDIRTY", tree)
	dumpTreeAccounts(t, "AFTER_BATCH_MARKDIRTY", tree, ids)
	dumpSerializedDirty(t, "AFTER_BATCH_MARKDIRTY", tree)

	rootAfterBatch := mgr.ComputeGlobalRoot()
	t.Logf("[STEP3] global root after batch MarkDirty=%x", rootAfterBatch[:8])

	if rootAfterMark0 == rootAfterBatch {
		t.Errorf("[STEP3] root did not change after batch MarkDirty: prev=%x batch=%x", rootAfterMark0[:8], rootAfterBatch[:8])
	}

	// ------------------------------------------------------------
	// 5. Невалидные ID
	// ------------------------------------------------------------
	rootBeforeInvalid := mgr.ComputeGlobalRoot()
	tree.MarkDirty(99999, 88888)
	rootAfterInvalid := mgr.ComputeGlobalRoot()

	t.Logf("[STEP4] root before invalid MarkDirty=%x after=%x", rootBeforeInvalid[:8], rootAfterInvalid[:8])
	if rootBeforeInvalid != rootAfterInvalid {
		t.Errorf("[STEP4] root changed after invalid MarkDirty: before=%x after=%x", rootBeforeInvalid[:8], rootAfterInvalid[:8])
	}

	// ------------------------------------------------------------
	// 6. Создание incremental snapshot
	// ------------------------------------------------------------
	snapVersion, err := mgr.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	t.Logf("[SNAP] incremental snapshot version=%x", snapVersion[:8])

	header, err := mgr.snapshotMgr.storage.LoadHeader(&snapVersion)
	if err != nil {
		t.Fatalf("LoadHeader failed: %v", err)
	}
	t.Logf(
		"[SNAP] target header kind=%v version=%x cpRef=%x parent=%x ts=%d schema=%d",
		header.Kind,
		header.Version[:8],
		header.CheckpointRef[:8],
		header.ParentVersion[:8],
		header.Timestamp,
		header.SchemaVersion,
	)

	if header.Kind != KindIncremental {
		t.Fatalf("[SNAP] expected KindIncremental, got %v", header.Kind)
	}

	dumpVersions(t, mgr, "AFTER_INCREMENTAL")

	chain, err := mgr.snapshotMgr.storage.BuildChain(header.CheckpointRef, snapVersion)
	if err != nil {
		t.Fatalf("BuildChain failed: %v", err)
	}
	t.Logf("[CHAIN] len=%d", len(chain))
	for i, e := range chain {
		t.Logf("[CHAIN]   chain[%d] version=%x ts=%d", i, e.Version[:8], e.Timestamp)
	}

	dumpIncrementalPayload(t, mgr, snapVersion, "RAW_INCREMENTAL_PAYLOAD")

	finalRoot := mgr.ComputeGlobalRoot()
	t.Logf("[SNAP] final source global root=%x", finalRoot[:8])

	// ВАЖНО: закрываем исходный manager до открытия нового на том же dir.
	if err := mgr.CloseSnapshots(); err != nil {
		t.Fatalf("CloseSnapshots source failed: %v", err)
	}

	// ------------------------------------------------------------
	// 7. Восстановление ТОЛЬКО checkpoint
	// ------------------------------------------------------------
	mgrCP := setupIncrementalManager(t, dir)

	cpRef := header.CheckpointRef
	if err := mgrCP.snapshotMgr.loadCheckpoint(mgrCP, &cpRef, makeFactory(mgrCP)); err != nil {
		_ = mgrCP.CloseSnapshots()
		t.Fatalf("[CP_RESTORE] loadCheckpoint failed: %v", err)
	}

	cpTree, ok := GetTree[*Account](mgrCP, "accounts")
	if !ok {
		_ = mgrCP.CloseSnapshots()
		t.Fatal("[CP_RESTORE] accounts tree not found")
	}

	t.Logf("[CP_RESTORE] restored size=%d", cpTree.Size())
	dumpTreeAccounts(t, "CP_RESTORE", cpTree, ids)
	checkStatuses(t, "CP_RESTORE_EXPECT_OLD", cpTree, map[uint64]AccountStatus{
		0:  StatusUser,
		10: StatusUser,
		20: StatusUser,
		30: StatusUser,
		50: StatusUser,
		99: StatusUser,
	})

	if err := mgrCP.CloseSnapshots(); err != nil {
		t.Fatalf("[CP_RESTORE] CloseSnapshots failed: %v", err)
	}

	// ------------------------------------------------------------
	// 8. Восстановление checkpoint + manual applyIncremental
	// ------------------------------------------------------------
	mgrManual := setupIncrementalManager(t, dir)

	cpRef2 := header.CheckpointRef
	if err := mgrManual.snapshotMgr.loadCheckpoint(mgrManual, &cpRef2, makeFactory(mgrManual)); err != nil {
		_ = mgrManual.CloseSnapshots()
		t.Fatalf("[MANUAL] loadCheckpoint failed: %v", err)
	}

	for i, entry := range chain {
		t.Logf("[MANUAL] applying chain[%d] version=%x", i, entry.Version[:8])
		if err := mgrManual.snapshotMgr.applyIncremental(mgrManual, entry); err != nil {
			_ = mgrManual.CloseSnapshots()
			t.Fatalf("[MANUAL] applyIncremental failed for %x: %v", entry.Version[:8], err)
		}

		manualTree, ok := GetTree[*Account](mgrManual, "accounts")
		if !ok {
			_ = mgrManual.CloseSnapshots()
			t.Fatal("[MANUAL] accounts tree missing after applyIncremental")
		}
		dumpTreeAccounts(t, "MANUAL_AFTER_APPLY", manualTree, ids)
		checkStatuses(t, "MANUAL_AFTER_APPLY", manualTree, mutations)
	}

	_, ok2 := GetTree[*Account](mgrManual, "accounts")
	if !ok2 {
		_ = mgrManual.CloseSnapshots()
		t.Fatal("[MANUAL] accounts tree missing at final check")
	}

	manualRoot := mgrManual.ComputeGlobalRoot()
	t.Logf("[MANUAL] final root=%x", manualRoot[:8])

	if err := mgrManual.CloseSnapshots(); err != nil {
		t.Fatalf("[MANUAL] CloseSnapshots failed: %v", err)
	}

	// ------------------------------------------------------------
	// 9. Полный LoadFromSnapshot
	// ------------------------------------------------------------
	mgr2 := setupIncrementalManager(t, dir)
	defer mgr2.CloseSnapshots()

	if err := mgr2.LoadFromSnapshot(snapVersion, makeFactory(mgr2)); err != nil {
		t.Fatalf("LoadFromSnapshot failed: %v", err)
	}

	restoredTree, ok := GetTree[*Account](mgr2, "accounts")
	if !ok {
		t.Fatal("[FULL_RESTORE] accounts tree not found")
	}

	t.Logf("[FULL_RESTORE] restored size=%d", restoredTree.Size())
	dumpTreeAccounts(t, "FULL_RESTORE", restoredTree, ids)
	checkStatuses(t, "FULL_RESTORE", restoredTree, mutations)

	rootAfterRestore := mgr2.ComputeGlobalRoot()
	t.Logf("[FULL_RESTORE] restored global root=%x expected=%x", rootAfterRestore[:8], finalRoot[:8])

	assertRootEqual(t, "markdirty restore", finalRoot, rootAfterRestore)

	// ------------------------------------------------------------
	// 10. Доп. сравнение manual vs full restore
	// ------------------------------------------------------------
	if manualRoot != rootAfterRestore {
		t.Errorf(
			"[COMPARE] manual root != full restore root: manual=%x full=%x",
			manualRoot[:8],
			rootAfterRestore[:8],
		)
	}
}