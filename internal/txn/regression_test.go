package txn

import (
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestC01_ReadWriteSet_NoDataRace exercises the regression guard for C-01:
// concurrent read/write operations on the same transaction must not race on
// the ReadSet/WriteSet maps. With -race this fails the previous code that
// mutated WriteSet without holding txn.mu.
func TestC01_ReadWriteSet_NoDataRace(t *testing.T) {
	catalog := storage.NewCatalog()
	catalog.Register(storage.NewTable("t", []storage.Column{{Name: "v", Type: types.TypeInt}}))
	mgr := NewManager(catalog)
	mvcc.SeedMVCCStore(mgr.MVCCStore, "t", []storage.Row{
		{Key: "k", Values: []types.Value{types.IntVal(0)}},
	}, 0)
	mgr.MarkCommitted(0)

	txn, err := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
	require.NoError(t, err)

	// Concurrent reads + writes on the same txn: previously a data race.
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _, _ = mgr.MVCCRead(txn, "t", "k")
		}()
		go func() {
			defer wg.Done()
			_ = mgr.MVCCWrite(txn, "t", "k", []types.Value{types.IntVal(1)}, UndoUpdate)
		}()
	}
	wg.Wait()

	_ = mgr.Abort(txn.ID, nil)
}

// TestC02_UndoUpdate_PreservesIntermediateVersions is the regression guard
// for C-02: when an aborted transaction's version sits at the chain head and
// a later transaction updates the visible version, the aborted version must
// remain reachable from the chain (so RemoveByXMin can later clean it up),
// not orphaned between head and visible.
func TestC02_UndoUpdate_PreservesIntermediateVersions(t *testing.T) {
	catalog := storage.NewCatalog()
	catalog.Register(storage.NewTable("t", []storage.Column{{Name: "v", Type: types.TypeInt}}))
	mgr := NewManager(catalog)
	mvcc.SeedMVCCStore(mgr.MVCCStore, "t", []storage.Row{
		{Key: "k", Values: []types.Value{types.IntVal(0)}},
	}, 0)
	mgr.MarkCommitted(0)

	// T1 writes a new version but aborts, leaving its version at the head.
	t1, _ := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
	require.NoError(t, mgr.MVCCWrite(t1, "t", "k", []types.Value{types.IntVal(99)}, UndoUpdate))
	_ = mgr.Abort(t1.ID, nil)

	// At this point the chain head is T1's aborted version. ApplyMVCCUndo
	// has removed T1's XMin==t1.ID version per RemoveByXMin, but if the
	// manager's undo path leaves any version at the head (e.g. an aborted
	// txn that wrote then aborted without undo — defensive), the next
	// update must not orphan it.

	// Simulate a stray aborted version sitting at the head by re-seeding one.
	chain := mgr.MVCCStore.GetOrCreateChain("t", "k")
	chain.Lock()
	chain.PrependUnsafe(&mvcc.Version{XMin: 999, XMax: 0, Data: []types.Value{types.IntVal(777)}})
	chain.Unlock()

	// T2 reads the visible version (still 0) and updates it.
	t2, _ := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
	vals, found, err := mgr.MVCCRead(t2, "t", "k")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(0), vals[0].Int)

	require.NoError(t, mgr.MVCCWrite(t2, "t", "k", []types.Value{types.IntVal(5)}, UndoUpdate))
	require.NoError(t, mgr.Commit(t2.ID))

	// The orphaned version (XMin==999) must still be reachable for cleanup.
	chain.Lock()
	head := chain.UnsafeHead()
	chain.Unlock()
	require.NotNil(t, head)
	// Walk the chain and confirm XMin==999 is still present.
	var found999 bool
	for v := head; v != nil; v = v.Prev {
		if v.XMin == 999 {
			found999 = true
			break
		}
	}
	assert.True(t, found999, "intermediate version must not be orphaned by UndoUpdate")
}

// TestC05_Reset_UnderConcurrentOps is the regression guard for C-05: calling
// Reset while another goroutine is mid-flight on a read must not panic or
// corrupt state. Previously MVCCStore.Clear() ran concurrently with
// MVCCRead/MVCCWrite, which could dereference a deleted chain.
//
// H-10: strengthened to exercise RepeatableRead and Serializable txns
// (whose getSnapshot does not take m.mu when the snapshot is cached), and
// to assert that a fresh txn after Reset completes successfully without
// panic.
func TestC05_Reset_UnderConcurrentOps(t *testing.T) {
	catalog := storage.NewCatalog()
	catalog.Register(storage.NewTable("t", []storage.Column{{Name: "v", Type: types.TypeInt}}))
	mgr := NewManager(catalog)
	mvcc.SeedMVCCStore(mgr.MVCCStore, "t", []storage.Row{
		{Key: "k", Values: []types.Value{types.IntVal(0)}},
	}, 0)
	mgr.MarkCommitted(0)

	isolations := []IsolationLevel{ReadCommitted, RepeatableRead, Serializable}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			iso := isolations[idx%len(isolations)]
			for {
				select {
				case <-stop:
					return
				default:
				}
				tx, err := mgr.Begin(ProtocolMVCC, iso, 200*time.Millisecond)
				if err != nil {
					continue
				}
				_, _, _ = mgr.MVCCRead(tx, "t", "k")
				_ = mgr.MVCCWrite(tx, "t", "k", []types.Value{types.IntVal(1)}, UndoUpdate)
				_ = mgr.Commit(tx.ID)
			}
		}(i)
	}

	// Hammer Reset concurrently with the readers/writers.
	for i := 0; i < 20; i++ {
		mgr.Reset()
	}
	close(stop)
	wg.Wait()

	// Post-reset liveness: a fresh txn can begin, read, and commit without
	// panic. We do not assert specific row visibility because the test's
	// catalog table holds no rows (it was only seeded into the MVCC store);
	// what we are guarding against is the prior concurrent-clear /
	// concurrent-read data race.
	tx, err := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
	require.NoError(t, err)
	_, _, err = mgr.MVCCRead(tx, "t", "k")
	require.NoError(t, err)
	require.NoError(t, mgr.Commit(tx.ID))
}

// TestC08_Reset_UnderConcurrent2PL_NoMapRace is the regression guard for C-08:
// previously Transaction.LocksHeld was a raw map[ResourceID]Mode written
// by AcquireLockCtx (with no synchronization on the txn's mutex) while
// Manager.Reset iterated the same map via ReleaseAllLocks from another
// goroutine — a concurrent map iteration + write flagged by the race
// detector and capable of panicking at runtime. The HeldSet wrapper
// serializes access. Run under -race to catch any regression.
func TestC08_Reset_UnderConcurrent2PL_NoMapRace(_ *testing.T) {
	catalog := storage.NewCatalog()
	catalog.Register(storage.NewTable("t", []storage.Column{{Name: "v", Type: types.TypeInt}}))
	mgr := NewManager(catalog)
	// Seed distinct keys so writers contend across resources.
	tbl, _ := catalog.Lookup("t")
	for i := 0; i < 8; i++ {
		tbl.PutRow(storage.RowKey(rune('a'+i)), []types.Value{types.IntVal(int64(i))})
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				tx, err := mgr.Begin(Protocol2PL, ReadCommitted, 200*time.Millisecond)
				if err != nil {
					continue // possible 200ms abort under heavy contention
				}
				key := string(rune('a' + (j % 8)))
				_ = mgr.TwoPLWrite(tx, "t", key, []types.Value{types.IntVal(int64(seed))}, UndoUpdate)
				_ = mgr.Commit(tx.ID)
			}
		}(i)
	}

	// Hammer Reset while 2PL writers are mid-AcquireLock.
	for i := 0; i < 30; i++ {
		mgr.Reset()
	}
	close(stop)
	wg.Wait()
}

// TestCT02_PruneHistory_BoundsMaps is the regression guard for CT-02:
// the committed/aborted maps must be bounded by MaxHistoryRetained so
// the process does not leak memory at sustained commit/abort rates.
func TestCT02_PruneHistory_BoundsMaps(t *testing.T) {
	mgr, _ := setupManager()
	// Mark many IDs as committed, far exceeding the cap.
	for i := ID(1); i <= ID(MaxHistoryRetained+100); i++ {
		mgr.MarkCommitted(i)
	}
	removed := mgr.PruneHistory()
	require.GreaterOrEqual(t, removed, 100, "PruneHistory should drop the excess entries")

	mgr.mu.RLock()
	committedSize := len(mgr.committed)
	abortedSize := len(mgr.aborted)
	mgr.mu.RUnlock()
	assert.LessOrEqual(t, committedSize, MaxHistoryRetained, "committed map must be bounded")
	assert.LessOrEqual(t, abortedSize, MaxHistoryRetained, "aborted map must be bounded")
}

// TestCT26_ReapEmptyMidWrite_NoSilentLoss reproduces CT-26: the
// CT-19 fix verified GetOrCreateChain in isolation, but MVCCWrite
// calls GetOrCreateChain and THEN takes chain.Lock. ReapEmpty can
// race in between — it tombstones + deletes the entry from the
// sync.Map — leaving MVCCWrite's chain pointer dangling. The fix
// in MVCCWrite re-checks IsTombstonedLocked under its own write
// lock and retries via EvictChain + GetOrCreateChain.
//
// This test exercises the manager path (MVCCRead/MVCCWrite/Commit)
// to catch any regression of CT-26.
func TestCT26_ReapEmptyMidWrite_NoSilentLoss(t *testing.T) {
	mgr, _ := setupManager()

	// Pre-populate a chain with one version, then commit (so the
	// chain has a live version a future read should see).
	txn, err := mgr.Begin(ProtocolMVCC, Serializable, 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, mgr.MVCCWrite(txn, "accounts", "k_ct26",
		[]types.Value{types.IntVal(1), types.TextVal("o"), types.FloatVal(100), types.TextVal("b")},
		UndoInsert))
	require.NoError(t, mgr.Commit(txn.ID))

	// Simulate a concurrent ReapEmpty that has set tombstone=true
	// and evicted the entry. The manager's MVCCWrite must observe
	// tombstone and retry on a fresh chain.
	require.True(t, mgr.MVCCStore.MarkChainTombstoned("accounts", "k_ct26"))

	// Read-then-write: the write must NOT silently append to a
	// tombstoned chain. Note: the prior MVCCRead sees (nil, false)
	// because the chain was tombstoned+evicted, so the read
	// returns found=false. That is expected — what we are
	// guarding is that the write itself succeeds on a fresh chain.
	txn2, err := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
	require.NoError(t, err)
	require.NoError(t,
		mgr.MVCCWrite(txn2, "accounts", "k_ct26",
			[]types.Value{types.IntVal(2), types.TextVal("o"), types.FloatVal(200), types.TextVal("b")},
			UndoInsert))
	require.NoError(t, mgr.Commit(txn2.ID))

	// Verify the write landed on a fresh chain that's still in
	// the store (CT-26 regression guard: a silent loss would
	// leave the new version missing).
	got, ok := mgr.MVCCStore.GetChain("accounts", "k_ct26")
	require.True(t, ok, "CT-26: chain entry must remain in the store after the write")
	require.GreaterOrEqual(t, len(got.All()), 1, "CT-26: new version must be visible after the write")
}
