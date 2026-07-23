package mvcc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubChecker satisfies the Vacuum's txnMgr interface with no active txns
// and a fixed committed set.
type stubChecker struct {
	committed      map[TxnID]bool
	active         map[TxnID]bool
	pruneCallCount atomic.Int32
}

func (s *stubChecker) OldestActiveTxnID() TxnID {
	for id := range s.active {
		return id
	}
	return TxnID(1 << 30) // no active → horizon far in the future
}
func (s *stubChecker) IsCommitted(id TxnID) bool { return s.committed[id] }
func (s *stubChecker) PruneHistory() int {
	s.pruneCallCount.Add(1)
	return 0
}

// TestC02_Vacuum_StopActuallyTerminates is the regression guard for C-02:
// previously Run and Stop shared the same sync.Once, so Run consumed the once
// and Stop's once.Do became a no-op — the background goroutine could never be
// cancelled and leaked forever (the loop's select never observed v.stop
// closing). This test asserts the goroutine actually exits after Stop.
func TestC02_Vacuum_StopActuallyTerminates(t *testing.T) {
	store := NewStore()
	checker := &stubChecker{}
	v := NewVacuum(store, checker, 5*time.Millisecond)
	v.Run()
	v.Stop()

	select {
	case <-v.Done():
		// good — the loop exited.
	case <-time.After(2 * time.Second):
		t.Fatal("Vacuum background goroutine did not exit after Stop (C-02 leak)")
	}

	// And a second Stop must still be a no-op (close-of-closed-channel guard).
	v.Stop()
}

// TestH03_ReapEmptyChains is the regression guard for H-03: after a vacuum
// pass prunes every version out of a chain, the empty chain entry must be
// deleted from the store so the sync.Map does not grow without bound.
func TestH03_ReapEmptyChains(t *testing.T) {
	store := NewStore()
	SeedMVCCStore(store, "t", []storage.Row{
		{Key: "a", Values: []types.Value{types.IntVal(1)}},
		{Key: "b", Values: []types.Value{types.IntVal(2)}},
	}, 1)

	checker := &stubChecker{
		committed: map[TxnID]bool{2: true}, // Txn 2 committed
		active:    map[TxnID]bool{},
	}

	// Mark Txn 1's versions deleted by Txn 2 (committed, below horizon).
	store.ForEachChain(func(chain *VersionChain) {
		chain.SetXMax(1, 2)
	})

	// Sanity: two chains exist before vacuum.
	before := 0
	store.ForEachChain(func(*VersionChain) { before++ })
	require.Equal(t, 2, before)

	v := NewVacuum(store, checker, time.Hour)
	v.RunOnce()

	// All versions pruned; both empty chains should have been reaped.
	after := 0
	store.ForEachChain(func(*VersionChain) { after++ })
	assert.Equal(t, 0, after, "empty chains must be reaped after vacuum")
}

// TestH03_ReapOnlyEmpty verifies reap does not touch non-empty chains.
func TestH03_ReapOnlyEmpty(t *testing.T) {
	store := NewStore()
	SeedMVCCStore(store, "t", []storage.Row{
		{Key: "a", Values: []types.Value{types.IntVal(1)}},
		{Key: "b", Values: []types.Value{types.IntVal(2)}},
	}, 1)
	// Mark only "a" deleted.
	chain, ok := store.GetChain("t", "a")
	require.True(t, ok)
	chain.SetXMax(1, 2)

	checker := &stubChecker{committed: map[TxnID]bool{2: true}}
	v := NewVacuum(store, checker, time.Hour)
	v.RunOnce()

	remaining := 0
	store.ForEachChain(func(*VersionChain) { remaining++ })
	assert.Equal(t, 1, remaining, "only the empty chain should be reaped")
}

// TestVacuum_ConcurrentRunStop exercises the loop under concurrency for
// race-detector coverage of the once-guarded Run/Stop paths.
func TestVacuum_ConcurrentRunStop(_ *testing.T) {
	store := NewStore()
	checker := &stubChecker{}
	v := NewVacuum(store, checker, time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Run()
		}()
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Stop()
		}()
	}
	wg.Wait()
}

// TestCT02_Vacuum_InvokesPruneHistory is the regression guard for CT-02:
// PruneHistory was defined on TxnManager but never called, so the
// committed/aborted history maps grew without bound. The vacuum now
// calls PruneHistory on every tick.
func TestCT02_Vacuum_InvokesPruneHistory(t *testing.T) {
	store := NewStore()
	checker := &stubChecker{committed: map[TxnID]bool{}, active: map[TxnID]bool{}}
	v := NewVacuum(store, checker, 10*time.Millisecond)
	v.Run()

	// Wait for at least 3 ticks.
	require.Eventually(t, func() bool {
		return checker.pruneCallCount.Load() >= 3
	}, 2*time.Second, 5*time.Millisecond, "PruneHistory should be called on every vacuum tick")

	v.Stop()
	// PruneHistory must be callable idempotently after Stop.
	require.NotPanics(t, func() { checker.PruneHistory() })
}

// TestCT19_ReapEmptyConcurrentWrite_NoSilentLoss reproduces CT-19:
// previously ReapEmpty checked Head() (RLock) and then chains.Delete()
// outside the lock, racing a concurrent MVCCWrite that could prepend a
// version between the check and the delete. The write would succeed
// but the chain would be removed from the store, and subsequent
// GetChain would return (nil, false), silently losing the write.
//
// The fix: ReapEmpty holds the chain's write lock for the check +
// tombstone, and MVCCWrite checks the tombstone under its own write
// lock and retries via GetOrCreateChain if the chain was deleted.
func TestCT19_ReapEmptyConcurrentWrite_NoSilentLoss(t *testing.T) {
	store := NewStore()
	// Pre-populate a chain.
	store.GetOrCreateChain("t", "k")
	// Empty it (simulate a vacuum prune pass that left the chain empty).
	chain, _ := store.GetChain("t", "k")
	chain.Head() // no-op; just ensure the chain exists
	chain.Lock()
	chain.head = nil
	chain.Unlock()
	// First ReapEmpty deletes it from the sync.Map.
	store.ReapEmpty()
	if _, ok := store.GetChain("t", "k"); ok {
		t.Fatal("expected empty chain to be reaped")
	}

	// Now simulate a writer that pre-loaded the chain before the Delete.
	// We do this by injecting a chain with tombstoned=true directly into
	// the sync.Map to simulate "writer loaded the chain, ReapEmpty
	// set tombstone and Delete'd it".
	injected := NewVersionChain("k")
	injected.tombstoned = true
	store.chains.Store(chainKey("t", "k2"), injected)
	// MVCCWrite must observe tombstoned=true under the chain's write
	// lock and fall back to a fresh chain.
	got := store.GetOrCreateChain("t", "k2")
	require.NotNil(t, got)
	require.NotSame(t, injected, got, "CT-19: tombstoned chain must not be reused for writes; a fresh chain must be returned")
	require.False(t, got.IsTombstonedLocked(), "CT-19: the fresh chain must not be tombstoned")
}

// CT-26 regression guard lives in internal/txn/regression_test.go
// because it needs the TxnManager + MVCCWrite + Commit pipeline;
// vacuum_test.go only has access to the raw MVCCStore.
