package lock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockWFG is a no-op wait-for graph for testing
type mockWFG struct{}

func (m *mockWFG) AddEdge(_, _ uint64)  {}
func (m *mockWFG) RemoveEdges(_ uint64) {}

func makeAcquirer() *Acquirer {
	return NewAcquirer(NewTable(), &mockWFG{})
}

func makeLocksHeld() *HeldSet {
	return NewHeldSet()
}

func TestAcquire_ConcurrentReadsAllGranted(t *testing.T) {
	acq := makeAcquirer()
	res := ResourceForRow("accounts", "1")
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			lh := makeLocksHeld()
			abortCh := make(chan struct{})
			err := acq.AcquireLock(id, res, LockS, lh, 5*time.Second, abortCh)
			assert.NoError(t, err)
		}(uint64(i + 1))
	}
	wg.Wait()
}

func TestAcquire_WriteBlocksConcurrentReads(t *testing.T) {
	acq := makeAcquirer()
	res := ResourceForRow("accounts", "1")

	lh1 := makeLocksHeld()
	abortCh1 := make(chan struct{})
	err := acq.AcquireLock(1, res, LockX, lh1, 5*time.Second, abortCh1)
	require.NoError(t, err)

	// Txn 2 tries to read — should block then get it after txn 1 releases
	done := make(chan error)
	go func() {
		lh2 := makeLocksHeld()
		abortCh2 := make(chan struct{})
		done <- acq.AcquireLock(2, res, LockS, lh2, 5*time.Second, abortCh2)
	}()

	// Let the goroutine start waiting
	time.Sleep(50 * time.Millisecond)

	// Release txn 1's lock
	acq.ReleaseAllLocks(1, lh1)

	// Txn 2 should now be granted
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for lock")
	}
}

func TestAcquire_Timeout(t *testing.T) {
	acq := makeAcquirer()
	res := ResourceForRow("accounts", "1")

	lh1 := makeLocksHeld()
	abortCh1 := make(chan struct{})
	err := acq.AcquireLock(1, res, LockX, lh1, 5*time.Second, abortCh1)
	require.NoError(t, err)

	// Txn 2 tries to read with short timeout
	lh2 := makeLocksHeld()
	abortCh2 := make(chan struct{})
	err = acq.AcquireLock(2, res, LockS, lh2, 100*time.Millisecond, abortCh2)
	assert.Error(t, err)
	var txnErr *types.TxnError
	require.ErrorAs(t, err, &txnErr)
	assert.Equal(t, types.ErrLockTimeout, txnErr.Code)
}

// TestC03_AcquireLockCtx_GrantVsAbort_NoLeak reproduces C-03: when a grant
// (GrantedCh closed by another goroutine's GrantWaiters) races against
// abortCh/ctx.Done/timer.C in the select, the prior code only called
// RemoveWaiting — which returns false because the request already moved to
// `granted`. The granted entry was left dangling, `locksHeld[resource]` was
// never set, and ReleaseAllLocks never released it. The lock leaked forever,
// blocking all future acquirers of the resource.
//
// We force the race by closing T2's abortCh concurrently with T1 releasing
// the lock (which closes T2's GrantedCh). Whichever case the select picks,
// the resource MUST be free for T3 afterward. Run many iterations because the
// buggy branch is only hit when the select picks the non-grant case.
func TestC03_AcquireLockCtx_GrantVsAbort_NoLeak(t *testing.T) {
	acq := makeAcquirer()
	res := ResourceForRow("accounts", "c03")

	for iter := 0; iter < 60; iter++ {
		lh1 := makeLocksHeld()
		abortCh1 := make(chan struct{})
		require.NoError(t, acq.AcquireLock(1, res, LockX, lh1, 5*time.Second, abortCh1))

		lh2 := makeLocksHeld()
		abortCh2 := make(chan struct{})
		t2Done := make(chan error, 1)

		go func() {
			t2Done <- acq.AcquireLockCtx(context.Background(), 2, res, LockX, lh2, 5*time.Second, abortCh2)
		}()

		// Wait until T2 is parked in the waiting queue.
		require.Eventually(t, func() bool {
			q, ok := acq.table.Get(res)
			if !ok {
				return false
			}
			q.mu.Lock()
			defer q.mu.Unlock()
			return len(q.waiting) == 1 && q.waiting[0].TxnID == 2
		}, time.Second, time.Millisecond)

		// Race: close abortCh and release T1 (which calls GrantWaiters, closing
		// T2's GrantedCh) near-simultaneously.
		close(abortCh2)
		acq.ReleaseAllLocks(1, lh1)

		// T2 returns — either granted (nil) or aborted. Either is acceptable.
		var t2err error
		select {
		case t2err = <-t2Done:
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: T2 never returned", iter)
		}

		if t2err == nil {
			// T2 won the grant and believes it holds the lock — release it.
			acq.ReleaseAllLocks(2, lh2)
		}
		// If t2err != nil (aborted), T2 must NOT hold the lock; the fix removes
		// the granted entry too. Verify T3 can acquire promptly (no leak).
		lh3 := makeLocksHeld()
		abortCh3 := make(chan struct{})
		err := acq.AcquireLock(3, res, LockX, lh3, 500*time.Millisecond, abortCh3)
		if err != nil {
			t.Fatalf("iter %d: T3 could not acquire after T2 abort — lock leaked (C-03): %v", iter, err)
		}
		acq.ReleaseAllLocks(3, lh3)
	}
}

func TestAcquire_AbortSignal(t *testing.T) {
	acq := makeAcquirer()
	res := ResourceForRow("accounts", "1")

	lh1 := makeLocksHeld()
	abortCh1 := make(chan struct{})
	err := acq.AcquireLock(1, res, LockX, lh1, 5*time.Second, abortCh1)
	require.NoError(t, err)

	abortCh2 := make(chan struct{})
	done := make(chan error)
	go func() {
		lh2 := makeLocksHeld()
		done <- acq.AcquireLock(2, res, LockS, lh2, 5*time.Second, abortCh2)
	}()

	time.Sleep(50 * time.Millisecond)
	close(abortCh2)

	select {
	case err := <-done:
		var txnErr *types.TxnError
		require.ErrorAs(t, err, &txnErr)
		assert.Equal(t, types.ErrTxnAborted, txnErr.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

// TestCT27_UpgradeS_To_X_NoSelfDeadlock covers CT-27: a txn that
// already holds S on a resource and then requests X must not
// self-deadlock or inflate q.granted. Previously:
//   - No conflict: the upgrade path appended a SECOND granted entry
//     (one S, one X), inflating q.granted and breaking the invariant
//     "each txn appears at most once per resource".
//   - Conflict (another txn holds X): the upgrade went to waiting;
//     when the competing txn released, GrantWaiters recomputed
//     currentMode from ALL of q.granted (including our own leftover
//     S) and refused to grant X (S and X are incompatible). The
//     upgrade spun until lockTimeout.
func TestCT27_UpgradeS_To_X_NoSelfDeadlock(t *testing.T) {
	acq := makeAcquirer()
	res := ResourceForRow("accounts", "k_ct27")
	lh := makeLocksHeld()

	// No-conflict upgrade: T1 holds S, then requests X. No
	// competitor. Previously this appended both S and X entries
	// to q.granted; the fix removes the S entry on upgrade.
	require.NoError(t, acq.AcquireLock(1, res, LockS, lh, 5*time.Second, nil))
	require.NoError(t, acq.AcquireLock(1, res, LockX, lh, 5*time.Second, nil))

	q, _ := acq.table.Get(res)
	q.mu.Lock()
	count := 0
	for _, g := range q.granted {
		if g.TxnID == 1 {
			count++
		}
	}
	q.mu.Unlock()
	assert.Equal(t, 1, count,
		"CT-27: upgrade must replace the S entry with X, not append (no duplicate S+X for same txn)")

	// Verify the held mode is now X (not stale S).
	got, ok := lh.Get(res)
	require.True(t, ok)
	assert.Equal(t, LockX, got, "CT-27: held mode must reflect the upgraded X")
}

// TestCT27_UpgradeConcurrentXHolder_NoSelfDeadlock covers the
// conflict path: T1 holds S, T2 holds X, then T1 upgrades to X
// after T2 releases. Previously the upgrade was blocked forever
// (GrantWaiters refused to grant X while T1's own S was in
// q.granted). The fix removes T1's S on upgrade so T2's release
// unblocks T1.
func TestReleaseLock(t *testing.T) {
	acq := makeAcquirer()
	res := ResourceForRow("accounts", "release_test")

	lh := makeLocksHeld()
	abortCh := make(chan struct{})
	require.NoError(t, acq.AcquireLock(1, res, LockX, lh, 5*time.Second, abortCh))

	_, ok := lh.Get(res)
	require.True(t, ok)

	acq.ReleaseLock(1, res, lh)

	_, ok = lh.Get(res)
	assert.False(t, ok)

	// Idempotent: releasing again should not panic
	acq.ReleaseLock(1, res, lh)
}

func TestCT27_UpgradeConcurrentXHolder_NoSelfDeadlock(t *testing.T) {
	acq := makeAcquirer()
	res := ResourceForRow("accounts", "k_ct27b")
	lh2 := makeLocksHeld()
	lh1 := makeLocksHeld()

	// T2 takes X first (T1 then acquires S after — but S and X
	// conflict, so T1's S will block until T2 releases).
	require.NoError(t, acq.AcquireLock(2, res, LockX, lh2, 5*time.Second, nil))

	// Start T1's S acquisition in a goroutine; it'll block.
	gotS := make(chan error, 1)
	go func() {
		gotS <- acq.AcquireLock(1, res, LockS, lh1, 5*time.Second, nil)
	}()

	// Wait for T1 to enter waiting.
	time.Sleep(50 * time.Millisecond)

	// T2 releases X.
	acq.ReleaseAllLocks(2, lh2)

	// T1 should now acquire S.
	select {
	case err := <-gotS:
		require.NoError(t, err, "T1 must acquire S after T2 releases X")
	case <-time.After(2 * time.Second):
		t.Fatal("T1 stuck waiting for S")
	}

	// T1 upgrades S → X. Previously this self-deadlocked (the
	// leftover S in q.granted was incompatible with X). The fix
	// removes T1's S on upgrade so the upgrade goes through.
	require.NoError(t, acq.AcquireLock(1, res, LockX, lh1, 5*time.Second, nil))

	// Verify q.granted has exactly one entry for txn 1 (the X),
	// and held mode is X.
	q, _ := acq.table.Get(res)
	q.mu.Lock()
	count := 0
	for _, g := range q.granted {
		if g.TxnID == 1 {
			count++
		}
	}
	q.mu.Unlock()
	assert.Equal(t, 1, count, "CT-27: upgraded X must be the only entry for txn 1")
	got, ok := lh1.Get(res)
	require.True(t, ok)
	assert.Equal(t, LockX, got, "CT-27: held mode must be X after upgrade")
}
