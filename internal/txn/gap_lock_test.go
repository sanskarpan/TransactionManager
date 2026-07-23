package txn

import (
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupGapLockManager creates a TxnManager with zero-padded keys "01", "05", "10"
// so that lexicographic ordering matches numeric ordering.
func setupGapLockManager() *Manager {
	catalog := storage.NewCatalog()
	catalog.Register(storage.NewTable("items", []storage.Column{
		{Name: "val", Type: types.TypeInt},
	}))
	mgr := NewManager(catalog)
	tbl, _ := catalog.Lookup("items")
	tbl.PutRow("01", []types.Value{types.IntVal(1)})
	tbl.PutRow("05", []types.Value{types.IntVal(5)})
	tbl.PutRow("10", []types.Value{types.IntVal(10)})
	return mgr
}

// TestGapLock_InsertBlockedByRangeScan verifies that a range scan (gap lock S)
// blocks a concurrent insert into that range.
func TestGapLock_InsertBlockedByRangeScan(t *testing.T) {
	mgr := setupGapLockManager()

	// T1 scans [01, 09] — acquires gap locks on that range
	t1, err := mgr.Begin(Protocol2PL, Serializable, 2*time.Second)
	require.NoError(t, err)
	require.NoError(t, mgr.LockForRangeScan(t1, "items", "01", "09"))

	// T2 tries to insert key "03" (inside range) — should block
	t2, err := mgr.Begin(Protocol2PL, Serializable, 200*time.Millisecond)
	require.NoError(t, err)

	var insertErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		insertErr = mgr.LockForInsert(t2, "items", "03") // gap (01,05] is S-locked by T1
	}()

	// Give T2 time to block
	time.Sleep(50 * time.Millisecond)

	// Commit T1 (releases gap locks), T2 should proceed
	require.NoError(t, mgr.Commit(t1.ID))

	wg.Wait()
	// L-01: previously the test passed whether the insert succeeded or
	// timed out, which gave false confidence. With a 200 ms timeout and
	// T1 releasing after 50 ms, the insert MUST succeed.
	assert.NoError(t, insertErr, "insert should succeed after T1 releases gap locks")

	_ = mgr.Commit(t2.ID)
}

// TestGapLock_InsertOutsideRange succeeds immediately.
func TestGapLock_InsertOutsideRange(t *testing.T) {
	mgr := setupGapLockManager()

	// T1 scans [01, 09] — locks gaps within that range
	t1, err := mgr.Begin(Protocol2PL, Serializable, 2*time.Second)
	require.NoError(t, err)
	require.NoError(t, mgr.LockForRangeScan(t1, "items", "01", "09"))

	// T2 inserts key "11" (outside the range [01,09], gap (10, ∞)) — should succeed immediately
	t2, err := mgr.Begin(Protocol2PL, Serializable, 500*time.Millisecond)
	require.NoError(t, err)

	insertErr := mgr.LockForInsert(t2, "items", "11")
	assert.NoError(t, insertErr, "insert outside range should not be blocked")

	_ = mgr.Commit(t1.ID)
	_ = mgr.Commit(t2.ID)
}

// TestTwoPLScan_Serializable uses gap locks for full table scan.
func TestTwoPLScan_Serializable(t *testing.T) {
	mgr := setupGapLockManager()

	t1, err := mgr.Begin(Protocol2PL, Serializable, 2*time.Second)
	require.NoError(t, err)

	rows, err := mgr.TwoPLScan(t1, "items", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 3)

	require.NoError(t, mgr.Commit(t1.ID))
}

// TestTwoPLScan_RepeatableRead acquires S on table.
func TestTwoPLScan_RepeatableRead(t *testing.T) {
	mgr := setupGapLockManager()

	t1, err := mgr.Begin(Protocol2PL, RepeatableRead, 2*time.Second)
	require.NoError(t, err)

	rows, err := mgr.TwoPLScan(t1, "items", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 3)

	require.NoError(t, mgr.Commit(t1.ID))
}

// TestH09_GapLock_TrailingBoundaryInsertBlocked verifies the H-09 fix: a
// range scan [lo, hi] where `hi` is NOT an existing row key (the typical
// predicate case) must block an insert of a key that lands in the trailing
// gap (between the last existing key <= hi and the next existing key > hi).
// Previously the range scan locked the gap (lastInRange, hi] using the
// caller-supplied hi, while LockForInsert computed (prev, nextExistingKey] —
// mismatched ResourceIDs, so the insert slipped through as a phantom.
//
// Setup: rows 01, 05, 10. Scan [01, 09]. Trailing gap must be (05, 10], so an
// insert of "09" must block until the scanner releases.
func TestH09_GapLock_TrailingBoundaryInsertBlocked(t *testing.T) {
	mgr := setupGapLockManager()

	t1, err := mgr.Begin(Protocol2PL, Serializable, 2*time.Second)
	require.NoError(t, err)
	require.NoError(t, mgr.LockForRangeScan(t1, "items", "01", "09"))

	t2, err := mgr.Begin(Protocol2PL, Serializable, 300*time.Millisecond)
	require.NoError(t, err)

	blocked := make(chan error, 1)
	go func() {
		blocked <- mgr.LockForInsert(t2, "items", "09") // in trailing gap (05, 10]
	}()

	// T2 must still be blocked after giving it time to attempt the insert.
	select {
	case <-blocked:
		t.Fatal("insert into trailing gap must block while T1 holds the range scan gap locks (H-09)")
	case <-time.After(80 * time.Millisecond):
		// good — still blocked.
	}

	// Release T1; T2 must now proceed.
	require.NoError(t, mgr.Commit(t1.ID))
	select {
	case err := <-blocked:
		assert.NoError(t, err, "insert should succeed once T1 releases the gap lock")
	case <-time.After(1 * time.Second):
		t.Fatal("insert did not proceed after T1 released gap locks (H-09)")
	}
	_ = mgr.Commit(t2.ID)
}

// TestGapLock_AfterCommit_InsertSucceeds verifies that after the range scanner
// commits (releasing gap locks), a blocked insert can proceed.
func TestGapLock_AfterCommit_InsertSucceeds(t *testing.T) {
	mgr := setupGapLockManager()

	t1, err := mgr.Begin(Protocol2PL, Serializable, 2*time.Second)
	require.NoError(t, err)
	require.NoError(t, mgr.LockForRangeScan(t1, "items", "01", "09"))

	t2, err := mgr.Begin(Protocol2PL, Serializable, 3*time.Second)
	require.NoError(t, err)

	var insertErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		insertErr = mgr.LockForInsert(t2, "items", "03")
	}()

	time.Sleep(60 * time.Millisecond) // let T2 block
	require.NoError(t, mgr.Commit(t1.ID))

	wg.Wait()
	assert.NoError(t, insertErr, "insert should succeed after T1 releases gap locks")
	_ = mgr.Commit(t2.ID)
}
