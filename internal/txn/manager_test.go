package txn

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/lock"
	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupManager() (*Manager, *storage.Table) {
	catalog := storage.NewCatalog()
	table := storage.NewTable("accounts", []storage.Column{
		{Name: "balance", Type: types.TypeFloat},
	})
	catalog.Register(table)
	table.PutRow("1", []types.Value{types.FloatVal(1000.0)})
	table.PutRow("2", []types.Value{types.FloatVal(2000.0)})
	return NewManager(catalog), table
}

func TestManager_BeginCommit(t *testing.T) {
	mgr, table := setupManager()

	txn, err := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	require.NoError(t, err)
	assert.True(t, txn.IsActive())

	// Write
	err = mgr.TwoPLWrite(txn, "accounts", "1", []types.Value{types.FloatVal(900.0)}, UndoUpdate)
	require.NoError(t, err)

	// Commit
	err = mgr.Commit(txn.ID)
	require.NoError(t, err)
	assert.True(t, txn.IsCommitted())

	// Data should be visible
	v, ok := table.GetRow("1")
	require.True(t, ok)
	assert.Equal(t, 900.0, v[0].Float)
}

func TestManager_Abort_RestoresData(t *testing.T) {
	mgr, table := setupManager()

	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	_ = mgr.TwoPLWrite(txn, "accounts", "1", []types.Value{types.FloatVal(0.0)}, UndoUpdate)

	err := mgr.Abort(txn.ID, nil)
	require.NoError(t, err)
	assert.True(t, txn.IsAborted())

	// Data should be restored
	v, ok := table.GetRow("1")
	require.True(t, ok)
	assert.Equal(t, 1000.0, v[0].Float)
}

func TestManager_Savepoint(t *testing.T) {
	mgr, table := setupManager()

	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)

	// Write 1
	_ = mgr.TwoPLWrite(txn, "accounts", "1", []types.Value{types.FloatVal(500.0)}, UndoUpdate)

	// Savepoint
	_ = mgr.CreateSavepoint(txn.ID, "sp1")

	// Write 2
	_ = mgr.TwoPLWrite(txn, "accounts", "2", []types.Value{types.FloatVal(100.0)}, UndoUpdate)

	// Rollback to savepoint
	err := mgr.RollbackToSavepoint(txn.ID, "sp1")
	require.NoError(t, err)

	// Commit
	_ = mgr.Commit(txn.ID)

	// First write committed, second rolled back
	v1, _ := table.GetRow("1")
	assert.Equal(t, 500.0, v1[0].Float)

	v2, _ := table.GetRow("2")
	assert.Equal(t, 2000.0, v2[0].Float) // restored
}

// TestCT17_LockTimeout_FiresCallback reproduces CT-17: a 2PL lock
// acquisition that times out must fire the OnLockTimeout callback so the
// LockTimeouts metric increments. Previously the only signal was the
// error return; the OnAbort path doesn't fire (the txn is not aborted).
func TestCT17_LockTimeout_FiresCallback(t *testing.T) {
	mgr, _ := setupManager()
	var fired int
	mgr.OnLockTimeout = func(_ *Transaction, _ lock.ResourceID) {
		fired++
	}

	t1, err := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	require.NoError(t, err)
	// Take X on key 1.
	require.NoError(t, mgr.TwoPLWrite(t1, "accounts", "1",
		[]types.Value{types.IntVal(1), types.TextVal("a"), types.FloatVal(1), types.TextVal("n")},
		UndoUpdate))

	// T2 with a tiny lock timeout tries to read the same row.
	t2, err := mgr.Begin(Protocol2PL, ReadCommitted, 50*time.Millisecond)
	require.NoError(t, err)
	_, _, err = mgr.TwoPLRead(t2, "accounts", "1")
	require.Error(t, err, "expected lock-timeout error")
	var txnErr *types.TxnError
	require.ErrorAs(t, err, &txnErr)
	assert.Equal(t, types.ErrLockTimeout, txnErr.Code)

	// Callback must have fired for the row resource (and possibly the
	// table-resource acquisition before it). At least once.
	assert.GreaterOrEqual(t, fired, 1, "OnLockTimeout must fire for lock-timeout failures (CT-17)")

	_ = mgr.Abort(t1.ID, nil)
	_ = mgr.Abort(t2.ID, nil)
}

// TestCT17_LockTimeout_DoesNotFireOnAbort confirms the callback only
// fires for ErrLockTimeout, not for ErrTxnAborted (a separate signal).
func TestCT17_LockTimeout_DoesNotFireOnAbort(t *testing.T) {
	mgr, _ := setupManager()
	fired := 0
	mgr.OnLockTimeout = func(_ *Transaction, _ lock.ResourceID) {
		fired++
	}
	t1, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	require.NoError(t, mgr.TwoPLWrite(t1, "accounts", "1",
		[]types.Value{types.IntVal(1), types.TextVal("a"), types.FloatVal(1), types.TextVal("n")},
		UndoUpdate))
	// Abort t1, then t2 tries to acquire — would block then be aborted.
	require.NoError(t, mgr.Abort(t1.ID, types.NewDeadlockError()))
	assert.Equal(t, 0, fired, "OnLockTimeout must not fire for Abort errors")
}

func TestManager_ConcurrentCommitAbort(_ *testing.T) {
	mgr, _ := setupManager()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
			if i%2 == 0 {
				_ = mgr.Commit(txn.ID)
			} else {
				_ = mgr.Abort(txn.ID, nil)
			}
		}(i)
	}
	wg.Wait()
}

func TestManager_IdempotentAbort(t *testing.T) {
	mgr, _ := setupManager()
	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	_ = mgr.Abort(txn.ID, nil)
	err := mgr.Abort(txn.ID, nil) // second abort should not error
	assert.NoError(t, err)
}

// TestC05_Commit_AbortedTxn_ReturnsError reproduces C-05: previously Commit
// returned nil for any non-active transaction, including already-aborted ones.
// A client whose txn was aborted by the deadlock detector / SSI / lock timeout
// would then call Commit, receive nil, and believe the commit succeeded while
// none of its writes were durable.
func TestC05_Commit_AbortedTxn_ReturnsError(t *testing.T) {
	mgr, _ := setupManager()
	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	require.NoError(t, mgr.Abort(txn.ID, types.NewDeadlockError()))

	err := mgr.Commit(txn.ID)
	require.Error(t, err, "Commit on an aborted txn must not return nil (C-05)")

	var txnErr *types.TxnError
	require.ErrorAs(t, err, &txnErr)
	assert.Equal(t, types.ErrTxnAborted, txnErr.Code)
}

// TestC05_Commit_AlreadyCommitted_IsBenign verifies the benign idempotent path:
// calling Commit twice on a successfully committed txn does not error.
func TestC05_Commit_AlreadyCommitted_IsBenign(t *testing.T) {
	mgr, _ := setupManager()
	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	require.NoError(t, mgr.Commit(txn.ID))
	// Second commit is a benign no-op (already committed).
	require.NoError(t, mgr.Commit(txn.ID))
}

func TestManager_TakeSnapshot(t *testing.T) {
	mgr, _ := setupManager()
	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)

	snap := mgr.TakeSnapshot()
	require.NotNil(t, snap)
	assert.True(t, snap.Contains(txn.ID))
	assert.Greater(t, uint64(snap.Xmax), uint64(0))
}

func TestVacuumChecker_IsCommitted(t *testing.T) {
	mgr, _ := setupManager()
	vc := VacuumChecker{M: mgr}
	assert.False(t, vc.IsCommitted(999))

	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	_ = mgr.Commit(txn.ID)
	assert.True(t, vc.IsCommitted(mvcc.TxnID(txn.ID)))
}

func TestTransaction_ConflictFlags(t *testing.T) {
	mgr, _ := setupManager()
	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)

	assert.False(t, txn.GetInConflict())
	assert.False(t, txn.GetOutConflict())

	txn.SetInConflict()
	assert.True(t, txn.GetInConflict())

	txn.SetOutConflict()
	assert.True(t, txn.GetOutConflict())
}

// TestC06_Abort_AfterCommit_ReturnsError reproduces C-06: previously Abort
// returned nil for any non-active status, including TxnCommitted. A client
// that aborted an already-committed txn received nil and could believe the
// rollback succeeded while the commit's writes were durable (fail-open).
func TestC06_Abort_AfterCommit_ReturnsError(t *testing.T) {
	mgr, _ := setupManager()
	txn, _ := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	require.NoError(t, mgr.Commit(txn.ID))

	err := mgr.Abort(txn.ID, nil)
	require.Error(t, err, "Abort on a committed txn must not return nil (C-06)")

	var txnErr *types.TxnError
	require.ErrorAs(t, err, &txnErr)
	assert.Equal(t, types.ErrTxnCommitted, txnErr.Code)
}

// TestPruneHistory_AbortRecordNotCapPruned verifies that PruneHistory does NOT
// remove abort records for IDs at or above the oldest-active horizon, even when
// the aborted map exceeds MaxHistoryRetained. Removing such records would cause
// IsAborted to return false for a genuinely aborted txn, leading IsVisible to
// treat its rolled-back writes as committed (dirty read).
func TestPruneHistory_AbortRecordNotCapPruned(t *testing.T) {
	mgr, _ := setupManager()

	// Start a long-lived transaction to act as the oldest-active horizon.
	// Any abort record with an ID >= this txn's ID must survive pruning.
	activeTxn, err := mgr.Begin(Protocol2PL, RepeatableRead, 30*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Abort(activeTxn.ID, nil) })

	// Begin and immediately abort a transaction above the active horizon.
	// Its record must survive PruneHistory no matter how large the map grows.
	abortedTxn, err := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, mgr.Abort(abortedTxn.ID, nil))
	targetID := abortedTxn.ID

	// Directly populate the aborted map with MaxHistoryRetained+10 synthetic
	// IDs that are all BELOW activeTxn.ID (safe to prune by the horizon rule).
	// This inflates the map well beyond the cap.
	mgr.mu.Lock()
	for i := ID(1); i <= ID(MaxHistoryRetained+10); i++ {
		if i < activeTxn.ID {
			mgr.aborted[i] = struct{}{}
		}
	}
	mgr.mu.Unlock()

	// PruneHistory should horizon-prune the synthetic low IDs and must NOT
	// cap-prune targetID (which is above the active-txn horizon).
	mgr.PruneHistory()

	assert.True(t, mgr.IsAborted(targetID),
		"IsAborted must still return true for %v after PruneHistory; "+
			"cap-pruning this record would allow dirty reads", targetID)
}

// TestBegin_ConcurrentCapEnforcement verifies that Manager.Begin never allows
// more than MaxActive concurrent transactions, even when many goroutines call
// Begin simultaneously.
func TestBegin_ConcurrentCapEnforcement(t *testing.T) {
	const maxActive = 5
	const goroutines = 20

	mgr, _ := setupManager()
	mgr.MaxActive = maxActive

	type result struct {
		txn *Transaction
		err error
	}
	results := make([]result, goroutines)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	// barrier ensures all goroutines start as simultaneously as possible.
	barrier := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-barrier
			t, err := mgr.Begin(Protocol2PL, ReadCommitted, 5*time.Second)
			results[i] = result{t, err}
		}()
	}
	close(barrier) // release all goroutines at once
	wg.Wait()

	// Count successes and ErrTooManyTransactions failures.
	var succeeded, tooMany, otherErr int
	for _, r := range results {
		switch {
		case r.err == nil:
			succeeded++
		case errors.Is(r.err, ErrTooManyTransactions):
			tooMany++
		default:
			otherErr++
			t.Errorf("unexpected error: %v", r.err)
		}
	}

	assert.Equal(t, 0, otherErr, "no errors other than ErrTooManyTransactions expected")
	assert.LessOrEqual(t, succeeded, maxActive, "active transactions must never exceed MaxActive")
	assert.Greater(t, tooMany, 0, "at least one goroutine must have been rejected")
	assert.Equal(t, succeeded+tooMany, goroutines, "every Begin must either succeed or return ErrTooManyTransactions")

	// Confirm the live active count also never exceeded the cap.
	assert.LessOrEqual(t, mgr.ActiveCount(), maxActive,
		"ActiveCount must not exceed MaxActive after all goroutines complete")
}
