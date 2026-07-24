package txn

import (
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupMVCCManager creates a Manager seeded with MVCC data for SSI tests.
func setupMVCCManager() *Manager {
	catalog := storage.NewCatalog()
	catalog.Register(storage.NewTable("doctors", []storage.Column{
		{Name: "name", Type: types.TypeText},
		{Name: "on_call", Type: types.TypeBool},
	}))
	mgr := NewManager(catalog)

	// Seed two doctors both on-call via txn ID 0
	const seedID mvcc.TxnID = 0
	mvcc.SeedMVCCStore(mgr.MVCCStore, "doctors", []storage.Row{
		{Key: "A", Values: []types.Value{types.TextVal("Alice"), types.BoolVal(true)}},
		{Key: "B", Values: []types.Value{types.TextVal("Bob"), types.BoolVal(true)}},
	}, seedID)
	mgr.MarkCommitted(ID(seedID))
	return mgr
}

// TestSSI_WriteSkew_DoctorOnCall implements the classic doctor on-call write skew scenario.
// Invariant: at least one doctor must be on_call=true.
// T1 reads A and B (both on call), then sets A off.
// T2 reads A and B (both on call), then sets B off.
// Without SSI: 0 doctors on call (invariant violated).
// With SSI: one transaction gets ErrSerializationFailure.
func TestSSI_WriteSkew_DoctorOnCall(t *testing.T) {
	mgr := setupMVCCManager()

	offCall := []types.Value{types.TextVal("off"), types.BoolVal(false)}

	t1, err := mgr.Begin(ProtocolMVCC, Serializable, 5*time.Second)
	require.NoError(t, err)
	t2, err := mgr.Begin(ProtocolMVCC, Serializable, 5*time.Second)
	require.NoError(t, err)

	// T1 reads both doctors
	_, _, err = mgr.MVCCRead(t1, "doctors", "A")
	require.NoError(t, err)
	_, _, err = mgr.MVCCRead(t1, "doctors", "B")
	require.NoError(t, err)

	// T2 reads both doctors
	_, _, err = mgr.MVCCRead(t2, "doctors", "A")
	require.NoError(t, err)
	_, _, err = mgr.MVCCRead(t2, "doctors", "B")
	require.NoError(t, err)

	// T1 sets doctor A off-call
	err = mgr.MVCCWrite(t1, "doctors", "A", offCall, UndoUpdate)
	require.NoError(t, err)

	// T2 sets doctor B off-call
	err = mgr.MVCCWrite(t2, "doctors", "B", offCall, UndoUpdate)
	require.NoError(t, err)

	// T1 commits — may or may not be the pivot
	err1 := mgr.Commit(t1.ID)
	// T2 commits — one of them must fail
	err2 := mgr.Commit(t2.ID)

	// Exactly one must fail with serialization failure
	aborted := 0
	if err1 != nil {
		var txnErr *types.TxnError
		require.ErrorAs(t, err1, &txnErr, "T1 error must be TxnError")
		assert.Equal(t, types.ErrSerializationFailure, txnErr.Code, "T1 failure must be serialization failure")
		aborted++
	}
	if err2 != nil {
		var txnErr *types.TxnError
		require.ErrorAs(t, err2, &txnErr, "T2 error must be TxnError")
		assert.Equal(t, types.ErrSerializationFailure, txnErr.Code, "T2 failure must be serialization failure")
		aborted++
	}

	assert.GreaterOrEqual(t, aborted, 1, "SSI must abort at least one transaction to prevent write skew")

	// Verify invariant: count how many doctors are still on call
	reader, _ := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
	rows, err := mgr.MVCCScan(reader, "doctors", nil)
	require.NoError(t, err)
	_ = mgr.Commit(reader.ID)

	onCall := 0
	for _, row := range rows {
		if len(row.Values) >= 2 && row.Values[1].Bool {
			onCall++
		}
	}
	assert.GreaterOrEqual(t, onCall, 1, "invariant: at least one doctor must remain on call")
}

// TestSSI_RepeatableRead_AllowsWriteSkew verifies that at RepeatableRead level,
// write skew is NOT prevented (both transactions commit, violating the invariant).
func TestSSI_RepeatableRead_AllowsWriteSkew(t *testing.T) {
	mgr := setupMVCCManager()
	offCall := []types.Value{types.TextVal("off"), types.BoolVal(false)}

	t1, _ := mgr.Begin(ProtocolMVCC, RepeatableRead, 5*time.Second)
	t2, _ := mgr.Begin(ProtocolMVCC, RepeatableRead, 5*time.Second)

	_, _, _ = mgr.MVCCRead(t1, "doctors", "A")
	_, _, _ = mgr.MVCCRead(t1, "doctors", "B")
	_, _, _ = mgr.MVCCRead(t2, "doctors", "A")
	_, _, _ = mgr.MVCCRead(t2, "doctors", "B")

	_ = mgr.MVCCWrite(t1, "doctors", "A", offCall, UndoUpdate)
	_ = mgr.MVCCWrite(t2, "doctors", "B", offCall, UndoUpdate)

	err1 := mgr.Commit(t1.ID)
	err2 := mgr.Commit(t2.ID)

	// At RepeatableRead level, no SSI — both can commit (write skew allowed)
	// T2's write to B doesn't conflict with T1's write to A
	// Note: if write-write conflict triggers (unlikely with disjoint keys), skip
	committed := 0
	if err1 == nil {
		committed++
	}
	if err2 == nil {
		committed++
	}
	// At RR level, both should commit (write skew is allowed)
	assert.Equal(t, 2, committed, "at RepeatableRead, write skew is allowed (both txns commit)")
}

// TestMVCC_NonRepeatableRead verifies RC allows non-repeatable reads but RR prevents them.
func TestMVCC_NonRepeatableRead(t *testing.T) {
	catalog := storage.NewCatalog()
	catalog.Register(storage.NewTable("accounts", []storage.Column{
		{Name: "balance", Type: types.TypeFloat},
	}))
	mgr := NewManager(catalog)

	// Seed row
	mvcc.SeedMVCCStore(mgr.MVCCStore, "accounts", []storage.Row{
		{Key: "1", Values: []types.Value{types.FloatVal(100.0)}},
	}, 0)
	mgr.MarkCommitted(0)

	t.Run("ReadCommitted_NonRepeatableRead_Possible", func(t *testing.T) {
		t1, _ := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)

		// First read
		v1, found, _ := mgr.MVCCRead(t1, "accounts", "1")
		require.True(t, found)
		assert.Equal(t, 100.0, v1[0].Float)

		// Another transaction updates and commits
		t2, _ := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
		_ = mgr.MVCCWrite(t2, "accounts", "1", []types.Value{types.FloatVal(200.0)}, UndoUpdate)
		_ = mgr.Commit(t2.ID)

		// RC: fresh snapshot per statement — sees the update
		v2, found, _ := mgr.MVCCRead(t1, "accounts", "1")
		require.True(t, found)
		assert.Equal(t, 200.0, v2[0].Float, "RC should see committed update (non-repeatable read)")

		_ = mgr.Abort(t1.ID, nil)
	})

	t.Run("RepeatableRead_SameSnapshotThroughout", func(t *testing.T) {
		t1, _ := mgr.Begin(ProtocolMVCC, RepeatableRead, 5*time.Second)

		// First read
		v1, found, _ := mgr.MVCCRead(t1, "accounts", "1")
		require.True(t, found)
		bal1 := v1[0].Float

		// Another transaction updates and commits
		t2, _ := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
		_ = mgr.MVCCWrite(t2, "accounts", "1", []types.Value{types.FloatVal(bal1 + 500.0)}, UndoUpdate)
		_ = mgr.Commit(t2.ID)

		// RR: same snapshot — should see original value
		v2, found, _ := mgr.MVCCRead(t1, "accounts", "1")
		require.True(t, found)
		assert.Equal(t, bal1, v2[0].Float, "RR should see same value as first read")

		_ = mgr.Commit(t1.ID)
	})
}

// TestMVCC_DirtyRead verifies dirty reads don't occur above ReadUncommitted.
func TestMVCC_DirtyRead(t *testing.T) {
	catalog := storage.NewCatalog()
	catalog.Register(storage.NewTable("data", []storage.Column{
		{Name: "val", Type: types.TypeInt},
	}))
	mgr := NewManager(catalog)
	mvcc.SeedMVCCStore(mgr.MVCCStore, "data", []storage.Row{
		{Key: "x", Values: []types.Value{types.IntVal(0)}},
	}, 0)
	mgr.MarkCommitted(0)

	// T1 writes dirty value but doesn't commit
	t1, _ := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
	_ = mgr.MVCCWrite(t1, "data", "x", []types.Value{types.IntVal(999)}, UndoUpdate)

	// T2 at RC should NOT see T1's uncommitted write
	t2, _ := mgr.Begin(ProtocolMVCC, ReadCommitted, 5*time.Second)
	v, found, _ := mgr.MVCCRead(t2, "data", "x")
	require.True(t, found)
	assert.Equal(t, int64(0), v[0].Int, "RC must not see dirty (uncommitted) data")

	_ = mgr.Abort(t1.ID, nil)
	_ = mgr.Commit(t2.ID)
}
