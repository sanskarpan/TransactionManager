package mvcc

import (
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckWriteConflict_NoConflict(t *testing.T) {
	mgr := newMockMgr()
	s := snap(10, 20)

	chain := NewVersionChain("k")
	v1 := &Version{XMin: 1, XMax: 0, Data: []types.Value{types.IntVal(100)}, CreatedAt: time.Now()}
	chain.Prepend(v1)
	mgr.setCommitted(1)

	err := CheckWriteConflict(15, chain, s, mgr)
	assert.NoError(t, err)
}

func TestCheckWriteConflict_OwnWrite(t *testing.T) {
	mgr := newMockMgr()
	s := snap(10, 20)

	chain := NewVersionChain("k")
	chain.Prepend(&Version{XMin: 15, CreatedAt: time.Now()})

	err := CheckWriteConflict(15, chain, s, mgr)
	assert.NoError(t, err)
}

func TestCheckWriteConflict_ActiveWriter(t *testing.T) {
	mgr := newMockMgr()
	s := snap(10, 20, 12)

	chain := NewVersionChain("k")
	chain.Prepend(&Version{XMin: 12, CreatedAt: time.Now()})

	err := CheckWriteConflict(15, chain, s, mgr)
	require.Error(t, err)
	var wce *WriteConflictError
	require.ErrorAs(t, err, &wce)
	assert.Equal(t, TxnID(12), wce.ConflictingTxn)
}

func TestCheckWriteConflict_CommittedConcurrent(t *testing.T) {
	mgr := newMockMgr()
	s := snap(10, 20, 12)

	chain := NewVersionChain("k")
	chain.Prepend(&Version{XMin: 12, CreatedAt: time.Now()})
	mgr.setCommitted(12)

	err := CheckWriteConflict(15, chain, s, mgr)
	require.Error(t, err)
	var wce *WriteConflictError
	require.ErrorAs(t, err, &wce)
	assert.Equal(t, TxnID(12), wce.ConflictingTxn)
}

func TestWriteConflictError_Error(t *testing.T) {
	err := &WriteConflictError{ConflictingTxn: 42}
	assert.Equal(t, "write-write conflict with concurrent transaction", err.Error())
}

func TestCheckWriteConflictNoLock(t *testing.T) {
	mgr := newMockMgr()
	s := snap(10, 20)
	chain := NewVersionChain("k")
	chain.Prepend(&Version{XMin: 1, XMax: 0, Data: []types.Value{types.IntVal(100)}, CreatedAt: time.Now()})
	mgr.setCommitted(1)

	err := CheckWriteConflictNoLock(15, chain, s, mgr)
	assert.NoError(t, err)
}
