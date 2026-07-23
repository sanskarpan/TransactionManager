package mvcc

import (
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionChain_Prepend(t *testing.T) {
	chain := NewVersionChain("row1")
	v1 := &Version{XMin: 1, Data: []types.Value{types.IntVal(100)}, CreatedAt: time.Now()}
	v2 := &Version{XMin: 2, Data: []types.Value{types.IntVal(200)}, CreatedAt: time.Now()}

	chain.Prepend(v1)
	chain.Prepend(v2)

	all := chain.All()
	assert.Len(t, all, 2)
	assert.Equal(t, TxnID(2), all[0].XMin) // newest first
	assert.Equal(t, TxnID(1), all[1].XMin)
}

func TestVersionChain_SetXMax(t *testing.T) {
	chain := NewVersionChain("row1")
	v1 := &Version{XMin: 1, XMax: 0, Data: []types.Value{types.IntVal(100)}}
	chain.Prepend(v1)

	ok := chain.SetXMax(1, 5)
	assert.True(t, ok)
	assert.Equal(t, TxnID(5), chain.Head().XMax)
}

func TestVersionChain_ClearXMax(t *testing.T) {
	chain := NewVersionChain("row1")
	v1 := &Version{XMin: 1, XMax: 5}
	chain.Prepend(v1)
	chain.ClearXMax(5)
	assert.Equal(t, TxnID(0), chain.Head().XMax)
}

func TestVersionChain_RemoveByXMin(t *testing.T) {
	chain := NewVersionChain("row1")
	chain.Prepend(&Version{XMin: 1})
	chain.Prepend(&Version{XMin: 2})
	chain.Prepend(&Version{XMin: 3})

	chain.RemoveByXMin(2)
	all := chain.All()
	assert.Len(t, all, 2)
	for _, v := range all {
		assert.NotEqual(t, TxnID(2), v.XMin)
	}
}

func TestVersionChain_Prune(t *testing.T) {
	chain := NewVersionChain("row1")
	chain.Prepend(&Version{XMin: 3, XMax: 0})
	chain.Prepend(&Version{XMin: 4, XMax: 5})
	chain.Prepend(&Version{XMin: 5, XMax: 0})

	// Prune version with XMax=5 (simulating vacuum of committed delete)
	chain.Prune(func(v *Version) bool {
		return v.XMax != 0 && v.XMax <= 10
	})
	all := chain.All()
	assert.Len(t, all, 2)
}

func TestVersionChain_ConcurrentAccess(t *testing.T) {
	chain := NewVersionChain("row1")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			chain.Prepend(&Version{XMin: id})
		}(uint64(i))
	}
	wg.Wait()
	assert.Len(t, chain.All(), 50)
}

// TestC07_WriteConflict_WalksEntireChain_AbortedHeadMasksCommittedDeep
// reproduces C-07: a chain whose head is an aborted transaction's version
// (not yet removed by applyMVCCUndo, e.g. inside the abort window) with a
// deeper version written by a concurrent *committed* transaction that was
// active at our snapshot. The prior head-only check in MVCCWrite saw the
// aborted head (IsActive=false, IsCommitted=false) and reported no conflict,
// missing the deeper committed concurrent writer → lost update. The full-chain
// walk must detect the conflict.
func TestVersionChain_SetHead(t *testing.T) {
	chain := NewVersionChain("row1")
	v1 := &Version{XMin: 1}
	v2 := &Version{XMin: 2}

	chain.Prepend(v1)
	assert.Equal(t, TxnID(1), chain.Head().XMin)

	chain.Lock()
	chain.SetHead(v2)
	chain.Unlock()

	assert.Equal(t, TxnID(2), chain.Head().XMin)
	all := chain.All()
	assert.Len(t, all, 1)
	assert.Equal(t, TxnID(2), all[0].XMin)
}

func TestC07_WriteConflict_WalksEntireChain_AbortedHeadMasksCommittedDeep(t *testing.T) {
	mgr := newMockMgr()

	// Build chain: head = aborted txn 5's version; deeper = committed
	// txn 3's version (3 was active at our snapshot and committed); base = 1.
	chain := NewVersionChain("k")
	chain.Prepend(&Version{XMin: 1}) // base, committed below Xmin
	chain.Prepend(&Version{XMin: 3}) // concurrent committed writer
	chain.Prepend(&Version{XMin: 5}) // aborted — sits at head
	mgr.setCommitted(1)
	mgr.setCommitted(3)
	mgr.setAborted(5)

	// Our snapshot: Xmin=2, Xmax=10, active=[3] (txn 3 was active when we
	// began; txn 5 began after our snapshot, hence aborted and above Xmax).
	s := &mockSnapshot{xmin: 2, xmax: 10, active: []uint64{3}}

	// We are txn 7. The conflict must surface on txn 3 (the committed
	// concurrent writer), not be masked by the aborted head.
	err := CheckWriteConflictNoLock(7, chain, s, mgr)
	require.Error(t, err)

	var wce *WriteConflictError
	require.ErrorAs(t, err, &wce)
	assert.Equal(t, TxnID(3), wce.ConflictingTxn,
		"must detect the deeper committed concurrent writer (C-07), not be masked by the aborted head")
}
