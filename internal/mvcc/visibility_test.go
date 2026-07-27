package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockMgr is a test-only TxnStatusChecker
type mockMgr struct {
	committed map[TxnID]bool
	aborted   map[TxnID]bool
}

func newMockMgr() *mockMgr {
	return &mockMgr{
		committed: make(map[TxnID]bool),
		aborted:   make(map[TxnID]bool),
	}
}

func (m *mockMgr) IsCommitted(id TxnID) bool { return m.committed[id] }
func (m *mockMgr) IsAborted(id TxnID) bool   { return m.aborted[id] }
func (m *mockMgr) IsActive(id TxnID) bool {
	return !m.committed[id] && !m.aborted[id]
}

func (m *mockMgr) TxnStatus(id TxnID) (active, committed bool) {
	return !m.committed[id] && !m.aborted[id], m.committed[id]
}

func (m *mockMgr) setCommitted(id TxnID) { m.committed[id] = true }
func (m *mockMgr) setAborted(id TxnID)   { m.aborted[id] = true }

func makeTxnID(id TxnID) TxnID { return id }

// mockSnapshot implements SnapshotView without importing txn package
type mockSnapshot struct {
	xmin   uint64
	xmax   uint64
	active []uint64
}

func (s *mockSnapshot) ContainsID(id uint64) bool {
	for _, a := range s.active {
		if a == id {
			return true
		}
	}
	return false
}

func (s *mockSnapshot) GetXmin() uint64 { return s.xmin }
func (s *mockSnapshot) GetXmax() uint64 { return s.xmax }

func snap(xmin, xmax uint64, active ...uint64) *mockSnapshot {
	return &mockSnapshot{xmin: xmin, xmax: xmax, active: active}
}

func TestMVCC_Visibility(t *testing.T) {
	mgr := newMockMgr()

	t.Run("committed_before_snapshot_visible", func(t *testing.T) {
		s := snap(10, 20)
		v := &Version{XMin: 5, XMax: 0}
		mgr.setCommitted(5)
		assert.True(t, IsVisible(v, makeTxnID(15), s, mgr))
	})

	t.Run("active_xmin_not_visible", func(t *testing.T) {
		s := snap(10, 20, 12)
		v := &Version{XMin: 12, XMax: 0}
		assert.False(t, IsVisible(v, makeTxnID(15), s, mgr))
	})

	t.Run("own_version_visible", func(t *testing.T) {
		s := snap(10, 20, 15)
		v := &Version{XMin: 15, XMax: 0}
		assert.True(t, IsVisible(v, makeTxnID(15), s, mgr))
	})

	t.Run("deleted_by_committed_txn_invisible", func(t *testing.T) {
		s := snap(10, 20)
		v := &Version{XMin: 5, XMax: 8}
		mgr.setCommitted(5)
		mgr.setCommitted(8)
		assert.False(t, IsVisible(v, makeTxnID(15), s, mgr))
	})

	t.Run("deleted_by_active_txn_still_visible", func(t *testing.T) {
		s := snap(10, 20, 14)
		v := &Version{XMin: 5, XMax: 14}
		mgr.setCommitted(5)
		assert.True(t, IsVisible(v, makeTxnID(15), s, mgr))
	})

	t.Run("deleted_by_aborted_txn_still_visible", func(t *testing.T) {
		s := snap(10, 20)
		v := &Version{XMin: 5, XMax: 13}
		mgr.setCommitted(5)
		mgr.setAborted(13)
		assert.True(t, IsVisible(v, makeTxnID(15), s, mgr))
	})

	t.Run("future_xmin_not_visible", func(t *testing.T) {
		s := snap(10, 20)
		v := &Version{XMin: 25, XMax: 0} // started after our snapshot
		assert.False(t, IsVisible(v, makeTxnID(15), s, mgr))
	})

	t.Run("xmax_future_version_still_visible", func(t *testing.T) {
		// xmax >= Xmax (deleter started after our snapshot) → we still see the row
		s := snap(10, 20)
		v := &Version{XMin: 5, XMax: 25}
		mgr.setCommitted(5)
		assert.True(t, IsVisible(v, makeTxnID(15), s, mgr))
	})

	t.Run("uncommitted_xmin_in_window_not_visible", func(t *testing.T) {
		// xmin in window but not committed → not visible
		s := snap(10, 20)
		v := &Version{XMin: 15, XMax: 0}
		// txn 15 is not in committed or aborted → active
		assert.False(t, IsVisible(v, makeTxnID(99), s, mgr)) // different txn
	})

	t.Run("own_deletion_invisible", func(t *testing.T) {
		// xmax == txnID → we deleted it ourselves → not visible
		s := snap(10, 20, 15)
		v := &Version{XMin: 5, XMax: 15}
		mgr.setCommitted(5)
		assert.False(t, IsVisible(v, makeTxnID(15), s, mgr))
	})
}

// TestC04_AbortedVersionBelowXmin_NotVisible reproduces C-04: a version
// written by an aborted transaction whose ID is below the reader's snapshot
// Xmin must NOT be visible. Previously IsVisible treated any xmin < Xmin as
// committed, so an aborted txn whose versions were still in the chain (not
// yet removed by applyMVCCUndo, or sitting in the window between m.mu.Unlock
// and undo) was read back as committed — a dirty read of rolled-back data.
//
// This is the unit-level guard; the end-to-end race through TxnManager.Abort
// is covered by the txn package regression suite.
func TestC04_AbortedVersionBelowXmin_NotVisible(t *testing.T) {
	mgr := newMockMgr()
	s := snap(10, 20) // snapshot Xmin=10: any creator with id<10 is "before" it

	// Version written by txn 5, which was aborted (and not yet vacuumed).
	v := &Version{XMin: 5, XMax: 0}
	mgr.setAborted(5)

	assert.False(t, IsVisible(v, makeTxnID(15), s, mgr),
		"aborted txn's version below Xmin must be invisible (C-04)")

	// And the symmetric guard: a committed txn 5 below Xmin is visible.
	mgr.setCommitted(5)
	mgr.aborted[5] = false
	assert.True(t, IsVisible(v, makeTxnID(15), s, mgr),
		"committed txn's version below Xmin must remain visible (C-04 regression guard)")
}
