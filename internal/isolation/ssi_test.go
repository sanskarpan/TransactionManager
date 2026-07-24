package isolation

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTxn struct {
	mu          sync.Mutex
	id          TxnID
	active      bool
	inConflict  bool
	outConflict bool
}

func newMockTxn(id TxnID) *mockTxn { return &mockTxn{id: id, active: true} }

func (t *mockTxn) GetID() TxnID { return t.id }
func (t *mockTxn) IsActive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.active
}
func (t *mockTxn) SetInConflict() {
	t.mu.Lock()
	t.inConflict = true
	t.mu.Unlock()
}
func (t *mockTxn) SetOutConflict() {
	t.mu.Lock()
	t.outConflict = true
	t.mu.Unlock()
}
func (t *mockTxn) GetInConflict() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inConflict
}
func (t *mockTxn) GetOutConflict() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.outConflict
}

func TestSSI_WriteSkewDetected(t *testing.T) {
	tracker := NewSSITracker()

	t1 := newMockTxn(1)
	t2 := newMockTxn(2)

	// Both read x and y
	tracker.RecordRead(t1, "doctors:A")
	tracker.RecordRead(t1, "doctors:B")
	tracker.RecordRead(t2, "doctors:A")
	tracker.RecordRead(t2, "doctors:B")

	// T1 writes A
	tracker.RecordWrite(t1, "doctors:A")
	// T2 writes B
	tracker.RecordWrite(t2, "doctors:B")

	// T1 should have outConflict (wrote what T2 read... wait)
	// Actually: RecordWrite(T1, A): siread[A] = {T1, T2} → T2.InConflict=true, T1.OutConflict=true
	// RecordWrite(T2, B): siread[B] = {T1, T2} → T1.InConflict=true, T2.OutConflict=true
	// So BOTH t1 and t2 have InConflict && OutConflict

	// T1 commits → is a pivot
	err := tracker.CheckCommit(t1)
	assert.Error(t, err, "T1 should be detected as pivot")

	// T2 commits → also pivot
	err = tracker.CheckCommit(t2)
	assert.Error(t, err, "T2 should be detected as pivot")
}

func TestSSI_NonConflictingTxnsAllCommit(t *testing.T) {
	tracker := NewSSITracker()

	t1 := newMockTxn(1)
	t2 := newMockTxn(2)

	// T1 reads and writes disjoint keys from T2
	tracker.RecordRead(t1, "table:A")
	tracker.RecordWrite(t1, "table:A")

	tracker.RecordRead(t2, "table:B")
	tracker.RecordWrite(t2, "table:B")

	require.NoError(t, tracker.CheckCommit(t1))
	require.NoError(t, tracker.CheckCommit(t2))
}

func TestSSI_SingleWriterNeverPivot(t *testing.T) {
	tracker := NewSSITracker()
	t1 := newMockTxn(1)
	tracker.RecordWrite(t1, "table:A")
	require.NoError(t, tracker.CheckCommit(t1))
}

func TestSSI_CleanupRemovesSIREAD(t *testing.T) {
	tracker := NewSSITracker()
	t1 := newMockTxn(1)
	t2 := newMockTxn(2)

	tracker.RecordRead(t1, "table:A")
	tracker.Cleanup(t1.GetID())

	// After cleanup, t1 should not trigger conflicts for t2's writes
	tracker.RecordWrite(t2, "table:A")
	assert.False(t, t2.GetOutConflict())
}

func TestSSIError_Error(t *testing.T) {
	err := &SSIError{}
	assert.Equal(t, "serialization failure: SSI detected dangerous structure", err.Error())
}

// TestH11_SSI_CleanupUnflagsSurvivor reproduces H-11: in a write-skew
// scenario both txns are mutually pivots. When the first aborts (Cleanup
// called on it), the second must NO LONGER be a pivot, because the
// dangerous structure is broken. The prior implementation cached
// InConflict/OutConflict flags on the Transaction and never cleared them in
// Cleanup, so the survivor was over-aborted. With recomputation from the
// live edge set, the survivor commits cleanly.
func TestH11_SSI_CleanupUnflagsSurvivor(t *testing.T) {
	tracker := NewSSITracker()
	t1 := newMockTxn(1)
	t2 := newMockTxn(2)

	// Both read both doctors (classic write-skew setup).
	tracker.RecordRead(t1, "d:A")
	tracker.RecordRead(t1, "d:B")
	tracker.RecordRead(t2, "d:A")
	tracker.RecordRead(t2, "d:B")

	// T1 writes A, T2 writes B → both become pivots.
	tracker.RecordWrite(t1, "d:A") // T2.InConflict, T1.OutConflict
	tracker.RecordWrite(t2, "d:B") // T1.InConflict, T2.OutConflict

	require.Error(t, tracker.CheckCommit(t1), "T1 is a pivot pre-cleanup")
	require.Error(t, tracker.CheckCommit(t2), "T2 is a pivot pre-cleanup")

	// Abort T1: remove its edges. T2's dangerous structure is broken.
	tracker.Cleanup(t1.GetID())

	// T2 must now commit cleanly (no longer a pivot). The buggy impl still
	// saw both cached flags set on T2 and aborted it.
	require.NoError(t, tracker.CheckCommit(t2),
		"T2 must not be a pivot after T1's Cleanup (H-11 over-abort regression)")
}
