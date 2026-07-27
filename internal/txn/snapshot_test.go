package txn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshot_Contains(t *testing.T) {
	snap := &Snapshot{Xmin: 10, Xmax: 20, Active: []ID{12, 14}}
	assert.True(t, snap.Contains(12))
	assert.True(t, snap.Contains(14))
	assert.False(t, snap.Contains(11))
	assert.False(t, snap.Contains(20))
}

func TestSnapshot_IsCommitted(t *testing.T) {
	// Realistic snapshot: Xmin is always included in Active (invariant enforced
	// by takeSnapshot). Active=[10,12] means txns 10 and 12 are still in flight.
	snap := &Snapshot{Xmin: 10, Xmax: 20, Active: []ID{10, 12}}
	// id < Xmin — definitely committed
	assert.True(t, snap.IsCommitted(5))
	// id = Xmin — still active (Xmin is in Active), not committed
	assert.False(t, snap.IsCommitted(10))
	// id in Active but > Xmin — not committed
	assert.False(t, snap.IsCommitted(12))
	// id between Xmin and Xmax but not in Active — committed
	assert.True(t, snap.IsCommitted(11))
	// id >= Xmax — not committed (started after snapshot)
	assert.False(t, snap.IsCommitted(20))
	assert.False(t, snap.IsCommitted(25))
}

// TestSnapshot_IsCommitted_XminInActive explicitly verifies the Xmin-in-Active
// invariant: a snapshot where Xmin is in Active must report Xmin as not committed,
// while a snapshot where Xmin has been committed (not in Active) reports it as committed.
func TestSnapshot_IsCommitted_XminInActive(t *testing.T) {
	// Xmin still active (normal snapshot state from takeSnapshot)
	snapActive := &Snapshot{Xmin: 10, Xmax: 20, Active: []ID{10, 12}}
	assert.False(t, snapActive.IsCommitted(10), "Xmin in Active must not be committed")

	// Hypothetical snapshot taken after txn 10 committed but 12 still active.
	// In this case Xmin advances to 12 and 10 is no longer in Active.
	snapCommitted := &Snapshot{Xmin: 12, Xmax: 20, Active: []ID{12}}
	assert.True(t, snapCommitted.IsCommitted(10), "txn below Xmin must be committed")
}
