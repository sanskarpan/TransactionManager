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
	snap := &Snapshot{Xmin: 10, Xmax: 20, Active: []ID{12}}
	assert.True(t, snap.IsCommitted(5))
	// id = Xmin - not committed (Xmin is the oldest active)
	assert.False(t, snap.IsCommitted(10))
	// id in Active = not committed
	assert.False(t, snap.IsCommitted(12))
	// id > Xmin but not in Active = committed
	assert.True(t, snap.IsCommitted(11))
}
