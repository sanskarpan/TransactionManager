package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeldSet_Len(t *testing.T) {
	hs := NewHeldSet()
	assert.Equal(t, 0, hs.Len())

	hs.Set(ResourceForRow("t", "1"), LockS)
	assert.Equal(t, 1, hs.Len())

	hs.Set(ResourceForRow("t", "2"), LockX)
	assert.Equal(t, 2, hs.Len())

	hs.Delete(ResourceForRow("t", "1"))
	assert.Equal(t, 1, hs.Len())

	hs.Delete(ResourceForRow("t", "2"))
	assert.Equal(t, 0, hs.Len())
}

func TestHeldSet_GetMissing(t *testing.T) {
	hs := NewHeldSet()
	_, ok := hs.Get(ResourceForRow("nonexistent", "0"))
	assert.False(t, ok)
}

func TestHeldSet_Overwrite(t *testing.T) {
	hs := NewHeldSet()
	hs.Set(ResourceForRow("t", "1"), LockS)
	m, ok := hs.Get(ResourceForRow("t", "1"))
	assert.True(t, ok)
	assert.Equal(t, LockS, m)

	hs.Set(ResourceForRow("t", "1"), LockX)
	m, ok = hs.Get(ResourceForRow("t", "1"))
	assert.True(t, ok)
	assert.Equal(t, LockX, m)
}

func TestHeldSet_Keys(t *testing.T) {
	hs := NewHeldSet()
	r1 := ResourceForRow("t", "1")
	r2 := ResourceForRow("t", "2")
	hs.Set(r1, LockS)
	hs.Set(r2, LockS)
	keys := hs.Keys()
	assert.Len(t, keys, 2)
}
