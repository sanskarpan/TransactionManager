package mvcc

import (
	"testing"

	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_EvictChain(t *testing.T) {
	s := NewStore()
	chain := s.GetOrCreateChain("t", "k")
	require.NotNil(t, chain)

	s.EvictChain("t", "k")
	_, ok := s.GetChain("t", "k")
	assert.False(t, ok, "chain should be evicted")
}

func TestStore_EvictChain_NopForMissing(_ *testing.T) {
	s := NewStore()
	s.EvictChain("nonexistent", "missing")
}

func TestStore_AllVersions(t *testing.T) {
	s := NewStore()
	c1 := s.GetOrCreateChain("t", "k1")
	c1.Prepend(&Version{XMin: 1, XMax: 0, Data: []types.Value{types.IntVal(10)}})

	c2 := s.GetOrCreateChain("t", "k2")
	c2.Prepend(&Version{XMin: 2, XMax: 0, Data: []types.Value{types.IntVal(20)}})

	all := s.AllVersions()
	assert.Len(t, all, 2)
	assert.Contains(t, all, "t:k1")
	assert.Contains(t, all, "t:k2")
}

func TestStore_AllVersions_Empty(t *testing.T) {
	s := NewStore()
	all := s.AllVersions()
	assert.Empty(t, all)
}

func TestStore_GetChain_Missing(t *testing.T) {
	s := NewStore()
	_, ok := s.GetChain("t", "missing")
	assert.False(t, ok)
}

func TestStore_SeedMVCCStore(_ *testing.T) {
	s := NewStore()
	SeedMVCCStore(s, "t", nil, 42)
}
