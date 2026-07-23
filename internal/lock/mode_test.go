package lock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLockCompatibilityMatrix(t *testing.T) {
	cases := []struct {
		req, held Mode
		want      bool
	}{
		// NL is compatible with everything
		{LockNL, LockNL, true}, {LockNL, LockX, true},
		// IS is compatible with IS, IX, S, SIX, U but NOT X
		{LockIS, LockIS, true}, {LockIS, LockIX, true},
		{LockIS, LockS, true}, {LockIS, LockSIX, true},
		{LockIS, LockX, false}, {LockIS, LockU, true},
		// IX compatible with IS, IX but NOT S, SIX, X, U
		{LockIX, LockIS, true}, {LockIX, LockIX, true},
		{LockIX, LockS, false}, {LockIX, LockSIX, false},
		{LockIX, LockX, false}, {LockIX, LockU, false},
		// S compatible with IS, S, U but NOT IX, SIX, X
		{LockS, LockIS, true}, {LockS, LockIX, false},
		{LockS, LockS, true}, {LockS, LockSIX, false},
		{LockS, LockX, false}, {LockS, LockU, true},
		// SIX compatible with IS but NOT IX, S, SIX, X, U
		{LockSIX, LockIS, true}, {LockSIX, LockIX, false},
		{LockSIX, LockS, false}, {LockSIX, LockSIX, false},
		{LockSIX, LockX, false}, {LockSIX, LockU, false},
		// X compatible with nothing except NL
		{LockX, LockNL, true}, {LockX, LockIS, false},
		{LockX, LockIX, false}, {LockX, LockS, false},
		{LockX, LockSIX, false}, {LockX, LockX, false},
		{LockX, LockU, false},
		// U compatible with IS, S but NOT IX, SIX, X, U
		{LockU, LockIS, true}, {LockU, LockIX, false},
		{LockU, LockS, true}, {LockU, LockSIX, false},
		{LockU, LockX, false}, {LockU, LockU, false},
	}
	for _, c := range cases {
		got := LockCompatible[c.req][c.held]
		assert.Equal(t, c.want, got, "LockCompatible[%v][%v]", c.req, c.held)
	}
}

func TestDominantMode(t *testing.T) {
	assert.Equal(t, LockX, DominantMode(LockS, LockX))
	assert.Equal(t, LockX, DominantMode(LockX, LockS))
	assert.Equal(t, LockS, DominantMode(LockS, LockIS))
	assert.Equal(t, LockSIX, DominantMode(LockSIX, LockIX))
}

func TestMode_String(t *testing.T) {
	assert.Equal(t, "S", LockS.String())
	assert.Equal(t, "X", LockX.String())
	assert.Equal(t, "NL", LockNL.String())
	assert.Equal(t, "IS", LockIS.String())
	assert.Equal(t, "IX", LockIX.String())
	assert.Equal(t, "SIX", LockSIX.String())
	assert.Equal(t, "U", LockU.String())
	assert.Equal(t, "?", Mode(99).String())
}


