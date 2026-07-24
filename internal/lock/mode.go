package lock

// Mode is the kind of lock a transaction holds or is requesting. The
// ordering matches the standard SQL intention-lock hierarchy. LockU
// (update) is reserved: it appears in the compatibility matrix so the
// matrix is complete, but no caller in this codebase requests it.
type Mode int

const (
	// LockNL is the null lock (no lock held).
	LockNL Mode = 0
	// LockIS is the intention-shared lock (table-level).
	LockIS Mode = 1
	// LockIX is the intention-exclusive lock (table-level).
	LockIX Mode = 2
	// LockS is the shared lock (row-level).
	LockS Mode = 3
	// LockSIX is the shared-intention-exclusive lock (table-level).
	LockSIX Mode = 4
	// LockX is the exclusive lock (row-level).
	LockX Mode = 5
	// LockU is the update lock (reserved; not requested by any current caller).
	LockU Mode = 6
)

func (m Mode) String() string {
	switch m {
	case LockNL:
		return "NL"
	case LockIS:
		return "IS"
	case LockIX:
		return "IX"
	case LockS:
		return "S"
	case LockSIX:
		return "SIX"
	case LockX:
		return "X"
	case LockU:
		return "U"
	}
	return "?"
}

// LockCompatible is the compatibility matrix indexed by [requesting][held].
var LockCompatible = [7][7]bool{
	//      NL     IS     IX     S      SIX    X      U
	/* NL */ {true, true, true, true, true, true, true},
	/* IS */ {true, true, true, true, true, false, true},
	/* IX */ {true, true, true, false, false, false, false},
	/* S  */ {true, true, false, true, false, false, true},
	/* SIX*/ {true, true, false, false, false, false, false},
	/* X  */ {true, false, false, false, false, false, false},
	/* U  */ {true, true, false, true, false, false, false},
}

// LockStrength for upgrade decisions (higher = stronger).
var LockStrength = [7]int{0, 1, 2, 3, 4, 6, 5}

// DominantMode returns the stronger of the two lock modes.
func DominantMode(a, b Mode) Mode {
	if LockStrength[a] >= LockStrength[b] {
		return a
	}
	return b
}
