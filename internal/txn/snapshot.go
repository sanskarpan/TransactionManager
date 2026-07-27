package txn

import "time"

// Snapshot captures the active transaction set at a point in time for
// MVCC visibility decisions.
type Snapshot struct {
	Xmin    ID
	Xmax    ID
	Active  []ID
	TakenAt time.Time
}

// Contains reports whether id was in the active set at snapshot time.
func (s *Snapshot) Contains(id ID) bool {
	for _, a := range s.Active {
		if a == id {
			return true
		}
	}
	return false
}

// IsCommitted reports whether the given transaction was committed at snapshot time.
func (s *Snapshot) IsCommitted(id ID) bool {
	if id >= s.Xmax {
		return false
	}
	if id < s.Xmin {
		return true
	}
	// Xmin <= id < Xmax: committed only if not in Active list.
	// takeSnapshot() always includes Xmin in Active, so Contains(Xmin) is always
	// true and the former "id != s.Xmin" guard was redundant dead code.
	return !s.Contains(id)
}

// ContainsID implements mvcc.SnapshotView — reports whether txnID was active at snapshot time.
func (s *Snapshot) ContainsID(id uint64) bool {
	return s.Contains(ID(id))
}

// GetXmin implements mvcc.SnapshotView.
func (s *Snapshot) GetXmin() uint64 { return uint64(s.Xmin) }

// GetXmax implements mvcc.SnapshotView.
func (s *Snapshot) GetXmax() uint64 { return uint64(s.Xmax) }
