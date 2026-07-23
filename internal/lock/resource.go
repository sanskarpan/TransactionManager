package lock

import "fmt"

// ResourceType identifies the granularity of a lockable resource. The set
// is closed; adding a new type requires a corresponding ResourceForXxx
// constructor and (usually) a new row in the compatibility reasoning.
type ResourceType int

const (
	// ResourceTable identifies a table-level lock resource.
	ResourceTable ResourceType = iota
	// ResourceRow identifies a row-level lock resource.
	ResourceRow
	// ResourceGap identifies a gap-level lock resource (for phantom prevention).
	ResourceGap
)

// ResourceID is the key for a lockable resource. String() is the
// canonical serialization used by the lock table's sync.Map.
type ResourceID struct {
	Type  ResourceType
	Table string
	Key   string
	GapLo string
	GapHi string
}

func (r ResourceID) String() string {
	switch r.Type {
	case ResourceTable:
		return fmt.Sprintf("table:%s", r.Table)
	case ResourceRow:
		return fmt.Sprintf("row:%s:%s", r.Table, r.Key)
	case ResourceGap:
		return fmt.Sprintf("gap:%s:(%s,%s]", r.Table, r.GapLo, r.GapHi)
	}
	return "unknown"
}

// ResourceForTable constructs a table-level resource ID.
func ResourceForTable(table string) ResourceID {
	return ResourceID{Type: ResourceTable, Table: table}
}

// ResourceForRow constructs a row-level resource ID.
func ResourceForRow(table, key string) ResourceID {
	return ResourceID{Type: ResourceRow, Table: table, Key: key}
}

// ResourceForGap constructs the resource ID for the open interval
// (lo, hi]. The GapLocking ADR (docs/adr/0006-gap-locking.md) explains
// the lexicographic ordering and the upper-bound open flag.
func ResourceForGap(table, lo, hi string) ResourceID {
	return ResourceID{Type: ResourceGap, Table: table, GapLo: lo, GapHi: hi}
}
