package storage

import (
	"sort"
	"sync"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

// Column declares one column of a Table. NotNull and PK are advisory
// only — the storage layer does not enforce them; enforcement is the
// responsibility of the calling handler (CT-23 row-shape validation).
type Column struct {
	Name    string
	Type    types.ValueType
	NotNull bool
	PK      bool
}

// FilterFunc is the predicate type passed to Table.Scan. A nil filter
// matches every row.
type FilterFunc func(Row) bool

// Table is an in-memory row store keyed by RowKey. The storage layer
// is the source of truth that the 2PL write path mutates; the MVCC
// store is built on top of it via SeedMVCCStore. All exported methods
// are safe for concurrent use.
type Table struct {
	Name    string
	Columns []Column
	rows    map[RowKey][]types.Value
	mu      sync.RWMutex
}

// NewTable constructs an empty table with the given schema. The rows
// map is allocated immediately so the first PutRow/Scan does not
// trigger a lazy initialization.
func NewTable(name string, columns []Column) *Table {
	return &Table{
		Name:    name,
		Columns: columns,
		rows:    make(map[RowKey][]types.Value),
	}
}

// GetRow returns the value slice stored for key, plus a boolean
// indicating presence. Safe for concurrent reads; callers doing
// read-then-write must use the lock manager (2PL) instead.
func (t *Table) GetRow(key RowKey) ([]types.Value, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	v, ok := t.rows[key]
	return v, ok
}

// PutRow writes (or overwrites) the value slice for key. The map is
// guarded by t.mu; under 2PL this is only called while the txn holds
// the X lock on this row, so no extra synchronization is needed.
func (t *Table) PutRow(key RowKey, values []types.Value) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rows[key] = values
}

// DeleteRow removes the entry for key. Returns true if a row was
// removed, false if the key was not present (used by the undo path
// to decide whether to record the delete in the before-image log).
func (t *Table) DeleteRow(key RowKey) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.rows[key]
	if ok {
		delete(t.rows, key)
	}
	return ok
}

// Scan returns every row matching the (optional) filter. The returned
// slice is a snapshot — callers may iterate without holding t.mu. Order
// is non-deterministic; use Table.Keys for a deterministic view.
func (t *Table) Scan(filter FilterFunc) []Row {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var result []Row
	for k, v := range t.rows {
		row := Row{Key: k, Values: v}
		if filter == nil || filter(row) {
			result = append(result, row)
		}
	}
	return result
}

// Keys returns every row key in sorted order. Used by smoke tests and
// the /api/storage/keys endpoint; expensive on large tables.
func (t *Table) Keys() []RowKey {
	t.mu.RLock()
	defer t.mu.RUnlock()
	keys := make([]RowKey, 0, len(t.rows))
	for k := range t.rows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// Count returns the number of rows currently in the table. Cheap;
// constant-time under the read lock.
func (t *Table) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.rows)
}

// TableName returns the name of the table. Satisfies TableIface.
func (t *Table) TableName() string { return t.Name }

// TableColumns returns the column schema of the table. Satisfies TableIface.
func (t *Table) TableColumns() []Column { return t.Columns }
