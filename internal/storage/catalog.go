// Package storage provides the in-memory catalog, table, row, and column
// abstractions that back both the 2PL and MVCC protocols.
package storage

import (
	"sort"
	"sync"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

// TableIface is the row-store contract satisfied by both Table (in-memory)
// and PagedTable (page-backed persistent). Callers should use this interface
// rather than *Table directly so that STORAGE_MODE=paged can be swapped in.
type TableIface interface {
	GetRow(key RowKey) ([]types.Value, bool)
	PutRow(key RowKey, values []types.Value)
	DeleteRow(key RowKey) bool
	Scan(filter FilterFunc) []Row
	Keys() []RowKey
	Count() int
	TableName() string
	TableColumns() []Column
}

// Catalog is a thread-safe registry of named tables.
type Catalog struct {
	mu     sync.RWMutex
	tables map[string]TableIface
}

// NewCatalog creates an empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{tables: make(map[string]TableIface)}
}

// Register adds a table to the catalog.
func (c *Catalog) Register(table TableIface) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables[table.TableName()] = table
}

// Lookup returns the table by name, or false if not found.
func (c *Catalog) Lookup(name string) (TableIface, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.tables[name]
	return t, ok
}

// List returns all registered table names in sorted order.
func (c *Catalog) List() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
