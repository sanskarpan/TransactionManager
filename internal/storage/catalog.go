// Package storage provides the in-memory catalog, table, row, and column
// abstractions that back both the 2PL and MVCC protocols.
package storage

import (
	"sort"
	"sync"
)

// Catalog is a thread-safe registry of named tables.
type Catalog struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

// NewCatalog creates an empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{tables: make(map[string]*Table)}
}

// Register adds a table to the catalog.
func (c *Catalog) Register(table *Table) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables[table.Name] = table
}

// Lookup returns the table by name, or false if not found.
func (c *Catalog) Lookup(name string) (*Table, bool) {
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
