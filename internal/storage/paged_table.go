package storage

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

type rowLocation struct {
	pageID PageID
	slotID int
}

// PagedTable is a persistent Table implementation backed by a HeapFile + BufferPool.
type PagedTable struct {
	name    string
	columns []Column

	mu           sync.RWMutex
	rowIndex     map[RowKey]rowLocation
	insertPageID PageID

	pool    *BufferPool
	heap    *HeapFile
	tableID string

	nextLSN atomic.Uint64
}

// NewPagedTable creates a new PagedTable. Calls RebuildIndex internally.
func NewPagedTable(name string, cols []Column, pool *BufferPool, heap *HeapFile) (*PagedTable, error) {
	pt := &PagedTable{
		name:     name,
		columns:  cols,
		rowIndex: make(map[RowKey]rowLocation),
		pool:     pool,
		heap:     heap,
		tableID:  name,
	}
	if err := pt.RebuildIndex(); err != nil {
		return nil, err
	}
	return pt, nil
}

// RebuildIndex scans all pages from 1..lastPageID and populates rowIndex.
func (pt *PagedTable) RebuildIndex() error {
	lastID := pt.heap.LastPageID()
	for pid := PageID(1); pid <= lastID; pid++ {
		f, err := pt.pool.FetchPage(pt.tableID, pid)
		if err != nil {
			return err
		}
		page := f.page
		hasSpace := false
		for sid := 0; sid < page.SlotCount(); sid++ {
			key, _, ok := page.GetRow(sid)
			if !ok {
				continue
			}
			pt.rowIndex[key] = rowLocation{pageID: pid, slotID: sid}
			hasSpace = true
		}
		pt.pool.UnpinPage(pt.tableID, pid, false)
		if hasSpace && !page.IsFull() {
			pt.insertPageID = pid
		}
	}
	return nil
}

// SetNextLSN is called by the txn layer before PutRow/DeleteRow.
func (pt *PagedTable) SetNextLSN(lsn LSN) {
	pt.nextLSN.Store(lsn)
}

// TableName returns the table name. Satisfies TableIface.
func (pt *PagedTable) TableName() string { return pt.name }

// TableColumns returns the column schema. Satisfies TableIface.
func (pt *PagedTable) TableColumns() []Column { return pt.columns }

// GetRow returns values for the given key. Satisfies TableIface.
func (pt *PagedTable) GetRow(key RowKey) ([]types.Value, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	loc, ok := pt.rowIndex[key]
	if !ok {
		return nil, false
	}

	f, err := pt.pool.FetchPage(pt.tableID, loc.pageID)
	if err != nil {
		return nil, false
	}
	_, valBytes, found := f.page.GetRow(loc.slotID)
	pt.pool.UnpinPage(pt.tableID, loc.pageID, false)
	if !found {
		return nil, false
	}

	vals, err := types.DecodeValues(valBytes)
	if err != nil {
		return nil, false
	}
	return vals, true
}

// PutRow writes (or overwrites) the value slice for key. Satisfies TableIface.
func (pt *PagedTable) PutRow(key RowKey, values []types.Value) {
	valBytes := types.EncodeValues(values)

	pt.mu.Lock()
	defer pt.mu.Unlock()

	lsn := pt.nextLSN.Load()

	if loc, ok := pt.rowIndex[key]; ok {
		// Update existing row
		f, err := pt.pool.FetchPage(pt.tableID, loc.pageID)
		if err == nil {
			newSlotID, err := f.page.UpdateRow(loc.slotID, key, valBytes)
			if err == nil {
				f.page.SetPageLSN(lsn)
				pt.rowIndex[key] = rowLocation{pageID: loc.pageID, slotID: newSlotID}
				pt.pool.UnpinPage(pt.tableID, loc.pageID, true)
				return
			}
			pt.pool.UnpinPage(pt.tableID, loc.pageID, false)
		}
	}

	// New row or update failed: find a page with space
	pt.putRowNew(key, valBytes, lsn)
}

// putRowNew inserts key/valBytes into an appropriate page. Caller holds pt.mu.
func (pt *PagedTable) putRowNew(key RowKey, valBytes []byte, lsn LSN) {
	// Try current insert page
	if pt.insertPageID != 0 {
		f, err := pt.pool.FetchPage(pt.tableID, pt.insertPageID)
		if err == nil {
			sid, err := f.page.InsertRow(key, valBytes)
			if err == nil {
				f.page.SetPageLSN(lsn)
				pt.rowIndex[key] = rowLocation{pageID: pt.insertPageID, slotID: sid}
				if f.page.IsFull() {
					pt.insertPageID = 0
				}
				pt.pool.UnpinPage(pt.tableID, pt.insertPageID, true)
				return
			}
			pt.pool.UnpinPage(pt.tableID, pt.insertPageID, false)
			pt.insertPageID = 0
		}
	}

	// Try free list
	if pid, ok := pt.heap.TakeFromFreeList(); ok {
		f, err := pt.pool.FetchPage(pt.tableID, pid)
		if err == nil {
			sid, err := f.page.InsertRow(key, valBytes)
			if err == nil {
				f.page.SetPageLSN(lsn)
				pt.rowIndex[key] = rowLocation{pageID: pid, slotID: sid}
				if !f.page.IsFull() {
					pt.insertPageID = pid
				}
				pt.pool.UnpinPage(pt.tableID, pid, true)
				return
			}
			pt.pool.UnpinPage(pt.tableID, pid, false)
		}
	}

	// Allocate new page
	pageID, f, err := pt.pool.NewPage(pt.tableID)
	if err != nil {
		return
	}
	sid, err := f.page.InsertRow(key, valBytes)
	if err != nil {
		pt.pool.UnpinPage(pt.tableID, pageID, false)
		return
	}
	f.page.SetPageLSN(lsn)
	pt.rowIndex[key] = rowLocation{pageID: pageID, slotID: sid}
	if !f.page.IsFull() {
		pt.insertPageID = pageID
	}
	pt.pool.UnpinPage(pt.tableID, pageID, true)
}

// DeleteRow removes the entry for key. Returns true if a row was removed.
func (pt *PagedTable) DeleteRow(key RowKey) bool {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	loc, ok := pt.rowIndex[key]
	if !ok {
		return false
	}

	f, err := pt.pool.FetchPage(pt.tableID, loc.pageID)
	if err != nil {
		return false
	}
	f.page.DeleteRow(loc.slotID)
	lsn := pt.nextLSN.Load()
	f.page.SetPageLSN(lsn)

	// Check if page is now empty (all slots tombstoned)
	allGone := true
	for i := 0; i < f.page.SlotCount(); i++ {
		_, _, alive := f.page.GetRow(i)
		if alive {
			allGone = false
			break
		}
	}
	pt.pool.UnpinPage(pt.tableID, loc.pageID, true)
	delete(pt.rowIndex, key)

	if allGone {
		pt.heap.AddToFreeList(loc.pageID)
		if pt.insertPageID == loc.pageID {
			pt.insertPageID = 0
		}
	}
	return true
}

// Scan returns every row matching the (optional) filter. Satisfies TableIface.
func (pt *PagedTable) Scan(filter FilterFunc) []Row {
	pt.mu.RLock()
	keys := make([]RowKey, 0, len(pt.rowIndex))
	for k := range pt.rowIndex {
		keys = append(keys, k)
	}
	pt.mu.RUnlock()

	var result []Row
	for _, key := range keys {
		vals, ok := pt.GetRow(key)
		if !ok {
			continue
		}
		row := Row{Key: key, Values: vals}
		if filter == nil || filter(row) {
			result = append(result, row)
		}
	}
	return result
}

// Keys returns every row key in sorted order. Satisfies TableIface.
func (pt *PagedTable) Keys() []RowKey {
	pt.mu.RLock()
	keys := make([]RowKey, 0, len(pt.rowIndex))
	for k := range pt.rowIndex {
		keys = append(keys, k)
	}
	pt.mu.RUnlock()
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// Count returns the number of rows. Satisfies TableIface.
func (pt *PagedTable) Count() int {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return len(pt.rowIndex)
}
