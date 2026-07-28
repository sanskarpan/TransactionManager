package storage

import (
	"fmt"
	"os"
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
//
// When STORAGE_MODE=memory (default) the row index is an in-memory hash map
// (backward-compatible with the original implementation).
// When STORAGE_MODE=paged the row index is a B+ tree backed by its own heap file
// ("btree-<tableName>") for O(log n) lookups and ordered range scans.
type PagedTable struct {
	name    string
	columns []Column

	mu           sync.RWMutex
	rowIndex     map[RowKey]rowLocation // non-nil only when useMap==true
	index        *BTree                 // non-nil only when useMap==false
	useMap       bool
	insertPageID PageID

	pool    *BufferPool
	heap    *HeapFile
	tableID string

	nextLSN atomic.Uint64
}

// NewPagedTable creates a new PagedTable. Calls RebuildIndex internally.
//
// When STORAGE_MODE != "memory" (and a btree heap can be registered), uses BTree.
// Otherwise falls back to the in-memory hash map.
func NewPagedTable(name string, cols []Column, pool *BufferPool, heap *HeapFile) (*PagedTable, error) {
	storageMode := os.Getenv("STORAGE_MODE")
	useMap := storageMode == "" || storageMode == "memory"

	pt := &PagedTable{
		name:    name,
		columns: cols,
		pool:    pool,
		heap:    heap,
		tableID: name,
		useMap:  useMap,
	}

	if useMap {
		pt.rowIndex = make(map[RowKey]rowLocation)
	} else {
		// Register a dedicated heap file for the B+ tree index.
		btreeTableID := "btree-" + name
		if _, ok := pool.heaps[btreeTableID]; !ok {
			// Auto-register an in-memory-only heap using the same directory as the
			// data heap. We infer the directory from the heap file path.
			btreeHeap, err := OpenHeapFile(heapDir(heap), btreeTableID)
			if err != nil {
				return nil, fmt.Errorf("btree: open heap for %s: %w", btreeTableID, err)
			}
			pool.heaps[btreeTableID] = btreeHeap
		}
		bt, err := NewBTree(pool, btreeTableID)
		if err != nil {
			return nil, fmt.Errorf("btree: init for table %s: %w", name, err)
		}
		pt.index = bt
	}

	if err := pt.RebuildIndex(); err != nil {
		return nil, err
	}
	return pt, nil
}

// heapDir extracts the directory path from a HeapFile's path field.
// We use the exported path — the HeapFile stores it in h.path.
func heapDir(h *HeapFile) string {
	// HeapFile.path is "<dir>/<tableName>.dat"
	// We strip the last component to get the directory.
	path := h.path
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[:i]
		}
	}
	return "."
}

// RebuildIndex scans all pages from 1..lastPageID and populates the index.
func (pt *PagedTable) RebuildIndex() error {
	// Reset map index for in-memory mode
	if pt.useMap {
		pt.rowIndex = make(map[RowKey]rowLocation)
	}
	// Note: BTree index is rebuilt by re-inserting; if existing data is already
	// in the btree heap file it persists across restarts — no rebuild needed.
	// But we still scan heap pages to populate insertPageID and to handle
	// cases where the btree heap was lost.

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
			loc := rowLocation{pageID: pid, slotID: sid}
			if pt.useMap {
				pt.rowIndex[key] = loc
			} else {
				if err := pt.index.Insert(key, loc); err != nil {
					pt.pool.UnpinPage(pt.tableID, pid, false)
					return fmt.Errorf("btree: rebuild insert for key %q: %w", key, err)
				}
			}
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

// indexSearch looks up key in whichever index is active.
func (pt *PagedTable) indexSearch(key RowKey) (rowLocation, bool) {
	if pt.useMap {
		loc, ok := pt.rowIndex[key]
		return loc, ok
	}
	return pt.index.Search(key)
}

// indexInsert inserts key→loc into the active index.
func (pt *PagedTable) indexInsert(key RowKey, loc rowLocation) {
	if pt.useMap {
		pt.rowIndex[key] = loc
		return
	}
	_ = pt.index.Insert(key, loc) // errors are best-effort in this research impl
}

// indexDelete removes key from the active index.
func (pt *PagedTable) indexDelete(key RowKey) {
	if pt.useMap {
		delete(pt.rowIndex, key)
		return
	}
	pt.index.Delete(key)
}

// indexCount returns the number of entries in the active index.
func (pt *PagedTable) indexCount() int {
	if pt.useMap {
		return len(pt.rowIndex)
	}
	return pt.index.Count()
}

// indexAllKeys returns all keys from the active index.
// For the map index, keys are sorted post-hoc (original behavior preserved).
// For the BTree index, keys come out in sorted order already.
func (pt *PagedTable) indexAllKeys() []RowKey {
	if pt.useMap {
		keys := make([]RowKey, 0, len(pt.rowIndex))
		for k := range pt.rowIndex {
			keys = append(keys, k)
		}
		return keys
	}
	return pt.index.AllKeys()
}

// GetRow returns values for the given key. Satisfies TableIface.
func (pt *PagedTable) GetRow(key RowKey) ([]types.Value, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	loc, ok := pt.indexSearch(key)
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

	if loc, ok := pt.indexSearch(key); ok {
		// Update existing row
		f, err := pt.pool.FetchPage(pt.tableID, loc.pageID)
		if err == nil {
			newSlotID, err := f.page.UpdateRow(loc.slotID, key, valBytes)
			if err == nil {
				f.page.SetPageLSN(lsn)
				pt.indexInsert(key, rowLocation{pageID: loc.pageID, slotID: newSlotID})
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
				pt.indexInsert(key, rowLocation{pageID: pt.insertPageID, slotID: sid})
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
				pt.indexInsert(key, rowLocation{pageID: pid, slotID: sid})
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
	pt.indexInsert(key, rowLocation{pageID: pageID, slotID: sid})
	if !f.page.IsFull() {
		pt.insertPageID = pageID
	}
	pt.pool.UnpinPage(pt.tableID, pageID, true)
}

// DeleteRow removes the entry for key. Returns true if a row was removed.
func (pt *PagedTable) DeleteRow(key RowKey) bool {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	loc, ok := pt.indexSearch(key)
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
	pt.indexDelete(key)

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
	keys := pt.indexAllKeys()
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
	keys := pt.indexAllKeys()
	pt.mu.RUnlock()
	if pt.useMap {
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	}
	// BTree already returns keys in sorted order
	return keys
}

// Count returns the number of rows. Satisfies TableIface.
func (pt *PagedTable) Count() int {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return pt.indexCount()
}
