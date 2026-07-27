package storage

import (
	"container/list"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// ErrAllFramesPinned is returned when all buffer pool frames are pinned.
var ErrAllFramesPinned = errors.New("buffer pool: all frames are pinned, cannot evict")

// flusherIface allows the WAL manager to be wired without importing the wal package.
type flusherIface interface {
	FlushedLSN() LSN
	Flush(lsn LSN) error
}

type frame struct {
	page     *Page
	tableID  string
	dirty    bool
	pinCount atomic.Int32
	lruElem  *list.Element
}

// BufferPool manages a fixed number of in-memory frames backed by heap files.
type BufferPool struct {
	mu       sync.Mutex
	frames   map[string]map[PageID]*frame
	lruList  *list.List
	capacity int
	heaps    map[string]*HeapFile
	flusher  flusherIface
}

// NewBufferPool creates a new buffer pool.
func NewBufferPool(capacity int, heaps map[string]*HeapFile, flusher flusherIface) *BufferPool {
	return &BufferPool{
		frames:   make(map[string]map[PageID]*frame),
		lruList:  list.New(),
		capacity: capacity,
		heaps:    heaps,
		flusher:  flusher,
	}
}

// totalFrames returns the total number of frames across all tables (caller holds mu).
func (bp *BufferPool) totalFrames() int {
	n := 0
	for _, m := range bp.frames {
		n += len(m)
	}
	return n
}

// FetchPage brings (tableID, pageID) into the pool. Returns a pinned frame.
func (bp *BufferPool) FetchPage(tableID string, pageID PageID) (*frame, error) {
	bp.mu.Lock()

	// Check if already cached
	if tbl, ok := bp.frames[tableID]; ok {
		if f, ok := tbl[pageID]; ok {
			f.pinCount.Add(1)
			// Remove from LRU (it's now pinned)
			if f.lruElem != nil {
				bp.lruList.Remove(f.lruElem)
				f.lruElem = nil
			}
			bp.mu.Unlock()
			return f, nil
		}
	}

	// Evict if at capacity
	for bp.totalFrames() >= bp.capacity {
		if err := bp.evictLocked(); err != nil {
			bp.mu.Unlock()
			return nil, err
		}
	}

	bp.mu.Unlock()

	// Read page from heap (without lock)
	heap, ok := bp.heaps[tableID]
	if !ok {
		return nil, fmt.Errorf("buffer pool: no heap for table %q", tableID)
	}
	page, err := heap.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("buffer pool: read page %d for %s: %w", pageID, tableID, err)
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Double-check: another goroutine may have loaded this page
	if tbl, ok := bp.frames[tableID]; ok {
		if f, ok := tbl[pageID]; ok {
			f.pinCount.Add(1)
			if f.lruElem != nil {
				bp.lruList.Remove(f.lruElem)
				f.lruElem = nil
			}
			return f, nil
		}
	}

	// Insert new frame
	f := &frame{
		page:    page,
		tableID: tableID,
	}
	f.pinCount.Store(1)
	if bp.frames[tableID] == nil {
		bp.frames[tableID] = make(map[PageID]*frame)
	}
	bp.frames[tableID][pageID] = f
	return f, nil
}

// NewPage allocates a new heap page for tableID and returns a pinned frame.
func (bp *BufferPool) NewPage(tableID string) (PageID, *frame, error) {
	heap, ok := bp.heaps[tableID]
	if !ok {
		return 0, nil, fmt.Errorf("buffer pool: no heap for table %q", tableID)
	}

	pageID, err := heap.AllocatePage()
	if err != nil {
		return 0, nil, fmt.Errorf("buffer pool: allocate page for %s: %w", tableID, err)
	}

	page := NewPage(pageID)

	bp.mu.Lock()
	defer bp.mu.Unlock()

	for bp.totalFrames() >= bp.capacity {
		if err := bp.evictLocked(); err != nil {
			return pageID, nil, err
		}
	}

	f := &frame{
		page:    page,
		tableID: tableID,
	}
	f.pinCount.Store(1)
	if bp.frames[tableID] == nil {
		bp.frames[tableID] = make(map[PageID]*frame)
	}
	bp.frames[tableID][pageID] = f
	return pageID, f, nil
}

// UnpinPage decrements pin count. If dirty=true, marks frame dirty.
func (bp *BufferPool) UnpinPage(tableID string, pageID PageID, dirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	tbl, ok := bp.frames[tableID]
	if !ok {
		return
	}
	f, ok := tbl[pageID]
	if !ok {
		return
	}
	if dirty {
		f.dirty = true
	}
	if f.pinCount.Add(-1) <= 0 {
		// Add to LRU tail (most recently used)
		f.lruElem = bp.lruList.PushBack(f)
	}
}

// FlushPage writes a dirty page to disk, enforcing WAL-before-page rule.
func (bp *BufferPool) FlushPage(tableID string, pageID PageID) error {
	bp.mu.Lock()
	tbl, ok := bp.frames[tableID]
	if !ok {
		bp.mu.Unlock()
		return nil
	}
	f, ok := tbl[pageID]
	if !ok {
		bp.mu.Unlock()
		return nil
	}
	if !f.dirty {
		bp.mu.Unlock()
		return nil
	}
	page := f.page
	bp.mu.Unlock()

	// WAL-before-page rule
	if bp.flusher != nil {
		pageLSN := page.PageLSN()
		if pageLSN > bp.flusher.FlushedLSN() {
			if err := bp.flusher.Flush(pageLSN); err != nil {
				return fmt.Errorf("buffer pool: WAL flush before page write: %w", err)
			}
		}
	}

	heap, ok := bp.heaps[tableID]
	if !ok {
		return fmt.Errorf("buffer pool: no heap for table %q", tableID)
	}
	if err := heap.WriteDurablePage(page); err != nil {
		return fmt.Errorf("buffer pool: flush page %d for %s: %w", pageID, tableID, err)
	}

	bp.mu.Lock()
	f.dirty = false
	bp.mu.Unlock()
	return nil
}

// FlushAllDirty flushes all dirty pages, sorted by PageLSN ascending.
func (bp *BufferPool) FlushAllDirty() error {
	bp.mu.Lock()
	type dirtyEntry struct {
		tableID string
		pageID  PageID
		lsn     LSN
	}
	var dirty []dirtyEntry
	for tableID, tbl := range bp.frames {
		for pageID, f := range tbl {
			if f.dirty {
				dirty = append(dirty, dirtyEntry{tableID, pageID, f.page.PageLSN()})
			}
		}
	}
	bp.mu.Unlock()

	sort.Slice(dirty, func(i, j int) bool { return dirty[i].lsn < dirty[j].lsn })
	for _, d := range dirty {
		if err := bp.FlushPage(d.tableID, d.pageID); err != nil {
			return err
		}
	}
	return nil
}

// Evict evicts the LRU unpinned frame. Caller must NOT hold mu.
func (bp *BufferPool) Evict() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.evictLocked()
}

// evictLocked evicts the LRU unpinned frame. Caller MUST hold mu.
func (bp *BufferPool) evictLocked() error {
	for e := bp.lruList.Front(); e != nil; e = e.Next() {
		f := e.Value.(*frame)
		if f.pinCount.Load() > 0 {
			continue
		}
		// Found evictable frame
		bp.lruList.Remove(e)
		f.lruElem = nil

		if f.dirty {
			// Must flush before evicting (release mu during I/O)
			bp.mu.Unlock()
			if bp.flusher != nil {
				pageLSN := f.page.PageLSN()
				if pageLSN > bp.flusher.FlushedLSN() {
					_ = bp.flusher.Flush(pageLSN)
				}
			}
			heap := bp.heaps[f.tableID]
			if heap != nil {
				_ = heap.WriteDurablePage(f.page)
			}
			bp.mu.Lock()
		}

		// Remove from frames map
		delete(bp.frames[f.tableID], f.page.ID())
		if len(bp.frames[f.tableID]) == 0 {
			delete(bp.frames, f.tableID)
		}
		return nil
	}
	return ErrAllFramesPinned
}
