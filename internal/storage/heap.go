package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ErrPageNotFound is returned when a requested page does not exist.
var ErrPageNotFound = errors.New("heap: page not found")

// HeapFile manages a single .dat file for one table.
// File layout: contiguous PageSize blocks indexed by PageID.
// Page 0 is the meta page (not used for row data).
type HeapFile struct {
	mu         sync.Mutex
	file       *os.File
	path       string
	tableName  string
	lastPageID PageID
	freeList   []PageID
}

// OpenHeapFile opens or creates <dir>/<tableName>.dat.
func OpenHeapFile(dir, tableName string) (*HeapFile, error) {
	path := filepath.Join(dir, tableName+".dat")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("heap: open %s: %w", path, err)
	}

	h := &HeapFile{
		file:      file,
		path:      path,
		tableName: tableName,
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("heap: stat %s: %w", path, err)
	}

	if info.Size() == 0 {
		// New file: write meta page
		if err := h.writeMeta(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("heap: init meta: %w", err)
		}
	} else {
		// Existing file: read meta
		if err := h.readMeta(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("heap: read meta: %w", err)
		}
	}

	return h, nil
}

// readMeta reads page 0 and restores lastPageID and freeList.
func (h *HeapFile) readMeta() error {
	var raw [PageSize]byte
	if _, err := h.file.ReadAt(raw[:], 0); err != nil {
		return fmt.Errorf("heap: read meta page: %w", err)
	}
	// Meta data starts at offset PageHeaderSize (after the page header)
	d := raw[PageHeaderSize:]
	if len(d) < 10 {
		return fmt.Errorf("heap: meta page too short")
	}
	h.lastPageID = PageID(binary.LittleEndian.Uint64(d[0:]))
	nameLen := int(binary.LittleEndian.Uint16(d[8:]))
	if len(d) < 10+nameLen+2 {
		return fmt.Errorf("heap: meta page truncated at table name")
	}
	h.tableName = string(d[10 : 10+nameLen])
	freeListLen := int(binary.LittleEndian.Uint16(d[10+nameLen:]))
	if len(d) < 10+nameLen+2+freeListLen*8 {
		return fmt.Errorf("heap: meta page truncated at free list")
	}
	h.freeList = make([]PageID, freeListLen)
	for i := 0; i < freeListLen; i++ {
		h.freeList[i] = PageID(binary.LittleEndian.Uint64(d[10+nameLen+2+i*8:]))
	}
	return nil
}

// writeMeta persists the meta page (page 0).
func (h *HeapFile) writeMeta() error {
	var raw [PageSize]byte
	d := raw[PageHeaderSize:]
	binary.LittleEndian.PutUint64(d[0:], uint64(h.lastPageID))
	nameBytes := []byte(h.tableName)
	binary.LittleEndian.PutUint16(d[8:], uint16(len(nameBytes)))
	copy(d[10:], nameBytes)
	binary.LittleEndian.PutUint16(d[10+len(nameBytes):], uint16(len(h.freeList)))
	for i, pid := range h.freeList {
		binary.LittleEndian.PutUint64(d[10+len(nameBytes)+2+i*8:], uint64(pid))
	}
	_, err := h.file.WriteAt(raw[:], 0)
	return err
}

// ReadPage reads the id'th 4 KB block and deserializes it.
func (h *HeapFile) ReadPage(id PageID) (*Page, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.readPageLocked(id)
}

func (h *HeapFile) readPageLocked(id PageID) (*Page, error) {
	var raw [PageSize]byte
	offset := int64(id) * PageSize
	if _, err := h.file.ReadAt(raw[:], offset); err != nil {
		return nil, fmt.Errorf("heap: read page %d: %w", id, err)
	}
	p := &Page{}
	if err := p.Deserialize(raw); err != nil {
		return nil, fmt.Errorf("heap: deserialize page %d: %w", id, err)
	}
	return p, nil
}

// WritePage writes page to disk at correct offset.
func (h *HeapFile) WritePage(p *Page) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.writePageLocked(p)
}

func (h *HeapFile) writePageLocked(p *Page) error {
	raw := p.Serialize()
	offset := int64(p.ID()) * PageSize
	_, err := h.file.WriteAt(raw[:], offset)
	return err
}

// WriteDurablePage writes page and fsyncs.
func (h *HeapFile) WriteDurablePage(p *Page) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.writePageLocked(p); err != nil {
		return err
	}
	return h.file.Sync()
}

// AllocatePage extends file by one page, increments lastPageID, persists meta.
func (h *HeapFile) AllocatePage() (PageID, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastPageID++
	newID := h.lastPageID
	// Write zeroed page at correct offset
	p := NewPage(newID)
	if err := h.writePageLocked(p); err != nil {
		h.lastPageID--
		return 0, fmt.Errorf("heap: allocate page: %w", err)
	}
	// Persist meta
	if err := h.writeMeta(); err != nil {
		return newID, fmt.Errorf("heap: persist meta after allocate: %w", err)
	}
	return newID, nil
}

// AddToFreeList adds a page to the free list.
func (h *HeapFile) AddToFreeList(id PageID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.freeList = append(h.freeList, id)
}

// TakeFromFreeList removes and returns a page from the free list.
func (h *HeapFile) TakeFromFreeList() (PageID, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.freeList) == 0 {
		return 0, false
	}
	id := h.freeList[len(h.freeList)-1]
	h.freeList = h.freeList[:len(h.freeList)-1]
	return id, true
}

// PageCount returns total pages including meta page.
func (h *HeapFile) PageCount() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return int64(h.lastPageID) + 1
}

// LastPageID returns the highest allocated data page ID.
func (h *HeapFile) LastPageID() PageID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastPageID
}

// Close flushes meta and closes file.
func (h *HeapFile) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.writeMeta(); err != nil {
		return err
	}
	return h.file.Close()
}
