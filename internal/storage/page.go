package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	PageSize       = 4096
	PageHeaderSize = 32
	SlotSize       = 4  // [offset:uint16][length:uint16]
	MinFreeSpace   = 64 // page is "full" when free space < this
)

// ErrPageFull is returned when a page has insufficient free space.
var ErrPageFull = errors.New("page: insufficient free space")

// ErrSlotNotFound is returned when a slot is not found or is tombstoned.
var ErrSlotNotFound = errors.New("page: slot not found or tombstoned")

// PageID is a page identifier (offset = PageID * PageSize in the heap file).
type PageID uint64

// LSN is a WAL log sequence number. Type alias to avoid importing wal package.
type LSN = uint64

// Page is a fixed-size (4 KB) storage unit.
//
// Layout in data:
//
//	[0..7]   PageID (uint64 LE)
//	[8..15]  PageLSN (uint64 LE)
//	[16..17] SlotCount (uint16 LE)
//	[18..19] FreeSpaceOffset (uint16 LE)   initially = PageSize
//	[20..23] HeaderCRC32 (uint32 LE)
//	[24]     Flags (uint8)
//	[25..31] Reserved
//	[32..]   Slot directory (grows upward): slot i = [Offset:uint16][Length:uint16]
//	[gap]    Free space
//	[bottom] Row data (grows downward)
type Page struct {
	id              PageID
	pageLSN         LSN
	slotCount       uint16
	freeSpaceOffset uint16
	flags           uint8
	data            [PageSize]byte
}

// NewPage creates a new empty page with the given ID.
func NewPage(id PageID) *Page {
	p := &Page{
		id:              id,
		freeSpaceOffset: PageSize,
	}
	p.syncHeader()
	return p
}

// ID returns the page's identifier.
func (p *Page) ID() PageID { return p.id }

// PageLSN returns the page's log sequence number.
func (p *Page) PageLSN() LSN { return p.pageLSN }

// SetPageLSN sets the page's log sequence number.
func (p *Page) SetPageLSN(lsn LSN) {
	p.pageLSN = lsn
	p.syncHeader()
}

// FreeSpace returns the number of free bytes between the slot directory end and the data area.
func (p *Page) FreeSpace() int {
	return int(p.freeSpaceOffset) - p.slotDirEnd()
}

// IsFull returns true when the page cannot accept even a minimal new record.
func (p *Page) IsFull() bool {
	return p.FreeSpace() < MinFreeSpace+SlotSize
}

// SlotCount returns the number of slots (including tombstones).
func (p *Page) SlotCount() int { return int(p.slotCount) }

// InsertRow writes key+encoded-vals into the data area, allocates a slot.
// Tries to reuse a tombstone slot before growing the directory.
// Returns the slot index or ErrPageFull.
func (p *Page) InsertRow(key RowKey, vals []byte) (slotID int, err error) {
	// Build record: [KeyLen:2][key][ValLen:2][vals]
	keyBytes := []byte(key)
	record := make([]byte, 2+len(keyBytes)+2+len(vals))
	binary.LittleEndian.PutUint16(record[0:], uint16(len(keyBytes)))
	copy(record[2:], keyBytes)
	binary.LittleEndian.PutUint16(record[2+len(keyBytes):], uint16(len(vals)))
	copy(record[2+len(keyBytes)+2:], vals)

	needed := len(record)
	// Check if we need a new slot (vs reusing a tombstone)
	tombstoneIdx := -1
	for i := 0; i < int(p.slotCount); i++ {
		off, ln := p.getSlot(i)
		if off == 0 && ln == 0 {
			tombstoneIdx = i
			break
		}
	}

	// Space check: if reusing tombstone, just need record space; else also need SlotSize
	extraSlotSpace := 0
	if tombstoneIdx == -1 {
		extraSlotSpace = SlotSize
	}
	if p.FreeSpace() < needed+extraSlotSpace {
		return 0, ErrPageFull
	}

	// Write record at top of data area (growing downward)
	p.freeSpaceOffset -= uint16(needed)
	copy(p.data[p.freeSpaceOffset:], record)

	// Assign slot
	if tombstoneIdx >= 0 {
		slotID = tombstoneIdx
	} else {
		slotID = int(p.slotCount)
		p.slotCount++
	}
	p.setSlot(slotID, p.freeSpaceOffset, uint16(needed))
	p.syncHeader()
	return slotID, nil
}

// UpdateRow replaces the data for slotID. If new data fits in the same
// space, writes in-place. Otherwise tombstones the old slot and calls
// InsertRow. Returns the (potentially new) slotID.
func (p *Page) UpdateRow(slotID int, key RowKey, vals []byte) (newSlotID int, err error) {
	if slotID < 0 || slotID >= int(p.slotCount) {
		return 0, ErrSlotNotFound
	}
	off, ln := p.getSlot(slotID)
	if off == 0 && ln == 0 {
		return 0, ErrSlotNotFound
	}

	keyBytes := []byte(key)
	newRecord := make([]byte, 2+len(keyBytes)+2+len(vals))
	binary.LittleEndian.PutUint16(newRecord[0:], uint16(len(keyBytes)))
	copy(newRecord[2:], keyBytes)
	binary.LittleEndian.PutUint16(newRecord[2+len(keyBytes):], uint16(len(vals)))
	copy(newRecord[2+len(keyBytes)+2:], vals)

	if len(newRecord) <= int(ln) {
		// Fits in place
		copy(p.data[off:], newRecord)
		// Update length if different
		p.setSlot(slotID, off, uint16(len(newRecord)))
		p.syncHeader()
		return slotID, nil
	}

	// Tombstone old slot and insert anew
	p.setSlot(slotID, 0, 0)
	// Don't decrement slotCount (tombstone stays)
	newSlotID, err = p.InsertRow(key, vals)
	return newSlotID, err
}

// DeleteRow tombstones slot slotID (sets offset=0, length=0).
func (p *Page) DeleteRow(slotID int) {
	if slotID < 0 || slotID >= int(p.slotCount) {
		return
	}
	p.setSlot(slotID, 0, 0)
	p.syncHeader()
}

// GetRow returns the raw key and encoded value bytes for slotID.
// Returns ("", nil, false) if tombstoned or out of range.
func (p *Page) GetRow(slotID int) (key RowKey, vals []byte, ok bool) {
	if slotID < 0 || slotID >= int(p.slotCount) {
		return "", nil, false
	}
	off, ln := p.getSlot(slotID)
	if off == 0 && ln == 0 {
		return "", nil, false
	}
	rec := p.data[off : off+ln]
	if len(rec) < 2 {
		return "", nil, false
	}
	klen := int(binary.LittleEndian.Uint16(rec[0:]))
	if len(rec) < 2+klen+2 {
		return "", nil, false
	}
	key = RowKey(rec[2 : 2+klen])
	vlen := int(binary.LittleEndian.Uint16(rec[2+klen:]))
	if len(rec) < 2+klen+2+vlen {
		return "", nil, false
	}
	vals = rec[2+klen+2 : 2+klen+2+vlen]
	return key, vals, true
}

// HasDeletedSlots returns true if any slot is a tombstone.
func (p *Page) HasDeletedSlots() bool {
	for i := 0; i < int(p.slotCount); i++ {
		off, ln := p.getSlot(i)
		if off == 0 && ln == 0 {
			return true
		}
	}
	return false
}

// Compact defragments the data area. Returns a map of key → new slotID
// for all live rows. Callers must update their row indexes.
func (p *Page) Compact() map[RowKey]int {
	type liveRow struct {
		key  RowKey
		vals []byte
	}
	var rows []liveRow
	for i := 0; i < int(p.slotCount); i++ {
		k, v, ok := p.GetRow(i)
		if !ok {
			continue
		}
		// Copy vals so we can overwrite data
		vc := make([]byte, len(v))
		copy(vc, v)
		rows = append(rows, liveRow{k, vc})
	}

	// Reset page data area
	p.slotCount = 0
	p.freeSpaceOffset = PageSize
	// Zero out slot directory and data area
	for i := PageHeaderSize; i < PageSize; i++ {
		p.data[i] = 0
	}

	result := make(map[RowKey]int, len(rows))
	for _, r := range rows {
		sid, err := p.InsertRow(r.key, r.vals)
		if err != nil {
			// Should not happen after compaction
			panic(fmt.Sprintf("page.Compact: InsertRow failed: %v", err))
		}
		result[r.key] = sid
	}
	return result
}

// Serialize writes the page (including updated header CRC) to a [PageSize]byte.
func (p *Page) Serialize() [PageSize]byte {
	p.syncHeader()
	return p.data
}

// Deserialize parses a [PageSize]byte block, verifying the header CRC.
func (p *Page) Deserialize(b [PageSize]byte) error {
	p.data = b
	// Verify CRC
	storedCRC := binary.LittleEndian.Uint32(p.data[20:24])
	p.loadHeader()
	computed := p.computeHeaderCRC()
	if storedCRC != computed {
		return fmt.Errorf("page: header CRC mismatch (stored=%08x computed=%08x)", storedCRC, computed)
	}
	return nil
}

// slotDirEnd returns the byte offset just past the last slot entry.
func (p *Page) slotDirEnd() int {
	return PageHeaderSize + int(p.slotCount)*SlotSize
}

// getSlot returns the offset and length for slot i.
func (p *Page) getSlot(i int) (uint16, uint16) {
	base := PageHeaderSize + i*SlotSize
	off := binary.LittleEndian.Uint16(p.data[base:])
	ln := binary.LittleEndian.Uint16(p.data[base+2:])
	return off, ln
}

// setSlot writes offset and length for slot i.
func (p *Page) setSlot(i int, offset, length uint16) {
	base := PageHeaderSize + i*SlotSize
	binary.LittleEndian.PutUint16(p.data[base:], offset)
	binary.LittleEndian.PutUint16(p.data[base+2:], length)
}

// computeHeaderCRC returns CRC32 of bytes [0..19] (before the CRC field).
func (p *Page) computeHeaderCRC() uint32 {
	return crc32.ChecksumIEEE(p.data[0:20])
}

// syncHeader writes in-memory fields to data[0:32].
func (p *Page) syncHeader() {
	binary.LittleEndian.PutUint64(p.data[0:], uint64(p.id))
	binary.LittleEndian.PutUint64(p.data[8:], p.pageLSN)
	binary.LittleEndian.PutUint16(p.data[16:], p.slotCount)
	binary.LittleEndian.PutUint16(p.data[18:], p.freeSpaceOffset)
	// CRC of bytes 0..19
	crc := p.computeHeaderCRC()
	binary.LittleEndian.PutUint32(p.data[20:], crc)
	p.data[24] = p.flags
	// bytes 25..31 = reserved zero
}

// loadHeader reads data[0:32] into in-memory fields.
func (p *Page) loadHeader() {
	p.id = PageID(binary.LittleEndian.Uint64(p.data[0:]))
	p.pageLSN = binary.LittleEndian.Uint64(p.data[8:])
	p.slotCount = binary.LittleEndian.Uint16(p.data[16:])
	p.freeSpaceOffset = binary.LittleEndian.Uint16(p.data[18:])
	// CRC at [20:24] — already read by caller
	p.flags = p.data[24]
}
