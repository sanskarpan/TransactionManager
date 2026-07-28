package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// BTreeOrder is the maximum number of keys per node.
// Each internal node holds up to BTreeOrder keys and BTreeOrder+1 child pointers.
// Each leaf node holds up to BTreeOrder key-value pairs.
const BTreeOrder = 100

// BTreeNodeType distinguishes leaf from internal nodes.
type BTreeNodeType uint8

const (
	// LeafNode stores (key → rowLocation) pairs in sorted order.
	LeafNode BTreeNodeType = 0
	// InternalNode stores separator keys and child PageIDs.
	InternalNode BTreeNodeType = 1
)

// btreeEntry is a single result from a range scan.
type btreeEntry struct {
	key RowKey
	loc rowLocation
}

// btreeNode is the in-memory representation of a B+ tree node.
// It is serialized into/from a raw Page (using the page's data array directly,
// bypassing the slot-directory mechanism).
//
// On-disk layout inside Page.data (starting at byte 0 — we own the full page):
//
//	[0]       NodeType uint8
//	[1..2]    KeyCount uint16
//	[3..10]   RightSibling PageID uint64  (leaf only; 0 for internal or rightmost leaf)
//	[11..]    Keys: for each key i in [0, KeyCount):
//	             [KeyLen: uint16][Key bytes: KeyLen bytes]
//	After all keys:
//	  Leaf:     for each i in [0, KeyCount): [PageID: uint64][SlotID: uint32]
//	  Internal: for each i in [0, KeyCount+1): [ChildPageID: uint64]
//
// Note: we do NOT use the Page header (bytes 0..31) for header fields — instead
// we store everything starting at offset 0 of the raw data array. This is safe
// because btree pages are in a separate heap file ("btree-<name>") and are never
// interpreted as slot-based pages. However, we still use Page.Serialize /
// Deserialize to persist to disk, so we must stay within the 4096-byte budget.
//
// We store our B+ tree data starting at pageDataOffset to avoid clobbering the
// 32-byte page header that Page.syncHeader() writes.
const btreeDataOffset = PageHeaderSize // 32

type btreeNode struct {
	nodeType     BTreeNodeType
	keys         []RowKey
	children     []PageID      // internal: len = len(keys)+1
	values       []rowLocation // leaf: len = len(keys)
	rightSibling PageID        // leaf only
	pageID       PageID
}

// maxDataBytes is the usable payload area in a page for our custom encoding.
const maxDataBytes = PageSize - btreeDataOffset

// encodeNode serializes n into raw bytes (at most maxDataBytes long).
func encodeNode(n *btreeNode) ([]byte, error) {
	// Estimate size first.
	// Header: 1 (type) + 2 (keyCount) + 8 (rightSibling) = 11 bytes
	// Keys: sum(2 + len(key)) for each key
	// Leaf values: 12 bytes each (8 pageID + 4 slotID)
	// Internal children: 8 bytes each (keyCount+1)

	buf := make([]byte, maxDataBytes)
	off := 0

	buf[off] = byte(n.nodeType)
	off++

	keyCount := len(n.keys)
	binary.LittleEndian.PutUint16(buf[off:], uint16(keyCount))
	off += 2

	binary.LittleEndian.PutUint64(buf[off:], uint64(n.rightSibling))
	off += 8

	// Keys
	for _, k := range n.keys {
		kb := []byte(k)
		if off+2+len(kb) > maxDataBytes {
			return nil, fmt.Errorf("btree: node too large (keys)")
		}
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(kb)))
		off += 2
		copy(buf[off:], kb)
		off += len(kb)
	}

	// Values or children
	if n.nodeType == LeafNode {
		for _, loc := range n.values {
			if off+12 > maxDataBytes {
				return nil, fmt.Errorf("btree: node too large (leaf values)")
			}
			binary.LittleEndian.PutUint64(buf[off:], uint64(loc.pageID))
			off += 8
			binary.LittleEndian.PutUint32(buf[off:], uint32(loc.slotID))
			off += 4
		}
	} else {
		for _, child := range n.children {
			if off+8 > maxDataBytes {
				return nil, fmt.Errorf("btree: node too large (children)")
			}
			binary.LittleEndian.PutUint64(buf[off:], uint64(child))
			off += 8
		}
	}

	return buf[:off], nil
}

// decodeNode parses the payload bytes back into a btreeNode.
func decodeNode(pageID PageID, data []byte) (*btreeNode, error) {
	if len(data) < 11 {
		return nil, fmt.Errorf("btree: page data too short (%d)", len(data))
	}
	off := 0

	n := &btreeNode{pageID: pageID}
	n.nodeType = BTreeNodeType(data[off])
	off++

	keyCount := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2

	n.rightSibling = PageID(binary.LittleEndian.Uint64(data[off:]))
	off += 8

	n.keys = make([]RowKey, keyCount)
	for i := 0; i < keyCount; i++ {
		if off+2 > len(data) {
			return nil, fmt.Errorf("btree: truncated key length at key %d", i)
		}
		klen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if off+klen > len(data) {
			return nil, fmt.Errorf("btree: truncated key bytes at key %d", i)
		}
		n.keys[i] = RowKey(data[off : off+klen])
		off += klen
	}

	if n.nodeType == LeafNode {
		n.values = make([]rowLocation, keyCount)
		for i := 0; i < keyCount; i++ {
			if off+12 > len(data) {
				return nil, fmt.Errorf("btree: truncated leaf value at %d", i)
			}
			pid := PageID(binary.LittleEndian.Uint64(data[off:]))
			off += 8
			sid := int(binary.LittleEndian.Uint32(data[off:]))
			off += 4
			n.values[i] = rowLocation{pageID: pid, slotID: sid}
		}
	} else {
		childCount := keyCount + 1
		n.children = make([]PageID, childCount)
		for i := 0; i < childCount; i++ {
			if off+8 > len(data) {
				return nil, fmt.Errorf("btree: truncated child at %d", i)
			}
			n.children[i] = PageID(binary.LittleEndian.Uint64(data[off:]))
			off += 8
		}
	}

	return n, nil
}

// BTree is a B+ tree index backed by buffer pool pages in a dedicated heap file.
// All mutations are serialized under BTree.mu (and the caller's PagedTable.mu).
type BTree struct {
	mu      sync.RWMutex
	pool    *BufferPool
	tableID string // "btree-<tableName>" — the heap file key
	rootID  PageID // page 1 is always the root (page 0 is meta)
}

// NewBTree opens (or creates) the B+ tree heap file for tableName.
// If the heap file already exists and has a root page, it reuses it;
// otherwise it writes an empty leaf as the initial root.
//
// The caller is responsible for registering the heap in the buffer pool's
// heaps map before calling NewBTree.
func NewBTree(pool *BufferPool, tableID string) (*BTree, error) {
	bt := &BTree{
		pool:    pool,
		tableID: tableID,
		rootID:  1, // page 1 is always root
	}

	// Check if a root page already exists
	heap, ok := pool.heaps[tableID]
	if !ok {
		return nil, fmt.Errorf("btree: no heap registered for %q", tableID)
	}

	if heap.LastPageID() == 0 {
		// New tree: allocate page 1 as an empty leaf root
		if err := bt.initRoot(); err != nil {
			return nil, fmt.Errorf("btree: init root: %w", err)
		}
	}
	// If LastPageID >= 1, the tree already exists — we read nodes on demand.

	return bt, nil
}

// initRoot allocates page 1 and writes an empty leaf node.
func (bt *BTree) initRoot() error {
	pageID, f, err := bt.pool.NewPage(bt.tableID)
	if err != nil {
		return err
	}
	if pageID != 1 {
		bt.pool.UnpinPage(bt.tableID, pageID, false)
		return fmt.Errorf("btree: expected root at page 1, got %d", pageID)
	}
	root := &btreeNode{
		nodeType: LeafNode,
		pageID:   1,
	}
	if err := bt.writeNode(f.page, root); err != nil {
		bt.pool.UnpinPage(bt.tableID, pageID, false)
		return err
	}
	bt.pool.UnpinPage(bt.tableID, pageID, true)
	return nil
}

// writeNode serializes node into page's data area (starting at btreeDataOffset).
func (bt *BTree) writeNode(p *Page, n *btreeNode) error {
	enc, err := encodeNode(n)
	if err != nil {
		return err
	}
	if len(enc) > maxDataBytes {
		return fmt.Errorf("btree: encoded node (%d bytes) exceeds page data area (%d)", len(enc), maxDataBytes)
	}
	// Zero the data area first, then copy encoded bytes
	for i := btreeDataOffset; i < PageSize; i++ {
		p.data[i] = 0
	}
	copy(p.data[btreeDataOffset:], enc)
	// Update the Page header to keep CRC consistent
	p.syncHeader()
	return nil
}

// readNode fetches page pid from the pool and decodes the btreeNode.
// Returns a pinned frame; caller must call UnpinPage when done.
func (bt *BTree) readNode(pid PageID) (*btreeNode, *frame, error) {
	f, err := bt.pool.FetchPage(bt.tableID, pid)
	if err != nil {
		return nil, nil, fmt.Errorf("btree: fetch page %d: %w", pid, err)
	}
	payload := f.page.data[btreeDataOffset:]
	n, err := decodeNode(pid, payload)
	if err != nil {
		bt.pool.UnpinPage(bt.tableID, pid, false)
		return nil, nil, fmt.Errorf("btree: decode page %d: %w", pid, err)
	}
	return n, f, nil
}

// allocNode allocates a new page and returns an empty btreeNode with the given type.
// Returns the node, its frame (pinned), and any error.
func (bt *BTree) allocNode(nt BTreeNodeType) (*btreeNode, *frame, error) {
	pid, f, err := bt.pool.NewPage(bt.tableID)
	if err != nil {
		return nil, nil, err
	}
	n := &btreeNode{
		nodeType: nt,
		pageID:   pid,
	}
	return n, f, nil
}

// flushNode writes n to its page (which must be pinned in f) and unpins it dirty.
func (bt *BTree) flushNode(n *btreeNode, f *frame) error {
	if err := bt.writeNode(f.page, n); err != nil {
		bt.pool.UnpinPage(bt.tableID, n.pageID, false)
		return err
	}
	bt.pool.UnpinPage(bt.tableID, n.pageID, true)
	return nil
}

// Search finds the rowLocation for key. Returns (loc, true) or (zero, false).
func (bt *BTree) Search(key RowKey) (rowLocation, bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.search(key)
}

func (bt *BTree) search(key RowKey) (rowLocation, bool) {
	pid := bt.rootID
	for {
		n, _, err := bt.readNode(pid)
		if err != nil {
			return rowLocation{}, false
		}
		if n.nodeType == LeafNode {
			idx, found := bt.leafSearch(n.keys, key)
			bt.pool.UnpinPage(bt.tableID, pid, false)
			if found {
				return n.values[idx], true
			}
			return rowLocation{}, false
		}
		// Internal node: find the child to descend into
		child := bt.internalFindChild(n.keys, n.children, key)
		bt.pool.UnpinPage(bt.tableID, pid, false)
		pid = child
	}
}

// leafSearch returns the index of key in a sorted leaf key slice (binary search).
// Returns (idx, true) if found, (insertPos, false) if not.
func (bt *BTree) leafSearch(keys []RowKey, key RowKey) (int, bool) {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := (lo + hi) / 2
		if keys[mid] == key {
			return mid, true
		} else if keys[mid] < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, false
}

// internalFindChild returns the child PageID to follow for the given key.
func (bt *BTree) internalFindChild(keys []RowKey, children []PageID, key RowKey) PageID {
	// keys[i] is the separator: go left if key < keys[i], else keep scanning
	for i, k := range keys {
		if key < k {
			return children[i]
		}
	}
	return children[len(keys)]
}

// Insert inserts or updates key → loc in the B+ tree.
func (bt *BTree) Insert(key RowKey, loc rowLocation) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Recursive insert; if the root splits, we create a new root.
	newKey, newRight, err := bt.insertRec(bt.rootID, key, loc)
	if err != nil {
		return err
	}
	if newRight == 0 {
		// No split at root level
		return nil
	}

	// Root was split: create a new internal root
	newRootNode, newRootFrame, err := bt.allocNode(InternalNode)
	if err != nil {
		return fmt.Errorf("btree: alloc new root: %w", err)
	}
	newRootNode.keys = []RowKey{newKey}
	newRootNode.children = []PageID{bt.rootID, newRight}

	if err := bt.flushNode(newRootNode, newRootFrame); err != nil {
		return fmt.Errorf("btree: flush new root: %w", err)
	}
	bt.rootID = newRootNode.pageID
	return nil
}

// insertRec recursively inserts key→loc into the subtree rooted at pid.
// Returns (splitKey, newRightPageID, error).
// If no split occurred, newRightPageID == 0.
func (bt *BTree) insertRec(pid PageID, key RowKey, loc rowLocation) (RowKey, PageID, error) {
	n, f, err := bt.readNode(pid)
	if err != nil {
		return "", 0, err
	}

	if n.nodeType == LeafNode {
		// Find insertion position
		idx, found := bt.leafSearch(n.keys, key)
		if found {
			// Update existing
			n.values[idx] = loc
			if err := bt.flushNode(n, f); err != nil {
				return "", 0, err
			}
			return "", 0, nil
		}

		// Insert at idx (shift right)
		n.keys = append(n.keys, "")
		copy(n.keys[idx+1:], n.keys[idx:])
		n.keys[idx] = key

		n.values = append(n.values, rowLocation{})
		copy(n.values[idx+1:], n.values[idx:])
		n.values[idx] = loc

		if len(n.keys) <= BTreeOrder {
			// No split needed
			if err := bt.flushNode(n, f); err != nil {
				return "", 0, err
			}
			return "", 0, nil
		}

		// Leaf split: left keeps [0..mid), right gets [mid..end)
		mid := len(n.keys) / 2
		splitKey := n.keys[mid]

		rightNode, rightFrame, err := bt.allocNode(LeafNode)
		if err != nil {
			bt.pool.UnpinPage(bt.tableID, pid, false)
			return "", 0, fmt.Errorf("btree: alloc right leaf: %w", err)
		}

		rightNode.keys = make([]RowKey, len(n.keys)-mid)
		copy(rightNode.keys, n.keys[mid:])
		rightNode.values = make([]rowLocation, len(n.values)-mid)
		copy(rightNode.values, n.values[mid:])
		rightNode.rightSibling = n.rightSibling

		n.keys = n.keys[:mid]
		n.values = n.values[:mid]
		n.rightSibling = rightNode.pageID

		if err := bt.flushNode(rightNode, rightFrame); err != nil {
			bt.pool.UnpinPage(bt.tableID, pid, false)
			return "", 0, err
		}
		if err := bt.flushNode(n, f); err != nil {
			return "", 0, err
		}
		return splitKey, rightNode.pageID, nil
	}

	// Internal node: recurse into the correct child
	childIdx := bt.internalChildIdx(n.keys, key)
	childPID := n.children[childIdx]

	// Unpin before recursing (avoid pinning too many at once)
	bt.pool.UnpinPage(bt.tableID, pid, false)

	splitKey, newRight, err := bt.insertRec(childPID, key, loc)
	if err != nil {
		return "", 0, err
	}
	if newRight == 0 {
		return "", 0, nil
	}

	// Re-read this internal node and incorporate the split
	n, f, err = bt.readNode(pid)
	if err != nil {
		return "", 0, err
	}

	// Insert splitKey at childIdx, newRight at childIdx+1
	n.keys = append(n.keys, "")
	copy(n.keys[childIdx+1:], n.keys[childIdx:])
	n.keys[childIdx] = splitKey

	n.children = append(n.children, 0)
	copy(n.children[childIdx+2:], n.children[childIdx+1:])
	n.children[childIdx+1] = newRight

	if len(n.keys) <= BTreeOrder {
		if err := bt.flushNode(n, f); err != nil {
			return "", 0, err
		}
		return "", 0, nil
	}

	// Internal node split
	mid := len(n.keys) / 2
	pushUpKey := n.keys[mid]

	rightInternal, rightFrame, err := bt.allocNode(InternalNode)
	if err != nil {
		bt.pool.UnpinPage(bt.tableID, pid, false)
		return "", 0, fmt.Errorf("btree: alloc right internal: %w", err)
	}

	rightInternal.keys = make([]RowKey, len(n.keys)-mid-1)
	copy(rightInternal.keys, n.keys[mid+1:])
	rightInternal.children = make([]PageID, len(n.children)-mid-1)
	copy(rightInternal.children, n.children[mid+1:])

	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]

	if err := bt.flushNode(rightInternal, rightFrame); err != nil {
		bt.pool.UnpinPage(bt.tableID, pid, false)
		return "", 0, err
	}
	if err := bt.flushNode(n, f); err != nil {
		return "", 0, err
	}
	return pushUpKey, rightInternal.pageID, nil
}

// internalChildIdx returns the index into children[] to follow for key.
func (bt *BTree) internalChildIdx(keys []RowKey, key RowKey) int {
	for i, k := range keys {
		if key < k {
			return i
		}
	}
	return len(keys)
}

// Delete removes key from the B+ tree. Returns true if the key was found and removed.
// This implementation uses lazy deletion (marks the key as deleted in the leaf by
// removing it from the keys/values arrays). It does NOT rebalance — underflow is
// tolerated since this is a research project.
func (bt *BTree) Delete(key RowKey) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	return bt.deleteRec(bt.rootID, key)
}

// deleteRec searches for key starting at pid and removes it from the leaf.
func (bt *BTree) deleteRec(pid PageID, key RowKey) bool {
	n, f, err := bt.readNode(pid)
	if err != nil {
		return false
	}

	if n.nodeType == LeafNode {
		idx, found := bt.leafSearch(n.keys, key)
		if !found {
			bt.pool.UnpinPage(bt.tableID, pid, false)
			return false
		}
		// Remove key and value at idx
		n.keys = append(n.keys[:idx], n.keys[idx+1:]...)
		n.values = append(n.values[:idx], n.values[idx+1:]...)
		if err := bt.flushNode(n, f); err != nil {
			return false
		}
		return true
	}

	// Internal: find child and recurse
	childIdx := bt.internalChildIdx(n.keys, key)
	childPID := n.children[childIdx]
	bt.pool.UnpinPage(bt.tableID, pid, false)

	return bt.deleteRec(childPID, key)
}

// RangeAsc returns all entries with from <= key <= to in ascending key order.
// If from == "" and to == "", returns all entries.
func (bt *BTree) RangeAsc(from, to RowKey) []btreeEntry {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	// Find the left-most leaf
	pid := bt.findLeafPage(from)

	var result []btreeEntry
	for pid != 0 {
		n, _, err := bt.readNode(pid)
		if err != nil {
			break
		}
		nextPID := n.rightSibling

		for i, k := range n.keys {
			if from != "" && k < from {
				continue
			}
			if to != "" && k > to {
				bt.pool.UnpinPage(bt.tableID, pid, false)
				return result
			}
			result = append(result, btreeEntry{key: k, loc: n.values[i]})
		}

		bt.pool.UnpinPage(bt.tableID, pid, false)
		pid = nextPID
	}
	return result
}

// findLeafPage descends the tree to find the leaf that would contain fromKey.
func (bt *BTree) findLeafPage(fromKey RowKey) PageID {
	pid := bt.rootID
	for {
		n, _, err := bt.readNode(pid)
		if err != nil {
			return 0
		}
		if n.nodeType == LeafNode {
			bt.pool.UnpinPage(bt.tableID, pid, false)
			return pid
		}
		var childPID PageID
		if fromKey == "" {
			childPID = n.children[0]
		} else {
			childPID = bt.internalFindChild(n.keys, n.children, fromKey)
		}
		bt.pool.UnpinPage(bt.tableID, pid, false)
		pid = childPID
	}
}

// AllKeys returns all keys in ascending order (convenience wrapper over RangeAsc).
func (bt *BTree) AllKeys() []RowKey {
	entries := bt.RangeAsc("", "")
	keys := make([]RowKey, len(entries))
	for i, e := range entries {
		keys[i] = e.key
	}
	return keys
}

// Count returns the number of entries in the B+ tree.
func (bt *BTree) Count() int {
	return len(bt.RangeAsc("", ""))
}
