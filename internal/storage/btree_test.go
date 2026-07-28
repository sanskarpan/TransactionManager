package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// newTestBTree creates a fresh BTree backed by a temp HeapFile.
func newTestBTree(t *testing.T) (*BTree, *BufferPool) {
	t.Helper()
	dir := t.TempDir()
	tableID := "btree-test"

	heap, err := OpenHeapFile(dir, tableID)
	if err != nil {
		t.Fatalf("OpenHeapFile: %v", err)
	}
	t.Cleanup(func() { _ = heap.Close() })

	heaps := map[string]*HeapFile{tableID: heap}
	pool := NewBufferPool(64, heaps, nil)

	bt, err := NewBTree(pool, tableID)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	return bt, pool
}

// makeLoc creates a rowLocation for testing purposes.
func makeLoc(pageID PageID, slotID int) rowLocation {
	return rowLocation{pageID: pageID, slotID: slotID}
}

// TestBTree_InsertSearch inserts 1000 keys and verifies each can be found.
func TestBTree_InsertSearch(t *testing.T) {
	bt, _ := newTestBTree(t)

	const n = 1000
	for i := 0; i < n; i++ {
		key := RowKey(fmt.Sprintf("key-%05d", i))
		loc := makeLoc(PageID(i+1), i)
		if err := bt.Insert(key, loc); err != nil {
			t.Fatalf("Insert %q: %v", key, err)
		}
	}

	for i := 0; i < n; i++ {
		key := RowKey(fmt.Sprintf("key-%05d", i))
		loc, ok := bt.Search(key)
		if !ok {
			t.Errorf("Search(%q): not found", key)
			continue
		}
		want := makeLoc(PageID(i+1), i)
		if loc != want {
			t.Errorf("Search(%q): got %+v, want %+v", key, loc, want)
		}
	}

	if got := bt.Count(); got != n {
		t.Errorf("Count: got %d, want %d", got, n)
	}
}

// TestBTree_RangeAsc inserts keys 1-100, queries range [20,40], verifies ordered subset.
func TestBTree_RangeAsc(t *testing.T) {
	bt, _ := newTestBTree(t)

	for i := 1; i <= 100; i++ {
		key := RowKey(fmt.Sprintf("%03d", i))
		if err := bt.Insert(key, makeLoc(PageID(i), i)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	entries := bt.RangeAsc("020", "040")
	if len(entries) != 21 { // 020..040 inclusive
		t.Errorf("RangeAsc [020,040]: got %d entries, want 21", len(entries))
	}

	// Verify ordered
	for i := 1; i < len(entries); i++ {
		if entries[i].key < entries[i-1].key {
			t.Errorf("RangeAsc: not sorted at index %d: %q < %q", i, entries[i].key, entries[i-1].key)
		}
	}

	// Verify range bounds
	for _, e := range entries {
		if e.key < "020" || e.key > "040" {
			t.Errorf("RangeAsc: key %q out of [020,040]", e.key)
		}
	}
}

// TestBTree_Split inserts enough keys to trigger multiple leaf splits.
func TestBTree_Split(t *testing.T) {
	bt, _ := newTestBTree(t)

	// BTreeOrder = 100; insert 500 keys to force multiple splits.
	const n = 500
	for i := 0; i < n; i++ {
		key := RowKey(fmt.Sprintf("split-%06d", i))
		if err := bt.Insert(key, makeLoc(PageID(i+1), i)); err != nil {
			t.Fatalf("Insert %q: %v", key, err)
		}
	}

	// Verify all keys are still accessible after splits
	for i := 0; i < n; i++ {
		key := RowKey(fmt.Sprintf("split-%06d", i))
		_, ok := bt.Search(key)
		if !ok {
			t.Errorf("after splits: Search(%q) not found", key)
		}
	}

	// Verify count
	if got := bt.Count(); got != n {
		t.Errorf("Count after splits: got %d, want %d", got, n)
	}

	// Verify order via AllKeys
	keys := bt.AllKeys()
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return keys[i] < keys[j] }) {
		t.Errorf("AllKeys: not sorted after splits")
	}
}

// TestBTree_Delete inserts 100, deletes 50, verifies correctness.
func TestBTree_Delete(t *testing.T) {
	bt, _ := newTestBTree(t)

	const n = 100
	for i := 0; i < n; i++ {
		key := RowKey(fmt.Sprintf("del-%03d", i))
		if err := bt.Insert(key, makeLoc(PageID(i+1), i)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Delete even-numbered keys
	for i := 0; i < n; i += 2 {
		key := RowKey(fmt.Sprintf("del-%03d", i))
		if !bt.Delete(key) {
			t.Errorf("Delete(%q): returned false, expected true", key)
		}
	}

	// Deleted keys must not be found
	for i := 0; i < n; i += 2 {
		key := RowKey(fmt.Sprintf("del-%03d", i))
		if _, ok := bt.Search(key); ok {
			t.Errorf("Search(%q): found after delete", key)
		}
	}

	// Odd-numbered keys must still be found
	for i := 1; i < n; i += 2 {
		key := RowKey(fmt.Sprintf("del-%03d", i))
		_, ok := bt.Search(key)
		if !ok {
			t.Errorf("Search(%q): not found (should still exist)", key)
		}
	}

	// Count should be 50
	if got := bt.Count(); got != n/2 {
		t.Errorf("Count after deletes: got %d, want %d", got, n/2)
	}
}

// TestBTree_Update verifies that inserting the same key twice updates the value.
func TestBTree_Update(t *testing.T) {
	bt, _ := newTestBTree(t)

	key := RowKey("update-me")
	first := makeLoc(PageID(1), 0)
	second := makeLoc(PageID(2), 5)

	if err := bt.Insert(key, first); err != nil {
		t.Fatalf("Insert first: %v", err)
	}
	if err := bt.Insert(key, second); err != nil {
		t.Fatalf("Insert second: %v", err)
	}

	loc, ok := bt.Search(key)
	if !ok {
		t.Fatalf("Search: key not found")
	}
	if loc != second {
		t.Errorf("Update: got %+v, want %+v", loc, second)
	}

	// Count should be 1 (not 2)
	if got := bt.Count(); got != 1 {
		t.Errorf("Count after update: got %d, want 1", got)
	}
}

// TestBTree_EmptySearch verifies searching an empty tree returns false.
func TestBTree_EmptySearch(t *testing.T) {
	bt, _ := newTestBTree(t)
	_, ok := bt.Search("nonexistent")
	if ok {
		t.Error("Search on empty tree: expected false")
	}
}

// TestBTree_DeleteNonExistent verifies deleting a missing key returns false.
func TestBTree_DeleteNonExistent(t *testing.T) {
	bt, _ := newTestBTree(t)
	if bt.Delete("ghost") {
		t.Error("Delete nonexistent: expected false")
	}
}

// TestBTree_RangeAll verifies RangeAsc("","") returns all keys in order.
func TestBTree_RangeAll(t *testing.T) {
	bt, _ := newTestBTree(t)

	var want []RowKey
	for i := 0; i < 50; i++ {
		key := RowKey(fmt.Sprintf("rng-%02d", i))
		want = append(want, key)
		if err := bt.Insert(key, makeLoc(PageID(i+1), i)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

	entries := bt.RangeAsc("", "")
	if len(entries) != len(want) {
		t.Fatalf("RangeAll: got %d entries, want %d", len(entries), len(want))
	}
	for i, e := range entries {
		if e.key != want[i] {
			t.Errorf("RangeAll[%d]: got %q, want %q", i, e.key, want[i])
		}
	}
}

// FuzzBTree_InsertSearch is a fuzz test that inserts random key sequences and
// verifies every inserted key can be found.
func FuzzBTree_InsertSearch(f *testing.F) {
	// Seed corpus
	f.Add([]byte("hello"), []byte("world"), []byte("foo"), []byte("bar"))
	f.Add([]byte("a"), []byte("b"), []byte("c"), []byte("d"))

	f.Fuzz(func(t *testing.T, k1, k2, k3, k4 []byte) {
		bt, _ := newTestBTree(t)

		keys := [][]byte{k1, k2, k3, k4}
		inserted := make(map[RowKey]rowLocation)

		for i, kb := range keys {
			if len(kb) == 0 {
				continue
			}
			key := RowKey(kb)
			loc := makeLoc(PageID(i+1), i)
			if err := bt.Insert(key, loc); err != nil {
				t.Errorf("Insert %q: %v", key, err)
				continue
			}
			inserted[key] = loc
		}

		for key := range inserted {
			_, ok := bt.Search(key)
			if !ok {
				t.Errorf("Search(%q): not found after insert", key)
			}
		}
	})
}

// TestBTree_LargeInsertRandomOrder inserts 300 keys in random order and verifies order.
func TestBTree_LargeInsertRandomOrder(t *testing.T) {
	bt, _ := newTestBTree(t)

	const n = 300
	keys := make([]RowKey, n)
	for i := 0; i < n; i++ {
		keys[i] = RowKey(fmt.Sprintf("rand-%05d", i))
	}

	// Shuffle using a fixed seed for reproducibility
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for i, key := range keys {
		if err := bt.Insert(key, makeLoc(PageID(i+1), i)); err != nil {
			t.Fatalf("Insert %q: %v", key, err)
		}
	}

	// Verify all found
	for _, key := range keys {
		if _, ok := bt.Search(key); !ok {
			t.Errorf("Search(%q): not found", key)
		}
	}

	// Verify AllKeys is sorted
	all := bt.AllKeys()
	if !sort.SliceIsSorted(all, func(i, j int) bool { return all[i] < all[j] }) {
		t.Error("AllKeys not sorted after random insertions")
	}
	if len(all) != n {
		t.Errorf("AllKeys: got %d, want %d", len(all), n)
	}
}
