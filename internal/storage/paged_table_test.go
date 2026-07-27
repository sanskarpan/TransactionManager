package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

func newTestPagedTable(t *testing.T) (*PagedTable, *BufferPool, *HeapFile) {
	t.Helper()
	dir := t.TempDir()
	heap, err := OpenHeapFile(dir, "test")
	if err != nil {
		t.Fatalf("OpenHeapFile: %v", err)
	}
	heaps := map[string]*HeapFile{"test": heap}
	pool := NewBufferPool(16, heaps, nil)
	cols := []Column{{Name: "val", Type: types.TypeInt}}
	pt, err := NewPagedTable("test", cols, pool, heap)
	if err != nil {
		t.Fatalf("NewPagedTable: %v", err)
	}
	return pt, pool, heap
}

func TestPagedTable_CRUD(t *testing.T) {
	pt, _, heap := newTestPagedTable(t)
	defer heap.Close()

	// Put
	pt.PutRow("k1", []types.Value{types.IntVal(100)})
	pt.PutRow("k2", []types.Value{types.IntVal(200)})

	// Get
	vals, ok := pt.GetRow("k1")
	if !ok || vals[0].Int != 100 {
		t.Errorf("GetRow k1: ok=%v val=%v", ok, vals)
	}

	// Update
	pt.PutRow("k1", []types.Value{types.IntVal(999)})
	vals, ok = pt.GetRow("k1")
	if !ok || vals[0].Int != 999 {
		t.Errorf("after update k1: ok=%v val=%v", ok, vals)
	}

	// Delete
	deleted := pt.DeleteRow("k1")
	if !deleted {
		t.Error("DeleteRow should return true")
	}
	_, ok = pt.GetRow("k1")
	if ok {
		t.Error("k1 should be gone after delete")
	}

	// Count
	if pt.Count() != 1 {
		t.Errorf("Count: got %d want 1", pt.Count())
	}
}

func TestPagedTable_Restart(t *testing.T) {
	dir := t.TempDir()
	cols := []Column{{Name: "val", Type: types.TypeInt}}

	// Write data
	{
		heap, _ := OpenHeapFile(dir, "tbl")
		heaps := map[string]*HeapFile{"tbl": heap}
		pool := NewBufferPool(16, heaps, nil)
		pt, _ := NewPagedTable("tbl", cols, pool, heap)
		pt.PutRow("a", []types.Value{types.IntVal(1)})
		pt.PutRow("b", []types.Value{types.IntVal(2)})
		pool.FlushAllDirty()
		heap.Close()
	}

	// Reopen and verify
	{
		heap, err := OpenHeapFile(dir, "tbl")
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		heaps := map[string]*HeapFile{"tbl": heap}
		pool := NewBufferPool(16, heaps, nil)
		pt, err := NewPagedTable("tbl", cols, pool, heap)
		if err != nil {
			t.Fatalf("NewPagedTable after restart: %v", err)
		}

		vals, ok := pt.GetRow("a")
		if !ok || vals[0].Int != 1 {
			t.Errorf("after restart, row a: ok=%v val=%v", ok, vals)
		}
		vals, ok = pt.GetRow("b")
		if !ok || vals[0].Int != 2 {
			t.Errorf("after restart, row b: ok=%v val=%v", ok, vals)
		}
		heap.Close()
	}
}

func TestPagedTable_Concurrent(t *testing.T) {
	pt, _, heap := newTestPagedTable(t)
	defer heap.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := RowKey(fmt.Sprintf("key-%d", n))
			pt.PutRow(key, []types.Value{types.IntVal(int64(n))})
			pt.GetRow(key)
			pt.DeleteRow(key)
		}(i)
	}
	wg.Wait()
}

func TestPagedTable_Scan(t *testing.T) {
	pt, _, heap := newTestPagedTable(t)
	defer heap.Close()

	pt.PutRow("a", []types.Value{types.IntVal(10)})
	pt.PutRow("b", []types.Value{types.IntVal(20)})
	pt.PutRow("c", []types.Value{types.IntVal(30)})

	rows := pt.Scan(func(r Row) bool {
		return r.Values[0].Int > 15
	})
	if len(rows) != 2 {
		t.Errorf("Scan: expected 2 rows, got %d", len(rows))
	}
}

func TestPagedTable_TableIface(t *testing.T) {
	pt, _, heap := newTestPagedTable(t)
	defer heap.Close()

	// Ensure *PagedTable satisfies TableIface at compile time
	var _ TableIface = pt
	if pt.TableName() != "test" {
		t.Errorf("TableName: got %q", pt.TableName())
	}
	if len(pt.TableColumns()) != 1 {
		t.Errorf("TableColumns: got %d", len(pt.TableColumns()))
	}
}
