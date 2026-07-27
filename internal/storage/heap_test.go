package storage

import (
	"os"
	"testing"
)

func TestHeapFile_OpenNew(t *testing.T) {
	dir := t.TempDir()
	h, err := OpenHeapFile(dir, "test")
	if err != nil {
		t.Fatalf("OpenHeapFile: %v", err)
	}
	defer h.Close()

	if h.LastPageID() != 0 {
		t.Errorf("expected lastPageID=0, got %d", h.LastPageID())
	}
}

func TestHeapFile_AllocateAndRead(t *testing.T) {
	dir := t.TempDir()
	h, err := OpenHeapFile(dir, "test")
	if err != nil {
		t.Fatalf("OpenHeapFile: %v", err)
	}
	defer h.Close()

	pid, err := h.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	if pid != 1 {
		t.Errorf("expected page ID 1, got %d", pid)
	}

	p, err := h.ReadPage(pid)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if p.ID() != pid {
		t.Errorf("page ID mismatch: got %d", p.ID())
	}
}

func TestHeapFile_MetaRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Create, allocate pages, close
	h, err := OpenHeapFile(dir, "mytable")
	if err != nil {
		t.Fatalf("OpenHeapFile: %v", err)
	}
	h.AllocatePage()
	h.AllocatePage()
	h.AddToFreeList(1)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify
	h2, err := OpenHeapFile(dir, "mytable")
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer h2.Close()

	if h2.LastPageID() != 2 {
		t.Errorf("lastPageID: got %d want 2", h2.LastPageID())
	}
	if h2.tableName != "mytable" {
		t.Errorf("tableName: got %q", h2.tableName)
	}
	pid, ok := h2.TakeFromFreeList()
	if !ok {
		t.Error("expected free list entry")
	}
	if pid != 1 {
		t.Errorf("free list entry: got %d want 1", pid)
	}
}

func TestHeapFile_WritePage(t *testing.T) {
	dir := t.TempDir()
	h, err := OpenHeapFile(dir, "test")
	if err != nil {
		t.Fatalf("OpenHeapFile: %v", err)
	}
	defer h.Close()

	pid, err := h.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}

	p := NewPage(pid)
	p.SetPageLSN(42)
	if err := h.WritePage(p); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	p2, err := h.ReadPage(pid)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if p2.PageLSN() != 42 {
		t.Errorf("PageLSN: got %d want 42", p2.PageLSN())
	}
}

// Ensure heap file creation works in the temp dir
func TestHeapFile_FileCreated(t *testing.T) {
	dir := t.TempDir()
	h, err := OpenHeapFile(dir, "tbl")
	if err != nil {
		t.Fatalf("OpenHeapFile: %v", err)
	}
	h.Close()

	if _, err := os.Stat(dir + "/tbl.dat"); err != nil {
		t.Errorf("file not created: %v", err)
	}
}
