package storage

import (
	"testing"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

func TestPage_InsertAndGet(t *testing.T) {
	p := NewPage(1)
	vals := types.EncodeValues([]types.Value{types.IntVal(42)})
	sid, err := p.InsertRow("key1", vals)
	if err != nil {
		t.Fatalf("InsertRow: %v", err)
	}
	key, got, ok := p.GetRow(sid)
	if !ok {
		t.Fatal("GetRow: not found")
	}
	if key != "key1" {
		t.Errorf("key mismatch: got %q", key)
	}
	if len(got) != len(vals) {
		t.Errorf("vals length mismatch")
	}
}

func TestPage_InsertUntilFull(t *testing.T) {
	p := NewPage(1)
	vals := types.EncodeValues([]types.Value{types.IntVal(0)})
	insertCount := 0
	for i := 0; i < 1000; i++ {
		_, err := p.InsertRow(RowKey("k"), vals)
		if err == ErrPageFull {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		insertCount++
	}
	if insertCount == 0 {
		t.Error("expected at least one insert before full")
	}
	if !p.IsFull() {
		t.Error("expected page to be full")
	}
}

func TestPage_DeleteAndCompact(t *testing.T) {
	p := NewPage(1)
	vals := types.EncodeValues([]types.Value{types.TextVal("hello")})
	sid0, _ := p.InsertRow("k0", vals)
	sid1, _ := p.InsertRow("k1", vals)
	_ = sid1

	p.DeleteRow(sid0)
	if !p.HasDeletedSlots() {
		t.Error("expected deleted slots")
	}

	newIndex := p.Compact()
	if p.HasDeletedSlots() {
		t.Error("expected no deleted slots after compact")
	}
	if _, ok := newIndex["k0"]; ok {
		t.Error("k0 should not be in index after delete+compact")
	}
	if _, ok := newIndex["k1"]; !ok {
		t.Error("k1 should be in index after compact")
	}
}

func TestPage_SerializeDeserialize(t *testing.T) {
	p := NewPage(7)
	vals := types.EncodeValues([]types.Value{types.IntVal(99)})
	_, err := p.InsertRow("testkey", vals)
	if err != nil {
		t.Fatalf("InsertRow: %v", err)
	}

	raw := p.Serialize()
	p2 := &Page{}
	if err := p2.Deserialize(raw); err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if p2.ID() != 7 {
		t.Errorf("ID mismatch: got %d", p2.ID())
	}
	if p2.SlotCount() != 1 {
		t.Errorf("SlotCount mismatch: got %d", p2.SlotCount())
	}
	key, _, ok := p2.GetRow(0)
	if !ok {
		t.Fatal("GetRow after deserialize: not found")
	}
	if key != "testkey" {
		t.Errorf("key mismatch: got %q", key)
	}
}

func TestPage_CRCCorruption(t *testing.T) {
	p := NewPage(1)
	raw := p.Serialize()
	raw[5] ^= 0xFF // corrupt header
	p2 := &Page{}
	if err := p2.Deserialize(raw); err == nil {
		t.Error("expected CRC error for corrupted page")
	}
}

func TestPage_UpdateRow(t *testing.T) {
	p := NewPage(1)
	vals := types.EncodeValues([]types.Value{types.IntVal(1)})
	sid, _ := p.InsertRow("key", vals)

	newVals := types.EncodeValues([]types.Value{types.IntVal(2)})
	newSid, err := p.UpdateRow(sid, "key", newVals)
	if err != nil {
		t.Fatalf("UpdateRow: %v", err)
	}
	_, got, ok := p.GetRow(newSid)
	if !ok {
		t.Fatal("GetRow after update: not found")
	}
	decoded, _ := types.DecodeValues(got)
	if decoded[0].Int != 2 {
		t.Errorf("expected 2, got %d", decoded[0].Int)
	}
}
