package storage

import (
	"testing"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

func TestEncodeDecodeRow_AllTypes(t *testing.T) {
	vals := []types.Value{
		types.IntVal(42),
		types.FloatVal(3.14),
		types.TextVal("hello"),
		types.BoolVal(true),
		types.NullVal(),
	}
	key := RowKey("test-key")

	encoded := EncodeRow(key, vals)
	gotKey, gotVals, n, err := DecodeRow(encoded)
	if err != nil {
		t.Fatalf("DecodeRow error: %v", err)
	}
	if gotKey != key {
		t.Errorf("key mismatch: got %q want %q", gotKey, key)
	}
	if n != len(encoded) {
		t.Errorf("bytes consumed: got %d want %d", n, len(encoded))
	}
	if len(gotVals) != len(vals) {
		t.Fatalf("values length mismatch: got %d want %d", len(gotVals), len(vals))
	}
	for i, v := range vals {
		if gotVals[i] != v {
			t.Errorf("value[%d] mismatch: got %+v want %+v", i, gotVals[i], v)
		}
	}
}

func TestEncodeDecodeRow_EmptyRow(t *testing.T) {
	key := RowKey("empty")
	encoded := EncodeRow(key, nil)
	gotKey, gotVals, _, err := DecodeRow(encoded)
	if err != nil {
		t.Fatalf("DecodeRow error: %v", err)
	}
	if gotKey != key {
		t.Errorf("key mismatch: got %q want %q", gotKey, key)
	}
	if len(gotVals) != 0 {
		t.Errorf("expected empty values, got %d", len(gotVals))
	}
}

func TestEncodeDecodeKey(t *testing.T) {
	key := RowKey("mykey")
	enc := EncodeKey(key)
	gotKey, n, err := DecodeKey(enc)
	if err != nil {
		t.Fatalf("DecodeKey error: %v", err)
	}
	if gotKey != key {
		t.Errorf("key mismatch: got %q want %q", gotKey, key)
	}
	if n != len(enc) {
		t.Errorf("bytes consumed: got %d want %d", n, len(enc))
	}
}

func TestDecodeRow_Truncated(t *testing.T) {
	_, _, _, err := DecodeRow([]byte{0x01})
	if err == nil {
		t.Error("expected error for truncated buffer")
	}
}
