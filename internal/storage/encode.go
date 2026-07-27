package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

// EncodeRow serialises a RowKey and its Values into a single byte slice.
// Format: [KeyLen: uint16 LE][Key bytes][ValLen: uint32 LE][Encoded values]
func EncodeRow(key RowKey, vals []types.Value) []byte {
	keyBytes := []byte(key)
	valBytes := types.EncodeValues(vals)
	keyLen := len(keyBytes)
	valLen := len(valBytes)
	buf := make([]byte, 2+keyLen+4+valLen)
	binary.LittleEndian.PutUint16(buf[0:], uint16(keyLen))
	copy(buf[2:], keyBytes)
	binary.LittleEndian.PutUint32(buf[2+keyLen:], uint32(valLen))
	copy(buf[2+keyLen+4:], valBytes)
	return buf
}

// DecodeRow parses a blob produced by EncodeRow.
// Returns the key, decoded values, total bytes consumed, and any error.
func DecodeRow(b []byte) (key RowKey, vals []types.Value, n int, err error) {
	if len(b) < 2 {
		return "", nil, 0, fmt.Errorf("DecodeRow: buffer too short for key length")
	}
	keyLen := int(binary.LittleEndian.Uint16(b[0:]))
	if len(b) < 2+keyLen+4 {
		return "", nil, 0, fmt.Errorf("DecodeRow: buffer too short for key")
	}
	key = RowKey(b[2 : 2+keyLen])
	valLen := int(binary.LittleEndian.Uint32(b[2+keyLen:]))
	if len(b) < 2+keyLen+4+valLen {
		return "", nil, 0, fmt.Errorf("DecodeRow: buffer too short for values")
	}
	vals, err = types.DecodeValues(b[2+keyLen+4 : 2+keyLen+4+valLen])
	if err != nil {
		return "", nil, 0, fmt.Errorf("DecodeRow: %w", err)
	}
	n = 2 + keyLen + 4 + valLen
	return key, vals, n, nil
}

// EncodeKey serialises a RowKey as [Len: uint16 LE][bytes].
func EncodeKey(key RowKey) []byte {
	kb := []byte(key)
	buf := make([]byte, 2+len(kb))
	binary.LittleEndian.PutUint16(buf[0:], uint16(len(kb)))
	copy(buf[2:], kb)
	return buf
}

// DecodeKey parses a length-prefixed RowKey. Returns key and bytes consumed.
func DecodeKey(b []byte) (RowKey, int, error) {
	if len(b) < 2 {
		return "", 0, fmt.Errorf("DecodeKey: buffer too short")
	}
	klen := int(binary.LittleEndian.Uint16(b[0:]))
	if len(b) < 2+klen {
		return "", 0, fmt.Errorf("DecodeKey: buffer too short for key body")
	}
	return RowKey(b[2 : 2+klen]), 2 + klen, nil
}
