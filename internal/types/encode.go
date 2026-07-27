package types

import (
	"encoding/binary"
	"fmt"
	"math"
)

// EncodeValues serialises a slice of Values into a compact binary blob.
//
// Wire format:
//
//	[count: uint16 LE]
//	per value:
//	  [Type: uint8][IsNull: uint8]
//	  body (absent when IsNull=1):
//	    TypeInt:   [int64 LE: 8 bytes]
//	    TypeFloat: [float64 IEEE-754 LE: 8 bytes]
//	    TypeText:  [len: uint16 LE][UTF-8 bytes]
//	    TypeBool:  [0x00|0x01: 1 byte]
//	    TypeNull:  (no body)
func EncodeValues(vals []Value) []byte {
	if len(vals) > 0xFFFF {
		panic(fmt.Sprintf("EncodeValues: too many values (%d > 65535)", len(vals)))
	}
	// Pre-compute size to avoid reallocations.
	size := 2 // count header
	for _, v := range vals {
		size += 2 // type + isNull
		if !v.IsNull {
			switch v.Type {
			case TypeInt:
				size += 8
			case TypeFloat:
				size += 8
			case TypeText:
				size += 2 + len(v.Text)
			case TypeBool:
				size++
			}
		}
	}

	buf := make([]byte, size)
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(vals)))
	pos += 2

	for _, v := range vals {
		buf[pos] = uint8(v.Type)
		pos++
		if v.IsNull {
			buf[pos] = 1
			pos++
			continue
		}
		buf[pos] = 0
		pos++
		switch v.Type {
		case TypeNull:
			// no body
		case TypeInt:
			binary.LittleEndian.PutUint64(buf[pos:], uint64(v.Int))
			pos += 8
		case TypeFloat:
			binary.LittleEndian.PutUint64(buf[pos:], math.Float64bits(v.Float))
			pos += 8
		case TypeText:
			if len(v.Text) > 0xFFFF {
				panic(fmt.Sprintf("EncodeValues: text too long (%d bytes)", len(v.Text)))
			}
			binary.LittleEndian.PutUint16(buf[pos:], uint16(len(v.Text)))
			pos += 2
			copy(buf[pos:], v.Text)
			pos += len(v.Text)
		case TypeBool:
			if v.Bool {
				buf[pos] = 1
			} else {
				buf[pos] = 0
			}
			pos++
		}
	}
	return buf
}

// DecodeValues parses a blob produced by EncodeValues.
// Returns an error on truncated or malformed input.
func DecodeValues(b []byte) ([]Value, error) {
	if len(b) < 2 {
		return nil, fmt.Errorf("DecodeValues: buffer too short (%d bytes)", len(b))
	}
	count := int(binary.LittleEndian.Uint16(b[0:]))
	pos := 2
	vals := make([]Value, 0, count)

	for i := 0; i < count; i++ {
		if pos+2 > len(b) {
			return nil, fmt.Errorf("DecodeValues: truncated at value %d header", i)
		}
		t := ValueType(b[pos])
		isNull := b[pos+1] != 0
		pos += 2

		v := Value{Type: t, IsNull: isNull}
		if isNull {
			vals = append(vals, v)
			continue
		}
		switch t {
		case TypeNull:
			// no body
		case TypeInt:
			if pos+8 > len(b) {
				return nil, fmt.Errorf("DecodeValues: truncated int at value %d", i)
			}
			v.Int = int64(binary.LittleEndian.Uint64(b[pos:]))
			pos += 8
		case TypeFloat:
			if pos+8 > len(b) {
				return nil, fmt.Errorf("DecodeValues: truncated float at value %d", i)
			}
			v.Float = math.Float64frombits(binary.LittleEndian.Uint64(b[pos:]))
			pos += 8
		case TypeText:
			if pos+2 > len(b) {
				return nil, fmt.Errorf("DecodeValues: truncated text length at value %d", i)
			}
			tlen := int(binary.LittleEndian.Uint16(b[pos:]))
			pos += 2
			if pos+tlen > len(b) {
				return nil, fmt.Errorf("DecodeValues: truncated text body at value %d", i)
			}
			v.Text = string(b[pos : pos+tlen])
			pos += tlen
		case TypeBool:
			if pos+1 > len(b) {
				return nil, fmt.Errorf("DecodeValues: truncated bool at value %d", i)
			}
			v.Bool = b[pos] != 0
			pos++
		default:
			return nil, fmt.Errorf("DecodeValues: unknown type %d at value %d", t, i)
		}
		vals = append(vals, v)
	}
	return vals, nil
}
