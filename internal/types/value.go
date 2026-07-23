package types

import (
	"fmt"
	"math"
)

// ValueType enumerates the possible types of a Value.
type ValueType int

const (
	// TypeNull represents a SQL NULL value.
	TypeNull  ValueType = 0
	// TypeInt represents a 64-bit signed integer.
	TypeInt   ValueType = 1
	// TypeFloat represents a 64-bit IEEE 754 float.
	TypeFloat ValueType = 2
	// TypeText represents a UTF-8 string.
	TypeText  ValueType = 3
	// TypeBool represents a boolean value.
	TypeBool  ValueType = 4
)

// Value is a dynamically-typed cell that can hold an int, float, text, bool, or null.
type Value struct {
	Type   ValueType
	IsNull bool
	Int    int64
	Float  float64
	Text   string
	Bool   bool
}

// NullVal returns a NULL Value.
func NullVal() Value {
	return Value{Type: TypeNull, IsNull: true}
}

// IntVal returns a Value holding an int64.
func IntVal(n int64) Value {
	return Value{Type: TypeInt, Int: n}
}

// FloatVal returns a Value holding a float64.
func FloatVal(f float64) Value {
	return Value{Type: TypeFloat, Float: f}
}

// TextVal returns a Value holding a string.
func TextVal(s string) Value {
	return Value{Type: TypeText, Text: s}
}

// BoolVal returns a Value holding a bool.
func BoolVal(b bool) Value {
	return Value{Type: TypeBool, Bool: b}
}

// Compare returns -1/0/1 and an error if either operand is NULL or types mismatch.
func (v Value) Compare(other Value) (int, error) {
	if v.IsNull || other.IsNull {
		return 0, ErrNullComparison
	}
	// Coerce int/float
	if (v.Type == TypeInt || v.Type == TypeFloat) && (other.Type == TypeInt || other.Type == TypeFloat) {
		var a, b float64
		if v.Type == TypeInt {
			a = float64(v.Int)
		} else {
			a = v.Float
		}
		if other.Type == TypeInt {
			b = float64(other.Int)
		} else {
			b = other.Float
		}
		if math.Abs(a-b) < 1e-10 {
			return 0, nil
		}
		if a < b {
			return -1, nil
		}
		return 1, nil
	}
	if v.Type != other.Type {
		return 0, fmt.Errorf("cannot compare %v and %v", v.Type, other.Type)
	}
	switch v.Type {
	case TypeText:
		if v.Text == other.Text {
			return 0, nil
		}
		if v.Text < other.Text {
			return -1, nil
		}
		return 1, nil
	case TypeBool:
		if v.Bool == other.Bool {
			return 0, nil
		}
		if !v.Bool && other.Bool {
			return -1, nil
		}
		return 1, nil
	}
	return 0, fmt.Errorf("unsupported type %v", v.Type)
}

// MustCompare panics on comparison error (e.g. NULL comparison).
func MustCompare(a, b Value) int {
	r, err := a.Compare(b)
	if err != nil {
		panic(err)
	}
	return r
}

// Equal returns true if the values are equal (comparison errors return false).
func (v Value) Equal(other Value) bool {
	r, err := v.Compare(other)
	if err != nil {
		return false
	}
	return r == 0
}

func (v Value) String() string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case TypeInt:
		return fmt.Sprintf("%d", v.Int)
	case TypeFloat:
		return fmt.Sprintf("%g", v.Float)
	case TypeText:
		return v.Text
	case TypeBool:
		if v.Bool {
			return "true"
		}
		return "false"
	}
	return "NULL"
}
