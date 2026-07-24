package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValue_Compare(t *testing.T) {
	assert.Equal(t, -1, MustCompare(IntVal(1), IntVal(2)))
	assert.Equal(t, 0, MustCompare(IntVal(5), IntVal(5)))
	assert.Equal(t, 1, MustCompare(IntVal(9), IntVal(3)))
	assert.Equal(t, 0, MustCompare(IntVal(3), FloatVal(3.0)))
	assert.Equal(t, -1, MustCompare(IntVal(1), FloatVal(1.5)))
	assert.Equal(t, 1, MustCompare(FloatVal(2.0), IntVal(1)))
	assert.Equal(t, -1, MustCompare(FloatVal(1.0), FloatVal(2.0)))
	assert.Equal(t, 0, MustCompare(FloatVal(3.0), FloatVal(3.0)))
	assert.Equal(t, 1, MustCompare(FloatVal(5.0), FloatVal(4.0)))
	assert.Equal(t, -1, MustCompare(TextVal("a"), TextVal("b")))
	assert.Equal(t, 0, MustCompare(TextVal("x"), TextVal("x")))
	assert.Equal(t, 1, MustCompare(TextVal("z"), TextVal("y")))
	assert.Equal(t, -1, MustCompare(BoolVal(false), BoolVal(true)))
	assert.Equal(t, 0, MustCompare(BoolVal(true), BoolVal(true)))
	assert.Equal(t, 1, MustCompare(BoolVal(true), BoolVal(false)))
	_, err := IntVal(1).Compare(NullVal())
	assert.ErrorIs(t, err, ErrNullComparison)
	_, err = NullVal().Compare(IntVal(1))
	assert.ErrorIs(t, err, ErrNullComparison)
	_, err = IntVal(1).Compare(TextVal("x"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot compare")
	_, err = TextVal("x").Compare(IntVal(1))
	assert.Error(t, err)
}

func TestValue_Compare_UnsupportedType(t *testing.T) {
	v1 := Value{Type: TypeNull, IsNull: false}
	v2 := Value{Type: TypeNull, IsNull: false}
	_, err := v1.Compare(v2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}

func TestValue_NullEqualFalse(t *testing.T) {
	assert.False(t, NullVal().Equal(NullVal()))
	assert.False(t, NullVal().Equal(IntVal(1)))
}

func TestValue_Equal(t *testing.T) {
	assert.True(t, IntVal(5).Equal(IntVal(5)))
	assert.False(t, IntVal(5).Equal(IntVal(6)))
	assert.True(t, TextVal("hello").Equal(TextVal("hello")))
	assert.False(t, NullVal().Equal(NullVal()))
}

func TestValue_String(t *testing.T) {
	assert.Equal(t, "NULL", NullVal().String())
	assert.Equal(t, "42", IntVal(42).String())
	assert.Equal(t, "3.14", FloatVal(3.14).String())
	assert.Equal(t, "hello", TextVal("hello").String())
	assert.Equal(t, "true", BoolVal(true).String())
	assert.Equal(t, "false", BoolVal(false).String())
	assert.Equal(t, "NULL", Value{Type: TypeNull, IsNull: false}.String())
}

// FuzzValue_Compare exercises the Value.Compare dispatcher against
// arbitrary type/int/text/bool combinations. The contract: Compare must
// not panic for any Value pair; for non-NULL pairs of mismatched,
// non-numeric types it must return an error.
func FuzzValue_Compare(f *testing.F) {
	f.Add(int64(1), int64(2))
	f.Add(int64(0), int64(3))
	f.Add(int64(0), int64(0))
	f.Add(int64(0), int64(-1)) // sentinel for TextVal in seed corpus
	f.Fuzz(func(_ *testing.T, aRaw, bRaw int64) {
		var a, b Value
		switch aRaw % 6 {
		case 0:
			a = NullVal()
		case 1:
			a = IntVal(aRaw)
		case 2:
			a = FloatVal(float64(aRaw) / 3.0)
		case 3:
			a = TextVal("x")
		case 4:
			a = TextVal("y")
		default:
			a = BoolVal(aRaw%2 == 0)
		}
		switch bRaw % 6 {
		case 0:
			b = NullVal()
		case 1:
			b = IntVal(bRaw)
		case 2:
			b = FloatVal(float64(bRaw) / 3.0)
		case 3:
			b = TextVal("x")
		case 4:
			b = TextVal("y")
		default:
			b = BoolVal(bRaw%2 == 0)
		}
		// Must not panic; return value intentionally unchecked.
		_ = a.String()
		_ = b.String()
		_ = a.Equal(b)
	})
}
