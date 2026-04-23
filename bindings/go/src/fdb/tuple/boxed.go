package tuple

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/google/uuid"
)

// A boxedType represents the value contained in a Boxed.
type boxedType byte

const (
	// Zero value
	boxedUnknown boxedType = iota

	// A control operator. The data being 1 = start of nested tuple, 2 = end of tuple.
	boxedCtrl

	// A boxed nil value.
	boxedNil

	// A boxed boolean value. The data being 1 = true, 0 = false.
	boxedBool

	// A boxed byte string. The pointer contains the slice start, and the data contains the length.
	boxedBytes
	// A boxed UTF-8 string. The pointer contains the slice start, and the data contains the length.
	boxedString
	// A boxed nested tuple. The pointer contains the slice start, and the data contains the length.
	boxedTuple
	// A boxed fixed-length byte string. The pointer contains the slice start, and the data contains the length.
	boxedFixedLen

	// A boxed int64. The data contains the value.
	boxedInt64
	// A boxed uint64. The data contains the value.
	boxedUint64
	// A boxed float32. The data contains the float32's bits in the lower 32 bits.
	boxedFloat32
	// A boxed float64. The data contains the float64's bits.
	boxedFloat64
	// A boxed UUID. The pointer points to the start of the 16 bytes of the UUID.
	boxedUUID
	// A boxed 12-byte versionstamp. The pointer points to the start of the 12 bytes
	// of the versionstamp.
	boxedVersionstamp
)

// A Boxed is a tagged union representing a tuple value that can be
// unboxed to retrieve the content.
//
// A boxed containing an integer can be cast to any integer type as long
// as it does not overflow. When using Unbox(), int64 is preferred over
// uint64.
type Boxed struct {
	// The type contained in the box.
	bt boxedType
	// The pointer in the box.
	ptr unsafe.Pointer
	// The data in the box, or the length of the data.
	data uint64
}

// A BoxedTuple is a tuple with values represented as Boxed.
type BoxedTuple []Boxed

func newBoxedNil() Boxed {
	return Boxed{bt: boxedNil}
}

func newBoxedBool(b bool) Boxed {
	val := Boxed{bt: boxedBool, data: 0}
	if b {
		val.data = 1
	}
	return val
}

func newBoxedBytes(b []byte) Boxed {
	val := Boxed{bt: boxedBytes, ptr: unsafe.Pointer(unsafe.SliceData(b)), data: uint64(len(b))}
	return val
}

func newBoxedFixedLen(b []byte) Boxed {
	val := Boxed{bt: boxedFixedLen, ptr: unsafe.Pointer(unsafe.SliceData(b)), data: uint64(len(b))}
	return val
}

func newBoxedTuple(b BoxedTuple) Boxed {
	val := Boxed{bt: boxedTuple, ptr: unsafe.Pointer(unsafe.SliceData(b)), data: uint64(len(b))}
	return val
}

func newBoxedUUID(b []byte) Boxed {
	val := Boxed{bt: boxedUUID, ptr: unsafe.Pointer(unsafe.SliceData(b)), data: 16}
	return val
}

func newBoxedVersionstamp(b []byte) Boxed {
	val := Boxed{bt: boxedVersionstamp, ptr: unsafe.Pointer(unsafe.SliceData(b)), data: 12}
	return val
}

func newBoxedString(b []byte) Boxed {
	val := Boxed{bt: boxedString, ptr: unsafe.Pointer(unsafe.SliceData(b)), data: uint64(len(b))}
	return val
}

func newBoxedInt64(b int64) Boxed {
	return Boxed{bt: boxedInt64, data: uint64(b)}
}

func newBoxedUInt64(b uint64) Boxed {
	return Boxed{bt: boxedInt64, data: b}
}

func newBoxedFloat64(b float64) Boxed {
	return Boxed{bt: boxedFloat64, data: math.Float64bits(b)}
}

func newBoxedFloat32(b float32) Boxed {
	return Boxed{bt: boxedFloat32, data: uint64(math.Float32bits(b))}
}

func (b Boxed) assert(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}

// Checks if the given Boxed contains a nil value.
func (b Boxed) IsNil() bool {
	return b.bt == boxedNil
}

// Tries to cast the Boxed to a boolean, and returns the
// value and if the cast succeded.
func (b Boxed) SafeBool() (bool, bool) {
	if b.bt != boxedBool {
		return false, false
	}
	return b.data != 0, true
}

// Tries to cast the Boxed to a boolean and panics if it fails.
func (b Boxed) Bool() bool {
	sb, ok := b.SafeBool()
	b.assert(ok, "cannot cast to bool")
	return sb
}

// Tries to cast the Boxed to a float64, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeFloat64() (float64, bool) {
	if b.bt != boxedFloat64 {
		return 0.0, false
	}
	return math.Float64frombits(b.data), true
}

// Tries to cast the Boxed to a float64 and panics if it fails.
func (b Boxed) Float64() float64 {
	v, ok := b.SafeFloat64()
	b.assert(ok, "cannot cast to float64")
	return v
}

// Tries to cast the Boxed to a float32, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeFloat32() (float32, bool) {
	if b.bt != boxedFloat32 {
		return 0.0, false
	}
	return math.Float32frombits(uint32(b.data)), true
}

// Tries to cast the Boxed to a float32 and panics if it fails.
func (b Boxed) Float32() float32 {
	v, ok := b.SafeFloat32()
	b.assert(ok, "cannot cast to float32")
	return v
}

// Tries to cast the Boxed to a BoxedTuple, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeTuple() (BoxedTuple, bool) {
	if b.bt != boxedTuple {
		return nil, false
	}
	return unsafe.Slice((*Boxed)(b.ptr), int(b.data)), true
}

// Tries to cast the Boxed to a Tuple and panics if it fails.
func (b Boxed) Tuple() BoxedTuple {
	v, ok := b.SafeTuple()
	b.assert(ok, "cannot cast to BoxedTuple")
	return v
}

// Tries to cast the Boxed to a byte slice, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeBytes() ([]byte, bool) {
	if b.bt != boxedBytes {
		return nil, false
	}
	return unsafe.Slice((*byte)(b.ptr), int(b.data)), true
}

// Tries to cast the Boxed to a byte slice and panics if it fails.
func (b Boxed) Bytes() []byte {
	v, ok := b.SafeBytes()
	b.assert(ok, "cannot cast to []byte")
	return v
}

// Tries to cast the Boxed to a string, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeString() (string, bool) {
	if b.bt != boxedString {
		return "", false
	}
	return unsafe.String((*byte)(b.ptr), int(b.data)), true
}

// Tries to cast the Boxed to a string and panics if it fails.
//
// This is not named String() to not conflict with the function used to
// cast to a string for printing.
func (b Boxed) AsString() string {
	v, ok := b.SafeString()
	b.assert(ok, "cannot cast to string")
	return v
}

// Tries to cast the Boxed to a FixedLen, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeFixedLen() (FixedLen, bool) {
	if b.bt != boxedFixedLen {
		return nil, false
	}
	return unsafe.Slice((*byte)(b.ptr), int(b.data)), true
}

// Tries to cast the Boxed to a FixedLen and panics if it fails.
func (b Boxed) FixedLen() FixedLen {
	v, ok := b.SafeFixedLen()
	b.assert(ok, "cannot cast to FixedLen")
	return v
}

// Tries to cast the Boxed to a UUID, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeUUID() (uuid.UUID, bool) {
	if b.bt != boxedUUID {
		return uuid.UUID{}, false
	}
	return uuid.UUID(unsafe.Slice((*byte)(b.ptr), 16)), true
}

// Tries to cast the Boxed to a UUID and panics if it fails.
func (b Boxed) UUID() uuid.UUID {
	v, ok := b.SafeUUID()
	b.assert(ok, "cannot cast to UUID")
	return v
}

// Tries to cast the Boxed to a int64, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeInt64() (int64, bool) {
	switch b.bt {
	case boxedInt64:
		return int64(b.data), true
	case boxedUint64:
		if b.data < 0x8000_0000_0000_0000 {
			return int64(b.data), true
		}
		return 0, false
	default:
		return 0, false
	}
}

// Tries to cast the Boxed to a int64 and panics if it fails.
func (b Boxed) Int64() int64 {
	v, ok := b.SafeInt64()
	b.assert(ok, "cannot cast to int64")
	return v
}

// Tries to cast the Boxed to a Versionstamp, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeVersionstamp() (Versionstamp, bool) {
	if b.bt != boxedVersionstamp {
		return Versionstamp{}, false
	}
	slice := unsafe.Slice((*byte)(b.ptr), 12)
	out := Versionstamp{}
	out.TransactionVersion = [10]byte(slice[0:10])
	out.UserVersion = binary.BigEndian.Uint16(slice[10:12])
	return out, true
}

// Tries to cast the Boxed to a Versionstamp and panics if it fails.
func (b Boxed) Versionstamp() Versionstamp {
	v, ok := b.SafeVersionstamp()
	b.assert(ok, "cannot cast to Versionstamp")
	return v
}

// Tries to cast the Boxed to a uint64, and returns the
// cast value and if the cast succeded.
func (b Boxed) SafeUint64() (uint64, bool) {
	switch b.bt {
	case boxedUint64:
		return b.data, true
	case boxedInt64:
		if b.data < 0x8000_0000_0000_0000 {
			return b.data, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// Tries to cast the Boxed to a uint64 and panics if it fails.
func (b Boxed) Uint64() uint64 {
	v, ok := b.SafeUint64()
	b.assert(ok, "cannot cast to uint64")
	return v
}

// Unboxes the content of the Boxed to its value.
func (b Boxed) Unbox() any {
	switch b.bt {
	case boxedNil:
		return nil
	case boxedString:
		return b.AsString()
	case boxedBytes:
		return b.Bytes()
	case boxedFixedLen:
		return b.FixedLen()
	case boxedFloat32:
		return b.Float32()
	case boxedFloat64:
		return b.Float64()
	case boxedTuple:
		return b.Tuple()
	case boxedInt64:
		return b.Int64()
	case boxedUint64:
		return b.Uint64()
	case boxedBool:
		return b.Bool()
	case boxedUUID:
		return b.UUID()
	case boxedVersionstamp:
		return b.Versionstamp()
	default:
		panic("unknown type")
	}
}

// Converts the BoxedTuple to a normal Tuple.
func (bt BoxedTuple) ToTuple() Tuple {
	out := make(Tuple, len(bt))
	for i, entry := range bt {
		if entry.bt == boxedTuple {
			out[i] = entry.Tuple().ToTuple()
		} else {
			out[i] = entry.Unbox()
		}
	}
	return out
}
