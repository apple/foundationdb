package tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

// The reason we do not pass a streamingUnpacker is that the
// unpacker may escape to the heap, as all parameters to
// closures are assumed to escape to the heap.
type unpackerState struct {
	buf     []byte
	ptr     int
	nesting int
}

type unpackFunc func(unpackerState) (Boxed, int, error)

type unpackFuncTab [256]unpackFunc

func createByteDecoder(bt boxedType) unpackFunc {
	return func(d unpackerState) (Boxed, int, error) {
		end, err := findEnd(d.buf, d.ptr+1)
		if err != nil {
			return Boxed{}, 0, err
		}
		cptr := d.ptr + 1
		buf := d.buf[cptr : d.ptr+1+end.length]
		if end.decoded != end.length {
			buf = bytes.Replace(buf, []byte{0x00, 0xFF}, []byte{0x00}, end.length-end.decoded)
		}
		return Boxed{bt: bt, ptr: unsafe.Pointer(unsafe.SliceData(buf)), data: uint64(len(buf))}, 1 + 1 + end.length, nil
	}
}

func createIntDecoder(length int, negative bool) unpackFunc {
	return func(d unpackerState) (Boxed, int, error) {
		if len(d.buf) < (d.ptr + length + 1) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for negative int64 (len %d)", length)
		}
		tmp := make([]byte, 8)
		if negative {
			for i := range tmp {
				tmp[i] = 0xff
			}
		}
		copy(tmp[8-length:], d.buf[d.ptr+1:d.ptr+1+length])
		if negative {
			return newBoxedInt64(-int64(^binary.BigEndian.Uint64(tmp))), 1 + length, nil
		} else {
			return newBoxedInt64(int64(binary.BigEndian.Uint64(tmp))), 1 + length, nil
		}
	}
}

func adjustFloatBytesDecode(b []byte) {
	if b[0]&0x80 == 0x00 {
		for i := 0; i < len(b); i++ {
			b[i] = b[i] ^ 0xff
		}
	} else {
		b[0] = b[0] ^ 0x80
	}
}

var unpackFunctab unpackFuncTab = unpackFuncTab{
	nilCode: func(d unpackerState) (Boxed, int, error) {
		if d.nesting > 0 {
			if d.ptr+1 < len(d.buf) &&
				d.buf[d.ptr+1] == 0xff {
				return newBoxedNil(), 2, nil
			} else {
				return Boxed{bt: boxedCtrl, data: 2}, 1, nil
			}
		} else {
			return newBoxedNil(), 1, nil
		}
	},

	bytesCode:  createByteDecoder(boxedBytes),
	stringCode: createByteDecoder(boxedString),

	trueCode: func(ds unpackerState) (Boxed, int, error) {
		return newBoxedBool(true), 1, nil
	},
	falseCode: func(ds unpackerState) (Boxed, int, error) {
		return newBoxedBool(false), 1, nil
	},

	floatCode: func(d unpackerState) (Boxed, int, error) {
		if len(d.buf) < (d.ptr + 4 + 1) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for float32")
		}
		bytes := make([]byte, 4)
		copy(bytes, d.buf[d.ptr+1:d.ptr+5])
		adjustFloatBytesDecode(bytes)
		return newBoxedFloat32(math.Float32frombits(binary.BigEndian.Uint32(bytes))), 5, nil
	},

	doubleCode: func(d unpackerState) (Boxed, int, error) {
		if len(d.buf) < (d.ptr + 8 + 1) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for float64")
		}
		bytes := make([]byte, 8)
		copy(bytes, d.buf[d.ptr+1:d.ptr+9])
		adjustFloatBytesDecode(bytes)
		return newBoxedFloat64(math.Float64frombits(binary.BigEndian.Uint64(bytes))), 9, nil
	},

	uuidCode: func(d unpackerState) (Boxed, int, error) {
		if len(d.buf) < (d.ptr + 16 + 1) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for UUID")
		}
		u := newBoxedUUID(d.buf[d.ptr+1 : d.ptr+17])
		return u, 17, nil
	},

	versionstampCode: func(d unpackerState) (Boxed, int, error) {
		if len(d.buf) < (d.ptr + 12 + 1) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for Versionstamp")
		}
		vs := newBoxedVersionstamp(d.buf[d.ptr+1 : d.ptr+13])
		return vs, 13, nil
	},

	0x0c: func(d unpackerState) (Boxed, int, error) {
		if len(d.buf) < (d.ptr + 8 + 1) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for negative int64 (len 8)")
		}
		buf := make([]byte, 8)
		copy(buf, d.buf[d.ptr+1:d.ptr+9])
		inv := ^binary.BigEndian.Uint64(buf)
		if inv < ^uint64(0x8000_0000_0000_0000) {
			return Boxed{}, 0, fmt.Errorf("bigint not supported")
		} else {
			return newBoxedInt64(-int64(inv)), 9, nil
		}
	},
	0x0d: createIntDecoder(7, true),
	0x0e: createIntDecoder(6, true),
	0x0f: createIntDecoder(5, true),
	0x10: createIntDecoder(4, true),
	0x11: createIntDecoder(3, true),
	0x12: createIntDecoder(2, true),
	0x13: createIntDecoder(1, true),
	// zero positive integer
	0x14: func(d unpackerState) (Boxed, int, error) {
		return newBoxedInt64(0), 1, nil
	},
	0x15: createIntDecoder(1, false),
	0x16: createIntDecoder(2, false),
	0x17: createIntDecoder(3, false),
	0x18: createIntDecoder(4, false),
	0x19: createIntDecoder(5, false),
	0x1a: createIntDecoder(6, false),
	0x1b: createIntDecoder(7, false),
	0x1c: func(d unpackerState) (Boxed, int, error) {
		if len(d.buf) < (d.ptr + 8 + 1) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for uint64 (len 8)")
		}
		buf := make([]byte, 8)
		copy(buf, d.buf[d.ptr+1:d.ptr+9])
		return newBoxedUInt64(binary.BigEndian.Uint64(buf)), 9, nil
	},

	fixedLengthCode: func(d unpackerState) (Boxed, int, error) {
		if d.ptr+1 >= len(d.buf) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for FixedLen")
		}
		dataLength := int(d.buf[d.ptr+1])
		if len(d.buf) < d.ptr+2+dataLength {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for FixedLen")
		}
		fl := newBoxedFixedLen(d.buf[d.ptr+2 : d.ptr+2+dataLength])
		return fl, 2 + dataLength, nil
	},
	fixedLengthCode + 1: func(d unpackerState) (Boxed, int, error) {
		if d.ptr+2 >= len(d.buf) {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for FixedLen")
		}
		dataLength := int(binary.BigEndian.Uint16(d.buf[d.ptr+1 : d.ptr+3]))
		if len(d.buf) < d.ptr+3+dataLength {
			return Boxed{}, 0, fmt.Errorf("insufficient bytes for FixedLen")
		}
		fl := newBoxedFixedLen(d.buf[d.ptr+3 : d.ptr+3+dataLength])
		return fl, 3 + dataLength, nil
	},

	nestedCode: func(d unpackerState) (Boxed, int, error) {
		return Boxed{bt: boxedCtrl, data: 1}, 1, nil
	},
}

type streamingUnpacker struct {
	buf     []byte
	ptr     int
	nesting int
}

type endInfo struct {
	length  int
	decoded int
}

//go:nosplit
func findEnd(buf []byte, start int) (ei endInfo, err error) {
	ptr := unsafe.Pointer(&buf[start])
	rem := len(buf) - start
	for rem > 0 {
		if *(*byte)(ptr) == 0 {
			ptr = unsafe.Add(ptr, 1)
			rem--
			if rem > 0 && *(*byte)(ptr) == 255 {
				ei.length++
			} else {
				return
			}
		}
		ei.length++
		ei.decoded++
		ptr = unsafe.Add(ptr, 1)
		rem--
	}
	err = fmt.Errorf("tuple byte string is not terminated")
	return
}

func (su *streamingUnpacker) Next() (Boxed, error) {
	if len(su.buf) <= su.ptr {
		return Boxed{}, nil
	}
	code := su.buf[su.ptr]
	f := unpackFunctab[code]
	if f == nil {
		return Boxed{}, fmt.Errorf("unknown tuple type code %2x", code)
	}
	bv, n, err := f(unpackerState{buf: su.buf, ptr: su.ptr, nesting: su.nesting})
	if err != nil {
		return Boxed{}, err
	}
	su.ptr += n
	if bv.bt == boxedCtrl {
		switch bv.data {
		case 1:
			su.nesting++
		case 2:
			su.nesting--
		}
	}
	return bv, nil
}

func (su *streamingUnpacker) HasMore() bool {
	return len(su.buf) > su.ptr
}

func unpackV2Internal(up *streamingUnpacker) (BoxedTuple, error) {
	// This allows implementing "perfect" allocation of the tuple
	// even though we don't know its length by:
	// 1. Allocating the output slice on the stack with a hard to reach capacity
	// 2. Appending to the output slice only without allowing it to escape
	// 3. Copying the slice to a new heap-allocated slice
	//
	// If we did not copy the tuple in the end, the allocation would
	// escape immediately on step 1, and we would allocate the entire
	// 64x 24B slice, even if we don't come anywhere close.
	//
	// Starting with an empty slice would result in append gradually
	// expanding the slice, which would result in multiple slices.
	//
	// This may spill to heap if we exceed 64 elements. This is very
	// unlikely.
	n := make(BoxedTuple, 0, 64)

	// unpacking loop
loop:
	for up.HasMore() {
		next, err := up.Next()
		if err != nil {
			return nil, err
		}
		if next.bt == boxedCtrl {
			switch next.data {
			case 1:
				v, err := unpackV2Internal(up)
				if err != nil {
					return nil, err
				}
				n = append(n, newBoxedTuple(v))
			case 2:
				break loop
			default:
				panic("unknown action")
			}
		} else {
			n = append(n, next)
		}
	}
	c := make(BoxedTuple, len(n))
	copy(c, n)
	return c, nil
}

// UnpackToBoxed unpacks a byte slice into a FoundationDB
// tuple using BoxedTuple.
//
// The provided byte slice must not be modified while the
// tuple is being used, as zero-copy decoding is attempted.
func UnpackToBoxed(b []byte) (BoxedTuple, error) {
	if len(b) == 0 {
		return BoxedTuple{}, nil
	}
	unpacker := &streamingUnpacker{buf: b}
	return unpackV2Internal(unpacker)
}

// Unpack unpacks a byte slice into a FoundationDB tuple.
//
// The provided byte slice must not be modified while the
// tuple is being used, as zero-copy decoding is attempted.
func UnpackV2(b []byte) (Tuple, error) {
	if len(b) == 0 {
		return Tuple{}, nil
	}
	unpacker := &streamingUnpacker{buf: b}
	u, err := unpackV2Internal(unpacker)
	if err != nil {
		return nil, err
	}
	return u.ToTuple(), nil
}
