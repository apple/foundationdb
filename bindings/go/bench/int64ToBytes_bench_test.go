package bench

import (
	"bytes"
	"encoding/binary"
	"testing"
)

var result []byte

func Benchmark_Int64ToBytesBuffer(b *testing.B) {
	b.ReportAllocs()

	var r []byte
	for n := 0; n < b.N; n++ {
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.LittleEndian, int64(n)); err != nil {
			b.Error("failed to write int64:", err)
		}

		b.SetBytes(int64(buf.Len()))
		r = buf.Bytes()
	}

	result = r
}

func Benchmark_Int64ToBytesPut(b *testing.B) {
	b.ReportAllocs()

	var r []byte
	for n := 0; n < b.N; n++ {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(n))

		b.SetBytes(int64(len(buf)))
		r = buf
	}

	result = r
}
