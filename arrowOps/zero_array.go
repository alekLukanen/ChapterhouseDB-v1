package arrowops

import (
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func ZeroUint32Array(mem *memory.GoAllocator, length int) *array.Uint32 {
	b := array.NewUint32Builder(mem)
	b.Resize(int(length))
	b.AppendValues(make([]uint32, length), nil)
	return b.NewUint32Array()
}
