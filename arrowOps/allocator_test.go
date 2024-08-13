package arrowops

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
)

func TestRetainFollowedByRelease(t *testing.T) {

	mem := memory.NewGoAllocator()
	arr := ZeroUint32Array(mem, 10)

	arr.Retain()
	arr.Release()

	if arr.Len() != 10 {
		t.Fatal("expected the array to still exist")
	}

}

func TestRelease(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			expectedErrStr := "runtime error: invalid memory address or nil pointer dereference"
			if errVal, ok := err.(error); ok && expectedErrStr != errVal.Error() {
				t.Log("panic occurred as expected:", err)
			} else {
				t.Logf("panic occured but was the wrong error: %s", err)
			}
		} else {
			t.Fatal("expected a panic to occur")
		}
	}()

	mem := memory.NewGoAllocator()
	arr := ZeroUint32Array(mem, 10)

	arr.Release()

	t.Logf("array value: %d", arr.Len())

}
