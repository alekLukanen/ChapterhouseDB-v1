package arrowops

import (
	"cmp"

	"github.com/apache/arrow/go/v17/arrow"
)

type orderableArray[E cmp.Ordered] interface {
	IsNull(i int) bool
	Value(i int) E
	Len() int
}

type valueArray[T comparable] interface {
	IsNull(i int) bool
	Value(i int) T
	Len() int
}

type arrayBuilder[T comparable] interface {
	Reserve(n int)
	Append(v T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}
