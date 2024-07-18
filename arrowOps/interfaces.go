package arrowops

import (
	"cmp"
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
