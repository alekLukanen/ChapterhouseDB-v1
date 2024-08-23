package arrowops

import (
	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
)

func CastArraysToBaseDataType[T arrow.Array](arrays ...arrow.Array) ([]T, error) {
	TArrays := make([]T, len(arrays))
	for i, arr := range arrays {
		TArr, ok := arr.(T)
		if !ok {
			return nil, errs.NewStackError(ErrUnsupportedDataType)
		}
		TArrays[i] = TArr
	}
	return TArrays, nil
}
