package arrowops

import (
	"slices"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
)

func RecordsEqual(rec1, rec2 arrow.Record, fields ...string) bool {
	for i := 0; i < int(rec1.NumCols()); i++ {
		columnName := rec1.ColumnName(i)
		if !slices.Contains(fields, columnName) {
			continue
		}
		if !array.Equal(rec1.Column(i), rec2.Column(i)) {
			return false
		}
	}
	return true
}
