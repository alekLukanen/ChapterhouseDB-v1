package arrowops

import (
	"github.com/apache/arrow/go/v16/arrow"
)

func RecordsEqual(a, b arrow.Record) bool {
	if !a.Schema().Equal(b.Schema()) {
		return false
	}

	for i := 0; i < int(a.NumCols()); i++ {
		if !ArraysEqual(a.Column(int(i)), b.Column(int(i))) {
			return false
		}
	}

	return true
}

func ArraysEqual(a, b arrow.Array) bool {
	if a.DataType().ID() != b.DataType().ID() {
		return false
	}

	return true
}
