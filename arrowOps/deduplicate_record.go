package arrowops

import (
	"fmt"

	"github.com/alekLukanen/errs"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

/*
* Takes record and deduplicates the rows based on the subset of columns provided.
* The rows are not garanteed to be in any particular order. All columns from the
* input record will be returned in the result record.
*/
func DeduplicateRecord(mem *memory.GoAllocator, record arrow.Record, columns []string, presortedByColumnsNames bool) (arrow.Record, error) {
  record.Retain()
  defer record.Release()

  if len(columns) == 0 {
    return nil, errs.NewStackError(ErrColumnNamesRequired)
  }

  var sortedRecord arrow.Record
  if !presortedByColumnsNames {
    r, err := SortRecord(mem, record, columns)
    if err != nil {
      return nil, errs.Wrap(err, fmt.Errorf("failed to sort record by columns: %v", columns))
    }
    defer r.Release()
    sortedRecord = r
  } else {
    sortedRecord = record
  }

  // find the first row of each group of duplicates
  rowIndices := make([]uint32, 0, record.NumRows()/2)
  rowIndices = append(rowIndices, 0)
  for i := 1; i < int(record.NumRows()); i++ {

    cmpRow, err := CompareRecordRows(sortedRecord, sortedRecord, i, i-1, columns...)
    if err != nil {
      return nil, errs.Wrap(err, fmt.Errorf("failed to compare rows %d and %d", i-1, i))
    }

    if cmpRow > 0 {
      rowIndices = append(rowIndices, uint32(i))
    }

  }

  // take the rows from the sorted record
  indiciesBuilder := array.NewUint32Builder(mem)
  indiciesBuilder.AppendValues(rowIndices, nil)
  defer indiciesBuilder.Release()

  indicies := indiciesBuilder.NewUint32Array()
  indiciesBuilder.Release()
  defer indicies.Release()

  deduplicatedRecord, err := TakeRecord(mem, sortedRecord, indicies)
  if err != nil {
    return nil, errs.Wrap(err, fmt.Errorf("failed to take %d rows from sorted record", indicies.Len()))
  }

  return deduplicatedRecord, nil

}
