package arrowops

import (
  "fmt"

  "github.com/alekLukanen/errs"

  "github.com/apache/arrow/go/v17/arrow"
  "github.com/apache/arrow/go/v17/arrow/array"
)

func TakeColumns(rec arrow.Record, columnNames []string) (arrow.Record, error) {
    var selectedCols []arrow.Array
    var selectedFields []arrow.Field

    for _, colName := range columnNames {
        colIndex := rec.Schema().FieldIndices(colName)
        if len(colIndex) == 0 {
          return nil, errs.NewStackError(fmt.Errorf("%w| column name: %s", ErrColumnNotFound, colName))
        }
        for _, colIndex := range colIndex {
          selectedCols = append(selectedCols, rec.Column(colIndex))
          selectedFields = append(selectedFields, rec.Schema().Field(colIndex))
        }
    }

    newSchema := arrow.NewSchema(selectedFields, nil)
    newRecord := array.NewRecord(newSchema, selectedCols, rec.NumRows())

    return newRecord, nil
}
