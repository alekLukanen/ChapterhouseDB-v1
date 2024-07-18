package arrowops

import (
	"github.com/apache/arrow/go/v16/arrow"
)

/*
* Determines if the row at index1 in record1 is less/equal/greater than
* the row at index2 in record2. If the column list is empty
* than all rows will be compared else only the columns in the
* list will be compared.
 */
func CompareRecordRows(record1 arrow.Record, record2 arrow.Record, index1 uint32, index2 uint32, fields ...string) (int, error) {

	return 0, nil
}
