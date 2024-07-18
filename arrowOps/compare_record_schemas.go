package arrowops

import (
	"github.com/apache/arrow/go/v16/arrow"
)

func RecordSchemasEqual(record1 arrow.Record, record2 arrow.Record, fields ...string) bool {

	record1Schema := record1.Schema()
	record2Schema := record2.Schema()
	if len(fields) == 0 {
		return record1Schema.Equal(record2Schema)
	} else {
		return SchemaSubSetEqual(record1Schema, record2Schema, fields...) &&
			SchemaSubSetEqual(record2Schema, record1Schema, fields...)
	}

}

func SchemaSubSetEqual(schema1 *arrow.Schema, schema2 *arrow.Schema, fields ...string) bool {

	for i := 0; i < schema1.NumFields(); i++ {
		if i >= schema2.NumFields() {
			return false
		}

		record1Field := schema1.Field(i)
		record2Field := schema2.Field(i)
		includeField := false
		for _, f := range fields {
			if record1Field.Name == f {
				includeField = true
				break
			}
		}
		if !includeField {
			continue
		}
		if !record1Field.Equal(record2Field) {
			return false
		}

	}

	return true
}
