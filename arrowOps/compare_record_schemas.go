package arrowops

import (
	"github.com/apache/arrow/go/v17/arrow"
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

	for _, field := range fields {
		rec1Fields, rec1HasFields := schema1.FieldsByName(field)
		rec2Fields, rec2HasFields := schema2.FieldsByName(field)
		if !rec1HasFields || !rec2HasFields {
			return false
		}
		for _, rec1Field := range rec1Fields {
			for _, rec2Field := range rec2Fields {
				if !rec1Field.Equal(rec2Field) {
					return false
				}
			}
		}

	}

	return true
}
