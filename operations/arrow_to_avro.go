package operations

import (
	"encoding/json"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/linkedin/goavro/v2"
)

/*
* Convert an arrow record to an avro array of serialized rows
 */
func ArrowToAvro(tuples arrow.Record) ([][]byte, error) {
	avroSchema, err := ArrowToAvroSchema(tuples.Schema())
	if err != nil {
		return nil, err
	}

	arrowSchema := tuples.Schema()
	columnNames := make([]string, arrowSchema.NumFields())
	for i, field := range arrowSchema.Fields() {
		columnNames[i] = field.Name
	}

	columnArrays := tuples.Columns()
	data := make([][]byte, tuples.NumRows())

	dataMap := make(map[string]interface{})
	for i := int64(0); i < tuples.NumRows(); i++ {
		for colIdx, col := range columnArrays {
			val, err := ArrowArrayValueToAvroValue(col, int(i))
			if err != nil {
				return nil, err
			}

			dataMap[columnNames[colIdx]] = val
		}

		msgData, err := avroSchema.BinaryFromNative(nil, dataMap)
		if err != nil {
			return nil, err
		}

		data[i] = msgData
		clear(dataMap)
	}

	return data, nil
}

func ArrowArrayValueToAvroValue(arr arrow.Array, idx int) (interface{}, error) {
	switch arr.DataType().ID() {
	case arrow.BOOL:
		return arr.(*array.Boolean).Value(idx), nil
	case arrow.INT8:
		return arr.(*array.Int8).Value(idx), nil
	case arrow.INT16:
		return arr.(*array.Int16).Value(idx), nil
	case arrow.INT32:
		return arr.(*array.Int32).Value(idx), nil
	case arrow.INT64:
		return arr.(*array.Int64).Value(idx), nil
	case arrow.UINT8:
		return arr.(*array.Uint8).Value(idx), nil
	case arrow.UINT16:
		return arr.(*array.Uint16).Value(idx), nil
	case arrow.UINT32:
		return arr.(*array.Uint32).Value(idx), nil
	case arrow.UINT64:
		return arr.(*array.Uint64).Value(idx), nil
	case arrow.FLOAT16:
		return arr.(*array.Float16).Value(idx), nil
	case arrow.FLOAT32:
		return arr.(*array.Float32).Value(idx), nil
	case arrow.FLOAT64:
		return arr.(*array.Float64).Value(idx), nil
	case arrow.STRING:
		return arr.(*array.String).Value(idx), nil
	case arrow.BINARY:
		return arr.(*array.Binary).Value(idx), nil
	case arrow.DATE32:
		return arr.(*array.Date32).Value(idx), nil
	case arrow.DATE64:
		return arr.(*array.Date64).Value(idx), nil
	case arrow.TIMESTAMP:
		return arr.(*array.Timestamp).Value(idx), nil
	case arrow.TIME32:
		return arr.(*array.Time32).Value(idx), nil
	case arrow.TIME64:
		return arr.(*array.Time64).Value(idx), nil
	case arrow.DURATION:
		return arr.(*array.Duration).Value(idx), nil
	default:
		return nil, ErrUnsupportedArrowToAvroTypeConversion
	}
}

func ArrowToAvroSchema(arrowSchema *arrow.Schema) (*goavro.Codec, error) {
	type avroField struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	type avroSchemaTemplate struct {
		Type   string      `json:"type"`
		Name   string      `json:"name"`
		Fields []avroField `json:"fields"`
	}

	avroSchema := avroSchemaTemplate{
		Type:   "record",
		Name:   "simpleRecord",
		Fields: make([]avroField, 0),
	}

	for _, field := range arrowSchema.Fields() {
		avroType, err := ArrowToAvroType(field.Type)
		if err != nil {
			return nil, err
		}
		avroSchema.Fields = append(avroSchema.Fields, avroField{
			Name: field.Name,
			Type: avroType,
		})
	}

	codecData, err := json.Marshal(avroSchema)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(string(codecData))
	if err != nil {
		return nil, err
	}

	return codec, nil
}

func ArrowToAvroType(arrowType arrow.DataType) (string, error) {
	switch arrowType.ID() {
	case arrow.BOOL:
		return "boolean", nil
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return "long", nil
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return "long", nil
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return "double", nil
	case arrow.STRING:
		return "string", nil
	case arrow.BINARY:
		return "bytes", nil
	case arrow.DATE32, arrow.DATE64:
		return "long", nil
	case arrow.TIMESTAMP:
		return "long", nil
	case arrow.TIME32, arrow.TIME64:
		return "long", nil
	case arrow.DURATION:
		return "long", nil
	default:
		return "", ErrUnsupportedArrowToAvroTypeConversion
	}
}
