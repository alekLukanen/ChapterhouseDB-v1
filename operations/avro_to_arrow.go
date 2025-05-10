package operations

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/float16"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/linkedin/goavro/v2"

	"github.com/alekLukanen/ChapterhouseDB-v1/elements"
)

func AvroToArrow(allocator *memory.GoAllocator, table *elements.Table, subscriptionSourceName string, tuples []string) (arrow.Record, error) {
	avroCodec, err := TablePartitionAvroSchema(table, subscriptionSourceName)
	if err != nil {
		return nil, err
	}

	arrowSchema, err := TablePartitionArrowSchema(table, subscriptionSourceName)
	if err != nil {
		return nil, err
	}

	recordBuilder := array.NewRecordBuilder(allocator, arrowSchema)
	defer recordBuilder.Release()

	for _, tuple := range tuples {

		mapData, _, err := avroCodec.NativeFromBinary([]byte(tuple))
		if err != nil {
			return nil, err
		}

		castMapData, ok := mapData.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to cast avro data to map")
		}

		err = CastAvroTypesToArrowTypes(arrowSchema, castMapData)
		if err != nil {
			return nil, err
		}

		err = AppendArrowRow(arrowSchema, recordBuilder, castMapData)
		if err != nil {
			return nil, err
		}

	}

	record := recordBuilder.NewRecord()

	return record, nil
}

func CastAvroTypesToArrowTypes(schema *arrow.Schema, avroData map[string]interface{}) error {
	for _, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.BOOL:
			avroData[field.Name] = avroData[field.Name].(bool)
		case arrow.INT8:
			avroData[field.Name] = int8(avroData[field.Name].(int64))
		case arrow.INT16:
			avroData[field.Name] = int16(avroData[field.Name].(int64))
		case arrow.INT32:
			avroData[field.Name] = int32(avroData[field.Name].(int64))
		case arrow.INT64:
			// do nothing
		case arrow.UINT8:
			avroData[field.Name] = uint8(avroData[field.Name].(int64))
		case arrow.UINT16:
			avroData[field.Name] = uint16(avroData[field.Name].(int64))
		case arrow.UINT32:
			avroData[field.Name] = uint32(avroData[field.Name].(int64))
		case arrow.UINT64:
			avroData[field.Name] = uint64(avroData[field.Name].(int64))
		case arrow.FLOAT16:
			avroData[field.Name] = float16.New(avroData[field.Name].(float32))
		case arrow.FLOAT32:
			avroData[field.Name] = float32(avroData[field.Name].(float64))
		case arrow.FLOAT64:
			// do nothing
		case arrow.STRING:
			avroData[field.Name] = avroData[field.Name].(string)
		case arrow.BINARY:
			avroData[field.Name] = avroData[field.Name].([]byte)
		default:
			return fmt.Errorf("unsupported data type for arrow cast: %s", field.Type.ID())
		}
	}
	return nil
}

func AppendArrowRow(schema *arrow.Schema, recordBuilder *array.RecordBuilder, avroData map[string]interface{}) error {

	for idx, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.BOOL:
			recordBuilder.Field(idx).(*array.BooleanBuilder).Append(avroData[field.Name].(bool))
		case arrow.INT8:
			recordBuilder.Field(idx).(*array.Int8Builder).Append(avroData[field.Name].(int8))
		case arrow.INT16:
			recordBuilder.Field(idx).(*array.Int16Builder).Append(avroData[field.Name].(int16))
		case arrow.INT32:
			recordBuilder.Field(idx).(*array.Int32Builder).Append(avroData[field.Name].(int32))
		case arrow.INT64:
			recordBuilder.Field(idx).(*array.Int64Builder).Append(avroData[field.Name].(int64))
		case arrow.UINT8:
			recordBuilder.Field(idx).(*array.Uint8Builder).Append(avroData[field.Name].(uint8))
		case arrow.UINT16:
			recordBuilder.Field(idx).(*array.Uint16Builder).Append(avroData[field.Name].(uint16))
		case arrow.UINT32:
			recordBuilder.Field(idx).(*array.Uint32Builder).Append(avroData[field.Name].(uint32))
		case arrow.UINT64:
			recordBuilder.Field(idx).(*array.Uint64Builder).Append(avroData[field.Name].(uint64))
		case arrow.FLOAT16:
			recordBuilder.Field(idx).(*array.Float16Builder).Append(avroData[field.Name].(float16.Num))
		case arrow.FLOAT32:
			recordBuilder.Field(idx).(*array.Float32Builder).Append(avroData[field.Name].(float32))
		case arrow.FLOAT64:
			recordBuilder.Field(idx).(*array.Float64Builder).Append(avroData[field.Name].(float64))
		case arrow.STRING:
			recordBuilder.Field(idx).(*array.StringBuilder).Append(avroData[field.Name].(string))
		case arrow.BINARY:
			recordBuilder.Field(idx).(*array.BinaryBuilder).Append(avroData[field.Name].([]byte))
		default:
			return fmt.Errorf("unsupported data type for arrow append: %s", field.Type.ID())
		}

	}

	return nil
}

func TablePartitionArrowSchema(table *elements.Table, subscriptionSourceName string) (*arrow.Schema, error) {
	subscription, err := table.GetSubscriptionBySourceName(subscriptionSourceName)
	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, 0)
	for _, column := range subscription.Columns() {
		fields = append(fields, arrow.Field{
			Name: column.Name,
			Type: column.Dtype,
		})
	}
	return arrow.NewSchema(fields, nil), nil
}

func TablePartitionAvroSchema(table *elements.Table, subscriptionSourceName string) (*goavro.Codec, error) {
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

	subscription, err := table.GetSubscriptionBySourceName(subscriptionSourceName)
	if err != nil {
		return nil, err
	}
	for _, column := range subscription.Columns() {
		avroType, err := ArrowToAvroType(column.Dtype)
		if err != nil {
			return nil, err
		}
		avroSchema.Fields = append(avroSchema.Fields, avroField{
			Name: column.Name,
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
