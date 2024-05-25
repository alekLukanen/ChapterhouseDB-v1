package tests

import (
	"fmt"
	"testing"

	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"

	"github.com/alekLukanen/chapterhouseDB/tests/serializers"
)

func BuildData(size int) ([]int64, []string) {
	intData := make([]int64, size)
	stringData := make([]string, size)
	for i := range intData {
		intData[i] = int64(i)
		stringData[i] = fmt.Sprintf("string-%d", i)
	}
	return intData, stringData
}

func TestProtobufTupleSerializer(t *testing.T) {

	strMsg := serializers.String{Value: "abc"}

	data, err := proto.Marshal(&strMsg)
	if err != nil {
		t.Error(err)
	}

	t.Log(data)
}

func BenchmarkProtobufMarshal(b *testing.B) {

	size := 1_000_000
	intData, stringData := BuildData(size)

	b.ResetTimer()

	data := make([][2][]byte, size)
	for i := range size {
		intMsg := serializers.Integer{Value: intData[i]}
		stringMsg := serializers.String{Value: stringData[i]}

		intMsgData, err := proto.Marshal(&intMsg)
		if err != nil {
			b.Error(err)
		}

		strMsgData, err := proto.Marshal(&stringMsg)
		if err != nil {
			b.Error(err)
		}

		data[i] = [2][]byte{intMsgData, strMsgData}

	}

	b.Log(len(data[0][0]) + len(data[0][1]))
	b.Log(len(data[len(data)-1][0]) + len(data[len(data)-1][1]))

}

func BenchmarkAvroMarshal(b *testing.B) {

	var Schema = `{
		"type": "record",
		"name": "simpleRecord",
		"namespace": "org.hamba.avro",
		"fields" : [
			{"name": "string", "type": "string"},
			{"name": "integer", "type": "long"}
		]
	}`

	codec, err := goavro.NewCodec(Schema)
	if err != nil {
		b.Fatal(err)
	}

	size := 1_000_000
	intData, stringData := BuildData(size)

	b.ResetTimer()

	data := make([][]byte, size)
	dataMap := make(map[string]interface{})
	for i := range size {

		dataMap["string"] = stringData[i]
		dataMap["integer"] = intData[i]

		msgData, err := codec.BinaryFromNative(nil, dataMap)
		if err != nil {
			b.Error(err)
		}

		data[i] = msgData

	}

	b.Log(len(data[0]))
	b.Log(len(data[len(data)-1]))

}
