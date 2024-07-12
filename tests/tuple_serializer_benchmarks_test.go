package tests

import (
	"fmt"
	"testing"

	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"

	"github.com/alekLukanen/chapterhouseDB/tests/serializers"
)

var TEST_SIZES [1]int = [1]int{1_000_000}

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

	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				intData, stringData := BuildData(size)
				data := make([][][]byte, size)
				b.StartTimer()
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

					data[i] = [][]byte{intMsgData, strMsgData}
				}
			}
		})
	}

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

	for _, size := range TEST_SIZES {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				b.StopTimer()
				intData, stringData := BuildData(size)
				data := make([][]byte, size)
				b.StartTimer()
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
			}
		})
	}

}
