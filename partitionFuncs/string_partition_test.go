package partitionFuncs

import (
	"errors"
	"hash/fnv"
	"math"
	"slices"
	"testing"

	"github.com/alekLukanen/ChapterhouseDB/elements"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/uuid"
)

func TestStringFNVHashPartition(t *testing.T) {

	testCases := []struct {
		caseName string
		bldRec   func(*memory.GoAllocator) arrow.Record
		column   string
		options  elements.IPartitionOptions

		bldExpArray func(*memory.GoAllocator, arrow.Record) arrow.Array
		expErr      error
	}{
		{
			caseName: "simple-static-values",
			bldRec: func(mem *memory.GoAllocator) arrow.Record {
				recBldr := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{
							Name: "A", Type: arrow.BinaryTypes.String,
						},
						{
							Name: "B", Type: arrow.PrimitiveTypes.Int64,
						},
					},
					nil,
				))
				recBldr.Field(0).(*array.StringBuilder).AppendValues([]string{"abc123", "bca", "cab"}, nil)
				recBldr.Field(1).(*array.Int64Builder).AppendValues([]int64{0, 1, 2}, nil)
				return recBldr.NewRecord()
			},
			column:  "A",
			options: NewStringHashPartitionOptions(10, MethodFNVHash),
			bldExpArray: func(mem *memory.GoAllocator, inputRec arrow.Record) arrow.Array {
				arrBldr := array.NewUint32Builder(mem)
				arrBldr.AppendValues([]uint32{8, 1, 2}, nil)
				return arrBldr.NewArray()
			},
		},
		{
			caseName: "one-partition-with-simple-static-values",
			bldRec: func(mem *memory.GoAllocator) arrow.Record {
				recBldr := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{
							Name: "A", Type: arrow.BinaryTypes.String,
						},
						{
							Name: "B", Type: arrow.PrimitiveTypes.Int64,
						},
					},
					nil,
				))
				recBldr.Field(0).(*array.StringBuilder).AppendValues([]string{"abc123", "bca", "cab"}, nil)
				recBldr.Field(1).(*array.Int64Builder).AppendValues([]int64{0, 1, 2}, nil)
				return recBldr.NewRecord()
			},
			column:  "A",
			options: NewStringHashPartitionOptions(1, MethodFNVHash),
			bldExpArray: func(mem *memory.GoAllocator, inputRec arrow.Record) arrow.Array {
				arrBldr := array.NewUint32Builder(mem)
				arrBldr.AppendValues([]uint32{0, 0, 0}, nil)
				return arrBldr.NewArray()
			},
		},
		{
			caseName: "two-partitions-with-simple-static-values",
			bldRec: func(mem *memory.GoAllocator) arrow.Record {
				recBldr := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{
							Name: "A", Type: arrow.BinaryTypes.String,
						},
						{
							Name: "B", Type: arrow.PrimitiveTypes.Int64,
						},
					},
					nil,
				))
				recBldr.Field(0).(*array.StringBuilder).AppendValues([]string{"abc123", "bca", "cab"}, nil)
				recBldr.Field(1).(*array.Int64Builder).AppendValues([]int64{0, 1, 2}, nil)
				return recBldr.NewRecord()
			},
			column:  "A",
			options: NewStringHashPartitionOptions(2, MethodFNVHash),
			bldExpArray: func(mem *memory.GoAllocator, inputRec arrow.Record) arrow.Array {
				arrBldr := array.NewUint32Builder(mem)
				arrBldr.AppendValues([]uint32{1, 0, 0}, nil)
				return arrBldr.NewArray()
			},
		},

		{
			caseName: "large-number-of-random-uuid-values",
			bldRec: func(mem *memory.GoAllocator) arrow.Record {
				recBldr := array.NewRecordBuilder(mem, arrow.NewSchema(
					[]arrow.Field{
						{
							Name: "A", Type: arrow.BinaryTypes.String,
						},
						{
							Name: "B", Type: arrow.PrimitiveTypes.Int64,
						},
					},
					nil,
				))

				for idx := 0; idx < 5_000; idx++ {
					id, err := uuid.NewRandom()
					if err != nil {
						panic(err)
					}
					recBldr.Field(0).(*array.StringBuilder).Append(id.String())
					recBldr.Field(1).(*array.Int64Builder).Append(int64(idx))
				}

				return recBldr.NewRecord()
			},
			column:  "A",
			options: NewStringHashPartitionOptions(10, MethodFNVHash),
			bldExpArray: func(mem *memory.GoAllocator, inputRec arrow.Record) arrow.Array {
				arrBldr := array.NewUint32Builder(mem)
				hasher := fnv.New32()
				for idx := 0; idx < int(inputRec.NumRows()); idx++ {
					val := inputRec.Column(0).(*array.String).Value(idx)
					valBytes := []byte(val)
					rn, err := hasher.Write(valBytes)
					if err != nil {
						panic(err)
					} else if rn != len(valBytes) {
						panic("bytes written not equal to bytes input")
					}
					arrBldr.Append(findInterval(10, hasher.Sum32()))
					hasher.Reset()
				}
				return arrBldr.NewArray()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {

			mem := memory.NewGoAllocator()
			rec := tc.bldRec(mem)
			expArr := tc.bldExpArray(mem, rec)

			arr, err := StringFNVHashPartition(mem, rec, tc.column, tc.options)
			if !errors.Is(err, tc.expErr) {
				t.Errorf("expected error '%s' but received '%s'", tc.expErr, err)
				return
			}
			if !array.Equal(arr, expArr) {
				t.Errorf("returns array does not match expected array")
				t.Log("result array: ", arr)
				t.Log("expected array", expArr)
				return
			}

			opts := tc.options.(*StringHashPartitonOptions)
			for idx := 0; idx < int(arr.Len()); idx++ {
				val := arr.(*array.Uint32).Value(idx)
				if val > uint32(opts.PartitionCount) {
					t.Errorf(
						"arr[%d] - partition index of %d was larger than the max of %d",
						idx,
						val,
						opts.PartitionCount)
					return
				}
			}

		})
	}

}

func TestFindInterval(t *testing.T) {

	testCases := []struct {
		caseName string
		n        uint32
		xs       []uint32
		expRes   []uint32
	}{
		{
			caseName: "simple-n-10",
			n:        10,
			xs:       []uint32{0, 1, uint32(math.MaxUint32)/2 - 3, uint32(math.MaxUint32) / 2, uint32(math.MaxUint32)},
			expRes:   []uint32{0, 0, 4, 5, 9},
		},
		{
			caseName: "simple-n-2",
			n:        2,
			xs:       []uint32{0, 1, uint32(math.MaxUint32)/2 - 1, uint32(math.MaxUint32) / 2, uint32(math.MaxUint32)},
			expRes:   []uint32{0, 0, 0, 1, 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			res := make([]uint32, len(tc.xs))
			for idx, x := range tc.xs {
				res[idx] = findInterval(tc.n, x)
			}
			if !slices.Equal(res, tc.expRes) {
				t.Errorf("result arrays are not eqaul")
				t.Log("result array: ", res)
				t.Log("expected array: ", tc.expRes)
			}
		})
	}

}
