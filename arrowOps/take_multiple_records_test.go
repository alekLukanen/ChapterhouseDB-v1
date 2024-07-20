package arrowops

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func TestTakeMultipleRecords(t *testing.T) {

	mem := memory.NewGoAllocator()

	testCases := []struct {
		records        []arrow.Record
		takeIndices    arrow.Record
		expectedRecord arrow.Record
		expectedErr    error
	}{
		{
			records: func() []arrow.Record {

			}(),
			takeIndices: func() arrow.Record {

			}(),
			expectedRecord: func() arrow.Record {

			}(),
			expectedErr: nil,
		},
	}

	for idx, testCase := range testCases {
		t.Run(fmt.Sprintf("case_%d", idx), func(t *testing.T) {

			result, err := TakeMultipleRecords(mem, testCase.records, testCase.takeIndices)
			if !errors.Is(err, testCase.expectedErr) {
				t.Errorf("expected error '%s' but received '%s'", testCase.expectedErr, err)
			}
			if !array.RecordEqual(testCase.expectedRecord, result) {
				t.Error("result record does not match the expected record")
			}

		})
	}

}
