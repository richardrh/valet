package data_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

func TestBatchPreservesInvalidVersusNullAndReleasesOwnership(t *testing.T) {
	budget := resource.New(4096)
	builder := array.NewInt64Builder(budget.Allocator())
	builder.AppendValues([]int64{7, 0, 0, 0}, []bool{true, false, false, false})
	values := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 4)
	values.Release()
	wantStates := []data.State{data.Valid, data.Null, data.Invalid, data.Missing}
	released := 0
	batch := &data.Batch{
		Record:  record,
		States:  map[string][]data.State{"id": wantStates},
		Cleanup: func() { released++ },
	}

	for row, want := range wantStates {
		if got := batch.State("id", row); got != want {
			t.Errorf("State(id, %d) = %v, want %v", row, got, want)
		}
	}
	if got := batch.State("absent", 0); got != data.Missing {
		t.Errorf("State(absent, 0) = %v, want %v", got, data.Missing)
	}
	if budget.Used() == 0 {
		t.Fatal("budget used before release = 0, want record to own live Arrow buffers")
	}

	batch.Release()
	batch.Release()
	if released != 1 {
		t.Errorf("cleanup calls after double Release = %d, want 1", released)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after Release = %d, want 0", got)
	}
}

func TestStateReportSpelling(t *testing.T) {
	for state, want := range map[data.State]string{
		data.Valid:   "valid",
		data.Missing: "missing",
		data.Null:    "null",
		data.Invalid: "invalid",
	} {
		t.Run(want, func(t *testing.T) {
			text, err := state.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText(%d): %v", state, err)
			}
			if string(text) != want {
				t.Errorf("MarshalText(%d) = %q, want %q", state, text, want)
			}
			if got := state.String(); got != want {
				t.Errorf("String(%d) = %q, want %q", state, got, want)
			}
			var decoded data.State
			if err = json.Unmarshal([]byte(`"`+want+`"`), &decoded); err != nil {
				t.Fatalf("decode %q: %v", want, err)
			}
			if decoded != state {
				t.Errorf("decode %q = %v, want %v", want, decoded, state)
			}
		})
	}
}

func TestUnknownStateIsRejected(t *testing.T) {
	var state data.State
	if err := state.UnmarshalText([]byte("bogus")); err == nil {
		t.Errorf("UnmarshalText(bogus) err = nil, want error (decoded %v)", state)
	}
	if text, err := data.State(9).MarshalText(); err == nil {
		t.Errorf("State(9).MarshalText() = %q, nil; want error", text)
	}
}

func TestBucketReportSpelling(t *testing.T) {
	for bucket, want := range map[data.Bucket]string{
		data.BucketPass:  "pass",
		data.BucketWarn:  "warn",
		data.BucketError: "error",
	} {
		t.Run(want, func(t *testing.T) {
			if !bucket.Valid() {
				t.Errorf("Bucket(%d).Valid() = false, want true", bucket)
			}
			text, err := bucket.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText(%d): %v", bucket, err)
			}
			if string(text) != want {
				t.Errorf("MarshalText(%d) = %q, want %q", bucket, text, want)
			}
			if got := bucket.String(); got != want {
				t.Errorf("String(%d) = %q, want %q", bucket, got, want)
			}
			var decoded data.Bucket
			if err = json.Unmarshal([]byte(`"`+want+`"`), &decoded); err != nil {
				t.Fatalf("decode %q: %v", want, err)
			}
			if decoded != bucket {
				t.Errorf("decode %q = %v, want %v", want, decoded, bucket)
			}
		})
	}
}

func TestUnknownBucketIsRejected(t *testing.T) {
	var bucket data.Bucket
	if err := bucket.UnmarshalText([]byte("fail")); err == nil {
		t.Errorf("UnmarshalText(fail) err = nil, want error (decoded %v)", bucket)
	}
	unknown := data.Bucket(3)
	if text, err := unknown.MarshalText(); err == nil {
		t.Errorf("Bucket(3).MarshalText() = %q, nil; want error", text)
	}
	if unknown.Valid() {
		t.Error("Bucket(3).Valid() = true, want false")
	}
}
