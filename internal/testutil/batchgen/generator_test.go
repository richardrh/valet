package batchgen_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/testutil/batchgen"
)

func TestGeneratedOrdinalsStatesAndRelease(t *testing.T) {
	const (
		rows           = 100
		duplicateEvery = 10
		fileRows       = 13
		// 100/9 = 11 periodic failures, plus the late failure on row 100.
		wantFailures = 12
		// 100/11 = 9 null texts.
		wantNulls = 9
	)
	budget := resource.New(2 << 20)
	reader := batchgen.New(batchgen.Options{
		Rows:           rows,
		BatchRows:      7,
		Seed:           17,
		FailureEvery:   9,
		NullEvery:      11,
		DuplicateEvery: duplicateEvery,
		FileRows:       fileRows,
		StringWidth:    64,
		LateFailure:    true,
	}, budget)
	defer reader.Close()

	var ordinal, failures, nulls uint64
	for {
		batch, err := reader.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		ids := batch.Record.Column(0).(*array.Uint64)
		values := batch.Record.Column(1).(*array.Int64)
		texts := batch.Record.Column(2).(*array.String)
		for i := range int(batch.Record.NumRows()) {
			ordinal++
			wantID := ordinal
			if ordinal%duplicateEvery == 0 {
				wantID-- // duplicates repeat the previous row's id
			}
			if got := ids.Value(i); got != wantID {
				t.Errorf("row %d id = %d, want %d", ordinal, got, wantID)
			}
			if values.Value(i) < 0 {
				failures++
			}
			if batch.State("text", i) == data.Null {
				nulls++
			} else if text := texts.Value(i); !utf8.ValidString(text) {
				t.Errorf("row %d text = %q, want valid UTF-8", ordinal, text)
			}
			file := (ordinal-1)/fileRows + 1
			wantLocation := data.Location{
				Source:        fmt.Sprintf("generated://part/%d", file),
				SourceOrdinal: file,
				Row:           (ordinal-1)%fileRows + 1,
				GlobalRow:     ordinal,
			}
			if got := batch.Locations[i]; got != wantLocation {
				t.Errorf("row %d location = %+v, want %+v", ordinal, got, wantLocation)
			}
		}
		batch.Release()
		batch.Release() // a second release must be a no-op
		if used := budget.Used(); used != 0 {
			t.Fatalf("budget used after batch release = %d, want 0", used)
		}
	}

	if ordinal != rows {
		t.Errorf("rows = %d, want %d", ordinal, rows)
	}
	if failures != wantFailures {
		t.Errorf("failures = %d, want %d", failures, wantFailures)
	}
	if nulls != wantNulls {
		t.Errorf("nulls = %d, want %d", nulls, wantNulls)
	}
	if reader.LogicalBytes == 0 {
		t.Error("reader logical bytes = 0, want > 0")
	}
}

func TestCancellationDoesNotAllocate(t *testing.T) {
	budget := resource.New(1 << 20)
	r := batchgen.New(batchgen.Options{Rows: 100}, budget)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	batch, err := r.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Next error = %v, want %v", err, context.Canceled)
	}
	if batch != nil {
		t.Errorf("Next batch = %v, want nil", batch)
	}
	if used := budget.Used(); used != 0 {
		t.Errorf("budget used = %d, want 0", used)
	}
}

// bufferBytes sums the lengths of the arrays' own (non-child) buffers.
func bufferBytes(arrays ...arrow.Array) uint64 {
	var total uint64
	for _, a := range arrays {
		for _, buffer := range a.Data().Buffers() {
			if buffer != nil {
				total += uint64(buffer.Len())
			}
		}
	}
	return total
}

func TestLogicalBytesDeduplicatesSharedBuffers(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer checked.AssertSize(t, 0)
	b := array.NewInt64Builder(checked)
	b.AppendValues([]int64{1, 2, 3}, nil)
	a := b.NewArray()
	b.Release()
	defer a.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{a, a}, 3)
	defer record.Release()

	// Both columns share one array, so its buffers count once.
	want := bufferBytes(a)
	if got := batchgen.LogicalBytes(record); got != want {
		t.Errorf("LogicalBytes = %d, want %d", got, want)
	}
}

func TestLogicalBytesDictionaryAndPlainSharedValues(t *testing.T) {
	checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer checked.AssertSize(t, 0)
	indicesBuilder := array.NewInt8Builder(checked)
	indicesBuilder.AppendValues([]int8{0, 1}, nil)
	indices := indicesBuilder.NewArray()
	indicesBuilder.Release()
	defer indices.Release()
	valuesBuilder := array.NewStringBuilder(checked)
	valuesBuilder.AppendValues([]string{"alpha", "β"}, nil)
	values := valuesBuilder.NewArray()
	valuesBuilder.Release()
	defer values.Release()
	typ := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}
	dictionary := array.NewDictionaryArray(typ, indices, values)
	defer dictionary.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "encoded", Type: typ},
		{Name: "plain", Type: arrow.BinaryTypes.String},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{dictionary, values}, 2)
	defer record.Release()

	// The dictionary's values are also the plain column, so they count once.
	want := bufferBytes(indices, values)
	if got := batchgen.LogicalBytes(record); got != want {
		t.Errorf("LogicalBytes = %d, want %d", got, want)
	}
}

func TestFreshPayloadAndUnicode(t *testing.T) {
	const width = 1024
	o := batchgen.Options{Seed: 123, StringWidth: width}
	texts := []struct {
		ordinal uint64
		text    string
	}{
		{ordinal: 1, text: batchgen.Text(o, 1)},
		{ordinal: 2, text: batchgen.Text(o, 2)},
	}
	if texts[0].text == texts[1].text {
		t.Error("Text(1) equals Text(2), want fresh per-ordinal payloads")
	}
	for _, tc := range texts {
		if got := len(tc.text); got < width {
			t.Errorf("len(Text(%d)) = %d, want >= %d", tc.ordinal, got, width)
		}
		if !utf8.ValidString(tc.text) {
			t.Errorf("Text(%d) = %q, want valid UTF-8", tc.ordinal, tc.text)
		}
	}
}

func TestTextEmptyAndWhitespaceSentinels(t *testing.T) {
	o := batchgen.Options{Seed: 123, StringWidth: 1024}
	cases := []struct {
		name    string
		ordinal uint64
		want    string
	}{
		{name: "every 29th row is empty", ordinal: 29, want: ""},
		{name: "every 31st row is whitespace", ordinal: 31, want: " \t"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := batchgen.Text(o, c.ordinal); got != c.want {
				t.Errorf("Text(%d) = %q, want %q", c.ordinal, got, c.want)
			}
		})
	}
}
