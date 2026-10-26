package source

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

func encodeParquet(t *testing.T, record arrow.RecordBatch) []byte {
	t.Helper()
	var encoded bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	w, err := pqarrow.NewFileWriter(record.Schema(), &encoded, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Write(record); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func nativeFixture(t *testing.T, record arrow.RecordBatch) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "native.parquet")
	if err := os.WriteFile(path, encodeParquet(t, record), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// ingestCodes returns the code of every ingestion issue in order.
func ingestCodes(b *data.Batch) []string {
	codes := make([]string, len(b.Ingest))
	for i, issue := range b.Ingest {
		codes[i] = issue.Code
	}
	return codes
}

// TestNativeBatchOutlivesReaderAndPreservesIntegers closes the reader, dataset
// and store before inspecting the batch: the batch must own its buffers.
func TestNativeBatchOutlivesReaderAndPreservesIntegers(t *testing.T) {
	resource.ConfigureParquetCodecs()
	b := array.NewUint64Builder(memory.NewGoAllocator())
	b.AppendValues([]uint64{9007199254740993, math.MaxUint64}, nil)
	values := b.NewArray()
	b.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 2)
	values.Release()
	path := nativeFixture(t, record)
	record.Release()

	budget := resource.New(128 << 20)
	store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Input:     config.Input{Type: "parquet"},
		Source:    config.Source{URIs: []string{path}},
		Execution: config.Execution{BatchTarget: "16KiB", MaxRecordBytes: "1MiB"},
	}
	ds, err := New(context.Background(), cfg, store, budget)
	if err != nil {
		t.Fatal(err)
	}
	r, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	batch, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("second Next error = %v, want io.EOF", err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	_ = ds.Close()
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	ids := batch.Record.Column(0).(*array.Uint64)
	got := []uint64{ids.Value(0), ids.Value(1)}
	if want := []uint64{9007199254740993, math.MaxUint64}; !slices.Equal(got, want) {
		t.Errorf("retained ids = %v, want %v", got, want)
	}
	if budget.Used() == 0 {
		t.Error("budget used while batch retained = 0, want > 0 (batch owns its buffers)")
	}
	batch.Release()
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after release = %d, want 0", got)
	}
}

func TestNativeFloatConversionKeepsInvalidSeparateFromNull(t *testing.T) {
	resource.ConfigureParquetCodecs()
	b := array.NewFloat64Builder(memory.NewGoAllocator())
	// Rows: 1.5 (valid), NaN (invalid for float64), null (rejected: not nullable).
	b.AppendValues([]float64{1.5, math.NaN(), 0}, []bool{true, true, false})
	values := b.NewArray()
	b.Release()
	field := arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
	values.Release()
	path := nativeFixture(t, record)
	record.Release()

	nullable := false
	input := config.Input{
		OnTypeError: "row_error",
		Schema: &config.Schema{Columns: map[string]config.Column{
			"value": {Type: "float64", Nullable: &nullable},
		}},
	}
	batch := readOne(t, openParquetDataset(t, path, input))

	for row, want := range []data.State{data.Valid, data.Invalid, data.Null} {
		if got := batch.State("value", row); got != want {
			t.Errorf("row %d state = %v, want %v", row, got, want)
		}
	}
	// The invalid slot is null in Arrow; the invalid mask keeps it distinct.
	col := batch.Record.Column(0).(*array.Float64)
	if got := col.Value(0); got != 1.5 {
		t.Errorf("row 0 value = %v, want 1.5", got)
	}
	if !col.IsNull(1) {
		t.Error("row 1 (invalid) null = false, want true")
	}
	if !col.IsNull(2) {
		t.Error("row 2 (null) null = false, want true")
	}
	if got, want := ingestCodes(batch), []string{"INGEST_TYPE", "INGEST_NULL"}; !slices.Equal(got, want) {
		t.Errorf("ingest codes = %v, want %v (issues %+v)", got, want, batch.Ingest)
	}
	if !bytes.Contains(batch.Raw[1], []byte("NaN")) {
		t.Errorf("row 1 reject record = %s, want it to contain NaN", batch.Raw[1])
	}
	if !bytes.Contains(batch.Raw[2], []byte("null")) {
		t.Errorf("row 2 reject record = %s, want it to contain null", batch.Raw[2])
	}
}

func TestNativeRejectsInvalidUTF8AndDecimalOverflow(t *testing.T) {
	resource.ConfigureParquetCodecs()
	mem := memory.NewGoAllocator()
	sb := array.NewStringBuilder(mem)
	sb.AppendValues([]string{"ok", "bad\xff", "fine"}, nil) // 0xff makes row 1 invalid UTF-8
	strs := sb.NewArray()
	sb.Release()
	dt := &arrow.Decimal128Type{Precision: 5, Scale: 2}
	db := array.NewDecimal128Builder(mem, dt)
	// 123.45 fits decimal(5,2); 100000.00 in row 2 overflows it.
	amounts := []decimal128.Num{decimal128.FromI64(12345), decimal128.FromI64(1), decimal128.FromI64(10_000_000)}
	db.AppendValues(amounts, nil)
	decs := db.NewArray()
	db.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: dt, Nullable: true},
	}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{strs, decs}, 3)
	strs.Release()
	decs.Release()
	path := nativeFixture(t, record)
	record.Release()

	input := config.Input{
		OnTypeError: "row_error",
		Schema: &config.Schema{
			Columns: map[string]config.Column{
				"name":   {Type: "string"},
				"amount": {Type: "decimal", Precision: 5, Scale: 2},
			},
			Order: []string{"name", "amount"},
		},
	}
	batch := readOne(t, openParquetDataset(t, path, input))

	for _, tc := range []struct {
		column string
		row    int
		want   data.State
	}{
		{column: "name", row: 0, want: data.Valid},
		{column: "name", row: 1, want: data.Invalid},
		{column: "amount", row: 0, want: data.Valid},
		{column: "amount", row: 2, want: data.Invalid},
	} {
		if got := batch.State(tc.column, tc.row); got != tc.want {
			t.Errorf("%s row %d state = %v, want %v", tc.column, tc.row, got, tc.want)
		}
	}
	type issueAt struct {
		row    uint64
		column string
	}
	var gotIssues []issueAt
	for _, issue := range batch.Ingest {
		gotIssues = append(gotIssues, issueAt{row: issue.Row, column: issue.Column})
	}
	// Issue rows are 1-based.
	wantIssues := []issueAt{{row: 2, column: "name"}, {row: 3, column: "amount"}}
	if !slices.Equal(gotIssues, wantIssues) {
		t.Errorf("ingest issues = %+v, want %+v", gotIssues, wantIssues)
	}
	if got := batch.Record.Column(1).(*array.Decimal128).Value(0); got != decimal128.FromI64(12345) {
		t.Errorf("amount row 0 = %v, want %v", got, decimal128.FromI64(12345))
	}
}

func TestNativeParquetCountsIgnoredColumnsAgainstRecordLimit(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ignored_a", Type: arrow.BinaryTypes.String},
		{Name: "ignored_b", Type: arrow.BinaryTypes.String},
	}, nil)
	// Two ignored 600KiB strings: together they exceed the 1MiB record limit.
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	builder.Field(0).(*array.Int64Builder).Append(1)
	builder.Field(1).(*array.StringBuilder).Append(string(bytes.Repeat([]byte{'a'}, 600<<10)))
	builder.Field(2).(*array.StringBuilder).Append(string(bytes.Repeat([]byte{'b'}, 600<<10)))
	record := builder.NewRecordBatch()
	builder.Release()
	path := nativeFixture(t, record)
	record.Release()

	input := config.Input{Schema: &config.Schema{
		ExtraFields: "ignore",
		Order:       []string{"id"},
		Columns:     map[string]config.Column{"id": {Type: "int64"}},
	}}
	ds := openParquetDataset(t, path, input)
	reader, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	_, err = reader.Next(context.Background())
	assertRecordLimitError(t, err)
}

func TestParquetFixedWidthUsesPhysicalBytes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int8}}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	builder.Field(0).(*array.Int8Builder).Append(127)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()

	// len("x") + per-cell overhead + one Int8 byte.
	const physicalRowBytes = int64(1 + parquetCellOverhead + 1)
	width := int64(len("x")+parquetCellOverhead) + parquetValueWidth(record.Column(0), 0)
	if width != physicalRowBytes {
		t.Fatalf("row width = %d, want %d", width, physicalRowBytes)
	}
	if err := validateNativeParquetRowWidth(record, 0, physicalRowBytes); err != nil {
		t.Errorf("row width check at exact limit = %v, want nil", err)
	}
	if err := validateNativeParquetRowWidth(record, 0, physicalRowBytes-1); err == nil {
		t.Error("row width check one byte below physical width = nil, want error")
	}
}
