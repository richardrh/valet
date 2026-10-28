package source

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// writeParquetFile encodes record with codec into a temporary file.
func writeParquetFile(t *testing.T, record arrow.RecordBatch, codec compress.Compression) string {
	t.Helper()
	var encoded bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCompression(codec))
	writer, err := pqarrow.NewFileWriter(record.Schema(), &encoded, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "rows.parquet")
	if err = os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// openParquetDataset builds a dataset over path and checks for budget leaks
// once the test's readers, dataset and store are closed.
func openParquetDataset(t *testing.T, path string, input config.Input) *Dataset {
	t.Helper()
	budget := resource.New(128 << 20)
	store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget, nil)
	if err != nil {
		t.Fatal(err)
	}
	input.Type = "parquet"
	cfg := &config.Config{
		Input:     input,
		Source:    config.Source{URIs: []string{path}},
		Execution: config.Execution{BatchTarget: "16KiB", MaxRecordBytes: "1MiB"},
	}
	ds, err := New(context.Background(), cfg, store, budget, nil, false)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ds.Close()
		if err := store.Close(); err != nil {
			t.Error(err)
		}
		if n := budget.Used(); n != 0 {
			t.Errorf("Parquet leaked %d bytes", n)
		}
	})
	return ds
}

func TestParquetRequiredCodecs(t *testing.T) {
	resource.ConfigureParquetCodecs()
	codecs := []compress.Compression{compress.Codecs.Uncompressed, compress.Codecs.Snappy, compress.Codecs.Zstd}
	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			}, nil)
			builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
			builder.Field(0).(*array.Int64Builder).AppendValues([]int64{9007199254740993, 2}, []bool{true, false})
			builder.Field(1).(*array.StringBuilder).AppendValues([]string{"001", "test"}, nil)
			record := builder.NewRecordBatch()
			builder.Release()
			defer record.Release()
			ds := openParquetDataset(t, writeParquetFile(t, record, codec), config.Input{})
			r, err := ds.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			batch, err := r.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer batch.Release()
			ids := batch.Record.Column(0).(*array.Int64)
			if got := ids.Value(0); got != 9007199254740993 {
				t.Errorf("id row 0 = %d, want 9007199254740993", got)
			}
			if !ids.IsNull(1) {
				t.Error("id row 1 null = false, want true")
			}
			if got := batch.Record.Column(1).(*array.String).Value(0); got != "001" {
				t.Errorf("name row 0 = %q, want %q", got, "001")
			}
			if _, err = r.Next(context.Background()); !errors.Is(err, io.EOF) {
				t.Fatalf("second Next error = %v, want io.EOF", err)
			}
		})
	}
}

// A declared type that differs from the file forces the lexical converter;
// only the row with a conversion failure carries a reject record.
func TestParquetLexicalRejectsOnlyInvalidRows(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	builder.Field(0).(*array.StringBuilder).AppendValues([]string{"7", "seven"}, nil)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	declared := &config.Schema{Order: []string{"id"}, Columns: map[string]config.Column{"id": {Type: "int64"}}}
	ds := openParquetDataset(t, writeParquetFile(t, record, compress.Codecs.Snappy), config.Input{Schema: declared})
	batch := readOne(t, ds)
	if got := batch.State("id", 0); got != data.Valid {
		t.Errorf("id row 0 state = %v, want %v", got, data.Valid)
	}
	if got := batch.State("id", 1); got != data.Invalid {
		t.Errorf("id row 1 state = %v, want %v", got, data.Invalid)
	}
	if batch.Raw[0] != nil {
		t.Errorf("valid row reject record = %s, want nil", batch.Raw[0])
	}
	if got, want := string(batch.Raw[1]), `{"id":"seven"}`; got != want {
		t.Errorf("invalid row reject record = %s, want %s", got, want)
	}
}

func TestCompactMetadataLengthGuard(t *testing.T) {
	for _, tc := range []struct {
		name   string
		footer []byte
		valid  bool
	}{
		{name: "empty struct", footer: []byte{0}, valid: true}, // stop byte only
		// Field 1: list of structs, count 2^28, but no encoded elements.
		{name: "unbounded collection", footer: []byte{0x19, 0xfc, 0x80, 0x80, 0x80, 0x80, 0x01, 0}},
		// Field 1: binary with length varint 2^35-1, but no payload bytes.
		{name: "unbounded string", footer: []byte{0x18, 0xff, 0xff, 0xff, 0xff, 0x7f, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCompact(tc.footer)
			if (err == nil) != tc.valid {
				t.Fatalf("validateCompact(% x) = %v, want valid=%v", tc.footer, err, tc.valid)
			}
		})
	}
}

func TestParquetTemporalValuesAvoidTextRoundTrip(t *testing.T) {
	timestampType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
	schema := arrow.NewSchema([]arrow.Field{{Name: "observed", Type: timestampType}}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	// Year 10000 has no four-digit-year text form; the value must stay native.
	value := arrow.Timestamp(time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	builder.Field(0).(*array.TimestampBuilder).Append(value)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	ds := openParquetDataset(t, writeParquetFile(t, record, compress.Codecs.Uncompressed), config.Input{})
	if got := ds.Passes(); got != 1 {
		t.Errorf("passes = %d, want 1", got)
	}
	if got := ds.DecoderPassesPerOpen(); got != 1 {
		t.Errorf("decoder passes per open = %d, want 1", got)
	}
	values := readOne(t, ds).Record.Column(0).(*array.Timestamp)
	if got := values.DataType().(*arrow.TimestampType).Unit; got != arrow.Millisecond {
		t.Errorf("timestamp unit = %s, want %s", got, arrow.Millisecond)
	}
	if got := values.Value(0); got != value {
		t.Errorf("timestamp value = %d, want %d", got, value)
	}
}
