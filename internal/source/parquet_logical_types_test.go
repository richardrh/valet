package source

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/config"
)

// encodeArrowParquet writes values as column x with the Arrow schema stored,
// so readers restore dictionary and large encodings as pyarrow files do.
func encodeArrowParquet(t *testing.T, column arrow.Array) []byte {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: column.DataType(), Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(column.Len()))
	defer record.Release()
	var encoded bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &encoded, parquet.NewWriterProperties(),
		pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

// readParquetColumn returns column x of every batch rendered as optional text.
func readParquetColumn(t *testing.T, ds *Dataset) (arrow.DataType, []*string) {
	t.Helper()
	r, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got []*string
	typ := ds.Schema().Field(0).Type
	for {
		batch, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		column := batch.Record.Column(0)
		if !arrow.TypeEqual(column.DataType(), typ) {
			t.Errorf("batch type %s, want %s", column.DataType(), typ)
		}
		for i := range column.Len() {
			if column.IsNull(i) {
				got = append(got, nil)
				continue
			}
			var s string
			switch v := column.(type) {
			case *array.String:
				s = v.Value(i)
			case *array.Binary:
				s = string(v.Value(i))
			default:
				t.Fatalf("unexpected batch array %T", column)
			}
			got = append(got, &s)
		}
		batch.Release()
	}
	return typ, got
}

func assertColumn(t *testing.T, got, want []*string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows=%d, want %d", len(got), len(want))
	}
	for i := range want {
		switch {
		case (got[i] == nil) != (want[i] == nil):
			t.Errorf("row %d null=%v, want %v", i, got[i] == nil, want[i] == nil)
		case got[i] != nil && *got[i] != *want[i]:
			t.Errorf("row %d = %q, want %q", i, *got[i], *want[i])
		}
	}
}

func TestParquetDictionaryAndLargeTypesDecodeAsValues(t *testing.T) {
	mem := memory.NewGoAllocator()
	want := []*string{new("a"), nil, new("b"), new("a")}
	valid := []bool{true, false, true, true}
	values := []string{"a", "", "b", "a"}
	cases := []struct {
		name  string
		build func() arrow.Array
		want  arrow.DataType
	}{
		{"dictionary_utf8", func() arrow.Array {
			b := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}).(*array.BinaryDictionaryBuilder)
			defer b.Release()
			for i, v := range values {
				if !valid[i] {
					b.AppendNull()
				} else if err := b.AppendString(v); err != nil {
					t.Fatal(err)
				}
			}
			return b.NewArray()
		}, arrow.BinaryTypes.String},
		{"dictionary_binary", func() arrow.Array {
			b := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary}).(*array.BinaryDictionaryBuilder)
			defer b.Release()
			for i, v := range values {
				if !valid[i] {
					b.AppendNull()
				} else if err := b.Append([]byte(v)); err != nil {
					t.Fatal(err)
				}
			}
			return b.NewArray()
		}, arrow.BinaryTypes.Binary},
		{"large_utf8", func() arrow.Array {
			b := array.NewLargeStringBuilder(mem)
			defer b.Release()
			b.AppendValues(values, valid)
			return b.NewArray()
		}, arrow.BinaryTypes.String},
		{"large_binary", func() arrow.Array {
			b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
			defer b.Release()
			for i, v := range values {
				if valid[i] {
					b.Append([]byte(v))
				} else {
					b.AppendNull()
				}
			}
			return b.NewArray()
		}, arrow.BinaryTypes.Binary},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			column := tc.build()
			defer column.Release()
			ds, _ := rowGroupDataset(t, [][]byte{encodeArrowParquet(t, column)}, 128<<20, config.Input{})
			typ, got := readParquetColumn(t, ds)
			if !arrow.TypeEqual(typ, tc.want) {
				t.Errorf("schema type %s, want %s", typ, tc.want)
			}
			assertColumn(t, got, want)
		})
	}
}

func TestParquetDictionaryThenPlainUTF8IsCompatible(t *testing.T) {
	mem := memory.NewGoAllocator()
	db := array.NewDictionaryBuilder(mem, &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}).(*array.BinaryDictionaryBuilder)
	defer db.Release()
	for _, v := range []string{"a", "b", "a"} {
		if err := db.AppendString(v); err != nil {
			t.Fatal(err)
		}
	}
	dict := db.NewArray()
	defer dict.Release()
	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	sb.AppendValues([]string{"c", ""}, []bool{true, false})
	plain := sb.NewArray()
	defer plain.Release()
	ds, _ := rowGroupDataset(t, [][]byte{encodeArrowParquet(t, dict), encodeArrowParquet(t, plain)}, 128<<20, config.Input{})
	_, got := readParquetColumn(t, ds)
	if issues := ds.SchemaIssues(); len(issues) != 0 {
		t.Fatalf("schema issues %v, want none", issues)
	}
	want := []*string{new("a"), new("b"), new("a"), new("c"), nil}
	assertColumn(t, got, want)
}
