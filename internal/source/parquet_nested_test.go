package source

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
)

// writeNestedParquet writes {x list<int64>, y int64, s struct<a int64>, z utf8}
// in row groups of two rows, so nested columns sit before, between and after
// flat ones and the row-group normalization path is exercised more than once.
func writeNestedParquet(t *testing.T) string {
	t.Helper()
	structType := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "y", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: structType, Nullable: true},
		{Name: "z", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	list := builder.Field(0).(*array.ListBuilder)
	items := list.ValueBuilder().(*array.Int64Builder)
	st := builder.Field(2).(*array.StructBuilder)
	a := st.FieldBuilder(0).(*array.Int64Builder)
	for row := range int64(5) {
		if row == 3 {
			list.AppendNull()
		} else {
			list.Append(true)
			items.AppendValues([]int64{row, row + 1}, nil)
		}
		if row == 1 {
			builder.Field(1).AppendNull()
		} else {
			builder.Field(1).(*array.Int64Builder).Append(row)
		}
		st.Append(true)
		a.Append(row * 10)
		builder.Field(3).(*array.StringBuilder).Append("v")
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	var encoded bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &encoded, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(2)), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "nested.parquet")
	if err = os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParquetNestedColumnsAreSkipped(t *testing.T) {
	resource.ConfigureParquetCodecs()
	ds := openParquetDataset(t, writeNestedParquet(t), config.Input{})
	got := []string{}
	for _, f := range ds.Schema().Fields() {
		got = append(got, f.Name+":"+f.Type.String())
	}
	if want := []string{"y:int64", "z:utf8"}; !slices.Equal(got, want) {
		t.Fatalf("schema = %v, want %v", got, want)
	}
	unsupported := ds.UnsupportedColumns()
	if len(unsupported) != 2 || !strings.HasPrefix(unsupported["x"], "list<") || !strings.HasPrefix(unsupported["s"], "struct<") {
		t.Fatalf("unsupported columns = %v, want x list and s struct", unsupported)
	}
	r, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var ys []string
	for {
		batch, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if batch.Record.NumCols() != 2 {
			t.Fatalf("decoded %d columns, want 2", batch.Record.NumCols())
		}
		for row := range int(batch.Record.NumRows()) {
			ys = append(ys, batch.Record.Column(0).ValueStr(row)+"/"+batch.Record.Column(1).ValueStr(row))
		}
		batch.Release()
	}
	if want := []string{"0/v", "(null)/v", "2/v", "3/v", "4/v"}; !slices.Equal(ys, want) {
		t.Fatalf("rows = %v, want %v", ys, want)
	}
}

// schemaElement encodes one compact-protocol SchemaElement. typ < 0 omits the
// physical type (a group); children < 0 omits num_children; list adds a LIST
// logical annotation.
func schemaElement(typ, repetition, children int, list bool) []byte {
	zigzag := func(v int) []byte { return binary.AppendUvarint(nil, uint64(v<<1^v>>63)) }
	var out []byte
	last := 0
	field := func(id int, kind byte) {
		out = append(out, byte(id-last)<<4|kind)
		last = id
	}
	if typ >= 0 {
		field(schemaTypeField, 5)
		out = append(out, zigzag(typ)...)
	}
	if repetition >= 0 {
		field(schemaRepetitionField, 5)
		out = append(out, zigzag(repetition)...)
	}
	field(4, 8) // name
	out = append(out, 1, 'c')
	if children >= 0 {
		field(schemaNumChildrenField, 5)
		out = append(out, zigzag(children)...)
	}
	if list {
		field(schemaLogicalTypeField, 12)
		out = append(out, byte(logicalList)<<4|12, 0, 0) // LIST{} then union stop
	}
	return append(out, 0)
}

// schemaFooter encodes a FileMetaData holding only the schema list.
func schemaFooter(elements ...[]byte) []byte {
	out := []byte{0x29} // field 2, list
	if len(elements) < 15 {
		out = append(out, byte(len(elements))<<4|12)
	} else {
		out = binary.AppendUvarint(append(out, 0xfc), uint64(len(elements)))
	}
	for _, e := range elements {
		out = append(out, e...)
	}
	return append(out, 0)
}

func TestSchemaFooterTreeWalk(t *testing.T) {
	const i64, opt, req, rep = 2, repetitionOptional, repetitionRequired, repetitionRepeated
	root := func(children int) []byte { return schemaElement(-1, -1, children, false) }
	leaf := func(repetition int) []byte { return schemaElement(i64, repetition, -1, false) }
	group := func(repetition, children int) []byte { return schemaElement(-1, repetition, children, false) }
	listGroup := schemaElement(-1, opt, 1, true)
	deep := [][]byte{root(1)}
	for range maxSchemaDepth {
		deep = append(deep, group(opt, 1))
	}
	deep = append(deep, leaf(opt))
	for _, tc := range []struct {
		name   string
		footer []byte
		valid  bool
	}{
		{name: "flat", footer: schemaFooter(root(2), leaf(opt), leaf(req)), valid: true},
		{name: "list before flat", footer: schemaFooter(root(2), listGroup, group(rep, 1), leaf(opt), leaf(opt)), valid: true},
		{name: "struct after flat", footer: schemaFooter(root(2), leaf(opt), group(opt, 2), leaf(opt), leaf(req)), valid: true},
		{name: "root child count too small", footer: schemaFooter(root(1), leaf(opt), leaf(opt))},
		{name: "root child count too large", footer: schemaFooter(root(3), leaf(opt), leaf(opt))},
		{name: "group child count overruns parent", footer: schemaFooter(root(1), group(opt, 2), leaf(opt))},
		{name: "repeated top-level leaf", footer: schemaFooter(root(1), leaf(rep))},
		{name: "repeated top-level group", footer: schemaFooter(root(1), group(rep, 1), leaf(opt))},
		{name: "typed group", footer: schemaFooter(root(1), schemaElement(i64, opt, 1, false), leaf(opt))},
		{name: "empty group", footer: schemaFooter(root(1), group(opt, 0))},
		{name: "LIST annotation on leaf", footer: schemaFooter(root(1), schemaElement(i64, opt, -1, true))},
		{name: "too deep", footer: schemaFooter(deep...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateCompact(tc.footer); (err == nil) != tc.valid {
				t.Fatalf("validateCompact(% x) = %v, want valid=%v", tc.footer, err, tc.valid)
			}
		})
	}
}
