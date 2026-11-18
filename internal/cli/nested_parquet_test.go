package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// nestedParquet writes {x list<int64>, y int64, s struct<a int64>, z utf8}
// over three row groups; y is null in row 2.
func nestedParquet(t *testing.T, root string) string {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "y", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), Nullable: true},
		{Name: "z", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	list := builder.Field(0).(*array.ListBuilder)
	st := builder.Field(2).(*array.StructBuilder)
	for row := range int64(5) {
		list.Append(true)
		list.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{row, row}, nil)
		if row == 1 {
			builder.Field(1).AppendNull()
		} else {
			builder.Field(1).(*array.Int64Builder).Append(row)
		}
		st.Append(true)
		st.FieldBuilder(0).(*array.Int64Builder).Append(row)
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
	path := filepath.Join(root, "nested.parquet")
	if err = os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Nested Parquet columns are skipped: rules on flat columns validate every
// row, and a rule on a nested column rejects the schema instead of failing.
func TestValidateSkipsNestedParquetColumns(t *testing.T) {
	dir := t.TempDir()
	input := nestedParquet(t, dir)
	for _, tc := range []struct {
		column, typ string
		exit        int
		errors      uint64
	}{
		{column: "y", exit: ExitValidation, errors: 1},
		{column: "z", exit: ExitOK},
		{column: "x", exit: ExitValidation, typ: "list<"},
		{column: "s", exit: ExitValidation, typ: "struct<"},
	} {
		t.Run(tc.column, func(t *testing.T) {
			pipeline := putFixture(t, dir, tc.column+".yml", "input: {type: parquet}\nrules: [{code: R, level: error, column: "+tc.column+", checks: [{type: not_null}]}]\n")
			r := invoke(t, validateArgs(pipeline, input, filepath.Join(dir, "out-"+tc.column), "--json-output", "-")...)
			assertExit(t, r, tc.exit)
			summary := decodeSummary(t, []byte(r.stdout))
			if tc.typ == "" {
				if summary.Total != 5 || summary.Errored != tc.errors || len(summary.SchemaIssues) != 0 {
					t.Fatalf("total/error/schema issues = %d/%d/%v, want 5/%d/none", summary.Total, summary.Errored, summary.SchemaIssues, tc.errors)
				}
				return
			}
			if summary.Status != "schema_rejected" || len(summary.SchemaIssues) != 1 {
				t.Fatalf("status = %q, schema issues = %v, want one schema_rejected issue", summary.Status, summary.SchemaIssues)
			}
			issue := summary.SchemaIssues[0]
			if issue.Column != tc.column || !strings.HasPrefix(issue.Reason, "unsupported Parquet column type "+tc.typ) {
				t.Errorf("issue = %s: %q, want %s: unsupported Parquet column type %s...", issue.Column, issue.Reason, tc.column, tc.typ)
			}
		})
	}
}
