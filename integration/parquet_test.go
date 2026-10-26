//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// readParquet decodes one published part with the upstream Arrow reader, not
// the CLI's own source package, and visits every row.
func readParquet(t testing.TB, path string, visit func(rec arrow.RecordBatch, row int)) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	table, err := pqarrow.ReadTable(context.Background(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	defer table.Release()
	reader := array.NewTableReader(table, -1)
	defer reader.Release()
	for reader.Next() {
		rec := reader.RecordBatch()
		for i := range int(rec.NumRows()) {
			visit(rec, i)
		}
	}
}
func arrowColumn(t testing.TB, rec arrow.RecordBatch, name string) arrow.Array {
	t.Helper()
	indices := rec.Schema().FieldIndices(name)
	if len(indices) != 1 {
		t.Fatalf("Parquet column %s matches %d fields, want 1", name, len(indices))
	}
	return rec.Column(indices[0])
}

// parquetGlobalRows returns __valet_global_row for every row of a bucket's
// Parquet parts.
func parquetGlobalRows(t testing.TB, r *runResult, bucket string) []string {
	t.Helper()
	var got []string
	for _, path := range files(t, filepath.Join(r.dir(t), bucket, "*.parquet")) {
		readParquet(t, path, func(rec arrow.RecordBatch, i int) {
			got = append(got, arrowColumn(t, rec, "__valet_global_row").ValueStr(i))
		})
	}
	return got
}

type fixtureBDecimal struct {
	ID, Type, Coefficient string
	Null                  bool
}

// fixtureBDecimals projects the published Parquet values into a comparable
// representation. The calling test owns all behavioral assertions.
func fixtureBDecimals(t testing.TB, r *runResult) []fixtureBDecimal {
	t.Helper()
	var values []fixtureBDecimal
	for _, path := range files(t, filepath.Join(r.dir(t), "pass", "*.parquet")) {
		readParquet(t, path, func(rec arrow.RecordBatch, i int) {
			id := arrowColumn(t, rec, "id").ValueStr(i)
			column := arrowColumn(t, rec, "amount")
			value := fixtureBDecimal{ID: id, Type: fmt.Sprintf("%T", column), Null: column.IsNull(i)}
			if amount, ok := column.(*array.Decimal128); ok {
				typ := amount.DataType().(*arrow.Decimal128Type)
				value.Type = fmt.Sprintf("decimal(%d,%d)", typ.Precision, typ.Scale)
				if !value.Null {
					value.Coefficient = amount.Value(i).BigInt().String()
				}
			}
			values = append(values, value)
		})
	}
	return values
}

func TestParquetMixedPhysicalTypesPreserveEveryInputRow(t *testing.T) {
	root := t.TempDir()
	var paths []string
	for i, typ := range []arrow.DataType{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64} {
		schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: typ}}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		if i == 0 {
			builder.Field(0).(*array.Int32Builder).Append(1)
		} else {
			for id := int64(1000); id < 2000; id++ {
				builder.Field(0).(*array.Int64Builder).Append(id)
			}
		}
		record := builder.NewRecordBatch()
		builder.Release()
		path := filepath.Join(root, fmt.Sprintf("input%d.parquet", i))
		f, err := os.Create(path)
		if err != nil {
			record.Release()
			t.Fatal(err)
		}
		writer, err := pqarrow.NewFileWriter(schema, f, nil, pqarrow.DefaultWriterProps())
		if err != nil {
			record.Release()
			f.Close()
			t.Fatal(err)
		}
		err = writer.Write(record)
		record.Release()
		if closeErr := writer.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}

	// Count the actual input with Arrow independently of valet's reader.
	var inputRows uint64
	for _, path := range paths {
		readParquet(t, path, func(arrow.RecordBatch, int) { inputRows++ })
	}
	p := defaultPipeline(paths[0], "parquet", filepath.Join(root, "output"), "csv")
	p.Source.URIs = paths
	p.Execution.BatchTarget = "16KiB"
	p.Execution.MaxRecordBytes = "1MiB"
	p.with(t, `
input:
  schema:
    columns:
      id: {type: int64}
rules:
  - code: ID_POSITIVE
    column: id
    level: error
    checks:
      - type: valid_range
        min: 0
`)
	r := runPipeline(t, root, p)
	if r.code != exitOK {
		t.Fatalf("exit = %d, want 0; stderr=%s", r.code, r.stderr)
	}
	if r.summary.Total != inputRows || r.summary.Passed != inputRows {
		t.Errorf("checked = %d, passed = %d, input = %d; every input row must be checked", r.summary.Total, r.summary.Passed, inputRows)
	}
	rows := r.rows(t, "pass")
	if uint64(len(rows)) != inputRows {
		t.Fatalf("output rows = %d, input rows = %d", len(rows), inputRows)
	}
	for i, row := range rows {
		id, source, sourceRow := 1, 1, 1
		if i > 0 {
			id, source, sourceRow = 999+i, 2, i
		}
		for name, want := range map[string]int{
			"id": id, "__valet_source_ordinal": source,
			"__valet_row": sourceRow, "__valet_global_row": i + 1,
		} {
			if got := row[name]; got != fmt.Sprint(want) {
				t.Fatalf("output row %d %s = %q, want %d", i+1, name, got, want)
			}
		}
	}
}

func TestSchemaCommandDoesNotClaimRowValidation(t *testing.T) {
	root := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, -2, 3}, nil)
	record := builder.NewRecordBatch()
	builder.Release()
	input := filepath.Join(root, "input.parquet")
	f, err := os.Create(input)
	if err != nil {
		record.Release()
		t.Fatal(err)
	}
	writer, err := pqarrow.NewFileWriter(schema, f, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		record.Release()
		f.Close()
		t.Fatal(err)
	}
	err = writer.Write(record)
	record.Release()
	if closeErr := writer.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	p := defaultPipeline(input, "parquet", filepath.Join(root, "output"), "csv")
	p.with(t, `
input:
  schema:
    columns:
      amount: {type: int64}
rules:
  - code: AMOUNT_POSITIVE
    column: amount
    level: error
    checks: [{type: valid_range, min: 0}]
`)
	path := p.write(t, filepath.Join(root, "pipeline.yml"))
	stdout, stderr, code := invoke(t, "schema", "-p", path)
	if code != exitOK {
		t.Fatalf("schema exit=%d, want 0; stderr=%s", code, stderr)
	}
	var report struct {
		Scope          string `json:"scope"`
		MetadataOnly   bool   `json:"metadata_only"`
		Validation     string `json:"validation"`
		Compatible     bool   `json:"compatible"`
		RowsChecked    uint64 `json:"rows_checked"`
		ObjectsChecked int    `json:"objects_checked"`
		DeclaredRows   uint64 `json:"declared_rows"`
		NotEvaluated   []struct {
			Code   string `json:"code"`
			Status string `json:"status"`
		} `json:"not_evaluated"`
	}
	if err = json.Unmarshal(stdout, &report); err != nil {
		t.Fatal(err)
	}
	if report.Scope != "schema" || !report.MetadataOnly || !report.Compatible || report.Validation != "not_evaluated" || report.RowsChecked != 0 || report.ObjectsChecked != 1 || report.DeclaredRows != 3 {
		t.Fatalf("schema report claims the wrong scope or work: %+v", report)
	}
	skipped := false
	for _, check := range report.NotEvaluated {
		if check.Code == "AMOUNT_POSITIVE" && check.Status == "not_evaluated" {
			skipped = true
		}
	}
	if !skipped {
		t.Fatal("schema report did not disclose the unevaluated row rule")
	}
	if _, err = os.Stat(p.Output.URI); !os.IsNotExist(err) {
		t.Fatalf("schema command created validation output: %v", err)
	}
	r := runPipeline(t, root, p)
	if r.code != exitValidationFailed || r.summary.Total != 3 || r.summary.Passed != 2 || r.summary.Errored != 1 {
		t.Fatalf("full validation did not check the negative row: exit=%d summary=%+v stderr=%s", r.code, r.summary, r.stderr)
	}
}
