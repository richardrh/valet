//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestParquetWorkersPreserveRowsValuesAndBucketOrder(t *testing.T) {
	const rowsPerFile = 2051
	const total = 3 * rowsPerFile
	root := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "amount", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	var paths []string
	for file := range 3 {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for row := range rowsPerFile {
			id := int64(file*rowsPerFile + row + 1)
			b.Field(0).(*array.Int64Builder).Append(id)
			amount := id
			if id%5 == 0 {
				amount = -id
			}
			b.Field(1).(*array.Int64Builder).Append(amount)
			if id%7 == 0 {
				b.Field(2).(*array.StringBuilder).AppendNull()
			} else {
				b.Field(2).(*array.StringBuilder).Append("ok")
			}
		}
		record := b.NewRecordBatch()
		b.Release()
		path := filepath.Join(root, fmt.Sprintf("input-%d.parquet", file))
		f, err := os.Create(path)
		if err != nil {
			record.Release()
			t.Fatal(err)
		}
		writer, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(128)), pqarrow.DefaultWriterProps())
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
	p := defaultPipeline(paths[0], "parquet", filepath.Join(root, "output"), "parquet")
	p.Source.URIs = paths
	p.Execution = executionSpec{MemoryBudget: "256MiB", BatchTarget: "16KiB", MaxRecordBytes: "1MiB", Workers: 8}
	p.with(t, `
input:
  schema:
    columns:
      id: {type: int64}
      amount: {type: int64}
      category: {type: string}
rules:
  - code: AMOUNT_NONNEGATIVE
    column: amount
    level: error
    checks: [{type: valid_range, min: 0}]
  - code: CATEGORY_REQUIRED
    column: category
    level: warn
    checks: [{type: not_null}]
`)
	r := runPipeline(t, root, p)
	if r.code != exitValidationFailed {
		t.Fatalf("exit=%d, want1; stderr=%s", r.code, r.stderr)
	}
	if r.summary.Total != total || r.summary.RuleCounts["AMOUNT_NONNEGATIVE"] != total/5 || r.summary.RuleCounts["CATEGORY_REQUIRED"] != total/7 {
		t.Fatalf("rows/check counts lost or duplicated: %+v", r.summary)
	}
	seen := make([]bool, total+1)
	var emitted int
	for _, bucket := range []string{"pass", "warn", "error"} {
		var previous int64
		for _, path := range files(t, filepath.Join(r.dir(t), bucket, "*.parquet")) {
			readParquet(t, path, func(record arrow.RecordBatch, row int) {
				id := arrowColumn(t, record, "id").(*array.Int64).Value(row)
				if id <= previous || id < 1 || id > total || seen[id] {
					t.Fatalf("%s id=%d after%d: duplicate, missing order or invalid value", bucket, id, previous)
				}
				previous = id
				seen[id] = true
				emitted++
				wantBucket := "pass"
				if id%7 == 0 {
					wantBucket = "warn"
				}
				amount := id
				if id%5 == 0 {
					wantBucket = "error"
					amount = -id
				}
				if bucket != wantBucket {
					t.Fatalf("row%d bucket=%s, want%s", id, bucket, wantBucket)
				}
				if got := arrowColumn(t, record, "amount").(*array.Int64).Value(row); got != amount {
					t.Fatalf("row%d amount=%d, want%d", id, got, amount)
				}
				category := arrowColumn(t, record, "category")
				if category.IsNull(row) != (id%7 == 0) {
					t.Fatalf("row%d category nullness changed", id)
				}
				if id%7 != 0 && category.(*array.String).Value(row) != "ok" {
					t.Fatalf("row%d category value changed", id)
				}
				source := (id-1)/rowsPerFile + 1
				sourceRow := (id-1)%rowsPerFile + 1
				for name, want := range map[string]string{
					"__valet_source":         paths[source-1],
					"__valet_source_ordinal": fmt.Sprint(source),
					"__valet_row":            fmt.Sprint(sourceRow),
					"__valet_global_row":     fmt.Sprint(id),
				} {
					if got := arrowColumn(t, record, name).ValueStr(row); got != want {
						t.Fatalf("row%d %s=%q,want%q", id, name, got, want)
					}
				}
			})
		}
	}
	if emitted != total {
		t.Fatalf("emitted=%d, input=%d", emitted, total)
	}
	for id := 1; id <= total; id++ {
		if !seen[id] {
			t.Fatalf("input row%d was not published", id)
		}
	}
}
