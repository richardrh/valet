package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/source"
)

func schemaParquet(t *testing.T, root string) string {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "raw_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 9}, []bool{true, false})
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	var encoded bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &encoded, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "input.parquet")
	if err = os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSchemaCommandReportsMetadataScopeWithoutPublishing(t *testing.T) {
	root := t.TempDir()
	input := schemaParquet(t, root)
	output := filepath.Join(root, "must-not-exist")
	for _, tc := range []struct {
		name, typ string
		exit      int
	}{
		{"compatible", "int64", ExitOK},
		{"declared physical mismatch", "string", ExitValidation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := putFixture(t, root, "pipeline.yml", fmt.Sprintf(`input:
  type: parquet
  schema:
    columns:
      id: {type: %s, required: true, nullable: false}
transforms:
  - type: rename_columns
    mapping: {raw_id: id}
output:
  uri: %q
rules:
  - code: ROW
    level: error
    column: id
    checks: [{type: not_null}]
  - code: CAST
    level: error
    checks:
      - type: schema_matches
        columns: {id: int64}
`, tc.typ, output))
			r := invoke(t, "schema", "-p", pipeline, "-i", input)
			if r.exit != tc.exit {
				t.Fatalf("schema exit=%d, want %d; stderr=%q; stdout=%q", r.exit, tc.exit, r.stderr, r.stdout)
			}
			var report source.SchemaReport
			if err := json.Unmarshal([]byte(r.stdout), &report); err != nil {
				t.Fatalf("schema JSON: %v: %s", err, r.stdout)
			}
			if report.Scope != "schema" || !report.MetadataOnly || report.Validation != "not_evaluated" || report.RowsChecked != 0 || report.ObjectsChecked != 1 || report.DeclaredRows != 2 {
				t.Fatalf("incorrect report scope/counts: %+v", report)
			}
			if report.Objects[0].Fields[0].Name != "id" || report.Objects[0].Fields[0].SourceName != "raw_id" || report.Objects[0].Fields[0].Type != "int64" {
				t.Fatalf("actual schema overwritten by target: %+v", report.Objects)
			}
			row, cast, nullability := false, false, false
			for _, check := range report.NotEvaluated {
				if check.Code == "ROW" && check.Status == "not_evaluated" {
					row = true
				}
				if check.Code == "CAST" && check.Status == "not_evaluated" {
					cast = true
				}
				if check.Type == "row_ingestion" && strings.Contains(check.Reason, "nullability") {
					nullability = true
				}
			}
			if !row || !cast || !nullability {
				t.Fatalf("data-dependent checks not explicitly unevaluated: %+v", report.NotEvaluated)
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("schema created output root: %v", err)
			}
		})
	}
}

func TestSchemaCommandOperationalFailures(t *testing.T) {
	root := t.TempDir()
	pipeline := putFixture(t, root, "pipeline.yml", "input: {type: parquet}\nrules: []\n")
	bad := putFixture(t, root, "bad.parquet", "not parquet")
	assertDiagnosticOnly(t, invoke(t, "schema", "-p", pipeline, "-i", bad), ExitOperational)
	csvPipeline := putFixture(t, root, "csv.yml", rawCSVPipeline)
	csv := putFixture(t, root, "rows.csv", okCSV)
	assertDiagnosticOnly(t, invoke(t, "schema", "-p", csvPipeline, "-i", csv), ExitOperational)
	assertDiagnosticOnly(t, invoke(t, "schema", "-p", pipeline, "-i", bad, "--output", filepath.Join(root, "out")), ExitOperational)
	assertExit(t, invoke(t, "help", "schema"), ExitOK)
}
