package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"go.yaml.in/yaml/v3"
)

type generatedPipeline struct {
	Input struct {
		Type   string `yaml:"type"`
		Mode   string `yaml:"mode"`
		Schema struct {
			ExtraFields string                    `yaml:"extra_fields"`
			Columns     map[string]map[string]any `yaml:"columns"`
		} `yaml:"schema"`
	} `yaml:"input"`
}

// planPipeline runs `valet plan --format pipeline` and checks that the block
// is a pipeline the same input then validates against.
func planPipeline(t *testing.T, dir, input, pipeline string) generatedPipeline {
	t.Helper()
	r := invoke(t, "plan", "-p", pipeline, "-i", input, "--format", "pipeline", "--log-level", "error")
	if r.exit != ExitOK {
		t.Fatalf("plan --format pipeline exit = %d; stderr:\n%s", r.exit, r.stderr)
	}
	var got generatedPipeline
	if err := yaml.Unmarshal([]byte(r.stdout), &got); err != nil {
		t.Fatalf("output is not YAML: %v\n%s", err, r.stdout)
	}
	generated := putFixture(t, dir, "generated.yml", r.stdout+"rules: []\n")
	if v := invoke(t, "validate", "-p", generated, "-i", input, "--log-level", "error"); v.exit != ExitOK {
		t.Fatalf("validate with the generated block exit = %d; block:\n%s\nstderr:\n%s", v.exit, r.stdout, v.stderr)
	}
	return got
}

func TestPlanPrintsPipelineBlockForCSV(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "orders.csv", "order id,amount,qty,ok,note\n1,2.50,3,true,a\n2,3.10,4,false,b\n")
	// The pipeline reads raw strings; the block still carries inferred types.
	pipeline := putFixture(t, dir, "pipeline.yml", "input: {type: csv}\nrules: []\n")
	got := planPipeline(t, dir, input, pipeline)
	if got.Input.Type != "csv" || got.Input.Schema.ExtraFields != "error" {
		t.Errorf("input type=%q extra_fields=%q, want csv and error", got.Input.Type, got.Input.Schema.ExtraFields)
	}
	want := map[string]string{"order id": "int64", "amount": "float64", "qty": "int64", "ok": "bool", "note": "string"}
	if len(got.Input.Schema.Columns) != len(want) {
		t.Errorf("columns = %v, want %v", got.Input.Schema.Columns, want)
	}
	for name, typ := range want {
		if got.Input.Schema.Columns[name]["type"] != typ {
			t.Errorf("column %q type = %v, want %s", name, got.Input.Schema.Columns[name]["type"], typ)
		}
	}
}

func TestPlanPrintsPipelineBlockForParquet(t *testing.T) {
	dir := t.TempDir()
	decimalType := &arrow.Decimal128Type{Precision: 18, Scale: 2}
	timestampType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "amount", Type: decimalType, Nullable: true},
		{Name: "at", Type: timestampType, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	builder.Field(1).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(250), decimal128.FromI64(310)}, nil)
	builder.Field(2).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2}, nil)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	var encoded bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &encoded, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(dir, "orders.parquet")
	if err = os.WriteFile(input, encoded.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	pipeline := putFixture(t, dir, "pipeline.yml", "input: {type: parquet}\nrules: []\n")
	got := planPipeline(t, dir, input, pipeline)
	columns := got.Input.Schema.Columns
	if columns["id"]["type"] != "int32" {
		t.Errorf("id = %v, want int32", columns["id"])
	}
	if columns["amount"]["type"] != "decimal" || columns["amount"]["precision"] != 18 || columns["amount"]["scale"] != 2 {
		t.Errorf("amount = %v, want decimal with precision 18 and scale 2", columns["amount"])
	}
	if columns["at"]["type"] != "timestamp" || columns["at"]["timezone"] != "UTC" {
		t.Errorf("at = %v, want timestamp with timezone UTC", columns["at"])
	}
}

func TestPipelineFormatIsAPlanOption(t *testing.T) {
	dir := t.TempDir()
	pipeline := putFixture(t, dir, "pipeline.yml", "input: {type: csv}\nrules: []\n")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"plan", "-p", pipeline, "--format", "text"}, "plan --format must be json or pipeline"},
		{[]string{"schema", "-p", pipeline, "--format", "pipeline"}, "schema --format must be json or text"},
		{[]string{"validate", "-p", pipeline, "--format", "pipeline"}, "--format is a plan and schema option"},
	} {
		r := invoke(t, tc.args...)
		if r.exit != ExitOperational || !strings.Contains(r.stderr, tc.want) {
			t.Errorf("%v: exit %d, want %d with %q; stderr:\n%s", tc.args, r.exit, ExitOperational, tc.want, r.stderr)
		}
	}
}
