//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// vmSchemaFile writes real typed data, not an implementation-shaped report.
func vmSchemaFile(t testing.TB, path string, fields []arrow.Field) string {
	t.Helper()
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i, field := range b.Fields() {
		if schema.Field(i).Type.ID() == arrow.STRING {
			field.(*array.StringBuilder).Append("VM_PRIVATE_PAYLOAD_MARKER")
		} else {
			field.AppendNull()
		}
	}
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := pqarrow.NewFileWriter(schema, f, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
func vmOneJSON(t testing.TB, raw []byte) map[string]any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v map[string]any
	if err := d.Decode(&v); err != nil {
		t.Fatalf("invalid result JSON: %v: %s", err, raw)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		t.Fatalf("trailing result: %s (%v)", raw, err)
	}
	return v
}
func vmSchemaInvoke(t testing.TB, p *pipeline, root, format string, code int) (map[string]any, string) {
	t.Helper()
	path := p.write(t, filepath.Join(root, "pipeline-"+format+".yml"))
	out, logs, got := invoke(t, "schema", "-p", path, "--format", format, "--log-format", "json")
	if got != code {
		t.Fatalf("schema %s exit %d want %d: %s", format, got, code, logs)
	}
	if bytes.Contains(out, []byte("VM_PRIVATE_PAYLOAD_MARKER")) || bytes.Contains(logs, []byte("VM_PRIVATE_PAYLOAD_MARKER")) {
		t.Fatal("schema leaked payload")
	}
	vmLogRecords(t, logs)
	if _, err := os.Stat(p.Output.URI); !os.IsNotExist(err) {
		t.Fatalf("schema created output: %v", err)
	}
	if format == "text" {
		if bytes.Contains(out, []byte{27}) {
			t.Fatal("raw ANSI in text")
		}
		return nil, string(out)
	}
	v := vmOneJSON(t, out)
	if v["scope"] != "schema" || v["validation"] != "not_evaluated" || v["report_version"] != json.Number("2") || v["rows_checked"] != json.Number("0") {
		t.Fatalf("wrong schema contract: %v", v)
	}
	if v["run_id"] == nil || v["run_id"] == "" {
		t.Fatal("missing correlation")
	}
	return v, string(out)
}
func vmDifferences(t testing.TB, v map[string]any) []any {
	t.Helper()
	d, ok := v["differences"].([]any)
	if !ok {
		t.Fatalf("missing differences: %v", v)
	}
	return d
}
func TestVMSchema(t *testing.T) {
	t.Run("VM-DIFF-01", func(t *testing.T) {
		root := t.TempDir()
		src := vmSchemaFile(t, filepath.Join(root, "actual.parquet"), []arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int32}, {Name: "extra", Type: arrow.BinaryTypes.String}})
		p := defaultPipeline(src, "parquet", filepath.Join(root, "result"), "csv").with(t, `input:
  schema:
    extra_fields: error
    columns:
      amount: {type: int64}
      missing: {type: string, required: true}
`)
		v, _ := vmSchemaInvoke(t, p, root, "json", 1)
		d := vmDifferences(t, v)
		if len(d) != 3 {
			t.Fatalf("want 3 canonical differences: %v", d)
		}
		seen := map[string]bool{}
		for _, x := range d {
			m := x.(map[string]any)
			kind, _ := m["kind"].(string)
			seen[kind] = true
			if kind == "missing" && m["actual"] != nil {
				t.Fatalf("fabricated missing type: %v", m)
			}
			if kind == "unexpected" && m["expected"] != nil {
				t.Fatalf("fabricated extra expectation: %v", m)
			}
		}
		for _, kind := range []string{"missing", "unexpected", "type_mismatch"} {
			if !seen[kind] {
				t.Errorf("missing kind %s", kind)
			}
		}
		_, text := vmSchemaInvoke(t, p, root, "text", 1)
		for _, token := range []string{"amount", "missing", "extra", "int32", "int64"} {
			if !strings.Contains(text, token) {
				t.Errorf("text lost %s: %s", token, text)
			}
		}
	})
	t.Run("VM-DIFF-02", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			actual arrow.DataType
			decl   string
			want   int
		}{
			{"signedness", arrow.PrimitiveTypes.Uint64, "{type: int64}", 1}, {"width", arrow.PrimitiveTypes.Int32, "{type: int64}", 1},
			{"decimal_precision", &arrow.Decimal128Type{Precision: 12, Scale: 2}, "{type: decimal, precision: 10, scale: 2}", 1},
			{"decimal_scale", &arrow.Decimal128Type{Precision: 12, Scale: 3}, "{type: decimal, precision: 12, scale: 2}", 1},
			{"timestamp_unit", &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, "{type: timestamp, timezone: UTC}", 1},
			{"timestamp_zone", &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "Europe/Paris"}, "{type: timestamp, timezone: UTC}", 1},
			{"alias", arrow.PrimitiveTypes.Int64, "{type: integer}", 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				root := t.TempDir()
				src := vmSchemaFile(t, filepath.Join(root, "typed.parquet"), []arrow.Field{{Name: "value", Type: tc.actual}})
				p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv").with(t, "input:\n  schema:\n    columns:\n      value: "+tc.decl+"\n")
				v, _ := vmSchemaInvoke(t, p, root, "json", tc.want)
				d := vmDifferences(t, v)
				if (len(d) > 0) != (tc.want == 1) {
					t.Fatalf("type semantics %v", d)
				}
				_, text := vmSchemaInvoke(t, p, root, "text", tc.want)
				if tc.want == 1 && !strings.Contains(text, "value") {
					t.Fatal("lost typed column")
				}
			})
		}
	})
	t.Run("VM-DIFF-03", func(t *testing.T) {
		root := t.TempDir()
		a := vmSchemaFile(t, filepath.Join(root, "z-first.parquet"), []arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}})
		b := vmSchemaFile(t, filepath.Join(root, "a-second.parquet"), []arrow.Field{{Name: "x", Type: arrow.BinaryTypes.String}})
		p := defaultPipeline(a, "parquet", filepath.Join(root, "out"), "csv")
		p.Source.URIs = []string{a, b}
		v, _ := vmSchemaInvoke(t, p, root, "json", 1)
		for _, x := range vmDifferences(t, v) {
			m := x.(map[string]any)
			if m["expectation_source"] != "baseline_object" {
				t.Fatalf("wrong baseline: %v", m)
			}
			raw, _ := json.Marshal(m)
			if !bytes.Contains(raw, []byte("a-second.parquet")) {
				t.Fatalf("misattributed source: %s", raw)
			}
		}
		_, text := vmSchemaInvoke(t, p, root, "text", 1)
		if !strings.Contains(strings.ToLower(text), "baseline") {
			t.Fatal("text hides baseline")
		}
	})
	t.Run("VM-DIFF-05", func(t *testing.T) {
		root := t.TempDir()
		src := vmSchemaFile(t, filepath.Join(root, "warn.parquet"), []arrow.Field{{Name: "extra", Type: arrow.BinaryTypes.String}})
		p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv").with(t, `rules:
- code: WARN_SCHEMA
  level: warn
  checks:
  - type: schema_matches
    mode: strict
    columns: {missing: int64}
    allow_extra_columns: true
`)
		v, _ := vmSchemaInvoke(t, p, root, "json", 0)
		d := vmDifferences(t, v)
		if len(d) != 1 || d[0].(map[string]any)["severity"] != "warn" {
			t.Fatalf("wrong warning/extra behavior %v", d)
		}
		vmSchemaInvoke(t, p, root, "text", 0)
	})
	t.Run("VM-DIFF-08", func(t *testing.T) {
		for _, kind := range []string{"format", "non-parquet", "malformed", "oversized"} {
			t.Run(kind, func(t *testing.T) {
				root := t.TempDir()
				src := put(t, filepath.Join(root, "bad.parquet"), "PAR1garbagePAR1")
				p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv")
				format := "json"
				if kind == "format" {
					format = "bogus"
					p.Source.URIs = []string{filepath.Join(root, "MUST_NOT_ACCESS")}
				}
				if kind == "non-parquet" {
					p.Input.Type = "csv"
				}
				if kind == "oversized" {
					if err := os.WriteFile(src, []byte{'P', 'A', 'R', '1', 255, 255, 255, 127, 'P', 'A', 'R', '1'}, 0600); err != nil {
						t.Fatal(err)
					}
				}
				path := p.write(t, filepath.Join(root, "p.yml"))
				_, stderr, code := invoke(t, "schema", "-p", path, "--format", format)
				if code != 2 || len(stderr) == 0 {
					t.Fatalf("invalid request exit=%d stderr=%s", code, stderr)
				}
				if _, err := os.Stat(p.Output.URI); !os.IsNotExist(err) {
					t.Fatal("invalid schema produced output")
				}
			})
		}
	})
	t.Run("VM-DIFF-09", func(t *testing.T) {
		root := t.TempDir()
		names := []string{"éclair", "line\nbreak", "\x1b[31mred", "tab\tname"}
		fields := make([]arrow.Field, len(names))
		cols := obj{}
		for i, n := range names {
			fields[i] = arrow.Field{Name: n, Type: arrow.PrimitiveTypes.Int32}
			cols[n] = obj{"type": "int64"}
		}
		src := vmSchemaFile(t, filepath.Join(root, "unicode.parquet"), fields)
		p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv").with(t, fragment(t, obj{"input": obj{"schema": obj{"columns": cols}}}))
		var prior []any
		for _, workers := range []int{1, 4, 1} {
			p.Execution.Workers = workers
			v, _ := vmSchemaInvoke(t, p, root, "json", 1)
			d := vmDifferences(t, v)
			if prior != nil && !reflect.DeepEqual(prior, d) {
				t.Fatal("nondeterministic canonical differences")
			}
			prior = d
			_, text := vmSchemaInvoke(t, p, root, "text", 1)
			if strings.Contains(text, "line\nbreak") || strings.ContainsRune(text, '\x1b') {
				t.Fatal("unescaped injected control")
			}
			for _, token := range []string{"éclair", "int32", "int64"} {
				if !strings.Contains(text, token) {
					t.Fatalf("lost semantic association %q", token)
				}
			}
		}
	})
}

func TestVMSchemaLogicalAttributes(t *testing.T) {
	root := t.TempDir()
	src := vmSchemaFile(t, filepath.Join(root, "logical.parquet"), []arrow.Field{
		{Name: "unsigned", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "money", Type: &arrow.Decimal128Type{Precision: 12, Scale: 3}},
		{Name: "instant", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "Europe/Paris"}},
	})
	p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv").with(t, `input:
  schema:
    columns:
      unsigned: {type: int64}
      money: {type: decimal, precision: 10, scale: 2}
      instant: {type: timestamp, timezone: UTC}
`)
	v, _ := vmSchemaInvoke(t, p, root, "json", 1)
	want := map[string]map[string]any{
		"unsigned": {"bit_width": json.Number("64"), "signed": false},
		"money":    {"precision": json.Number("12"), "scale": json.Number("3")},
		"instant":  {"unit": "ms", "timezone": "Europe/Paris"},
	}
	differences := vmDifferences(t, v)
	if len(differences) != 3 {
		t.Fatalf("VM-DIFF-02 logical differences: %v", differences)
	}
	for _, item := range differences {
		d := item.(map[string]any)
		name := d["column"].(string)
		attrs, ok := d["actual"].(map[string]any)
		if !ok {
			t.Fatalf("missing typed actual: %v", d)
		}
		for key, value := range want[name] {
			if !reflect.DeepEqual(attrs[key], value) {
				t.Errorf("%s.%s=%v want %v", name, key, attrs[key], value)
			}
		}
		if d["kind"] != "type_mismatch" || d["expectation_source"] != "input_schema" {
			t.Fatalf("wrong mismatch origin: %v", d)
		}
	}
	_, text := vmSchemaInvoke(t, p, root, "text", 1)
	for _, token := range []string{"unsigned", "money", "instant", "12", "10", "3", "2", "Europe/Paris", "UTC"} {
		if !strings.Contains(text, token) {
			t.Errorf("text lost logical attribute %q", token)
		}
	}
}

func TestVMSchemaDiscoveryOrder(t *testing.T) {
	root := t.TempDir()
	inputs := filepath.Join(root, "inputs")
	if err := os.Mkdir(inputs, 0700); err != nil {
		t.Fatal(err)
	}
	vmSchemaFile(t, filepath.Join(inputs, "a.parquet"), []arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}})
	vmSchemaFile(t, filepath.Join(inputs, "z.parquet"), []arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}})
	p := defaultPipeline("", "parquet", filepath.Join(root, "out"), "csv")
	p.Source.URIs = nil
	p.Source.Prefix = inputs
	p.Source.Filter = "parquet"
	var previous []any
	for range 2 {
		v, _ := vmSchemaInvoke(t, p, root, "json", 1)
		d := vmDifferences(t, v)
		if !strings.HasSuffix(v["baseline_source"].(string), "a.parquet") || v["baseline_source_ordinal"] != json.Number("1") {
			t.Fatalf("VM-DIFF-03 discovery baseline %v", v)
		}
		if len(d) != 1 || !strings.HasSuffix(d[0].(map[string]any)["source"].(string), "z.parquet") {
			t.Fatalf("discovery source attribution %v", d)
		}
		if previous != nil && !reflect.DeepEqual(previous, d) {
			t.Fatal("discovery difference ordering changed")
		}
		previous = d
	}
}
