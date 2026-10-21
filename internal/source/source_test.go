package source

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// fixture describes one input object and the configuration used to read it.
type fixture struct {
	name, body  string
	input       config.Input
	transforms  []config.Transform
	recordBytes string // defaults to 1MiB
}

// newDataset writes the fixture (gzip-compressing *.gz names) and runs New.
// Cleanup closes everything and fails the test if managed bytes leak.
func (f fixture) newDataset(t *testing.T) (*Dataset, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), f.name)
	payload := []byte(f.body)
	if strings.HasSuffix(f.name, ".gz") {
		var out bytes.Buffer
		z := gzip.NewWriter(&out)
		if _, err := z.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := z.Close(); err != nil {
			t.Fatal(err)
		}
		payload = out.Bytes()
	}
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	budget := resource.New(128 << 20)
	store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget)
	if err != nil {
		t.Fatal(err)
	}
	recordBytes := f.recordBytes
	if recordBytes == "" {
		recordBytes = "1MiB"
	}
	cfg := &config.Config{
		Input:      f.input,
		Source:     config.Source{URIs: []string{path}},
		Transforms: f.transforms,
		Execution: config.Execution{
			MemoryBudget:   "128MiB",
			BatchTarget:    "16KiB",
			MaxRecordBytes: recordBytes,
			MaxDepth:       64,
		},
	}
	ds, err := New(context.Background(), cfg, store, budget)
	t.Cleanup(func() {
		if ds != nil {
			_ = ds.Close()
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
		if n := budget.Used(); n != 0 {
			t.Errorf("source leaked %d managed bytes", n)
		}
	})
	return ds, err
}

func (f fixture) dataset(t *testing.T) *Dataset {
	t.Helper()
	ds, err := f.newDataset(t)
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func readOne(t *testing.T, ds *Dataset) *data.Batch {
	t.Helper()
	r, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	batch, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batch.Release)
	return batch
}

// readErr reads the first batch and returns its error, releasing any batch.
func readErr(t *testing.T, ds *Dataset) error {
	t.Helper()
	r, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := r.Next(context.Background())
	if err == nil {
		b.Release()
	}
	return err
}

func stringSchema(names ...string) *config.Schema {
	s := &config.Schema{Order: names, Columns: map[string]config.Column{}}
	for _, name := range names {
		s.Columns[name] = config.Column{Type: "string"}
	}
	return s
}

// sourceLines returns the source line of every record in the batch.
func sourceLines(b *data.Batch) []uint64 {
	lines := make([]uint64, len(b.Locations))
	for i, loc := range b.Locations {
		lines[i] = loc.Line
	}
	return lines
}

// assertRecordLimitError fails unless err reports the max_record_bytes limit.
func assertRecordLimitError(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "max_record_bytes") {
		t.Fatalf("read error = %v, want max_record_bytes error", err)
	}
}

func TestCSVRawGzipRenameAndReplay(t *testing.T) {
	// BOM before the header, CRLF record ends, and a bare LF inside a quoted
	// field: the second record therefore starts on line 4.
	body := "\ufeffID,description\r\n" +
		"001,\"hello\nworld\"\r\n" +
		"NULL,last\r\n"
	ds := fixture{
		name:       "rows.csv.gz",
		body:       body,
		input:      config.Input{Type: "csv", Mode: "raw_strings", NullValues: []string{"NULL"}},
		transforms: []config.Transform{{Type: "rename_columns", Mapping: map[string]string{"ID": "id"}}},
	}.dataset(t)
	// Each Open must replay the object from the start with identical results.
	for pass := range 2 {
		t.Run(fmt.Sprintf("open %d", pass+1), func(t *testing.T) {
			r, err := ds.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			b, err := r.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(b.Locations) != 2 {
				t.Fatalf("locations = %d, want 2", len(b.Locations))
			}
			ids := b.Record.Column(0).(*array.String)
			if got := ids.Value(0); got != "001" {
				t.Errorf("raw id row 0 = %q, want %q", got, "001")
			}
			if !ids.IsNull(1) {
				t.Errorf("id row 1 null = false, want true (NULL marker)")
			}
			if got := b.Record.Column(1).(*array.String).Value(0); got != "hello\nworld" {
				t.Errorf("multiline description = %q, want %q", got, "hello\nworld")
			}
			if got := b.Locations[0].Row; got != 1 {
				t.Errorf("row 0 Row = %d, want 1", got)
			}
			if got := b.Locations[1].GlobalRow; got != 2 {
				t.Errorf("row 1 GlobalRow = %d, want 2", got)
			}
			if got := b.Locations[1].SourceOrdinal; got != 1 {
				t.Errorf("row 1 SourceOrdinal = %d, want 1", got)
			}
			if got, want := sourceLines(b), []uint64{2, 4}; !slices.Equal(got, want) {
				t.Errorf("multiline record lines = %v, want %v", got, want)
			}
			b.Release()
			if _, err = r.Next(context.Background()); !errors.Is(err, io.EOF) {
				t.Fatalf("second Next error = %v, want io.EOF", err)
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCSVQuotedHeaderAfterBOMAndDelimiter(t *testing.T) {
	// BOM directly before a quoted header; CRLF inside the quoted field is
	// normalised to LF and pushes the second record onto line 4.
	body := "\ufeff\"id\";\"text\"\r\n" +
		"1;\"a;b\r\nc\"\r\n" +
		"2;x\r\n"
	input := config.Input{Type: "csv", Mode: "raw_strings", Delimiter: ";"}
	ds := fixture{name: "rows.csv", body: body, input: input}.dataset(t)
	if got := ds.Schema().Field(0).Name; got != "id" {
		t.Fatalf("first header = %q, want %q (BOM must not leak)", got, "id")
	}
	b := readOne(t, ds)
	if got := b.Record.Column(1).(*array.String).Value(0); got != "a;b\nc" {
		t.Errorf("quoted field = %q, want %q", got, "a;b\nc")
	}
	if len(b.Locations) < 2 {
		t.Fatalf("locations = %d, want at least 2", len(b.Locations))
	}
	if got := b.Locations[1].Line; got != 4 {
		t.Errorf("second record line = %d, want 4", got)
	}
}

func TestCSVRejectRecordOnlyForInvalidRows(t *testing.T) {
	schema := &config.Schema{Order: []string{"a"}, Columns: map[string]config.Column{"a": {Type: "int64"}}}
	body := `a
1
x
`
	ds := fixture{name: "rows.csv", body: body, input: config.Input{Type: "csv", Schema: schema}}.dataset(t)
	b := readOne(t, ds)
	if b.Raw[0] != nil {
		t.Errorf("valid row reject record = %s, want nil", b.Raw[0])
	}
	if got, want := string(b.Raw[1]), `{"a":"x"}`; got != want {
		t.Errorf("invalid row reject record = %s, want %s", got, want)
	}
}

func TestCSVRecordLimitIsFatal(t *testing.T) {
	ds := fixture{
		name:        "rows.csv",
		body:        "a\n" + strings.Repeat("x", 64) + "\n",
		input:       config.Input{Type: "csv", Mode: "raw_strings"},
		recordBytes: "32B",
	}.dataset(t)
	assertRecordLimitError(t, readErr(t, ds))
}

func TestMalformedCSVIsFatal(t *testing.T) {
	bodies := map[string]string{
		"short record":     "a,b\n1\n",
		"unterminated":     "a,b\n\"unterminated,2\n",
		"bare quote":       "a,b\nun\"quoted,2\n",
		"blank line":       "a,b\n\n1,2\n",
		"trailing blank":   "a,b\n1,2\n\r\n", // a CRLF-only line is still a blank record
		"invalid UTF-8":    "a,b\n\xff,2\n",  // 0xff is never valid UTF-8
		"text after quote": "a,b\n\"x\"y,2\n",
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			ds := fixture{name: "rows.csv", body: body, input: config.Input{Type: "csv", Mode: "raw_strings"}}.dataset(t)
			if err := readErr(t, ds); err == nil {
				t.Fatal("read error = nil, want malformed CSV error")
			}
		})
	}
}

func TestJSONExactConversionAndStates(t *testing.T) {
	no := false
	schema := &config.Schema{
		ExtraFields: "error",
		Order:       []string{"id", "amount", "postcode", "optional"},
		Columns: map[string]config.Column{
			"id":       {Type: "int64", Required: true, Nullable: &no},
			"amount":   {Type: "decimal", Precision: 20, Scale: 2},
			"postcode": {Type: "string", Path: "/address/postcode"},
			"optional": {Type: "string"},
		},
	}
	// Leading blank line: the first record is on line 2.
	body := `
{"id":9007199254740993,"amount":1.2300,"address":{"postcode":"001"}}
{"id":"1.0","amount":null,"address":{"postcode":{},"extra":1},"optional":"NULL"}
{"id":null}
{}
`
	input := config.Input{Type: "ndjson", Mode: "raw_strings", Schema: schema, NullValues: []string{"NULL"}}
	b := readOne(t, fixture{name: "rows.ndjson.gz", body: body, input: input}.dataset(t))

	t.Run("integer above 2^53 stays exact", func(t *testing.T) {
		if got := b.Record.Column(0).(*array.Int64).Value(0); got != 9007199254740993 {
			t.Errorf("id row 0 = %d, want 9007199254740993", got)
		}
	})
	t.Run("decimal is normalised to declared scale", func(t *testing.T) {
		if got := b.Record.Column(1).(*array.Decimal128).ValueStr(0); got != "1.23" {
			t.Errorf("amount row 0 = %q, want %q", got, "1.23")
		}
	})
	t.Run("cell states", func(t *testing.T) {
		for _, s := range []struct {
			column string
			row    int
			want   data.State
		}{
			{column: "id", row: 1, want: data.Invalid},
			{column: "id", row: 2, want: data.Null},
			{column: "id", row: 3, want: data.Missing},
			{column: "optional", row: 0, want: data.Missing},
			{column: "optional", row: 1, want: data.Valid},
		} {
			if got := b.State(s.column, s.row); got != s.want {
				t.Errorf("%s row %d = %v, want %v", s.column, s.row, got, s.want)
			}
		}
	})
	t.Run("blank line counts towards line not row", func(t *testing.T) {
		if got := b.Locations[0].Line; got != 2 {
			t.Errorf("row 0 line = %d, want 2", got)
		}
		if got := b.Locations[0].Row; got != 1 {
			t.Errorf("row 0 Row = %d, want 1", got)
		}
	})
	t.Run("reject record only for invalid row", func(t *testing.T) {
		if b.Raw[0] != nil {
			t.Errorf("valid row reject record = %s, want nil", b.Raw[0])
		}
		want := `{"id":"1.0","amount":null,"address":{"postcode":{},"extra":1},"optional":"NULL"}`
		if got := string(b.Raw[1]); got != want {
			t.Errorf("invalid row reject record = %s, want %s", got, want)
		}
	})
	t.Run("ingest issue codes", func(t *testing.T) {
		codes := map[string]int{}
		for _, issue := range b.Ingest {
			codes[issue.Code]++
		}
		want := map[string]int{"INGEST_TYPE": 2, "INGEST_EXTRA_FIELD": 1, "INGEST_NULL": 1, "INGEST_MISSING_FIELD": 1}
		for code, n := range want {
			if codes[code] != n {
				t.Errorf("%s issues = %d, want %d (all codes: %v)", code, codes[code], n, codes)
			}
		}
	})
}

func TestJSONArrayLineProvenance(t *testing.T) {
	// A blank line and indentation between elements: records start on lines 2 and 4.
	body := `[
{"a":"1"},

  {"a":"2"}
]
`
	input := config.Input{Type: "json", Mode: "raw_strings", Schema: stringSchema("a")}
	b := readOne(t, fixture{name: "rows.json", body: body, input: input}.dataset(t))
	if got, want := sourceLines(b), []uint64{2, 4}; !slices.Equal(got, want) {
		t.Errorf("record lines = %v, want %v", got, want)
	}
}

func TestJSONStructuralFailures(t *testing.T) {
	cases := []struct{ name, kind, body string }{
		{name: "duplicate key", kind: "json", body: `[{"a":1,"a":2}]`},
		{name: "non-object element", kind: "json", body: `[1]`},
		{name: "object instead of array", kind: "json", body: `{"a":1}`},
		{name: "trailing comma", kind: "json", body: `[{"a":1},]`},
		{name: "value after array", kind: "json", body: `[{"a":1}] false`},
		{name: "mismatched brackets", kind: "json", body: `[{"a":[1,2}]`},
		{name: "invalid UTF-8 string", kind: "json", body: "[{\"a\":\"\xff\"}]"}, // 0xff is never valid UTF-8
		{name: "unterminated array", kind: "json", body: `[{"a":1}`},
		{name: "ndjson array line", kind: "ndjson", body: "{\"a\":1}\n[1,2]\n"},
		{name: "ndjson two objects on a line", kind: "ndjson", body: "{\"a\":1} {\"a\":2}\n"},
		{name: "ndjson object spans lines", kind: "ndjson", body: "{\"a\":\n1}\n"},
		{name: "ndjson value after object", kind: "ndjson", body: "{\"a\":1} true\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			input := config.Input{Type: c.kind, Mode: "raw_strings", Schema: stringSchema("a")}
			ds := fixture{name: "rows." + c.kind, body: c.body, input: input}.dataset(t)
			if err := readErr(t, ds); err == nil {
				t.Fatalf("read %s %q error = nil, want malformed JSON error", c.kind, c.body)
			}
		})
	}
}

func TestJSONRecordLimitIncludesSkippedFields(t *testing.T) {
	schema := stringSchema("a")
	schema.ExtraFields = "ignore"
	ds := fixture{
		name:        "rows.ndjson",
		body:        `{"a":"1","skipped":"` + strings.Repeat("x", 64) + `"}` + "\n",
		input:       config.Input{Type: "ndjson", Schema: schema},
		recordBytes: "48B",
	}.dataset(t)
	assertRecordLimitError(t, readErr(t, ds))
}

func TestInferenceFreezesSchema(t *testing.T) {
	body := `id
1
wrong
`
	input := config.Input{Type: "csv", Mode: "infer_types", InferSchemaLength: 1}
	ds := fixture{name: "rows.csv", body: body, input: input}.dataset(t)
	if got := ds.Passes(); got != 1 {
		t.Errorf("passes = %d, want 1 (inference pass reported)", got)
	}
	if err := readErr(t, ds); err == nil {
		t.Error("read error = nil, want error for value incompatible with frozen schema")
	}
}

func TestSchemalessJSONInfersFlatFields(t *testing.T) {
	body := `{"a":1,"b":"x"}
{"a":2.5,"c":true}
`
	ds := fixture{name: "rows.ndjson", body: body, input: config.Input{Type: "ndjson", Mode: "infer_types"}}.dataset(t)
	got := map[string]string{}
	for _, f := range ds.Schema().Fields() {
		got[f.Name] = f.Type.String()
	}
	want := map[string]string{"a": "float64", "b": "utf8", "c": "bool"}
	for name, typ := range want {
		if got[name] != typ {
			t.Errorf("inferred %s = %q, want %q (schema %v)", name, got[name], typ, got)
		}
	}
	if state := readOne(t, ds).State("c", 0); state != data.Missing {
		t.Errorf("c row 0 = %v, want %v", state, data.Missing)
	}
}

func TestRequiredHeaderSchemaRejection(t *testing.T) {
	schema := &config.Schema{
		Order: []string{"required", "optional"},
		Columns: map[string]config.Column{
			"required": {Type: "string", Required: true},
			"optional": {Type: "string"},
		},
		ExtraFields: "error",
	}
	body := `other
value
`
	ds := fixture{name: "rows.csv", body: body, input: config.Input{Type: "csv", Schema: schema}}.dataset(t)
	if got := ds.SchemaIssues(); len(got) != 2 {
		t.Fatalf("schema issues = %v, want 2 (missing required header, extra header)", got)
	}
}

func TestPreflightDoesNotWeakenAbort(t *testing.T) {
	schema := &config.Schema{Order: []string{"a"}, Columns: map[string]config.Column{"a": {Type: "int64"}}}
	body := `a
bad
`
	input := config.Input{Type: "csv", OnTypeError: "abort", Schema: schema}
	ds := fixture{name: "rows.csv", body: body, input: input}.dataset(t)
	r, err := ds.OpenPreflight(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := b.State("a", 0); got != data.Invalid {
		t.Errorf("preflight a row 0 = %v, want %v", got, data.Invalid)
	}
	b.Release()
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := readErr(t, ds); err == nil {
		t.Error("normal read error = nil, want abort on type error")
	}
}

func TestMappingFilesAndOrderedTransforms(t *testing.T) {
	dir := t.TempDir()
	mapping := filepath.Join(dir, "mapping.csv")
	schema := filepath.Join(dir, "schema.json")
	mappingCSV := `source,target
original,id
unused,#N/A
`
	if err := os.WriteFile(mapping, []byte(mappingCSV), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(schema, []byte(`{"vendor":{"id":"key"},"other":{"id":"ignored"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	body := `original
001
`
	// original -> id (mapping file), then id -> key (schema_rename): order matters.
	ds := fixture{
		name:  "rows.csv",
		body:  body,
		input: config.Input{Type: "csv", Mode: "raw_strings"},
		transforms: []config.Transform{
			{Type: "rename_columns", MappingFile: mapping},
			{Type: "schema_rename", SchemaFile: schema, SourceKey: "vendor"},
		},
	}.dataset(t)
	if got := ds.Schema().Field(0).Name; got != "key" {
		t.Errorf("renamed column = %q, want %q", got, "key")
	}
	if got := readOne(t, ds).Record.Column(0).(*array.String).Value(0); got != "001" {
		t.Errorf("raw value = %q, want %q", got, "001")
	}
}

func TestSchemaRenameRejectsDuplicateKeys(t *testing.T) {
	schema := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(schema, []byte(`{"vendor":{"id":"a","id":"b"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	f := fixture{
		name:       "rows.csv",
		body:       "id\n1\n",
		input:      config.Input{Type: "csv", Mode: "raw_strings"},
		transforms: []config.Transform{{Type: "schema_rename", SchemaFile: schema, SourceKey: "vendor"}},
	}
	if _, err := f.newDataset(t); err == nil {
		t.Fatal("New error = nil, want duplicate schema rename key error")
	}
}

func TestEmptyJSONObjectsPreserveRowCount(t *testing.T) {
	ds := fixture{name: "rows.json", body: "[{},{}]", input: config.Input{Type: "json", Mode: "raw_strings"}}.dataset(t)
	b := readOne(t, ds)
	if got := b.Record.NumRows(); got != 2 {
		t.Errorf("rows = %d, want 2", got)
	}
	if got := len(b.Locations); got != 2 {
		t.Errorf("locations = %d, want 2", got)
	}
}

func TestTextOpenFailureDoesNotRetainTypedNil(t *testing.T) {
	ds := fixture{name: "rows.csv", body: "a\n1\n", input: config.Input{Type: "csv", Mode: "raw_strings"}}.dataset(t)
	if err := os.Remove(ds.Objects()[0].URI); err != nil {
		t.Fatal(err)
	}
	reader, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if batch, err := reader.Next(context.Background()); err == nil {
		batch.Release()
		t.Fatal("Next error = nil, want error for removed input")
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
}

type countingReadCloser struct {
	reader io.Reader
	bytes  int
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += n
	return n, err
}

func (*countingReadCloser) Close() error { return nil }

func TestCSVDecoderWorkspaceTracksReadAheadAndRejectsGrowth(t *testing.T) {
	budget := resource.New(512 << 10)
	d := &Dataset{budget: budget, recordLimit: 8 << 20}
	reader := &countingReadCloser{reader: strings.NewReader("column\nsmall\n" + strings.Repeat("x", 64<<10) + "\n")}
	src, err := newTextStream(d, storage.Object{URI: "rows.csv"}, reader, textReadBufferBytes, false)
	if err != nil {
		t.Fatal(err)
	}
	input, err := newCSVInput(d, src, csvOptions{comma: ','})
	if err != nil {
		t.Fatalf("small header with large record ceiling: %v", err)
	}
	if _, err = input.next(context.Background()); err != nil {
		t.Fatalf("small record: %v", err)
	}
	_, err = input.next(context.Background())
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("growing record error = %v, want *resource.ResourceLimitError", err)
	}
	if reader.bytes >= 64<<10 {
		t.Fatalf("read %d bytes before rejecting growth, want less than growing record", reader.bytes)
	}
	if err := input.close(); err != nil {
		t.Fatal(err)
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("budget used after close = %d, want 0", got)
	}
}

func TestCSVDecoderReservesBeforeRead(t *testing.T) {
	budget := resource.New(64 << 10)
	d := &Dataset{budget: budget, recordLimit: 1 << 20}
	reader := &countingReadCloser{reader: strings.NewReader("column\nvalue\n")}
	src, err := newTextStream(d, storage.Object{URI: "rows.csv"}, reader, textReadBufferBytes, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = newCSVInput(d, src, csvOptions{comma: ','})
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("newCSVInput error = %v, want *resource.ResourceLimitError", err)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after failure = %d, want 0", got)
	}
	if reader.bytes != 0 {
		t.Errorf("payload bytes read before reservation failure = %d, want 0", reader.bytes)
	}
}

func TestJSONDecoderReservesBeforeRead(t *testing.T) {
	budget := resource.New(128 << 10)
	d := &Dataset{cfg: &config.Config{}, budget: budget, recordLimit: 1 << 20, maxDepth: 64, kind: inputNDJSON}
	reader := &countingReadCloser{reader: strings.NewReader(`{"value":"payload"}`)}
	src, err := newTextStream(d, storage.Object{URI: "rows.ndjson"}, reader, jsonDecoderFixedBytes, true)
	if err != nil {
		t.Fatal(err)
	}
	input, err := newJSONInput(d, src)
	if input != nil {
		_ = input.close()
	}
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("newJSONInput error = %v, want *resource.ResourceLimitError", err)
	}
	if reader.bytes != 0 {
		t.Errorf("payload bytes read before reservation failure = %d, want 0", reader.bytes)
	}
}

func TestGzipJSONDecoderBudgetFailureIsBounded(t *testing.T) {
	// The decompressed record (>128KiB) exceeds the whole budget.
	var encoded bytes.Buffer
	writer := gzip.NewWriter(&encoded)
	_, _ = writer.Write([]byte(`{"value":"` + strings.Repeat("x", 128<<10) + `"}`))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	budget := resource.New(128 << 10)
	d := &Dataset{cfg: &config.Config{}, budget: budget, recordLimit: 1 << 20, maxDepth: 64, kind: inputNDJSON}
	body := io.NopCloser(bytes.NewReader(encoded.Bytes()))
	src, err := newTextStream(d, storage.Object{URI: "rows.ndjson.gz"}, body, jsonDecoderFixedBytes, true)
	if err != nil {
		t.Fatal(err)
	}
	input, err := newJSONInput(d, src)
	if input != nil {
		_, err = input.next(context.Background())
	}
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("JSON decode error = %v, want *resource.ResourceLimitError", err)
	}
	if input != nil {
		if err := input.close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after failure = %d, want 0", got)
	}
}

// TestTextDiscoveryPinsCompleteLocalIdentity rewrites the file with the same
// size and mtime after discovery; reading must still detect the change.
func TestTextDiscoveryPinsCompleteLocalIdentity(t *testing.T) {
	for _, mode := range []string{"infer_types", "raw_strings"} {
		t.Run(mode, func(t *testing.T) {
			ds := fixture{
				name:  "rows.csv",
				body:  "x\n1\n",
				input: config.Input{Type: "csv", Mode: mode, InferSchemaLength: 1},
			}.dataset(t)
			path := ds.Objects()[0].URI
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, []byte("x\n2\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
				t.Fatal(err)
			}
			reader, err := ds.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for {
				batch, nextErr := reader.Next(context.Background())
				if batch != nil {
					batch.Release()
				}
				if nextErr != nil {
					err = nextErr
					break
				}
			}
			err = errors.Join(err, reader.Close())
			if err == nil || !strings.Contains(err.Error(), "source identity changed") {
				t.Fatalf("read after same-metadata rewrite error = %v, want source identity changed", err)
			}
		})
	}
}
