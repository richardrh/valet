package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

var errInjected = errors.New("injected output failure")

type fakeStore struct {
	objects              map[string][]byte
	active               map[string]*fakeOutput
	events               []string
	failClose, failWrite string
}
type fakeOutput struct {
	store *fakeStore
	uri   string
	bytes.Buffer
	aborted bool
	closed  bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[string][]byte{}, active: map[string]*fakeOutput{}}
}

// CreateRunRoot claims the local run directory exactly as storage does, so
// run-directory exclusivity is exercised without a real Store.
func (s *fakeStore) CreateRunRoot(_ context.Context, uri string) error {
	if err := os.MkdirAll(filepath.Dir(uri), 0755); err != nil {
		return err
	}
	return os.Mkdir(uri, 0755)
}
func (s *fakeStore) Create(_ context.Context, uri string) (storage.Writer, error) {
	out := &fakeOutput{store: s, uri: uri}
	s.active[uri] = out
	return out, nil
}

// object returns the published bytes of name under the writer's run root.
func (s *fakeStore) object(t *testing.T, w *Writer, name string) []byte {
	t.Helper()
	b, ok := s.objects[storage.Join(w.root, name)]
	if !ok {
		t.Fatalf("object %s not published", name)
	}
	return b
}

func (o *fakeOutput) Write(b []byte) (int, error) {
	if o.store.failWrite != "" && strings.Contains(filepath.ToSlash(o.uri), o.store.failWrite) {
		return 0, errInjected
	}
	return o.Buffer.Write(b)
}
func (o *fakeOutput) Close() error {
	if o.store.failClose != "" && strings.Contains(filepath.ToSlash(o.uri), o.store.failClose) {
		return errInjected
	}
	o.closed = true
	o.store.objects[o.uri] = bytes.Clone(o.Bytes())
	delete(o.store.active, o.uri)
	o.store.events = append(o.store.events, "close "+o.uri)
	return nil
}
func (o *fakeOutput) Abort() error { o.aborted = true; delete(o.store.active, o.uri); return nil }

// fixture is three rows: row 1 passes, row 2 fails decimal conversion, row 3
// warns and has a null amount.
func fixture(t *testing.T) (*data.Batch, *data.Result, *data.Summary) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true},
		{Name: "large", Type: arrow.PrimitiveTypes.Uint64},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).AppendValues([]string{"0007", "bad", "with,\"quote\"\nline"}, nil)
	builder.Field(1).(*array.Decimal128Builder).AppendValues(
		[]decimal128.Num{decimal128.FromI64(1230), {}, {}},
		[]bool{true, false, false},
	)
	// 2^53+1 is the first integer float64 cannot represent exactly.
	builder.Field(2).(*array.Uint64Builder).AppendValues([]uint64{math.MaxUint64, 9007199254740993, 7}, nil)
	b := &data.Batch{
		Record: builder.NewRecordBatch(),
		States: map[string][]data.State{"amount": {data.Valid, data.Invalid, data.Null}},
		Raw:    []json.RawMessage{nil, json.RawMessage(`{"id":"bad","amount":"oops"}`), nil},
	}
	for row := range 3 {
		b.Locations = append(b.Locations, data.Location{
			Source:        "input.csv",
			SourceOrdinal: 1,
			Row:           uint64(row + 1),
			GlobalRow:     uint64(9007199254740993 + row),
		})
	}
	ingest := data.Issue{
		Location: b.Locations[1],
		Code:     "INGEST_TYPE",
		Severity: "error",
		Origin:   "ingest",
		Scope:    "row",
		Column:   "amount",
		Reason:   "INVALID_DECIMAL",
	}
	b.Ingest = []data.Issue{ingest}
	r := &data.Result{
		Buckets: []data.Bucket{data.BucketPass, data.BucketError, data.BucketWarn},
		Issues: []data.Issue{
			ingest,
			{Location: b.Locations[1], Code: "WARNING", Severity: "warning", Origin: "rule", Scope: "row"},
			{Location: b.Locations[2], Code: "WARNING", Severity: "warning", Origin: "rule", Scope: "row"},
		},
	}
	summary := data.NewSummary()
	summary.Total = 3
	summary.Passed = 1
	summary.Warned = 1
	summary.Errored = 1
	summary.Validation = "fail"
	summary.IngestCounts["INGEST_TYPE"] = 1
	summary.RuleCounts["WARNING"] = 2
	summary.DatasetIssues = []data.Issue{{Code: "DATASET", Severity: "error", Origin: "dataset", Scope: "dataset"}}
	return b, r, summary
}

// passOnlyFixture is fixture with every row passing and no ingest metadata.
func passOnlyFixture(t *testing.T) (*data.Batch, *data.Result) {
	t.Helper()
	batch, result, _ := fixture(t)
	result.Buckets = []data.Bucket{data.BucketPass, data.BucketPass, data.BucketPass}
	result.Issues = nil
	batch.States = nil
	batch.Ingest = nil
	return batch, result
}

func passedSummary(total uint64) *data.Summary {
	summary := data.NewSummary()
	summary.Total = total
	summary.Passed = total
	return summary
}

func testConfig(t *testing.T, format string) *config.Config {
	t.Helper()
	return &config.Config{
		Output:    config.Output{URI: t.TempDir(), Format: format, PartSize: "1MiB"},
		Execution: config.Execution{MemoryBudget: "64MiB", BatchTarget: "1MiB", MaxRecordBytes: "1MiB"},
		Rules:     []config.Rule{{Code: "WARNING", Level: "warning", Message: "warning message"}},
	}
}

// startWriter opens a writer for run "run" and records an empty input list.
// Callers own Abort.
func startWriter(
	t *testing.T, cfg *config.Config, store *fakeStore, budget *resource.Budget, schema *arrow.Schema,
) *Writer {
	t.Helper()
	w, err := newWriter(context.Background(), cfg, store, budget, "run", schema)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteInputs(nil); err != nil {
		w.Abort()
		t.Fatal(err)
	}
	return w
}

func decodeLines(t *testing.T, b []byte) []map[string]json.RawMessage {
	t.Helper()
	var out []map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(b))
	for {
		var record map[string]json.RawMessage
		err := decoder.Decode(&record)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, record)
	}
}

// rawField returns the raw JSON text of key in each record.
func rawField(records []map[string]json.RawMessage, key string) []string {
	out := make([]string, len(records))
	for i, record := range records {
		out[i] = string(record[key])
	}
	return out
}

func unmarshalField(t *testing.T, record map[string]json.RawMessage, key string, v any) {
	t.Helper()
	if err := json.Unmarshal(record[key], v); err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
}

func readCSV(t *testing.T, b []byte) [][]string {
	t.Helper()
	rows, err := csv.NewReader(bytes.NewReader(b)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func csvColumn(rows [][]string, column int) []string {
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = row[column]
	}
	return out
}

func issueCodes(issues []data.Issue) []string {
	out := make([]string, len(issues))
	for i, issue := range issues {
		out[i] = issue.Code
	}
	return out
}

func assertReference(t *testing.T, store *fakeStore, ref Reference) {
	t.Helper()
	b, ok := store.objects[ref.URI]
	if !ok {
		t.Fatalf("referenced object %s missing", ref.URI)
	}
	if got := int64(len(b)); got != ref.Size {
		t.Errorf("%s size = %d, want referenced %d", ref.URI, got, ref.Size)
	}
	hash := sha256.Sum256(b)
	if got := hex.EncodeToString(hash[:]); got != ref.SHA256 {
		t.Errorf("%s sha256 = %s, want referenced %s", ref.URI, got, ref.SHA256)
	}
}

var fixtureCSVHeader = []string{
	"id", "amount", "large", "__valet_source", "__valet_source_ordinal", "__valet_row", "__valet_global_row",
}

type publishedRun struct {
	store    *fakeStore
	budget   *resource.Budget
	writer   *Writer
	manifest string
}

// publishCSVFixture writes the full fixture with one recorded input and
// completes the run.
func publishCSVFixture(t *testing.T) publishedRun {
	t.Helper()
	cfg := testConfig(t, "csv")
	store := newFakeStore()
	budget := resource.New(64 << 20)
	batch, result, summary := fixture(t)
	defer batch.Release()
	w, err := newWriter(context.Background(), cfg, store, budget, "run", batch.Record.Schema())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err = w.WriteInputs([]storage.Object{{URI: "input.csv", Size: 123, ETag: "identity"}}); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteBatch(batch, result); err != nil {
		t.Fatal(err)
	}
	manifest, err := w.Complete(summary)
	if err != nil {
		t.Fatal(err)
	}
	return publishedRun{store: store, budget: budget, writer: w, manifest: manifest}
}

func TestCSVExactValuesProvenanceAndOrderedReports(t *testing.T) {
	run := publishCSVFixture(t)
	store, w := run.store, run.writer

	t.Run("managed bytes are released", func(t *testing.T) {
		if got := run.budget.Used(); got != 0 {
			t.Errorf("budget used after Complete = %d, want 0", got)
		}
	})

	t.Run("pass part keeps exact values and provenance", func(t *testing.T) {
		got := readCSV(t, store.object(t, w, "pass/part-000001.csv"))
		want := [][]string{
			fixtureCSVHeader,
			{"0007", "12.30", "18446744073709551615", "input.csv", "1", "1", "9007199254740993"},
		}
		if !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("pass CSV = %q, want %q", got, want)
		}
	})

	t.Run("warn part quotes text and encodes null", func(t *testing.T) {
		got := readCSV(t, store.object(t, w, "warn/part-000001.csv"))
		want := [][]string{
			fixtureCSVHeader,
			{"with,\"quote\"\nline", `\N`, "7", "input.csv", "1", "3", "9007199254740995"},
		}
		if !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("warn CSV = %q, want %q", got, want)
		}
	})

	issueBytes := store.object(t, w, "issues/part-000001.ndjson")
	issues := decodeLines(t, issueBytes)

	t.Run("issues stream ingest, rule, then dataset", func(t *testing.T) {
		got := rawField(issues, "code")
		want := []string{`"INGEST_TYPE"`, `"WARNING"`, `"WARNING"`, `"DATASET"`}
		if !slices.Equal(got, want) {
			t.Fatalf("issue codes = %s, want %s", got, want)
		}
		if row, ok := issues[3]["row"]; ok {
			t.Errorf("dataset issue row = %s, want absent", row)
		}
	})

	t.Run("issues keep exact global row", func(t *testing.T) {
		if len(issues) == 0 {
			t.Fatal("issues = none, want ingest issue first")
		}
		if got, want := string(issues[0]["global_row"]), "9007199254740994"; got != want {
			t.Errorf("ingest issue global_row = %s, want %s", got, want)
		}
	})

	t.Run("issues omit raw input", func(t *testing.T) {
		if bytes.Contains(issueBytes, []byte("oops")) {
			t.Errorf("issues contain raw input %q:\n%s", "oops", issueBytes)
		}
	})

	t.Run("reject keeps raw input, states and conversion reason", func(t *testing.T) {
		rejects := decodeLines(t, store.object(t, w, "rejects/part-000001.ndjson"))
		if len(rejects) != 1 {
			t.Fatalf("rejects = %d records, want 1: %s", len(rejects), rejects)
		}
		reject := rejects[0]
		if !bytes.Contains(reject["raw"], []byte("oops")) {
			t.Errorf("reject raw = %s, want it to contain %q", reject["raw"], "oops")
		}

		var states []rejectState
		unmarshalField(t, reject, "states", &states)
		wantStates := []rejectState{
			{Column: "id", State: data.Valid},
			{Column: "amount", State: data.Invalid},
			{Column: "large", State: data.Valid},
		}
		if !slices.Equal(states, wantStates) {
			t.Errorf("reject states = %+v, want %+v", states, wantStates)
		}

		var diagnostics []data.Issue
		unmarshalField(t, reject, "diagnostics", &diagnostics)
		type reason struct{ Column, Code, Reason string }
		var gotReasons []reason
		for _, d := range diagnostics {
			gotReasons = append(gotReasons, reason{Column: d.Column, Code: d.Code, Reason: d.Reason})
		}
		wantReasons := []reason{{Column: "amount", Code: "INGEST_TYPE", Reason: "INVALID_DECIMAL"}}
		if !slices.Equal(gotReasons, wantReasons) {
			t.Errorf("reject diagnostics = %+v, want %+v", gotReasons, wantReasons)
		}
	})

	t.Run("manifest references match stored bytes", func(t *testing.T) {
		var manifest struct {
			Artifacts Reference `json:"artifacts"`
			Inputs    Reference `json:"inputs"`
			Summary   Reference `json:"summary"`
			Rules     Reference `json:"rules"`
		}
		if err := json.Unmarshal(store.objects[run.manifest], &manifest); err != nil {
			t.Fatal(err)
		}
		for _, ref := range []Reference{manifest.Artifacts, manifest.Inputs, manifest.Summary, manifest.Rules} {
			assertReference(t, store, ref)
		}

		const wantArtifacts = 5
		if manifest.Artifacts.Count != wantArtifacts {
			t.Errorf("manifest artifacts count = %d, want %d", manifest.Artifacts.Count, wantArtifacts)
		}
		artifacts := decodeLines(t, store.objects[manifest.Artifacts.URI])
		if len(artifacts) != wantArtifacts {
			t.Errorf("artifact records = %d, want %d", len(artifacts), wantArtifacts)
		}
		for _, record := range artifacts {
			encoded, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			var ref Reference
			if err = json.Unmarshal(encoded, &ref); err != nil {
				t.Fatal(err)
			}
			assertReference(t, store, ref)
		}
	})

	t.Run("manifest is published last", func(t *testing.T) {
		if got, want := store.events[len(store.events)-1], "close "+run.manifest; got != want {
			t.Errorf("last store event = %q, want %q (all events %q)", got, want, store.events)
		}
	})

	t.Run("no outputs left open", func(t *testing.T) {
		if got := len(store.active); got != 0 {
			t.Errorf("open outputs = %d, want 0", got)
		}
	})
}

func TestParquetPreservesTypesAndBatchOwnership(t *testing.T) {
	cfg := testConfig(t, "parquet")
	store := newFakeStore()
	budget := resource.New(64 << 20)
	batch, result, summary := fixture(t)
	w, err := newWriter(context.Background(), cfg, store, budget, "run", batch.Record.Schema())
	if err != nil {
		batch.Release()
		t.Fatal(err)
	}
	defer w.Abort()
	if err = w.WriteInputs(nil); err != nil {
		batch.Release()
		t.Fatal(err)
	}
	if err = w.WriteBatch(batch, result); err != nil {
		batch.Release()
		t.Fatal(err)
	}
	// Parts close only after the caller's batch has been released.
	batch.Release()
	if _, err = w.Complete(summary); err != nil {
		t.Fatal(err)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after Complete = %d, want 0", got)
	}

	reader, err := file.NewParquetReader(bytes.NewReader(store.object(t, w, "pass/part-000001.parquet")))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	properties := pqarrow.ArrowReadProperties{BatchSize: 16}
	arrowReader, err := pqarrow.NewFileReader(reader, properties, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	records, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer records.Release()
	if !records.Next() {
		t.Fatalf("pass Parquet has no rows, want 1 (err %v)", records.Err())
	}
	rec := records.RecordBatch()

	if got, want := rec.Column(0).(*array.String).Value(0), "0007"; got != want {
		t.Errorf("id = %q, want %q", got, want)
	}
	if got, want := rec.Column(1).(*array.Decimal128).Value(0), decimal128.FromI64(1230); got != want {
		t.Errorf("amount = %v, want %v", got, want)
	}
	if got, want := rec.Column(2).(*array.Uint64).Value(0), uint64(math.MaxUint64); got != want {
		t.Errorf("large = %d, want %d", got, want)
	}
	if got, want := rec.Column(6).(*array.Uint64).Value(0), uint64(9007199254740993); got != want {
		t.Errorf("__valet_global_row = %d, want %d", got, want)
	}
	decimal := rec.Column(1).DataType().(*arrow.Decimal128Type)
	if decimal.Precision != 18 || decimal.Scale != 2 {
		t.Errorf("amount type = %s, want decimal(18, 2)", decimal)
	}
}

func TestPublicationFailuresAbortWithoutManifest(t *testing.T) {
	for _, tc := range []struct {
		name             string
		inject           func(*fakeStore)
		cancelAfterWrite bool
	}{
		{name: "bucket_write", inject: func(s *fakeStore) { s.failWrite = "pass/" }},
		{name: "bucket_close", inject: func(s *fakeStore) { s.failClose = "pass/" }},
		{name: "artifact_close", inject: func(s *fakeStore) { s.failClose = "artifacts.ndjson" }},
		{name: "summary_close", inject: func(s *fakeStore) { s.failClose = "summary.json" }},
		{name: "manifest_close", inject: func(s *fakeStore) { s.failClose = "manifest.json" }},
		{name: "cancellation", inject: func(*fakeStore) {}, cancelAfterWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, "csv")
			store := newFakeStore()
			budget := resource.New(64 << 20)
			batch, result, summary := fixture(t)
			defer batch.Release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w, err := newWriter(ctx, cfg, store, budget, "run", batch.Record.Schema())
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if err = w.WriteInputs(nil); err != nil {
				t.Fatal(err)
			}

			tc.inject(store)
			err = w.WriteBatch(batch, result)
			if tc.cancelAfterWrite {
				cancel()
			}
			if err == nil {
				_, err = w.Complete(summary)
			}

			if err == nil {
				t.Fatal("WriteBatch/Complete err = nil, want operational failure")
			}
			if _, ok := store.objects[storage.Join(w.root, "manifest.json")]; ok {
				t.Error("manifest.json published for failed run, want absent")
			}
			if got := len(store.active); got != 0 {
				t.Errorf("open outputs after failure = %d, want 0", got)
			}
			if got := budget.Used(); got != 0 {
				t.Errorf("budget used after failure = %d, want 0", got)
			}
		})
	}
}

func TestEmptySchemaRejectionHasNoBucketParts(t *testing.T) {
	store := newFakeStore()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil)
	w := startWriter(t, testConfig(t, "csv"), store, resource.New(1<<20), schema)
	defer w.Abort()
	summary := data.NewSummary()
	summary.Status = "schema_rejected"
	summary.Validation = "fail"
	summary.RowsScanned = 100
	summary.SchemaIssues = []data.Issue{{Code: "SCHEMA", Scope: "schema", Origin: "rule", Severity: "error"}}
	if _, err := w.Complete(summary); err != nil {
		t.Fatal(err)
	}

	for uri := range store.objects {
		for _, bucket := range []string{"/pass/", "/warn/", "/error/"} {
			if strings.Contains(uri, bucket) {
				t.Errorf("bucket part %s published, want no %s parts", uri, bucket)
			}
		}
	}
	issues := decodeLines(t, store.object(t, w, "issues/part-000001.ndjson"))
	if len(issues) == 0 {
		t.Fatal("issues = none, want schema issue")
	}
	if row, ok := issues[0]["row"]; ok {
		t.Errorf("schema issue row = %s, want absent", row)
	}
}

func TestSmallPartTargetRollsWithoutRetainingParts(t *testing.T) {
	const batches = 10
	cfg := testConfig(t, "csv")
	cfg.Output.PartSize = "1B"
	budget := resource.New(8 << 20)
	batch, result := passOnlyFixture(t)
	defer batch.Release()
	w := startWriter(t, cfg, newFakeStore(), budget, batch.Record.Schema())
	defer w.Abort()
	for range batches {
		if err := w.WriteBatch(batch, result); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Complete(passedSummary(3 * batches)); err != nil {
		t.Fatal(err)
	}

	passParts := w.sequences[0]
	if passParts < batches {
		t.Errorf("pass parts = %d, want at least %d", passParts, batches)
	}
	if got := w.artifacts.count; got != passParts {
		t.Errorf("artifact records = %d, want one per pass part (%d)", got, passParts)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after Complete = %d, want 0", got)
	}
}

func TestExistingRunDirectoryIsNotOverwritten(t *testing.T) {
	cfg := testConfig(t, "csv")
	store := newFakeStore()
	budget := resource.New(1 << 20)
	schema := arrow.NewSchema(nil, nil)
	first, err := newWriter(context.Background(), cfg, store, budget, "same", schema)
	if err != nil {
		t.Fatal(err)
	}
	first.Abort()
	if _, err = newWriter(context.Background(), cfg, store, budget, "same", schema); err == nil {
		t.Error("second newWriter for run \"same\" err = nil, want existing-directory error")
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after rejected constructor = %d, want 0", got)
	}
}

func TestParquetResourceFailureReleasesPartialEncoder(t *testing.T) {
	// Leaves enough budget to start encoding but not to finish the batch.
	const headroom = 120000
	store := newFakeStore()
	budget := resource.New(2 << 20)
	batch, result, _ := fixture(t)
	defer batch.Release()
	w := startWriter(t, testConfig(t, "parquet"), store, budget, batch.Record.Schema())
	held := budget.Limit() - budget.Used() - headroom
	if err := budget.Reserve(held, "test pressure"); err != nil {
		t.Fatal(err)
	}

	// The Arrow encoder reports budget exhaustion by panicking with the limit
	// error; the writer may also return it.
	exhausted := false
	func() {
		defer func() {
			if value := recover(); value != nil {
				if _, ok := value.(*resource.ResourceLimitError); !ok {
					panic(value)
				}
				exhausted = true
			}
		}()
		if err := w.WriteBatch(batch, result); err != nil {
			var limit *resource.ResourceLimitError
			if !errors.As(err, &limit) {
				t.Fatalf("WriteBatch err = %v, want *resource.ResourceLimitError", err)
			}
			exhausted = true
		}
	}()
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	budget.Release(held)

	if !exhausted {
		t.Fatal("WriteBatch under pressure succeeded, want budget exhaustion")
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after Abort = %d, want 0", got)
	}
	if got := len(store.active); got != 0 {
		t.Errorf("open outputs after Abort = %d, want 0", got)
	}
	if _, ok := store.objects[storage.Join(w.root, "manifest.json")]; ok {
		t.Error("manifest.json published after resource failure, want absent")
	}
}

func TestConstructorResourceFailureDoesNotRetainState(t *testing.T) {
	// Too small for the writer's own report streams.
	const tinyBudget = 10000
	store := newFakeStore()
	budget := resource.New(tinyBudget)
	_, err := newWriter(context.Background(), testConfig(t, "csv"), store, budget, "run", arrow.NewSchema(nil, nil))
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("newWriter err = %v, want *resource.ResourceLimitError", err)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after failed constructor = %d, want 0", got)
	}
	if got := len(store.active); got != 0 {
		t.Errorf("open outputs after failed constructor = %d, want 0", got)
	}
}

func TestParquetRowGroupsAndPartsStayBounded(t *testing.T) {
	store := newFakeStore()
	budget := resource.New(64 << 20)
	batch, result := passOnlyFixture(t)
	defer batch.Release()
	cfg := testConfig(t, "parquet")
	// A tiny group byte target exercises the footer cap independently of the
	// part byte target, including when many groups arrive in the same batch.
	cfg.Execution.BatchTarget = "1B"
	cfg.Output.PartSize = "32MiB"
	w := startWriter(t, cfg, store, budget, batch.Record.Schema())
	defer w.Abort()
	for range maxRowGroups + 1 {
		if err := w.WriteBatch(batch, result); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Complete(passedSummary(uint64(3 * (maxRowGroups + 1)))); err != nil {
		t.Fatal(err)
	}

	if got := w.sequences[0]; got < 2 {
		t.Errorf("pass parts = %d, want at least 2 once row groups exceed %d", got, maxRowGroups)
	}
	for uri, b := range store.objects {
		if strings.HasSuffix(uri, ".parquet") {
			assertParquetPartBounded(t, uri, b)
			reader, err := file.NewParquetReader(bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			for group := range reader.NumRowGroups() {
				if got := reader.MetaData().RowGroup(group).NumRows(); got != 1 {
					t.Errorf("%s group %d rows = %d, want 1 for tiny byte target", uri, group, got)
				}
			}
			reader.Close()
		}
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after Complete = %d, want 0", got)
	}
}

// assertParquetPartBounded checks a part stays within maxRowGroups and has no
// column statistics, whose size the budget does not account for.
func assertParquetPartBounded(t *testing.T, uri string, b []byte) {
	t.Helper()
	reader, err := file.NewParquetReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	groups := reader.NumRowGroups()
	if groups > maxRowGroups {
		t.Fatalf("%s row groups = %d, want at most %d", uri, groups, maxRowGroups)
	}
	for group := range groups {
		metadata := reader.MetaData().RowGroup(group)
		for column := range metadata.NumColumns() {
			chunk, err := metadata.ColumnChunk(column)
			if err != nil {
				t.Fatal(err)
			}
			set, err := chunk.StatsSet()
			if err != nil {
				t.Fatal(err)
			}
			if set {
				t.Fatalf("%s group %d column %d has statistics, want none", uri, group, column)
			}
		}
	}
}

func TestCSVNullEmptyAndBackslashesAreReversible(t *testing.T) {
	store := newFakeStore()
	schema := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]string{"", `\N`, `a\b`, ""}, []bool{true, true, true, false})
	values := builder.NewArray()
	defer values.Release()
	batch := &data.Batch{Record: array.NewRecordBatch(schema, []arrow.Array{values}, 4)}
	defer batch.Release()
	for row := range 4 {
		batch.Locations = append(batch.Locations, data.Location{
			Source:        `C:\input.csv`,
			SourceOrdinal: 1,
			Row:           uint64(row + 1),
			GlobalRow:     uint64(row + 1),
		})
	}
	w := startWriter(t, testConfig(t, "csv"), store, resource.New(8<<20), schema)
	defer w.Abort()
	result := &data.Result{Buckets: []data.Bucket{data.BucketPass, data.BucketPass, data.BucketPass, data.BucketPass}}
	if err := w.WriteBatch(batch, result); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Complete(passedSummary(4)); err != nil {
		t.Fatal(err)
	}

	rows := readCSV(t, store.object(t, w, "pass/part-000001.csv"))
	if len(rows) != 5 {
		t.Fatalf("pass CSV = %d rows, want header + 4: %q", len(rows), rows)
	}
	records := rows[1:]
	// Backslashes double so the null marker \N stays unambiguous.
	wantText := []string{"", `\\N`, `a\\b`, `\N`}
	if got := csvColumn(records, 0); !slices.Equal(got, wantText) {
		t.Errorf("text column = %q, want %q", got, wantText)
	}
	wantSource := []string{`C:\\input.csv`, `C:\\input.csv`, `C:\\input.csv`, `C:\\input.csv`}
	if got := csvColumn(records, 1); !slices.Equal(got, wantSource) {
		t.Errorf("source column = %q, want %q", got, wantSource)
	}

	type csvEncoding struct {
		Null   string `json:"csv_null"`
		Escape string `json:"csv_string_escape"`
	}
	var got csvEncoding
	if err := json.Unmarshal(store.object(t, w, "rules.json"), &got); err != nil {
		t.Fatal(err)
	}
	if want := (csvEncoding{Null: `\N`, Escape: "double_backslash"}); got != want {
		t.Errorf("rules.json CSV encoding = %+v, want %+v", got, want)
	}
}

func TestPreflightSchemaIssuesStreamWithoutDuplicates(t *testing.T) {
	store := newFakeStore()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil)
	w := startWriter(t, testConfig(t, "csv"), store, resource.New(8<<20), schema)
	defer w.Abort()
	issue := data.Issue{
		Location: data.Location{Source: "input.csv", SourceOrdinal: 1, Row: 42, GlobalRow: 42},
		Code:     "SCHEMA",
		Severity: "error",
		Origin:   "rule",
		Scope:    "schema",
		Column:   "id",
	}
	if err := w.WriteIssue(issue); err != nil {
		t.Fatal(err)
	}
	summary := data.NewSummary()
	summary.Status = "schema_rejected"
	summary.Validation = "fail"
	summary.RowsScanned = 42
	summary.SchemaIssues = []data.Issue{
		{Code: "SCHEMA", Scope: "schema", Severity: "error", Origin: "rule", Column: "id", Count: 1},
		{Code: "OTHER", Scope: "schema", Severity: "warn", Origin: "rule", Column: "other", Reason: "TYPE_MISMATCH"},
	}
	if _, err := w.Complete(summary); err != nil {
		t.Fatal(err)
	}

	issues := decodeLines(t, store.object(t, w, "issues/part-000001.ndjson"))
	if len(issues) != 2 {
		t.Fatalf("issues = %d records, want streamed SCHEMA + OTHER without duplicate: %s", len(issues), issues)
	}
	if got := string(issues[0]["row"]); got != "42" {
		t.Errorf("streamed SCHEMA issue row = %s, want 42", got)
	}
	if got := string(issues[1]["code"]); got != `"OTHER"` {
		t.Errorf("second issue code = %s, want \"OTHER\"", got)
	}
}

func TestRejectStatesAndDiagnosticsAreIndexedByRow(t *testing.T) {
	store := newFakeStore()
	budget := resource.New(8 << 20)
	batch, result, summary := fixture(t)
	defer batch.Release()
	batch.States["id"] = []data.State{data.Valid, data.Missing, data.Valid}
	batch.States["large"] = []data.State{data.Valid, data.Null, data.Valid}
	missing := data.Issue{
		Location: batch.Locations[1],
		Code:     "INGEST_MISSING_FIELD",
		Severity: "error",
		Origin:   "ingest",
		Scope:    "row",
		Column:   "id",
		Reason:   "REQUIRED",
	}
	null := data.Issue{
		Location: batch.Locations[2],
		Code:     "INGEST_NULL",
		Severity: "error",
		Origin:   "ingest",
		Scope:    "row",
		Column:   "amount",
		Reason:   "NOT_NULLABLE",
	}
	// An issue for another row between two diagnostics must not change their
	// row association or their original order in the reject record.
	batch.Ingest = []data.Issue{missing, null, batch.Ingest[0]}
	batch.Raw[2] = json.RawMessage(`{"id":"third","amount":null}`)
	result.Issues = batch.Ingest
	result.Buckets = []data.Bucket{data.BucketPass, data.BucketError, data.BucketError}
	summary.Warned = 0
	summary.Errored = 2
	w := startWriter(t, testConfig(t, "csv"), store, budget, batch.Record.Schema())
	defer w.Abort()
	if err := w.WriteBatch(batch, result); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Complete(summary); err != nil {
		t.Fatal(err)
	}

	records := decodeLines(t, store.object(t, w, "rejects/part-000001.ndjson"))
	if len(records) != 2 {
		t.Fatalf("rejects = %d records, want 2 (conversion row and null row)", len(records))
	}
	var states []rejectState
	unmarshalField(t, records[0], "states", &states)
	wantStates := []rejectState{
		{Column: "id", State: data.Missing},
		{Column: "amount", State: data.Invalid},
		{Column: "large", State: data.Null},
	}
	if !slices.Equal(states, wantStates) {
		t.Errorf("row 2 reject states = %+v, want %+v", states, wantStates)
	}

	var conversion, nullRow []data.Issue
	unmarshalField(t, records[0], "diagnostics", &conversion)
	if got, want := issueCodes(conversion), []string{"INGEST_MISSING_FIELD", "INGEST_TYPE"}; !slices.Equal(got, want) {
		t.Errorf("row 2 reject diagnostic codes = %q, want %q", got, want)
	}
	unmarshalField(t, records[1], "diagnostics", &nullRow)
	if got, want := issueCodes(nullRow), []string{"INGEST_NULL"}; !slices.Equal(got, want) {
		t.Errorf("row 3 reject diagnostic codes = %q, want %q", got, want)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after Complete = %d, want 0", got)
	}
}

func TestCSVDecimalUsesDeclaredScale(t *testing.T) {
	for _, tc := range []struct {
		coefficient int64
		scale       int32
		want        string
	}{
		{coefficient: 1230, scale: 2, want: "12.30"},
		{coefficient: 0, scale: 2, want: "0.00"},
		{coefficient: 1, scale: 2, want: "0.01"},
		{coefficient: -1230, scale: 2, want: "-12.30"},
		{coefficient: -1, scale: 2, want: "-0.01"},
		{coefficient: 12, scale: 0, want: "12"},
		{coefficient: 12, scale: -2, want: "1200"},
	} {
		decimalType := &arrow.Decimal128Type{Precision: 18, Scale: tc.scale}
		builder := array.NewDecimal128Builder(memory.DefaultAllocator, decimalType)
		builder.Append(decimal128.FromI64(tc.coefficient))
		values := builder.NewArray()
		got := csvValue(values, 0)
		values.Release()
		builder.Release()
		if got != tc.want {
			t.Errorf("csvValue(%d scale %d) = %q, want %q", tc.coefficient, tc.scale, got, tc.want)
		}
	}
}

func TestCompletionAllocationIncludedInManagedPeak(t *testing.T) {
	budget := resource.New(8 << 20)
	batch, result, summary := fixture(t)
	defer batch.Release()
	writer := startWriter(t, testConfig(t, "csv"), newFakeStore(), budget, batch.Record.Schema())
	defer writer.Abort()
	if err := writer.WriteBatch(batch, result); err != nil {
		t.Fatal(err)
	}
	before := budget.Peak()
	var usedDuringCommit int64
	if _, err := writer.Complete(summary, func(*data.Summary) error {
		usedDuringCommit = budget.Used()
		if writer.completionCharge == 0 {
			return errors.New("completion reservation released before callback")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if summary.ManagedPeak <= before {
		t.Errorf("summary managed peak = %d, want above pre-completion peak %d", summary.ManagedPeak, before)
	}
	if got := budget.Peak(); summary.ManagedPeak != got {
		t.Errorf("summary managed peak = %d, want budget peak %d", summary.ManagedPeak, got)
	}
	if usedDuringCommit == 0 {
		t.Error("budget used during commit callback = 0, want completion reservation held")
	}
	if writer.completionCharge != 0 {
		t.Errorf("completion charge after Complete = %d, want 0", writer.completionCharge)
	}
}
