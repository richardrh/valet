package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/richardrh/valet/internal/checks"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/source"
	"github.com/richardrh/valet/internal/storage"
)

func TestRecoverOnlyTypedResourcePanic(t *testing.T) {
	limit := &resource.ResourceLimitError{Phase: "test", Limit: 10, Used: 8, Requested: 4}
	existing := errors.New("cleanup failed")
	err := existing
	recoverResource(limit, &err)
	var got *resource.ResourceLimitError
	if !errors.As(err, &got) {
		t.Fatalf("recovered error = %v, want a *resource.ResourceLimitError", err)
	}
	if got != limit {
		t.Errorf("recovered limit error = %p, want the panicked value %p", got, limit)
	}
	if !errors.Is(err, existing) {
		t.Errorf("recovered error = %v, want it to keep the cleanup error %v", err, existing)
	}
}

func TestUnrelatedPanicPropagates(t *testing.T) {
	sentinel := errors.New("programming defect")
	defer func() {
		if value := recover(); value != sentinel {
			t.Fatalf("recovered panic = %v, want the original %v", value, sentinel)
		}
	}()
	var err error
	recoverResource(sentinel, &err)
	t.Fatal("unrelated panic was swallowed")
}

func TestCanceledRunAndPlanDoNotOpenInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := newFakes()
	cfg := &config.Config{}
	summary, err := f.deps().Run(ctx, Runtime{}, cfg)
	if summary != nil {
		t.Errorf("Run summary = %+v, want nil", summary)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled", err)
	}
	plan, err := f.deps().Plan(ctx, Runtime{}, cfg)
	if plan != nil {
		t.Errorf("Plan = %+v, want nil", plan)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Plan error = %v, want context.Canceled", err)
	}
	if f.opened != 0 {
		t.Errorf("resources opened by canceled commands = %d, want 0", f.opened)
	}
}

func TestRunIDsAreDistinctSafePathComponents(t *testing.T) {
	first, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Errorf("run IDs = %q twice, want distinct", first)
	}
	if strings.ContainsAny(first, "/\\: ") {
		t.Errorf("run ID %q contains a path separator, colon or space", first)
	}
	const runIDLength = len("20060102T150405Z-") + 32 // UTC timestamp, dash, 16 random bytes as hex
	if len(first) != runIDLength {
		t.Errorf("len(run ID %q) = %d, want %d", first, len(first), runIDLength)
	}
}

func TestRunPublishesAndClosesResources(t *testing.T) {
	f := newFakes()
	summary, err := f.deps().Run(context.Background(), Runtime{}, runConfig())
	if err != nil {
		t.Fatal(err)
	}
	if summary.ManifestURI != "memory://manifest.json" {
		t.Errorf("manifest URI = %q, want %q", summary.ManifestURI, "memory://manifest.json")
	}
	if summary.RunID == "" {
		t.Error("run ID is empty")
	}
	if !f.writer.completed || f.writer.aborted {
		t.Errorf("writer completed/aborted = %v/%v, want true/false", f.writer.completed, f.writer.aborted)
	}
	if f.writer.batches != 2 {
		t.Errorf("batches written = %d, want 2", f.writer.batches)
	}
	assertClosed(t, map[string]bool{"store": f.store.closed, "dataset": f.ds.closed, "program": f.program.closed})
}

func TestRunReportsWithoutPublishing(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "rows.csv")
	if err := os.WriteFile(input, []byte("id\n1\n2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Source: config.Source{URIs: []string{input}},
		Output: config.Output{S3: config.S3{Profile: "valet-profile-that-does-not-exist"}},
		Rules:  []config.Rule{{Code: "ONE", Level: "error", Column: "id", Checks: []config.Check{{Type: "equals", Params: map[string]any{"value": 1}}}}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	called := false
	summary, err := Run(context.Background(), Runtime{BeforeCommit: func(summary *data.Summary) error {
		called = true
		if summary.ManifestURI != "" || summary.OutputMode != "report" || summary.Total != 2 || summary.Errored != 1 {
			t.Fatalf("completion summary = %+v", summary)
		}
		return nil
	}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !called || summary.ManifestURI != "" || summary.Passed != 1 || summary.Validation != data.ValidationFail {
		t.Fatalf("report summary = %+v, callback called = %v", summary, called)
	}
	if got := summary.RowOutput["error"]; got.Policy != "omit_by_policy" || got.Omitted != 1 || got.Emitted != 0 {
		t.Errorf("error materialization = %+v", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "rows.csv" {
		t.Fatalf("stdout-only run created output: %v", entries)
	}
}

func TestReportRetainsTypedRejectCountsWithoutArtifacts(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "rows.csv")
	if err := os.WriteFile(input, []byte("id\n1\nbad\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Source: config.Source{URIs: []string{input}},
		Input: config.Input{OnTypeError: "row_error", Schema: &config.Schema{
			Order: []string{"id"}, Columns: map[string]config.Column{"id": {Type: "int64"}},
		}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	summary, err := Run(context.Background(), Runtime{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Total != 2 || summary.Passed != 1 || summary.Errored != 1 || summary.RowOutput["rejects"].Omitted != 1 {
		t.Fatalf("typed report = %+v", summary)
	}
	callbackErr := errors.New("summary export failed")
	summary, err = Run(context.Background(), Runtime{BeforeCommit: func(*data.Summary) error {
		return callbackErr
	}}, cfg)
	if summary != nil || !errors.Is(err, callbackErr) {
		t.Fatalf("failed completion summary = %+v, error = %v", summary, err)
	}
}

func TestRunWithoutDestinationRejectsArtifactModes(t *testing.T) {
	for _, mode := range []string{"all", "failures"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakes()
			cfg := runConfig()
			cfg.Output.URI, cfg.Output.Mode = "", mode
			summary, err := f.deps().Run(context.Background(), Runtime{}, cfg)
			if err == nil || summary != nil || f.opened != 0 {
				t.Fatalf("summary = %+v, error = %v, resources opened = %d", summary, err, f.opened)
			}
		})
	}
}

// assertClosed reports every named resource that was not released.
func assertClosed(t *testing.T, closed map[string]bool) {
	t.Helper()
	for name, ok := range closed {
		if !ok {
			t.Errorf("%s closed = false, want true", name)
		}
	}
}

func TestCleanupFailureAfterPublicationKeepsSummary(t *testing.T) {
	f := newFakes()
	f.ds.closeErr = errors.New("dataset close failed")
	var logs bytes.Buffer
	rt := Runtime{Log: slog.New(slog.NewTextHandler(&logs, nil))}
	summary, err := f.deps().Run(context.Background(), rt, runConfig())
	if err != nil {
		t.Fatalf("Run error = %v, want nil after publication", err)
	}
	if summary == nil {
		t.Fatal("Run summary = nil, want the published summary")
	}
	for _, want := range []string{"close failed", "dataset close failed"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log missing %q:\n%s", want, logs.String())
		}
	}
	assertClosed(t, map[string]bool{"store": f.store.closed})
}

func TestFailureBeforePublicationAbortsAndJoinsCleanupErrors(t *testing.T) {
	f := newFakes()
	f.program.evalErr = errors.New("evaluation failed")
	f.store.closeErr = errors.New("store close failed")
	summary, err := f.deps().Run(context.Background(), Runtime{}, runConfig())
	if summary != nil {
		t.Errorf("Run summary = %+v, want nil", summary)
	}
	for _, want := range []error{f.program.evalErr, f.store.closeErr} {
		if !errors.Is(err, want) {
			t.Errorf("Run error = %v, want it to join %v", err, want)
		}
	}
	if !f.writer.aborted || f.writer.completed {
		t.Errorf("writer aborted/completed = %v/%v, want true/false", f.writer.aborted, f.writer.completed)
	}
	assertClosed(t, map[string]bool{"reader": f.reader.closed, "dataset": f.ds.closed})
}

func TestPlanSharesSetupAndCloses(t *testing.T) {
	f := newFakes()
	plan, err := f.deps().Plan(context.Background(), Runtime{}, runConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Format != "csv" {
		t.Errorf("plan format = %q, want %q", plan.Format, "csv")
	}
	if plan.SourceCount != 1 {
		t.Errorf("plan source count = %d, want 1", plan.SourceCount)
	}
	if plan.RequiredPasses != 2 {
		t.Errorf("plan required passes = %d, want 2", plan.RequiredPasses)
	}
	if len(plan.Schema) != 1 {
		t.Errorf("plan schema fields = %d, want 1", len(plan.Schema))
	}
	assertClosed(t, map[string]bool{"store": f.store.closed, "dataset": f.ds.closed})
	if f.writer.batches != 0 {
		t.Errorf("batches validated by plan = %d, want 0", f.writer.batches)
	}
}

func TestPlanUsesCanonicalLogicalTypes(t *testing.T) {
	f := newFakes()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "day", Type: arrow.FixedWidthTypes.Date32},
		{Name: "observed", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 12, Scale: 2}},
	}, nil)
	f.ds.schema, f.reader.schema = schema, schema
	plan, err := f.deps().Plan(context.Background(), Runtime{}, runConfig())
	if err != nil {
		t.Fatal(err)
	}
	type logicalType struct {
		Type, Timezone   string
		Precision, Scale int32
	}
	want := []logicalType{
		{Type: "date"},
		{Type: "timestamp", Timezone: "UTC"},
		{Type: "decimal", Precision: 12, Scale: 2},
	}
	got := make([]logicalType, 0, len(plan.Schema))
	for _, field := range plan.Schema {
		got = append(got, logicalType{
			Type: field.Type, Timezone: field.Timezone, Precision: field.Precision, Scale: field.Scale,
		})
	}
	if !slices.Equal(got, want) {
		t.Errorf("plan logical types = %+v, want %+v", got, want)
	}
}

func runConfig() *config.Config {
	cfg := &config.Config{Source: config.Source{URIs: []string{"rows.csv"}}, Output: config.Output{URI: "out"}}
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return cfg
}

type fakes struct {
	opened  int
	store   *fakeStore
	ds      *fakeDataset
	reader  *fakeReader
	writer  *fakeWriter
	program *fakeProgram
}

func newFakes() *fakes {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	reader := &fakeReader{schema: schema, remaining: 2}
	return &fakes{
		store:   &fakeStore{},
		reader:  reader,
		ds:      &fakeDataset{schema: schema, reader: reader},
		writer:  &fakeWriter{},
		program: &fakeProgram{summary: data.NewSummary()},
	}
}

func (f *fakes) deps() Deps {
	return Deps{
		OpenStorage: func(context.Context, *config.Config, *resource.Budget, *slog.Logger) (Store, error) {
			f.opened++
			return f.store, nil
		},
		OpenSource: func(
			context.Context, *config.Config, Store, *resource.Budget, *slog.Logger, bool,
		) (Dataset, error) {
			f.opened++
			return f.ds, nil
		},
		OpenSink: func(
			context.Context, *config.Config, Store, *resource.Budget, string, *arrow.Schema, *slog.Logger,
		) (Writer, error) {
			f.opened++
			return f.writer, nil
		},
		Compile: func(
			context.Context, *config.Config, Dataset, Store, *resource.Budget, *slog.Logger, func(data.Issue) error,
		) (Program, error) {
			return f.program, nil
		},
		Describe: func(context.Context, *config.Config, Dataset, Store, *resource.Budget) (*checks.Description, error) {
			return &checks.Description{RequiredPasses: 2}, nil
		},
	}
}

type fakeStore struct {
	closed   bool
	closeErr error
}

func (s *fakeStore) Close() error { s.closed = true; return s.closeErr }

type fakeDataset struct {
	schema   *arrow.Schema
	reader   *fakeReader
	closed   bool
	closeErr error
}

func (d *fakeDataset) Schema() *arrow.Schema { return d.schema }
func (d *fakeDataset) Objects() []storage.Object {
	return []storage.Object{{URI: "rows.csv", Size: 10}}
}
func (d *fakeDataset) SchemaIssues() []data.Issue { return nil }
func (d *fakeDataset) Passes() int                { return 0 }
func (d *fakeDataset) DecoderPassesPerOpen() int  { return 0 }
func (d *fakeDataset) Open(context.Context) (source.Reader, error) {
	return d.reader, nil
}
func (d *fakeDataset) Close() error { d.closed = true; return d.closeErr }

type fakeReader struct {
	schema    *arrow.Schema
	remaining int
	closed    bool
}

func (r *fakeReader) Next(context.Context) (*data.Batch, error) {
	if r.remaining == 0 {
		return nil, io.EOF
	}
	r.remaining--
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), r.schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	return &data.Batch{Record: builder.NewRecordBatch()}, nil
}
func (r *fakeReader) Close() error { r.closed = true; return nil }

type fakeWriter struct {
	batches   int
	completed bool
	aborted   bool
}

func (w *fakeWriter) WriteInputs([]storage.Object) error { return nil }
func (w *fakeWriter) WriteIssue(data.Issue) error        { return nil }
func (w *fakeWriter) WriteBatch(*data.Batch, *data.Result) error {
	w.batches++
	return nil
}
func (w *fakeWriter) Complete(summary *data.Summary, beforeCommit ...func(*data.Summary) error) (string, error) {
	if len(beforeCommit) > 0 && beforeCommit[0] != nil {
		if err := beforeCommit[0](summary); err != nil {
			return "", err
		}
	}
	w.completed = true
	return "memory://manifest.json", nil
}
func (w *fakeWriter) Abort() error { w.aborted = true; return nil }

type fakeProgram struct {
	summary *data.Summary
	evalErr error
	closed  bool
}

func (p *fakeProgram) Summary() *data.Summary { return p.summary }
func (p *fakeProgram) Evaluate(b *data.Batch) (*data.Result, error) {
	if p.evalErr != nil {
		return nil, p.evalErr
	}
	return &data.Result{Buckets: make([]data.Bucket, b.Record.NumRows())}, nil
}
func (p *fakeProgram) Finalize() error { return nil }
func (p *fakeProgram) Close() error    { p.closed = true; return nil }
