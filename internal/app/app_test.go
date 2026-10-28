package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
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
	dir := t.TempDir()
	// The input does not exist, so opening it would fail with a discovery error.
	cfg := runConfig(t, filepath.Join(dir, "missing.csv"), filepath.Join(dir, "out"))
	summary, err := Run(ctx, Runtime{}, cfg)
	if summary != nil {
		t.Errorf("Run summary = %+v, want nil", summary)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled", err)
	}
	plan, err := Plan(ctx, Runtime{}, cfg)
	if plan != nil {
		t.Errorf("Plan = %+v, want nil", plan)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Plan error = %v, want context.Canceled", err)
	}
	assertNoOutput(t, dir)
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
	dir := t.TempDir()
	var logs bytes.Buffer
	rt := Runtime{Log: slog.New(slog.NewTextHandler(&logs, nil))}
	summary, err := Run(context.Background(), rt, runConfig(t, writeInput(t, dir, "id\n1\n2\n"), filepath.Join(dir, "out")))
	if err != nil {
		t.Fatal(err)
	}
	if summary.RunID == "" {
		t.Error("run ID is empty")
	}
	if summary.Total != 2 {
		t.Errorf("rows = %d, want 2", summary.Total)
	}
	if _, err := os.Stat(summary.ManifestURI); err != nil {
		t.Errorf("published manifest %q: %v", summary.ManifestURI, err)
	}
	if !strings.Contains(logs.String(), `msg="budget peak"`) || strings.Contains(logs.String(), "close failed") || strings.Contains(logs.String(), "aborting") {
		t.Errorf("want session closed without abort or close failure:\n%s", logs.String())
	}
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
			// The input does not exist, so opening it would fail differently.
			cfg := runConfig(t, filepath.Join(t.TempDir(), "missing.csv"), "")
			cfg.Output.Mode = mode
			summary, err := Run(context.Background(), Runtime{}, cfg)
			if summary != nil || err == nil || err.Error() != "output.uri or --output is required for output.mode all or failures" {
				t.Fatalf("summary = %+v, error = %v", summary, err)
			}
		})
	}
}

func TestFailureBeforePublicationAborts(t *testing.T) {
	dir := t.TempDir()
	var logs bytes.Buffer
	callbackErr := errors.New("summary export failed")
	rt := Runtime{Log: slog.New(slog.NewTextHandler(&logs, nil)), BeforeCommit: func(*data.Summary) error { return callbackErr }}
	summary, err := Run(context.Background(), rt, runConfig(t, writeInput(t, dir, "id\n1\n2\n"), filepath.Join(dir, "out")))
	if summary != nil {
		t.Errorf("Run summary = %+v, want nil", summary)
	}
	if !errors.Is(err, callbackErr) {
		t.Errorf("Run error = %v, want it to join %v", err, callbackErr)
	}
	if !strings.Contains(logs.String(), "aborting unpublished output") {
		t.Errorf("log missing output abort:\n%s", logs.String())
	}
	err = filepath.WalkDir(filepath.Join(dir, "out"), func(path string, entry os.DirEntry, err error) error {
		if err == nil && strings.Contains(entry.Name(), "manifest") {
			t.Errorf("aborted run published %s", path)
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPlanSharesSetupAndCloses(t *testing.T) {
	dir := t.TempDir()
	plan, err := Plan(context.Background(), Runtime{}, runConfig(t, writeInput(t, dir, "id\n1\n2\n"), filepath.Join(dir, "out")))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Format != "csv" {
		t.Errorf("plan format = %q, want %q", plan.Format, "csv")
	}
	if plan.SourceCount != 1 {
		t.Errorf("plan source count = %d, want 1", plan.SourceCount)
	}
	if plan.RequiredPasses != plan.SourcePasses+plan.Checks.RequiredPasses {
		t.Errorf("plan required passes = %d, want source %d + checks %d", plan.RequiredPasses, plan.SourcePasses, plan.Checks.RequiredPasses)
	}
	if len(plan.Schema) != 1 {
		t.Errorf("plan schema fields = %d, want 1", len(plan.Schema))
	}
	assertNoOutput(t, dir)
}

func TestPlanUsesCanonicalLogicalTypes(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		Source: config.Source{URIs: []string{writeInput(t, dir, "day,observed,amount\n2026-01-02,2026-01-02T03:04:05Z,1.25\n")}},
		Output: config.Output{URI: filepath.Join(dir, "out")},
		Input: config.Input{Schema: &config.Schema{
			Order: []string{"day", "observed", "amount"},
			Columns: map[string]config.Column{
				"day":      {Type: "date"},
				"observed": {Type: "timestamp", Timezone: "UTC"},
				"amount":   {Type: "decimal", Precision: 12, Scale: 2},
			},
		}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	plan, err := Plan(context.Background(), Runtime{}, cfg)
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

func writeInput(t *testing.T, dir, body string) string {
	t.Helper()
	path := filepath.Join(dir, "rows.csv")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runConfig(t *testing.T, input, output string) *config.Config {
	t.Helper()
	cfg := &config.Config{Source: config.Source{URIs: []string{input}}, Output: config.Output{URI: output}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// assertNoOutput reports any output directory created under dir.
func assertNoOutput(t *testing.T, dir string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, "out")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("output directory stat error = %v, want it absent", err)
	}
}
