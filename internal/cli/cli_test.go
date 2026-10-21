package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/richardrh/valet/internal/app"
	"github.com/richardrh/valet/internal/data"
)

// result is one CLI invocation's exit code and captured streams.
type result struct {
	exit           int
	stdout, stderr string
}

func invoke(t *testing.T, args ...string) result {
	t.Helper()
	return invokeContext(t, context.Background(), args...)
}

func invokeContext(t *testing.T, ctx context.Context, args ...string) result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(ctx, app.Runtime{Stdout: &stdout, Stderr: &stderr, Version: "test"}, args)
	return result{exit: code, stdout: stdout.String(), stderr: stderr.String()}
}

// validateArgs builds a `valet validate` command line with the given extra flags.
func validateArgs(pipeline, input, output string, flags ...string) []string {
	return append([]string{"validate", "-p", pipeline, "-i", input, "-o", output}, flags...)
}

// assertExit stops the test when the exit code differs, showing both streams.
func assertExit(t *testing.T, r result, want int) {
	t.Helper()
	if r.exit != want {
		t.Fatalf("exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s", r.exit, want, r.stdout, r.stderr)
	}
}

// assertDiagnosticOnly checks a failed command exits with want, keeps stdout
// empty and explains the failure on stderr.
func assertDiagnosticOnly(t *testing.T, r result, want int) {
	t.Helper()
	assertExit(t, r, want)
	if r.stdout != "" {
		t.Errorf("stdout = %q, want empty", r.stdout)
	}
	if r.stderr == "" {
		t.Error("stderr is empty, want a diagnostic")
	}
}

func putFixture(t *testing.T, directory, name, content string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func decodeSummary(t *testing.T, encoded []byte) data.Summary {
	t.Helper()
	var summary data.Summary
	if err := json.Unmarshal(encoded, &summary); err != nil {
		t.Fatalf("summary is not JSON: %v\n%s", err, encoded)
	}
	return summary
}

// manifests lists every manifest.json under root; a missing root has none.
func manifests(t *testing.T, root string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Name() == "manifest.json" {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func assertNoManifest(t *testing.T, root string) {
	t.Helper()
	if found := manifests(t, root); len(found) != 0 {
		t.Errorf("manifests published = %q, want none", found)
	}
}

const rawCSVPipeline = `input: {type: csv, mode: raw_strings}
rules: []
`

// One-column CSV inputs shared by several tests.
const (
	okCSV = `id
ok
`
	valueCSV = `id
value
`
	// The quote never closes, so scanning any row fails.
	malformedCSV = `id
"unterminated
`
)

func TestHelpAndVersionDoNotLoadConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "no arguments", args: nil},
		{name: "help flag", args: []string{"--help"}},
		{name: "help for plan", args: []string{"help", "plan"}},
		{name: "help after missing pipeline", args: []string{"validate", "-p", "/does/not/exist", "--help"}},
		{name: "version flag", args: []string{"--version"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A canceled context proves no configuration or input is opened.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			r := invokeContext(t, ctx, tc.args...)
			assertExit(t, r, ExitOK)
			if r.stdout == "" {
				t.Error("stdout is empty, want help/version text")
			}
			if r.stderr != "" {
				t.Errorf("stderr = %q, want empty", r.stderr)
			}
		})
	}
}

func TestArgumentErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "unknown command", args: []string{"unknown"}},
		{name: "validate without pipeline", args: []string{"validate"}},
		{name: "unknown flag", args: []string{"validate", "--bogus"}},
		{name: "removed --stdout flag", args: []string{"validate", "--stdout"}},
		{name: "removed --stdout=false flag", args: []string{"validate", "--stdout=false"}},
		{name: "extra positional argument", args: []string{"validate", "-p", "anything", "unexpected"}},
		{name: "plan with --json-output", args: []string{"plan", "-p", "anything", "--json-output", "summary.json"}},
		{name: "stdin input", args: []string{"validate", "-p", "anything", "-i", "-"}},
		{name: "stdout output", args: []string{"validate", "-p", "anything", "-o", "-"}},
		{name: "conflicting stdout presentation", args: []string{"validate", "-p", "anything", "--plain", "--json-output", "-"}},
		{name: "unknown log format", args: []string{"validate", "-p", "anything", "--log-format", "xml"}},
		{name: "unknown log level", args: []string{"validate", "-p", "anything", "--log-level", "trace"}},
		{
			name: "verbose with explicit log level",
			args: []string{"validate", "-p", "anything", "-v", "--log-level", "warn"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := invoke(t, tc.args...)
			assertDiagnosticOnly(t, r, ExitOperational)
		})
	}
}

func TestValidateOverridesAndVersionedSummary(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.csv", `id
alpha
beta
`)
	// Every value here is invalid or points nowhere; the command-line overrides must win.
	pipeline := putFixture(t, dir, "pipeline.yml", `name: Overrides
source:
  prefix: /does/not/exist
  recursive: true
  filter: csv
input:
  type: csv
  mode: raw_strings
output:
  uri: /does/not/exist/output
  format: csv
execution:
  memory_budget: invalid
  batch_target: invalid
  max_record_bytes: 1MiB
  workers: -1
rules: []
`)
	r := invoke(t, validateArgs(pipeline, input, filepath.Join(dir, "results"),
		"--memory-budget", "64MiB",
		"--batch-target", "64KiB",
		"--workers", "1",
		"--json-output", "-",
		"--verbose",
	)...)
	assertExit(t, r, ExitOK)

	summary := decodeSummary(t, []byte(r.stdout))
	if summary.ReportVersion != 2 {
		t.Errorf("report version = %d, want 2", summary.ReportVersion)
	}
	if summary.Total != 2 || summary.Passed != 2 {
		t.Errorf("total/passed = %d/%d, want 2/2", summary.Total, summary.Passed)
	}
	if summary.Status != "completed" {
		t.Errorf("status = %q, want %q", summary.Status, "completed")
	}
	if summary.Validation != "pass" {
		t.Errorf("validation = %q, want %q", summary.Validation, "pass")
	}
	if summary.RunID == "" {
		t.Error("run ID is empty")
	}
	if summary.Passes < 1 {
		t.Errorf("passes = %d, want at least 1", summary.Passes)
	}
	if summary.ManagedPeak <= 0 {
		t.Errorf("managed peak = %d, want positive", summary.ManagedPeak)
	}
	if summary.ManifestURI == "" {
		t.Error("manifest URI is empty")
	} else if _, err := os.Stat(summary.ManifestURI); err != nil {
		t.Errorf("manifest %s: %v", summary.ManifestURI, err)
	}
}

func TestValidationExitSeverityAndSummaryFile(t *testing.T) {
	for _, tc := range []struct {
		name     string
		level    string
		wantExit int
	}{
		{name: "warning rule still passes", level: "warn", wantExit: ExitOK},
		{name: "error rule fails validation", level: "error", wantExit: ExitValidation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := putFixture(t, dir, "rows.csv", `id
NULL
`)
			pipeline := putFixture(t, dir, "pipeline.yml", fmt.Sprintf(`input:
  type: csv
  mode: raw_strings
  null_values: ["NULL"]
output:
  format: csv
rules:
  - code: REQUIRED
    column: id
    level: %s
    checks:
      - type: not_null
`, tc.level))
			summaryPath := filepath.Join(dir, "summary-copy.json")
			args := validateArgs(pipeline, input, filepath.Join(dir, "out"), "--json-output", summaryPath, "--stats")
			r := invoke(t, args...)
			assertExit(t, r, tc.wantExit)

			encoded, err := os.ReadFile(summaryPath)
			if err != nil {
				t.Fatal(err)
			}
			summary := decodeSummary(t, encoded)
			if got := summary.RuleCounts["REQUIRED"]; got != 1 {
				t.Errorf("REQUIRED failures = %d, want 1 (summary %+v)", got, summary)
			}
			stdoutSummary := decodeSummary(t, []byte(r.stdout))
			if stdoutSummary.RunID != summary.RunID || stdoutSummary.RuleCounts["REQUIRED"] != 1 {
				t.Errorf("stdout and exported summary differ: stdout=%+v export=%+v", stdoutSummary, summary)
			}
		})
	}
}

func TestPlanDoesNotScanValidationRows(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.csv", malformedCSV)
	pipeline := putFixture(t, dir, "pipeline.yml", `input:
  type: csv
  mode: raw_strings
rules:
  - code: UNIQUE
    level: error
    column: id
    checks:
      - type: is_unique
`)
	r := invoke(t, "plan", "-p", pipeline, "-i", input)
	assertExit(t, r, ExitOK) // a scan would hit the malformed row and fail
	var plan struct {
		ReportVersion      int  `json:"report_version"`
		RequiredPasses     int  `json:"required_passes"`
		StateFitGuaranteed bool `json:"state_fit_guaranteed"`
	}
	if err := json.Unmarshal([]byte(r.stdout), &plan); err != nil {
		t.Fatalf("plan is not JSON: %v\n%s", err, r.stdout)
	}
	if plan.ReportVersion != 2 {
		t.Errorf("report version = %d, want 2", plan.ReportVersion)
	}
	if plan.RequiredPasses < 2 {
		t.Errorf("required passes = %d, want at least 2 for is_unique", plan.RequiredPasses)
	}
	if plan.StateFitGuaranteed {
		t.Error("state fit guaranteed = true, want false")
	}
}

func TestSchemaRejectionIsPublishedValidationFailure(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.csv", `other
value
`)
	pipeline := putFixture(t, dir, "pipeline.yml", `input:
  type: csv
  mode: raw_strings
  schema:
    extra_fields: ignore
    columns:
      id: {type: string, required: true}
output:
  format: csv
rules: []
`)
	r := invoke(t, validateArgs(pipeline, input, filepath.Join(dir, "out"), "--json-output", "-")...)
	assertExit(t, r, ExitValidation)

	summary := decodeSummary(t, []byte(r.stdout))
	if summary.Status != "schema_rejected" {
		t.Errorf("status = %q, want %q", summary.Status, "schema_rejected")
	}
	if summary.Total != 0 || summary.Passed != 0 {
		t.Errorf("total/passed = %d/%d, want 0/0", summary.Total, summary.Passed)
	}
	if len(summary.SchemaIssues) == 0 {
		t.Error("schema issues = none, want the missing required column reported")
	}
	if _, err := os.Stat(summary.ManifestURI); err != nil {
		t.Errorf("manifest %q: %v", summary.ManifestURI, err)
	}
}

func TestCanceledCommandReturnsInterruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertDiagnosticOnly(t, invokeContext(t, ctx, "validate", "-p", "unopened.yml"), ExitInterrupted)
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

func TestOutputFailureReturnsOperationalExit(t *testing.T) {
	var stderr bytes.Buffer
	code := Run(context.Background(), app.Runtime{Stdout: failingWriter{}, Stderr: &stderr}, []string{"--help"})
	if code != ExitOperational {
		t.Fatalf("exit code = %d, want %d (stderr %q)", code, ExitOperational, stderr.String())
	}
}

func TestJSONOutputFailureLeavesNoManifest(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.csv", okCSV)
	pipeline := putFixture(t, dir, "pipeline.yml", rawCSVPipeline)
	output := filepath.Join(dir, "out")
	// A directory where the summary file should go makes the summary write fail.
	summaryPath := filepath.Join(dir, "summary-directory")
	if err := os.Mkdir(summaryPath, 0700); err != nil {
		t.Fatal(err)
	}
	r := invoke(t, validateArgs(pipeline, input, output, "--json-output", summaryPath)...)
	assertExit(t, r, ExitOperational)
	assertNoManifest(t, output)
}

func TestPostCommitPresentationFailureHasDistinctExit(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.csv", okCSV)
	pipeline := putFixture(t, dir, "pipeline.yml", rawCSVPipeline)
	output := filepath.Join(dir, "out")
	var stderr bytes.Buffer
	rt := app.Runtime{Stdout: failingWriter{}, Stderr: &stderr, Version: "test"}
	code := Run(context.Background(), rt, validateArgs(pipeline, input, output))
	if code != ExitPresentation {
		t.Fatalf("exit code = %d, want %d (stderr %q)", code, ExitPresentation, stderr.String())
	}
	if found := manifests(t, output); len(found) == 0 {
		t.Error("manifests = none, want the committed manifest kept")
	}
}

func TestOperationalFailuresDoNotPublishManifest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rows   string
		budget string
	}{
		{name: "malformed input", rows: malformedCSV, budget: "64MiB"},
		{name: "resource limit", rows: valueCSV, budget: "1B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := putFixture(t, dir, "rows.csv", tc.rows)
			pipeline := putFixture(t, dir, "pipeline.yml", `input:
  type: csv
  mode: raw_strings
output:
  format: csv
rules: []
`)
			out := filepath.Join(dir, "out")
			r := invoke(t, validateArgs(pipeline, input, out, "--memory-budget", tc.budget, "--json-output", "-")...)
			assertDiagnosticOnly(t, r, ExitOperational)
			assertNoManifest(t, out)
		})
	}
}

func TestJSONOutputDirectorySyncFailurePreventsCommit(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.csv", okCSV)
	pipeline := putFixture(t, dir, "pipeline.yml", `input: {type: csv, mode: raw_strings}
rules: []
execution: {memory_budget: 64MiB, max_record_bytes: 1MiB}
`)
	output, summaryPath := filepath.Join(dir, "out"), filepath.Join(dir, "summary.json")
	original := syncSummaryParent
	syncSummaryParent = func(string) error { return errors.New("injected summary directory sync failure") }
	t.Cleanup(func() { syncSummaryParent = original })

	r := invoke(t, validateArgs(pipeline, input, output, "--json-output", summaryPath)...)
	assertExit(t, r, ExitOperational)
	if _, err := os.Stat(summaryPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stat summary after failed directory sync: %v, want not exist", err)
	}
	assertNoManifest(t, output)
}

func TestInvalidExplicitOverridesAreNotDefaulted(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.csv", valueCSV)
	pipeline := putFixture(t, dir, "pipeline.yml", `input:
  type: csv
  mode: raw_strings
rules: []
`)
	for _, tc := range []struct {
		name  string
		flags []string
	}{
		{name: "zero workers", flags: []string{"--workers", "0"}},
		{name: "too many workers", flags: []string{"--workers", "33"}},
		{name: "empty memory budget", flags: []string{"--memory-budget", ""}},
		{name: "empty batch target", flags: []string{"--batch-target", ""}},
		{name: "empty output", flags: []string{"--output", ""}},
		{name: "empty input", flags: []string{"--input", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := invoke(t, validateArgs(pipeline, input, filepath.Join(dir, "out"), tc.flags...)...)
			assertDiagnosticOnly(t, r, ExitOperational)
		})
	}
}

// failingRun writes a pipeline whose single error rule fails once.
func failingRun(t *testing.T) (dir, pipeline, input string) {
	t.Helper()
	dir = t.TempDir()
	input = putFixture(t, dir, "rows.csv", `id,amount
NULL,2
b,3
`)
	pipeline = putFixture(t, dir, "pipeline.yml", `input:
  mode: infer_types
  null_values: ["NULL"]
output:
  format: csv
rules:
  - code: REQUIRED
    column: id
    level: error
    checks:
      - type: not_null
stats:
  - code: TOTAL
    column: amount
    aggregate: sum
`)
	return dir, pipeline, input
}

func TestLogsGoToStderrOnly(t *testing.T) {
	dir, pipeline, input := failingRun(t)
	t.Run("json", func(t *testing.T) {
		r := invoke(t, validateArgs(pipeline, input, filepath.Join(dir, "json"),
			"--log-format", "json", "--json-output", "-")...)
		assertExit(t, r, ExitValidation)
		summary := decodeSummary(t, []byte(r.stdout))
		runIDs := 0
		for _, line := range strings.Split(strings.TrimSpace(r.stderr), "\n") {
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatalf("non-JSON stderr line %q: %v", line, err)
			}
			if record["level"] == "DEBUG" {
				t.Errorf("debug record at default level: %s", line)
			}
			if record["run_id"] == summary.RunID {
				runIDs++
			}
		}
		if runIDs == 0 {
			t.Errorf("records carrying run_id %s = 0, want at least 1:\n%s", summary.RunID, r.stderr)
		}
	})
	t.Run("text verbose", func(t *testing.T) {
		r := invoke(t, validateArgs(pipeline, input, filepath.Join(dir, "text"), "-v")...)
		assertExit(t, r, ExitValidation)
		if strings.Contains(r.stdout, "level=") {
			t.Errorf("stdout contains log records:\n%s", r.stdout)
		}
	})
	t.Run("error level", func(t *testing.T) {
		r := invoke(t, validateArgs(pipeline, input, filepath.Join(dir, "quiet"), "--log-level", "error")...)
		assertExit(t, r, ExitValidation)
		if r.stderr != "" {
			t.Errorf("stderr = %q, want empty at error level", r.stderr)
		}
	})
}

func TestPlainSummaryAndStats(t *testing.T) {
	dir, pipeline, input := failingRun(t)
	t.Run("plain with stats", func(t *testing.T) {
		out := filepath.Join(dir, "plain")
		r := invoke(t, validateArgs(pipeline, input, out, "--plain", "--stats", "--log-level", "error")...)
		assertExit(t, r, ExitValidation)
		if r.stderr != "" {
			t.Errorf("stderr = %q, want empty", r.stderr)
		}
		lines := strings.Split(strings.TrimSuffix(r.stdout, "\n"), "\n")
		want := []string{
			"title=VALIDATION SUMMARY",
			"total=2",
			"pass=1",
			"warn=0",
			"error=1",
			"output_dir=" + out,
			"status=completed",
			"validation=fail",
		}
		// The fixed lines are followed by run_id= and manifest= lines.
		if len(lines) < len(want)+2 {
			t.Fatalf("plain summary has %d lines, want at least %d:\n%s", len(lines), len(want)+2, r.stdout)
		}
		if got := lines[:len(want)]; !slices.Equal(got, want) {
			t.Errorf("plain summary lines = %q, want %q", got, want)
		}
		for i, prefix := range []string{"run_id=", "manifest="} {
			if line := lines[len(want)+i]; !strings.HasPrefix(line, prefix) {
				t.Errorf("plain line %d = %q, want prefix %q", len(want)+i+1, line, prefix)
			}
		}
		for _, block := range []string{"check_stats={\n  REQUIRED=1\n}\n", "stats={\n  TOTAL="} {
			if !strings.Contains(r.stdout, block) {
				t.Errorf("plain stdout missing %q:\n%s", block, r.stdout)
			}
		}
	})
	t.Run("human with stats", func(t *testing.T) {
		args := validateArgs(pipeline, input, filepath.Join(dir, "human"), "--stats", "--log-level", "error")
		r := invoke(t, args...)
		assertExit(t, r, ExitValidation)
		for _, block := range []string{"Check statistics:\n  1 REQUIRED\n", "Statistics: "} {
			if !strings.Contains(r.stdout, block) {
				t.Errorf("human stdout missing %q:\n%s", block, r.stdout)
			}
		}
	})
	t.Run("plain without stats", func(t *testing.T) {
		args := validateArgs(pipeline, input, filepath.Join(dir, "nostats"), "--plain", "--log-level", "error")
		r := invoke(t, args...)
		assertExit(t, r, ExitValidation)
		for _, block := range []string{"check_stats", "stats={"} {
			if strings.Contains(r.stdout, block) {
				t.Errorf("stdout contains %q without --stats:\n%s", block, r.stdout)
			}
		}
	})
}

func TestExitCodeMapping(t *testing.T) {
	canceledBy := func(cause error) context.Context {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		return ctx
	}
	operational := errors.New("disk failed")
	sigint := Interrupted{Signal: os.Interrupt}
	sigterm := Interrupted{Signal: syscall.SIGTERM}
	exitSIGTERM := 128 + int(syscall.SIGTERM)
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want int
	}{
		{name: "success", ctx: context.Background(), err: nil, want: ExitOK},
		{name: "operational error", ctx: context.Background(), err: operational, want: ExitOperational},
		{name: "SIGINT", ctx: canceledBy(sigint), err: fmt.Errorf("read: %w", context.Canceled), want: ExitInterrupted},
		{name: "SIGTERM", ctx: canceledBy(sigterm), err: context.Canceled, want: exitSIGTERM},
		{
			name: "error wraps the signal cause",
			ctx:  canceledBy(sigterm),
			err:  fmt.Errorf("open: %w", context.Cause(canceledBy(sigterm))),
			want: exitSIGTERM,
		},
		{name: "unrelated error after signal", ctx: canceledBy(sigint), err: operational, want: ExitOperational},
		{name: "canceled without signal", ctx: canceledBy(nil), err: context.Canceled, want: ExitInterrupted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := exitCode(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("exitCode = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestInputTypeInferredFromInputSuffix(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.ndjson", `{"id":"a"}
{"id":"b"}
`)
	pipeline := putFixture(t, dir, "pipeline.yml", `input:
  mode: raw_strings
rules: []
`)
	r := invoke(t, "plan", "-p", pipeline, "-i", input)
	assertExit(t, r, ExitOK)
	var plan struct {
		Format string `json:"format"`
	}
	if err := json.Unmarshal([]byte(r.stdout), &plan); err != nil {
		t.Fatalf("plan is not JSON: %v\n%s", err, r.stdout)
	}
	if plan.Format != "ndjson" {
		t.Errorf("plan format = %q, want %q", plan.Format, "ndjson")
	}
}

func TestStdoutOnlyPresentationFailureIsOperational(t *testing.T) {
	dir := t.TempDir()
	input := putFixture(t, dir, "rows.csv", okCSV)
	pipeline := putFixture(t, dir, "pipeline.yml", rawCSVPipeline)
	var stderr bytes.Buffer
	code := Run(context.Background(), app.Runtime{Stdout: failingWriter{}, Stderr: &stderr}, []string{"validate", "-p", pipeline, "-i", input})
	if code != ExitOperational {
		t.Fatalf("exit code = %d, want %d; stderr=%s", code, ExitOperational, stderr.String())
	}
	if strings.Contains(stderr.String(), "after terminal commit") {
		t.Fatalf("stdout-only failure claimed a commit: %s", stderr.String())
	}
	assertNoManifest(t, dir)
}
