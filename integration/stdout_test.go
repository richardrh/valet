//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// stdoutInvoke isolates the process working directory so unexpected relative
// artifact publication is observable as well as publication beside the input.
func stdoutInvoke(t *testing.T, dir string, writer io.Writer, args ...string) ([]byte, []byte, int) {
	t.Helper()
	cmd, ctx, cancel := command(t, args...)
	defer cancel()
	cmd.Dir = dir
	var out, errout bytes.Buffer
	cmd.Stdout = &out
	if writer != nil {
		cmd.Stdout = writer
	}
	cmd.Stderr = &errout
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("CLI exceeded deadline: %s", errout.String())
	}
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("CLI: %v; stderr=%s", err, errout.String())
		}
		code = exit.ExitCode()
	}
	return out.Bytes(), errout.Bytes(), code
}

func stdoutEntries(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err == nil {
			paths = append(paths, path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return paths
}

func stdoutSummary(t *testing.T, out []byte) summary {
	t.Helper()
	var s summary
	if err := json.Unmarshal(out, &s); err != nil {
		t.Fatalf("expected exactly one summary JSON: %v\n%s", err, out)
	}
	return s
}

func TestStdoutOnlyValidation(t *testing.T) {
	for _, tc := range []struct {
		name, level, value string
		code               int
		pass, warn, fail   uint64
	}{
		{"pass", "error", "ok", 0, 1, 0, 0},
		{"warning", "warn", "NULL", 0, 0, 1, 0},
		{"failure", "error", "NULL", 1, 0, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := put(t, filepath.Join(dir, "input.csv"), "id\n"+tc.value+"\n")
			config := put(t, filepath.Join(dir, "pipeline.yml"), fmt.Sprintf("input: {type: csv, mode: raw_strings, null_values: [\"NULL\"]}\noutput:\n  s3: {profile: valet-nonexistent-output-profile, endpoint: 'http://127.0.0.1:1'}\nrules:\n  - code: REQUIRED\n    column: id\n    level: %s\n    checks: [{type: not_null}]\n", tc.level))
			before := stdoutEntries(t, dir)
			out, stderr, code := stdoutInvoke(t, dir, nil, "validate", "-p", config, "-i", input, "--log-format", "json")
			if code != tc.code {
				t.Fatalf("exit=%d want=%d; stderr=%s", code, tc.code, stderr)
			}
			s := stdoutSummary(t, out)
			if s.Total != 1 || s.Passed != tc.pass || s.Warned != tc.warn || s.Errored != tc.fail || s.RuleCounts["REQUIRED"] != tc.warn+tc.fail {
				t.Errorf("unexpected counts: %+v", s)
			}
			if s.ManifestURI != "" || s.OutputMode != "report" || s.Coverage.ValueRows != 1 || s.Coverage.AccountedRows != 1 {
				t.Errorf("unexpected stdout-only metadata: %+v", s)
			}
			if !reflect.DeepEqual(before, stdoutEntries(t, dir)) {
				t.Error("stdout-only validation created files")
			}
			if !bytes.Contains(stderr, []byte(`"manifest_committed":false`)) {
				t.Errorf("missing honest terminal commit state: %s", stderr)
			}
		})
	}
}

func TestStdoutArtifactAndExportSelection(t *testing.T) {
	for _, artifacts := range []bool{false, true} {
		t.Run(fmt.Sprintf("artifacts=%t", artifacts), func(t *testing.T) {
			dir := t.TempDir()
			input := put(t, filepath.Join(dir, "input.csv"), "id\nok\n")
			config := put(t, filepath.Join(dir, "pipeline.yml"), "input: {type: csv, mode: raw_strings}\nrules: []\n")
			export := filepath.Join(dir, "export.json")
			args := []string{"validate", "-p", config, "-i", input, "--json-output", export, "--stats", "--log-format", "json"}
			if artifacts {
				args = append(args, "-o", filepath.Join(dir, "output"))
			}
			out, stderr, code := stdoutInvoke(t, dir, nil, args...)
			if code != 0 {
				t.Fatalf("exit=%d; stderr=%s", code, stderr)
			}
			s := stdoutSummary(t, out)
			body, err := os.ReadFile(export)
			if err != nil {
				t.Fatal(err)
			}
			if exported := stdoutSummary(t, body); !reflect.DeepEqual(s, exported) {
				t.Error("export differs from stdout summary")
			}
			if (s.ManifestURI != "") != artifacts {
				t.Fatalf("manifest=%q; artifacts=%t", s.ManifestURI, artifacts)
			}
			if artifacts {
				if _, err := os.Stat(s.ManifestURI); err != nil {
					t.Fatalf("published manifest: %v", err)
				}
			}
			if !strings.Contains(string(stderr), fmt.Sprintf(`"manifest_committed":%t`, artifacts)) {
				t.Errorf("terminal commit state: %s", stderr)
			}
		})
	}
}

func TestStdoutOperationalFailure(t *testing.T) {
	dir := t.TempDir()
	config := put(t, filepath.Join(dir, "pipeline.yml"), "input: {type: csv, mode: raw_strings}\nrules: []\n")
	before := stdoutEntries(t, dir)
	out, stderr, code := stdoutInvoke(t, dir, nil, "validate", "-p", config, "-i", filepath.Join(dir, "missing.csv"))
	if code != exitOperational || len(out) != 0 || len(stderr) == 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	if !reflect.DeepEqual(before, stdoutEntries(t, dir)) {
		t.Error("failed validation created files")
	}
}

func TestStdoutPresentationFailureWithoutManifest(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("uses Linux /dev/full to fail stdout writes without a signal")
	}
	full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	dir := t.TempDir()
	input := put(t, filepath.Join(dir, "input.csv"), "id\nok\n")
	config := put(t, filepath.Join(dir, "pipeline.yml"), "input: {type: csv, mode: raw_strings}\nrules: []\n")
	before := stdoutEntries(t, dir)
	_, stderr, code := stdoutInvoke(t, dir, full, "validate", "-p", config, "-i", input, "--log-format", "json")
	if code != exitOperational || !bytes.Contains(stderr, []byte(`"manifest_committed":false`)) || bytes.Contains(stderr, []byte("after terminal commit")) {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	if !reflect.DeepEqual(before, stdoutEntries(t, dir)) {
		t.Error("failed stdout presentation created files")
	}
}

func TestVMStdoutMinIOSource(t *testing.T) {
	e := minioEnvironment(t)
	e.put(t, "input.csv", "id\nok\n")
	beforeKeys := e.keys(t, "")
	dir := t.TempDir()
	config := put(t, filepath.Join(dir, "pipeline.yml"), fmt.Sprintf("source:\n  uris: [%q]\n  s3: {region: %q, endpoint: %q, path_style: true}\ninput: {type: csv, mode: raw_strings}\noutput:\n  s3: {profile: valet-nonexistent-output-profile, endpoint: 'http://127.0.0.1:1'}\nrules: []\n", e.uri("input.csv"), e.region, e.endpoint))
	before := stdoutEntries(t, dir)
	out, stderr, code := stdoutInvoke(t, dir, nil, "validate", "-p", config)
	if code != exitOK {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	s := stdoutSummary(t, out)
	if s.Total != 1 || s.Passed != 1 || s.ManifestURI != "" || s.OutputMode != "report" {
		t.Fatalf("unexpected S3 stdout summary: %+v", s)
	}
	if !reflect.DeepEqual(before, stdoutEntries(t, dir)) || !reflect.DeepEqual(beforeKeys, e.keys(t, "")) {
		t.Error("stdout-only S3 validation created files or objects")
	}
}

func TestStdoutTrustedParquet(t *testing.T) {
	input := vmStatInt(t, "1", "2")
	dir := t.TempDir()
	config := put(t, filepath.Join(dir, "pipeline.yml"), "input:\n  type: parquet\n  schema:\n    columns:\n      amount: {type: int64}\n"+vmStatRange("0")+"\n")
	before := stdoutEntries(t, dir)
	out, stderr, code := stdoutInvoke(t, dir, nil, "validate", "-p", config, "-i", input, "--parquet-stats", "trusted")
	if code != exitOK {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	s := stdoutSummary(t, out)
	if s.Total != 0 || s.Passed != 0 || s.Coverage.AccountedRows != 2 || s.Coverage.MetadataClaimedRows != 2 || s.Coverage.ValueRows != 0 || s.TrustPolicy != "producer_statistics" || s.ManifestURI != "" {
		t.Fatalf("unexpected trusted stdout summary: %+v", s)
	}
	if !reflect.DeepEqual(before, stdoutEntries(t, dir)) {
		t.Error("trusted stdout validation created files")
	}
}

func TestStdoutManyIssuesReportsAggregates(t *testing.T) {
	dir := t.TempDir()
	const rows = 1000
	input := put(t, filepath.Join(dir, "input.csv"), "record_id,location,amount\n"+strings.Repeat("NULL,NULL,-1\n", rows))
	config := put(t, filepath.Join(dir, "pipeline.yml"), `input: {type: csv, mode: raw_strings, null_values: ["NULL"]}
rules:
  - {code: ID, column: record_id, level: error, checks: [{type: not_null}]}
  - {code: LOCATION, column: location, level: warn, checks: [{type: not_null}]}
  - {code: RANGE, column: amount, level: error, checks: [{type: valid_range, min: 0, max: 100}]}
  - {code: POSITIVE, column: amount, level: error, checks: [{type: valid_range, min: 0}]}
`)
	before := stdoutEntries(t, dir)
	out, stderr, code := stdoutInvoke(t, dir, nil, "validate", "-p", config, "-i", input)
	if code != exitValidationFailed {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	s := stdoutSummary(t, out)
	if s.Total != rows || s.Errored != rows || s.Warned != 0 || s.Passed != 0 {
		t.Fatalf("row counts must not multiply by issue count: %+v", s)
	}
	for _, rule := range []string{"ID", "LOCATION", "RANGE", "POSITIVE"} {
		if s.RuleCounts[rule] != rows {
			t.Errorf("%s count=%d want=%d", rule, s.RuleCounts[rule], rows)
		}
	}
	if !reflect.DeepEqual(before, stdoutEntries(t, dir)) {
		t.Error("aggregate reporting created artifact files")
	}
}

func TestStdoutSchemaRejection(t *testing.T) {
	dir := t.TempDir()
	input := put(t, filepath.Join(dir, "input.csv"), "id\nok\n")
	config := put(t, filepath.Join(dir, "pipeline.yml"), `input: {type: csv, mode: raw_strings}
rules:
  - code: SCHEMA
    level: error
    checks:
      - type: schema_matches
        mode: strict
        columns: {missing: int64}
        allow_extra_columns: true
`)
	before := stdoutEntries(t, dir)
	out, stderr, code := stdoutInvoke(t, dir, nil, "validate", "-p", config, "-i", input)
	if code != exitValidationFailed {
		t.Fatalf("exit=%d stderr=%s", code, stderr)
	}
	s := stdoutSummary(t, out)
	if s.Status != "schema_rejected" || s.Validation != "fail" || s.Total != 0 || len(s.SchemaIssues) == 0 || s.Verification != "not_evaluated" {
		t.Fatalf("schema rejection must not claim rows were validated: %+v", s)
	}
	if !reflect.DeepEqual(before, stdoutEntries(t, dir)) {
		t.Error("schema rejection created artifact files")
	}
}
