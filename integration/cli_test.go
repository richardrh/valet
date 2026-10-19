//go:build integration

package integration_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func lines(b []byte) []string {
	var out []string
	for _, line := range strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}

// plainValues parses --plain stdout, failing on any line that is not
// key=value so log or human-summary leakage onto stdout is caught.
func plainValues(t testing.TB, stdout []byte) (keys []string, values map[string]string) {
	t.Helper()
	values = map[string]string{}
	for _, line := range lines(stdout) {
		key, value, ok := strings.Cut(line, "=")
		validKey := key != "" && !strings.ContainsAny(key, " \t")
		if !ok || !validKey {
			t.Fatalf("non key=value line on --plain stdout: %q\nstdout=%s", line, stdout)
		}
		keys = append(keys, key)
		values[key] = value
	}
	return keys, values
}

// block returns the indented lines of a `name={` ... `}` --plain block.
func block(t testing.TB, stdout []byte, name string) []string {
	t.Helper()
	all := lines(stdout)
	for i, line := range all {
		if line != name+"={" {
			continue
		}
		for j := i + 1; j < len(all); j++ {
			if all[j] == "}" {
				return all[i+1 : j]
			}
		}
		t.Fatalf("unterminated %s block:\n%s", name, stdout)
	}
	t.Fatalf("missing %s block:\n%s", name, stdout)
	return nil
}

// logRecord is what the log-format tests need from one stderr line.
type logRecord struct{ runScoped, debug bool }

func parseJSONLog(t testing.TB, line string) logRecord {
	t.Helper()
	var record map[string]any
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		t.Fatalf("stderr line is not a JSON log record: %q: %v", line, err)
	}
	for _, key := range []string{"time", "level", "msg"} {
		if _, ok := record[key]; !ok {
			t.Fatalf("JSON log record lacks %s: %q", key, line)
		}
	}
	return logRecord{runScoped: record["run_id"] != nil, debug: record["level"] == "DEBUG"}
}

func parseTextLog(t testing.TB, line string) logRecord {
	t.Helper()
	for _, key := range []string{"level=", "msg="} {
		if !strings.Contains(line, key) {
			t.Fatalf("stderr text log record lacks %s: %q", key, line)
		}
	}
	return logRecord{runScoped: strings.Contains(line, "run_id="), debug: strings.Contains(line, "level=DEBUG")}
}

func TestCLILogsOnlyOnStderr(t *testing.T) {
	r := run(t, scenario{input: "id\na\n"})
	if r.code != exitOK {
		t.Fatalf("validate logging fixture exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
	}
	path := filepath.Join(r.root, "pipeline.yml")
	formats := []struct {
		name  string
		parse func(testing.TB, string) logRecord
	}{
		{name: "text", parse: parseTextLog},
		{name: "json", parse: parseJSONLog},
	}
	for _, format := range formats {
		t.Run(format.name, func(t *testing.T) {
			for _, flags := range [][]string{{"-v"}, {"--log-level", "debug"}} {
				t.Run(strings.Join(flags, " "), func(t *testing.T) {
					args := append([]string{"validate", "-p", path, "--plain", "--log-format", format.name}, flags...)
					stdout, stderr, code := invoke(t, args...)
					if code != exitOK {
						t.Fatalf("%v exit = %d, want %d; stderr=%s", args, code, exitOK, stderr)
					}
					if _, values := plainValues(t, stdout); values["total"] != "1" {
						t.Errorf("stdout total = %q, want \"1\"; stdout=%s", values["total"], stdout)
					}
					logs := lines(stderr)
					if len(logs) == 0 {
						t.Fatalf("%v stderr has no log lines, want some", args)
					}
					var seen logRecord
					for _, line := range logs {
						record := format.parse(t, line)
						seen.runScoped = seen.runScoped || record.runScoped
						seen.debug = seen.debug || record.debug
					}
					if !seen.runScoped {
						t.Errorf("no log record carries run_id:\n%s", stderr)
					}
				})
			}
			t.Run("default level omits debug", func(t *testing.T) {
				_, stderr, code := invoke(t, "validate", "-p", path, "--plain", "--log-format", format.name)
				if code != exitOK {
					t.Fatalf("exit = %d, want %d; stderr=%s", code, exitOK, stderr)
				}
				if bytes.Contains(stderr, []byte("DEBUG")) {
					t.Errorf("stderr contains DEBUG records, want none:\n%s", stderr)
				}
			})
		})
	}
	t.Run("rejected values", func(t *testing.T) {
		for _, args := range [][]string{{"--log-format", "xml"}, {"--log-level", "loud"}} {
			stdout, stderr, code := invoke(t, append([]string{"validate", "-p", path}, args...)...)
			if code != exitOperational {
				t.Errorf("%v exit = %d, want %d; stderr=%s", args, code, exitOperational, stderr)
			}
			if len(stdout) != 0 {
				t.Errorf("%v stdout = %q, want empty", args, stdout)
			}
		}
	})
	t.Run("json diagnostic", func(t *testing.T) {
		stdout, stderr, code := invoke(t, "validate", "-p", filepath.Join(t.TempDir(), "absent.yml"), "--log-format", "json")
		if code != exitOperational {
			t.Errorf("exit = %d, want %d; stderr=%s", code, exitOperational, stderr)
		}
		if len(stdout) != 0 {
			t.Errorf("stdout = %q, want empty", stdout)
		}
		failed := false
		for _, line := range lines(stderr) {
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatalf("stderr line is not a JSON record: %q: %v", line, err)
			}
			if record["level"] != "ERROR" || record["msg"] != "command failed" {
				continue
			}
			failed = failed || record["error"] != nil
		}
		if !failed {
			t.Errorf("no ERROR \"command failed\" record with an error field:\n%s", stderr)
		}
	})
}

func TestCLIPlainSummary(t *testing.T) {
	t.Run("completed run", func(t *testing.T) {
		root := t.TempDir()
		input := put(t, filepath.Join(root, "input.csv"), fixtureA)
		p := defaultPipeline(input, "csv", filepath.Join(root, "output"), "csv").with(t, rulesA)
		p.Input.NullValues = nulls("")
		r := runPipeline(t, root, p, "--plain")
		if r.code != exitValidationFailed {
			t.Fatalf("validate --plain exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		_, values := plainValues(t, r.stdout)
		want := map[string]string{
			"title":      "VALIDATION SUMMARY",
			"total":      "5",
			"pass":       "1",
			"warn":       "1",
			"error":      "3",
			"status":     r.summary.Status,
			"validation": "fail",
			"run_id":     r.summary.RunID,
		}
		for key, value := range want {
			if values[key] != value {
				t.Errorf("--plain %s = %q, want %q", key, values[key], value)
			}
		}
		if values["output_dir"] == "" {
			t.Errorf("--plain output_dir is empty, want the run directory")
		}
		if !strings.HasSuffix(values["manifest"], "manifest.json") {
			t.Errorf("--plain manifest = %q, want a manifest.json path", values["manifest"])
		}
	})
	t.Run("schema rejected run", func(t *testing.T) {
		rejected := run(t, scenario{
			input:  "other\nx\n",
			config: []string{"input: {schema: {columns: {id: {type: string, required: true}}}}"},
		})
		if rejected.code != exitValidationFailed {
			t.Fatalf("validate schema-rejected fixture exit = %d, want %d; stderr=%s", rejected.code, exitValidationFailed, rejected.stderr)
		}
		stdout, stderr, code := invoke(t, "validate", "-p", filepath.Join(rejected.root, "pipeline.yml"), "--plain")
		if code != exitValidationFailed {
			t.Fatalf("exit = %d, want %d; stderr=%s", code, exitValidationFailed, stderr)
		}
		_, values := plainValues(t, stdout)
		want := map[string]string{
			"status":        "schema_rejected",
			"rows_scanned":  strconv.FormatUint(rejected.summary.RowsScanned, 10),
			"schema_issues": strconv.Itoa(len(rejected.summary.SchemaIssues)),
		}
		for key, value := range want {
			if values[key] != value {
				t.Errorf("--plain %s = %q, want %q", key, values[key], value)
			}
		}
	})
}

func TestCLIStats(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		config     []string
		checkStats []string
	}{
		{
			name:       "rule counts",
			input:      fixtureA,
			config:     []string{rulesA},
			checkStats: []string{"  AMOUNT_RANGE=2", "  COUNTRY_REQUIRED=2", "  ID_REQUIRED=1"},
		},
		{
			name:       "ingest counts",
			input:      fixtureB,
			config:     []string{schemaB, rulesB},
			checkStats: []string{"  INGEST_TYPE=3"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := run(t, scenario{input: c.input, config: c.config})
			if r.code != exitValidationFailed {
				t.Fatalf("validate %s stats fixture exit = %d, want %d; stderr=%s", c.name, r.code, exitValidationFailed, r.stderr)
			}
			path := filepath.Join(r.root, "pipeline.yml")
			stdout, stderr, code := invoke(t, "validate", "-p", path, "--plain", "--stats")
			if code != exitValidationFailed {
				t.Fatalf("--plain --stats exit = %d, want %d; stderr=%s", code, exitValidationFailed, stderr)
			}
			if got := block(t, stdout, "check_stats"); !slices.Equal(got, c.checkStats) {
				t.Errorf("check_stats = %q, want %q", got, c.checkStats)
			}
			stdout, stderr, code = invoke(t, "validate", "-p", path, "--stats")
			if code != exitValidationFailed {
				t.Fatalf("--stats exit = %d, want %d; stderr=%s", code, exitValidationFailed, stderr)
			}
			text := string(stdout)
			at := strings.Index(text, "Check statistics:")
			if at < 0 {
				t.Fatalf("human --stats lacks check statistics:\n%s", stdout)
			}
			for _, line := range c.checkStats {
				rule, count, _ := strings.Cut(strings.TrimSpace(line), "=")
				if want := "  " + count + " " + rule; !strings.Contains(text[at:], want) {
					t.Errorf("human --stats lacks %q:\n%s", want, stdout)
				}
			}
		})
	}
}

func gzipped(t testing.TB, body string) string {
	t.Helper()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := io.WriteString(w, body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// parquetFixtureA returns fixture A encoded as an uncompressed Parquet file.
func parquetFixtureA(t testing.TB) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture-a.parquet")
	writeParquetFixture(t, path, 0)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestCLIInputTypeDetection(t *testing.T) {
	cases := []struct {
		name     string
		file     string
		body     string
		explicit string // input.type; empty means detect from the file name
		rules    string
		want     counts
	}{
		{name: "csv", file: "data.csv", body: fixtureA, rules: rulesA, want: fixtureACounts},
		{name: "gzip csv", file: "data.csv.gz", body: gzipped(t, fixtureA), rules: rulesA, want: fixtureACounts},
		{name: "unknown suffix is csv", file: "data.txt", body: fixtureA, rules: rulesA, want: fixtureACounts},
		{name: "ndjson", file: "data.ndjson", body: fixtureC, rules: rulesC, want: fixtureCCounts},
		{name: "jsonl", file: "data.jsonl", body: fixtureC, rules: rulesC, want: fixtureCCounts},
		{name: "gzip ndjson", file: "data.ndjson.gz", body: gzipped(t, fixtureC), rules: rulesC, want: fixtureCCounts},
		{name: "json", file: "data.json", body: fixtureCArray, rules: rulesC, want: fixtureCCounts},
		{name: "parquet", file: "data.parquet", body: parquetFixtureA(t), rules: rulesA, want: fixtureACounts},
		{
			name:     "explicit type wins over suffix",
			file:     "data.json",
			body:     fixtureA,
			explicit: "csv",
			rules:    rulesA,
			want:     fixtureACounts,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			path := put(t, filepath.Join(root, c.file), c.body)
			p := defaultPipeline(path, c.explicit, filepath.Join(root, "output"), "csv").with(t, c.rules)
			p.Input.NullValues = nulls("")
			r := runPipeline(t, root, p)
			if r.code != exitValidationFailed {
				t.Fatalf("validate input type %s exit = %d, want %d; stderr=%s", c.name, r.code, exitValidationFailed, r.stderr)
			}
			if diff := cmp.Diff(c.want, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
				t.Errorf("validate input type %s counts diff (-want +got):\n%s", c.name, diff)
			}
		})
	}
}

// Numeric literals against string-typed (raw_strings) columns parse the cell
// as a number; an unparseable cell is unevaluable, never silently false.
// String literals keep exact text comparison.
func TestCLIStringColumnNumericLiterals(t *testing.T) {
	t.Run("condition", func(t *testing.T) {
		r := run(t, scenario{
			input: `id,amount
x,10
,-1
,abc
,10.0
`,
			config: []string{
				"rules: [{code: POSITIVE_ID, level: error, column: id, " +
					"when: {column: amount, operator: gt, value: 0}, checks: [{type: not_null}]}]",
			},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate numeric condition exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if diff := cmp.Diff(counts{total: 4, pass: 3, error: 1}, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
			t.Errorf("validate numeric condition counts diff (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]issueAt{{4, "POSITIVE_ID"}}, issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
			t.Errorf("validate numeric condition issues diff (-want +got):\n%s", diff)
		}
		if got := r.summary.NotApplicable["POSITIVE_ID"]; got != 1 {
			t.Errorf("not_applicable[POSITIVE_ID] = %d, want 1", got)
		}
		if got := r.summary.Unevaluable["POSITIVE_ID"]; got != 1 {
			t.Errorf("unevaluable[POSITIVE_ID] = %d, want 1", got)
		}
	})
	const input = `x
10
10.0
11
abc
`
	cases := []struct {
		name        string
		check       string
		failed      []uint64
		unevaluable uint64
	}{
		{name: "equals string literal", check: "{type: equals, value: '10'}", failed: []uint64{2, 3, 4}},
		{name: "equals numeric literal", check: "{type: equals, value: 10}", failed: []uint64{3}, unevaluable: 1},
		{
			name:   "valid_values string literals",
			check:  "{type: valid_values, values: ['10']}",
			failed: []uint64{2, 3, 4},
		},
		{
			name:        "valid_values numeric literals",
			check:       "{type: valid_values, values: [10]}",
			failed:      []uint64{3},
			unevaluable: 1,
		},
		// valid_range keeps spec semantics: a non-numeric cell is a violation.
		{name: "valid_range non-numeric violates", check: "{type: valid_range, min: 0, max: 10}", failed: []uint64{3, 4}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rules := fragment(t, obj{"rules": []obj{{
				"code":   "CHECK",
				"level":  "error",
				"column": "x",
				"checks": []any{yamlValue(t, c.check)},
			}}})
			r := run(t, scenario{input: input, config: []string{rules}})
			if r.code != exitValidationFailed {
				t.Fatalf("validate %s exit = %d, want %d; stderr=%s", c.name, r.code, exitValidationFailed, r.stderr)
			}
			if diff := cmp.Diff(checkIssues(c.failed), issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
				t.Errorf("validate %s issues diff (-want +got):\n%s", c.name, diff)
			}
			if got := r.summary.Unevaluable["CHECK"]; got != c.unevaluable {
				t.Errorf("unevaluable[CHECK] = %d, want %d", got, c.unevaluable)
			}
			failed := uint64(len(c.failed))
			wantCounts := counts{total: 4, pass: 4 - failed, error: failed}
			if diff := cmp.Diff(wantCounts, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
				t.Errorf("validate %s counts diff (-want +got):\n%s", c.name, diff)
			}
		})
	}
}

func TestCLIPipelineFormatsAreEquivalent(t *testing.T) {
	root := t.TempDir()
	input := put(t, filepath.Join(root, "rows.csv"), `x
a
b
`)
	yamlPath := put(t, filepath.Join(root, "pipeline.yaml"), `input:
  type: csv
  mode: raw_strings
execution: {memory_budget: 64MiB, max_record_bytes: 1MiB}
rules:
  - code: MUST_BE_A
    level: error
    column: x
    checks: [{type: equals, value: a}]
`)
	jsonPath := put(t, filepath.Join(root, "pipeline.json"), `{
  "input": {"type": "csv", "mode": "raw_strings"},
  "execution": {"memory_budget": "64MiB", "max_record_bytes": "1MiB"},
  "rules": [{
    "code": "MUST_BE_A",
    "level": "error",
    "column": "x",
    "checks": [{"type": "equals", "value": "a"}]
  }]
}`)
	var plans []map[string]any
	var summaries []summary
	for i, pipeline := range []string{yamlPath, jsonPath} {
		stdout, stderr, code := invoke(t, "plan", "-p", pipeline, "-i", input)
		if code != exitOK {
			t.Fatalf("plan %s exit = %d, want %d; stderr=%s", pipeline, code, exitOK, stderr)
		}
		var plan map[string]any
		if err := json.Unmarshal(stdout, &plan); err != nil {
			t.Fatal(err)
		}
		delete(plan, "run_id")
		plans = append(plans, plan)

		out := filepath.Join(root, "out-"+strconv.Itoa(i))
		stdout, stderr, code = invoke(t, "validate", "-p", pipeline, "-i", input, "-o", out, "--json-output", "-")
		if code != exitValidationFailed {
			t.Fatalf("validate %s exit = %d, want %d; stderr=%s", pipeline, code, exitValidationFailed, stderr)
		}
		var result summary
		if err := json.Unmarshal(stdout, &result); err != nil {
			t.Fatal(err)
		}
		summaries = append(summaries, result)
	}
	if !reflect.DeepEqual(plans[0], plans[1]) {
		t.Errorf("YAML and JSON plans differ:\nYAML=%v\nJSON=%v", plans[0], plans[1])
	}
	left, right := summaries[0], summaries[1]
	left.RunID, left.ManifestURI, left.DurationSeconds = "", "", 0
	right.RunID, right.ManifestURI, right.DurationSeconds = "", "", 0
	if !reflect.DeepEqual(left, right) {
		t.Errorf("YAML and JSON validation summaries differ:\nYAML=%+v\nJSON=%+v", left, right)
	}
}
