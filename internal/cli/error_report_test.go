package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// commandFailure returns the JSON log record that reports why the command
// failed.
func commandFailure(t *testing.T, r result) map[string]any {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(r.stderr), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("non-JSON stderr line %q: %v", line, err)
		}
		if record["msg"] == "command failed" {
			return record
		}
	}
	t.Fatalf("no command failed record in stderr:\n%s", r.stderr)
	return nil
}

func TestConfigurationErrorsAreShown(t *testing.T) {
	for _, tc := range []struct {
		name, pipeline, want, class string
	}{
		{
			name: "unknown field",
			pipeline: `input: {type: csv}
rules:
  - code: R
    column: id
    chekcs: []
`,
			want:  "unknown field config.rules[0].chekcs (line 5)",
			class: "operation",
		},
		{
			name:     "invalid execution limit",
			pipeline: "input: {type: csv}\nexecution: {max_depth: 0}\nrules: []\n",
			want:     "execution.max_depth must be between 1 and 1024",
			class:    "operation",
		},
		{name: "missing pipeline", want: "open configuration", class: "not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			pipeline := filepath.Join(dir, "missing.yml")
			if tc.pipeline != "" {
				pipeline = putFixture(t, dir, "pipeline.yml", tc.pipeline)
			}
			r := invoke(t, "validate", "-p", pipeline, "-i", filepath.Join(dir, "rows.csv"), "--log-format", "json")
			assertDiagnosticOnly(t, r, ExitOperational)
			failure := commandFailure(t, r)
			if got, _ := failure["error"].(string); !strings.Contains(got, tc.want) {
				t.Errorf("error = %q, want it to contain %q", got, tc.want)
			}
			if failure["error_class"] != tc.class {
				t.Errorf("error_class = %v, want %s", failure["error_class"], tc.class)
			}
		})
	}
}

// Malformed records are reported by position and reason; the record's
// contents never reach stderr.
func TestMalformedInputIsLocated(t *testing.T) {
	const secret = "SECRET-CELL"
	for _, tc := range []struct {
		name, inputType, rows string
		fields                map[string]any
		want                  string
	}{
		{
			name:      "csv extra field",
			inputType: "csv",
			rows:      "id,amount\n1,2\n" + secret + ",3,EXTRA\n",
			fields:    map[string]any{"input_format": "csv", "input_line": 3.0, "framing_reason": "field_count", "expected_fields": 2.0, "found_fields": 3.0},
			want:      "rows.csv at line 3: found 3 fields; the header has 2",
		},
		{
			name:      "csv missing field",
			inputType: "csv",
			rows:      "id,amount\n1,2\n" + secret + "\n",
			fields:    map[string]any{"input_format": "csv", "input_line": 3.0, "framing_reason": "field_count", "expected_fields": 2.0, "found_fields": 1.0},
			want:      "rows.csv at line 3: found 1 field; the header has 2",
		},
		{
			name:      "csv unterminated quote",
			inputType: "csv",
			rows:      "id,amount\n1,2\n3,\"" + secret + "\n4,5\n",
			fields:    map[string]any{"input_format": "csv", "input_line": 3.0, "framing_reason": "quote"},
			want:      "malformed CSV in ",
		},
		{
			// The stray quote is on line 4, but the record starts on line 3;
			// a column counted on line 4 must not be paired with line 3.
			name:      "csv bad quote in multi-line field",
			inputType: "csv",
			rows:      "id,amount\n1,2\n\"" + secret + "\nx\"z,1\n",
			fields:    map[string]any{"input_format": "csv", "input_line": 3.0, "input_column": nil, "framing_reason": "quote"},
			want:      "rows.csv at line 3: extraneous",
		},
		{
			name:      "ndjson syntax",
			inputType: "ndjson",
			rows:      "{\"id\":\"1\"}\n{\"id\": " + secret + "}\n",
			fields:    map[string]any{"input_format": "ndjson", "input_line": 2.0, "framing_reason": "json_syntax"},
			want:      "rows.ndjson at line 2: invalid JSON syntax",
		},
		{
			name:      "ndjson duplicate key",
			inputType: "ndjson",
			rows:      "{\"id\":\"1\"}\n{\"id\":\"" + secret + "\",\"id\":\"2\"}\n",
			fields:    map[string]any{"input_format": "ndjson", "input_line": 2.0, "framing_reason": "duplicate_key"},
			want:      "duplicate object key",
		},
		{
			// {"id":"1"} plus newline is 11 bytes, so the bad value starts at
			// offset 11, on line 2.
			name:      "ndjson record not an object",
			inputType: "ndjson",
			rows:      "{\"id\":\"1\"}\n123456789\n",
			fields:    map[string]any{"input_format": "ndjson", "input_line": 2.0, "input_offset": 11.0, "framing_reason": "not_object"},
			want:      "rows.ndjson at line 2: records must be objects",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			input := putFixture(t, dir, "rows."+tc.inputType, tc.rows)
			pipeline := putFixture(t, dir, "pipeline.yml", "input: {type: "+tc.inputType+", mode: raw_strings}\nrules: []\n")
			r := invoke(t, "validate", "-p", pipeline, "-i", input, "--log-format", "json")
			assertDiagnosticOnly(t, r, ExitOperational)
			if strings.Contains(r.stderr, secret) {
				t.Errorf("stderr exposes record contents:\n%s", r.stderr)
			}
			failure := commandFailure(t, r)
			if failure["error_class"] != "input_format" {
				t.Errorf("error_class = %v, want input_format", failure["error_class"])
			}
			if failure["input_source"] != input {
				t.Errorf("input_source = %v, want %s", failure["input_source"], input)
			}
			if hint, _ := failure["hint"].(string); hint == "" {
				t.Error("hint is empty, want advice for the reason")
			}
			for key, want := range tc.fields {
				if failure[key] != want {
					t.Errorf("%s = %v, want %v", key, failure[key], want)
				}
			}
			if got, _ := failure["error"].(string); !strings.Contains(got, tc.want) {
				t.Errorf("error = %q, want it to contain %q", got, tc.want)
			}
		})
	}
}

// In the default text log format, a failed run ends with a plain-language
// error and hint after the log records, so the reason is the last thing read.
func TestFailedRunEndsWithReadableError(t *testing.T) {
	const secret = "SECRET-CELL"
	dir := t.TempDir()
	input := putFixture(t, dir, "orders.csv", "id,amount,qty\n1,2.50,3\n"+secret+",3.00,4,EXTRA\n")
	pipeline := putFixture(t, dir, "pipeline.yml", "input: {type: csv}\nrules: []\n")
	broken := putFixture(t, dir, "broken.yml", "input: {type: csv}\nrules:\n  - {code: R, level: error, column: id, chekcs: []}\n")
	for _, tc := range []struct {
		name     string
		args     []string
		wantTail []string
	}{
		{
			name: "malformed csv",
			args: []string{"validate", "-p", pipeline, "-i", input},
			wantTail: []string{
				"valet: error: malformed CSV in " + input + " at line 3: found 4 fields; the header has 3",
				`valet: hint: quote any field that contains ",", and check this line for a missing or extra ","; set input.on_malformed: reject to reject such rows and check the rest of the file`,
			},
		},
		{
			name:     "configuration",
			args:     []string{"validate", "-p", broken, "-i", input},
			wantTail: []string{"valet: error: load pipeline: unknown field config.rules[0].chekcs (line 3)"},
		},
		{
			name:     "invalid flag",
			args:     []string{"validate", "-p", pipeline, "--log-format", "xml"},
			wantTail: []string{`valet: error: --log-format must be text or json, not "xml"`},
		},
		{
			name:     "schema on csv",
			args:     []string{"schema", "-p", pipeline, "-i", input},
			wantTail: []string{"valet: error: valet schema reads Parquet metadata only; for csv input, run valet plan --format pipeline to infer a schema"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := invoke(t, tc.args...)
			assertDiagnosticOnly(t, r, ExitOperational)
			lines := strings.Split(strings.TrimRight(r.stderr, "\n"), "\n")
			if len(lines) < len(tc.wantTail) {
				t.Fatalf("stderr has %d lines, want at least %d:\n%s", len(lines), len(tc.wantTail), r.stderr)
			}
			got := lines[len(lines)-len(tc.wantTail):]
			if strings.Join(got, "\n") != strings.Join(tc.wantTail, "\n") {
				t.Errorf("stderr ends with:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(tc.wantTail, "\n"))
			}
			if strings.Contains(r.stderr, secret) {
				t.Errorf("stderr exposes record contents:\n%s", r.stderr)
			}
		})
	}
}
