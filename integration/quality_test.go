//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestE2E04CSVFraming(t *testing.T) {
	t.Run("quoted fields, CRLF and BOM", func(t *testing.T) {
		// Escaped on purpose: the BOM, CRLF record ends and the bare LF inside
		// the quoted field are the bytes under test.
		input := "\ufeffid,text\r\n1,\"a,b\"\r\n2,\"line1\nline2\"\r\n3,\"say \"\"yes\"\"\"\r\n"
		r := run(t, scenario{input: input})
		if r.code != exitOK {
			t.Fatalf("validate quoted CSV exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
		}
		if got, want := r.globalRows(t, "pass"), rowNumbers(1, 2, 3); !slices.Equal(got, want) {
			t.Errorf("validate quoted CSV pass rows = %v, want %v", got, want)
		}
		got := column(r.rows(t, "pass"), "text")
		if want := []string{"a,b", "line1\nline2", `say "yes"`}; !slices.Equal(got, want) {
			t.Errorf("decoded text = %q, want %q", got, want)
		}
	})
	malformed := []struct{ name, input string }{
		{name: "unterminated quote", input: joinLines("a,b", `1,"unterminated`)},
		{name: "junk after closing quote", input: joinLines("a,b", `1,"close"junk`)},
		{name: "duplicate header", input: joinLines("a,a", "1,2")},
		{name: "extra field", input: joinLines("a,b", "1,2,3")},
		{name: "invalid UTF-8", input: joinLines("a", "\xff")},
	}
	for _, c := range malformed {
		t.Run(c.name, func(t *testing.T) {
			r := run(t, scenario{input: c.input})
			if r.code != exitOperational {
				t.Errorf("validate malformed CSV %s exit = %d, want %d", c.name, r.code, exitOperational)
			}
		})
	}
	t.Run("configured delimiter", func(t *testing.T) {
		r := run(t, scenario{
			input: `id;text
1;"a;b"
`,
			config: []string{"input: {delimiter: ';'}"},
		})
		if r.code != exitOK {
			t.Fatalf("validate configured delimiter exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
		}
		if got, want := column(r.rows(t, "pass"), "text"), []string{"a;b"}; !slices.Equal(got, want) {
			t.Errorf("pass text = %q, want %q", got, want)
		}
	})
	t.Run("headerless with declared columns", func(t *testing.T) {
		r := run(t, scenario{
			input: `1,a
2,b
`,
			config: []string{"input: {header: false, columns: [id, text]}"},
		})
		if r.code != exitOK {
			t.Fatalf("validate headerless CSV exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
		}
		if got := countsOf(r); got != (counts{total: 2, pass: 2}) {
			t.Errorf("validate headerless CSV counts = %+v, want total:2 pass:2", got)
		}
	})
}

func TestE2E05JSONStructuralLimits(t *testing.T) {
	const schema = "input: {schema: {columns: {id: {type: int64}}}}"
	cases := []struct{ name, inputType, input string }{
		{name: "duplicate", inputType: "ndjson", input: joinLines(`{"id":1,"id":2}`)},
		{name: "duplicate escaped", inputType: "ndjson", input: joinLines(`{"id":1,"\u0069d":2}`)},
		{name: "trailing", inputType: "json", input: `[{"id":1}] true`},
		{name: "late truncation", inputType: "json", input: `[{"id":1},{"id":`},
		{name: "nonrecord", inputType: "ndjson", input: joinLines(`[1,2]`)},
		// 70KiB exceeds the default 64KiB max_record_bytes.
		{name: "oversized", inputType: "ndjson", input: joinLines(`{"id":"` + strings.Repeat("x", 70<<10) + `"}`)},
		{
			name:      "depth",
			inputType: "ndjson",
			input:     joinLines(`{"id":` + strings.Repeat("[", 80) + "0" + strings.Repeat("]", 80) + "}"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := run(t, scenario{input: c.input, inputType: c.inputType, config: []string{schema}})
			if r.code != exitOperational {
				t.Errorf("validate malformed %s %s exit = %d, want %d", c.inputType, c.name, r.code, exitOperational)
			}
		})
	}
	t.Run("non-scalar value is a row reject", func(t *testing.T) {
		r := run(t, scenario{
			input:     joinLines(`{"id":{"nested":1}}`),
			inputType: "ndjson",
			config:    []string{schema},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate non-scalar JSON exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if got, want := issuesOf(r), []issueAt{{1, "INGEST_TYPE"}}; !slices.Equal(got, want) {
			t.Errorf("validate non-scalar JSON issues = %v, want %v", got, want)
		}
		if got := countsOf(r); got != (counts{total: 1, error: 1}) {
			t.Errorf("validate non-scalar JSON counts = %+v, want total:1 error:1", got)
		}
	})
}

func TestE2E06PresenceAndSchema(t *testing.T) {
	t.Run("missing required header rejects schema", func(t *testing.T) {
		r := run(t, scenario{
			input:  "other\nx\n",
			config: []string{"input: {schema: {columns: {id: {type: string, required: true}}}}"},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate missing required header exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if r.summary.Status != "schema_rejected" {
			t.Errorf("status = %q, want schema_rejected", r.summary.Status)
		}
		if r.summary.Passed != 0 {
			t.Errorf("pass = %d, want 0", r.summary.Passed)
		}
		if len(r.summary.SchemaIssues) == 0 {
			t.Errorf("schema_issues is empty, want the missing header")
		}
		if got := len(r.manifests); got != 1 {
			t.Errorf("validate missing required header manifests = %d, want 1", got)
		}
	})
	t.Run("unexpected JSON key fails the row", func(t *testing.T) {
		r := run(t, scenario{
			input:     joinLines(`{"id":1,"unexpected":2}`),
			inputType: "ndjson",
			config:    []string{"input: {schema: {extra_fields: error, columns: {id: {type: int64}}}}"},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate unexpected JSON key exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if got := countsOf(r); got != (counts{total: 1, error: 1}) {
			t.Errorf("validate unexpected JSON key counts = %+v, want total:1 error:1", got)
		}
		if len(r.issues) == 0 {
			t.Errorf("issues are empty, want the unexpected key reported")
		}
	})
	t.Run("optional absent field is not an ingest failure", func(t *testing.T) {
		r := run(t, scenario{
			input: "id\na\n",
			config: []string{
				"input: {schema: {columns: {id: {type: string}, optional: {type: string}}}}",
				"rules: [{code: PRESENT, level: error, column: optional, checks: [{type: is_present}]}]",
			},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate absent optional field exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if got, want := issuesOf(r), []issueAt{{1, "PRESENT"}}; !slices.Equal(got, want) {
			t.Errorf("validate absent optional field issues = %v, want %v", got, want)
		}
		if got := r.summary.IngestCounts["INGEST_MISSING_FIELD"]; got != 0 {
			t.Errorf("ingest_counts[INGEST_MISSING_FIELD] = %d, want 0", got)
		}
	})
}

func TestE2E07WholeDatasetGates(t *testing.T) {
	// The only uncastable value is the last of 1101 rows, so the gate fails
	// after earlier batches would already have been evaluated.
	const rows = 1101
	input := "id\n" + strings.Repeat("1\n", rows-1) + "bad\n"
	castGate := func(level string) string {
		return fmt.Sprintf(`rules:
  - code: CAST_GATE
    level: %s
    checks: [{type: schema_matches, columns: {id: int64}, mode: castable}]
`, level)
	}
	t.Run("error gate rejects before any row output", func(t *testing.T) {
		r := run(t, scenario{input: input, config: []string{castGate("error")}})
		if r.code != exitValidationFailed {
			t.Fatalf("validate error schema gate exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if r.summary.Status != "schema_rejected" {
			t.Errorf("status = %q, want schema_rejected", r.summary.Status)
		}
		if got := len(r.manifests); got != 1 {
			t.Errorf("validate error schema gate manifests = %d, want 1 terminal schema-rejected result", got)
		}
	})
	t.Run("warn gate applies to every row", func(t *testing.T) {
		r := run(t, scenario{input: input, config: []string{castGate("warn")}})
		if r.code != exitOK {
			t.Fatalf("validate warning schema gate exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
		}
		if got := countsOf(r); got != (counts{total: rows, warn: rows}) {
			t.Errorf("validate warning schema gate counts = %+v, want total:%d warn:%d", got, rows, rows)
		}
		if got := r.summary.RuleCounts["CAST_GATE"]; got != rows {
			t.Errorf("rule_counts[CAST_GATE] = %d, want %d", got, rows)
		}
	})
}

func TestE2E10ConditionalEvaluation(t *testing.T) {
	input := `id,active
,true
,false
,NULL
,garbage
x,true
`
	config := `input:
  null_values: ["", "NULL"]
  schema:
    columns:
      id: {type: string}
      active: {type: bool, true_values: ["true"], false_values: ["false"]}
rules:
  - code: ACTIVE_ID
    level: error
    column: id
    when: {column: active, operator: eq, value: true}
    checks: [{type: not_blank}]
`
	r := run(t, scenario{input: input, config: []string{config}})
	if r.code != exitValidationFailed {
		t.Fatalf("validate conditional rule exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
	}
	if got := countsOf(r); got != (counts{total: 5, pass: 3, error: 2}) {
		t.Errorf("validate conditional rule counts = %+v, want total:5 pass:3 error:2", got)
	}
	if got, want := issuesOf(r), []issueAt{{1, "ACTIVE_ID"}, {4, "INGEST_TYPE"}}; !slices.Equal(got, want) {
		t.Errorf("validate conditional rule issues = %v, want %v", got, want)
	}
	if got := r.summary.NotApplicable["ACTIVE_ID"]; got != 2 {
		t.Errorf("not_applicable[ACTIVE_ID] = %d, want 2", got)
	}
	if got := r.summary.Unevaluable["ACTIVE_ID"]; got != 1 {
		t.Errorf("unevaluable[ACTIVE_ID] = %d, want 1", got)
	}
}

func TestE2E12DatesAndTimezones(t *testing.T) {
	t.Run("leap calendar", func(t *testing.T) {
		r := run(t, scenario{
			input: `date
2024-02-29
2023-02-29
2024-02-30
2024-03-01
`,
			config: []string{`rules:
  - code: DATE
    level: error
    column: date
    checks: [{type: date_format, formats: ['%Y-%m-%d']}]
`},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate leap dates exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if got, want := issuesOf(r), []issueAt{{2, "DATE"}, {3, "DATE"}}; !slices.Equal(got, want) {
			t.Errorf("validate leap dates issues = %v, want %v", got, want)
		}
	})
	t.Run("inclusive instants", func(t *testing.T) {
		input := `at
2024-01-01T00:00:00Z
2023-12-31T19:00:00-05:00
2024-01-02T00:00:00Z
2024-01-02T00:00:01Z
`
		config := `input: {schema: {columns: {at: {type: timestamp, timezone: UTC}}}}
rules:
  - code: WINDOW
    level: error
    column: at
    checks: [{type: timestamp_window, min: '2024-01-01T00:00:00Z', max: '2024-01-02T00:00:00Z'}]
`
		// A host zone far from UTC exposes any host-timezone dependence.
		t.Setenv("TZ", "Pacific/Honolulu")
		r := run(t, scenario{input: input, config: []string{config}})
		if r.code != exitValidationFailed {
			t.Fatalf("validate timestamp window exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if got, want := issuesOf(r), []issueAt{{4, "WINDOW"}}; !slices.Equal(got, want) {
			t.Errorf("validate timestamp window issues = %v, want %v", got, want)
		}
		for bucket, want := range map[string][]string{"pass": rowNumbers(1, 2, 3), "warn": nil, "error": rowNumbers(4)} {
			if got := r.globalRows(t, bucket); !slices.Equal(got, want) {
				t.Errorf("validate timestamp window %s rows = %v, want %v", bucket, got, want)
			}
		}
	})
	t.Run("ambiguous local timestamp", func(t *testing.T) {
		r := run(t, scenario{
			input:  "at\n2024-11-03T01:30:00\n",
			config: []string{"input: {schema: {columns: {at: {type: timestamp, timezone: America/New_York}}}}"},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate ambiguous timestamp exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if got, want := issuesOf(r), []issueAt{{1, "INGEST_TYPE"}}; !slices.Equal(got, want) {
			t.Errorf("validate ambiguous timestamp issues = %v, want %v", got, want)
		}
	})
}

func TestE2E13RenamesAndRelativeReferences(t *testing.T) {
	t.Run("inline mapping and relative reference", func(t *testing.T) {
		root := t.TempDir()
		input := put(t, filepath.Join(root, "input.csv"), `old
A
Z
`)
		put(t, filepath.Join(root, "reference.csv"), "key\nA\n")
		r := run(t, scenario{
			inputFile: input,
			config: []string{`transforms: [{type: rename_columns, mapping: {old: key}}]
rules:
  - code: REF
    level: error
    column: key
    checks: [{type: referential_integrity, reference_file: reference.csv, reference_column: key}]
`},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate inline rename/reference exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if got, want := issuesOf(r), []issueAt{{2, "REF"}}; !slices.Equal(got, want) {
			t.Errorf("validate inline rename/reference issues = %v, want %v", got, want)
		}
		if got, want := column(r.rows(t, "pass"), "key"), []string{"A"}; !slices.Equal(got, want) {
			t.Errorf("renamed pass keys = %q, want %q", got, want)
		}
	})
	t.Run("mapping file", func(t *testing.T) {
		root := t.TempDir()
		input := put(t, filepath.Join(root, "input.csv"), "old\nA\n")
		put(t, filepath.Join(root, "mapping.csv"), "old,new\nold,key\n")
		r := run(t, scenario{
			inputFile: input,
			config: []string{`transforms: [{type: rename_columns, mapping_file: mapping.csv}]
rules: [{code: KEY, level: error, column: key, checks: [{type: not_null}]}]
`},
		})
		if r.code != exitOK {
			t.Fatalf("validate mapping file exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
		}
		if got, want := column(r.rows(t, "pass"), "key"), []string{"A"}; !slices.Equal(got, want) {
			t.Errorf("renamed pass keys = %q, want %q", got, want)
		}
	})
	t.Run("schema rename with source key and excluded keys", func(t *testing.T) {
		root := t.TempDir()
		input := put(t, filepath.Join(root, "input.csv"), "old,untouched\nA,B\n")
		put(t, filepath.Join(root, "schema.json"), `{"vendor":{"old":"key"}}`)
		r := run(t, scenario{
			inputFile: input,
			config: []string{`transforms:
  - {type: schema_rename, schema_file: schema.json, source_key: vendor}
  - {type: rename_columns, mapping: {untouched: excluded}, exclude_keys: [untouched]}
rules: [{code: KEY, level: error, column: key, checks: [{type: not_null}]}]
`},
		})
		if r.code != exitOK {
			t.Fatalf("validate schema rename exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
		}
		pass := r.rows(t, "pass")
		if got, want := column(pass, "key"), []string{"A"}; !slices.Equal(got, want) {
			t.Errorf("schema-renamed keys = %q, want %q", got, want)
		}
		if got, want := column(pass, "untouched"), []string{"B"}; !slices.Equal(got, want) {
			t.Errorf("excluded column values = %q, want %q", got, want)
		}
	})
	t.Run("rename collision is rejected", func(t *testing.T) {
		r := run(t, scenario{
			input:  "a,b\nx,y\n",
			config: []string{"transforms: [{type: rename_columns, mapping: {a: b}}]"},
		})
		if r.code != exitOperational {
			t.Errorf("validate rename collision exit = %d, want %d", r.code, exitOperational)
		}
	})
	t.Run("missing operand is rejected", func(t *testing.T) {
		r := run(t, scenario{
			input:  "a\nx\n",
			config: []string{"rules: [{code: MISSING, level: error, column: absent, checks: [{type: not_null}]}]"},
		})
		if r.code != exitOperational {
			t.Errorf("validate missing operand exit = %d, want %d", r.code, exitOperational)
		}
	})
}

func TestE2E19ConfigurationFailures(t *testing.T) {
	expectRejected := func(t *testing.T, path string) {
		t.Helper()
		for _, command := range []string{"validate", "plan"} {
			_, stderr, code := invoke(t, command, "-p", path)
			if code != exitOperational {
				t.Errorf("%s exit = %d, want %d; stderr=%s", command, code, exitOperational, stderr)
			}
		}
	}
	cases := []struct{ name, config string }{
		{
			name:   "unknown check",
			config: "rules: [{code: X, level: error, column: id, checks: [{type: nonexistent}]}]",
		},
		{
			name:   "duplicate rule",
			config: "rules: [{code: X, level: error, column: id, checks: []}, {code: X, level: error, column: id, checks: []}]",
		},
		{
			name:   "unsupported regex",
			config: "rules: [{code: REGEX, level: error, column: id, checks: [{type: matches_pattern, pattern: '(?<=a)b'}]}]",
		},
		{
			name:   "unsupported date",
			config: "rules: [{code: DATE, level: error, column: id, checks: [{type: date_format, format: '%Q'}]}]",
		},
		{name: "unknown key", config: "typo: 1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := run(t, scenario{input: "id\na\n", config: []string{c.config}})
			if r.code != exitOperational {
				t.Errorf("validate invalid config %s exit = %d, want %d", c.name, r.code, exitOperational)
			}
			expectRejected(t, filepath.Join(r.root, "pipeline.yml"))
		})
	}
	t.Run("duplicate YAML", func(t *testing.T) {
		// Duplicate keys cannot be marshalled, so the raw lines are appended
		// to an otherwise valid document.
		root := t.TempDir()
		input := put(t, filepath.Join(root, "input.csv"), "id\na\n")
		path := defaultPipeline(input, "csv", filepath.Join(root, "output"), "csv").write(t, filepath.Join(root, "pipeline.yml"))
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("name: one\nname: two\n")
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			t.Fatal(err)
		}
		expectRejected(t, path)
	})
	t.Run("stdout migration", func(t *testing.T) {
		_, stderr, code := invoke(t, "validate", "--stdout")
		if code != exitOperational {
			t.Errorf("exit = %d, want %d", code, exitOperational)
		}
		if !strings.Contains(strings.ToLower(string(stderr)), "manifest") {
			t.Errorf("stderr = %s, want migration guidance mentioning manifest", stderr)
		}
	})
}
