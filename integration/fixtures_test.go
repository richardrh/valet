//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

const fixtureA = `id,amount,country
a,10,GB
b,-1,GB
c,20,
d,-1,
,5,GB
`
const rulesA = `rules:
  - code: ID_REQUIRED
    column: id
    level: error
    checks: [{type: not_null}]
  - code: AMOUNT_RANGE
    column: amount
    level: error
    checks: [{type: valid_range, min: 0}]
  - code: COUNTRY_REQUIRED
    column: country
    level: warn
    checks: [{type: not_null}]
`

// Fixture A's oracle under rulesA.
var (
	fixtureACounts = counts{total: 5, pass: 1, warn: 1, error: 3}
	fixtureAIssues = []issueAt{
		{2, "AMOUNT_RANGE"},
		{3, "COUNTRY_REQUIRED"},
		{4, "AMOUNT_RANGE"},
		{4, "COUNTRY_REQUIRED"},
		{5, "ID_REQUIRED"},
	}
)

const fixtureB = `id,amount,units
0007,12.30,2
0008,oops,2
0009,1.001,3
0010,5.00,1.5
0011,,4
`
const schemaB = `input:
  schema:
    columns:
      id: {type: string}
      amount: {type: decimal, precision: 10, scale: 2}
      units: {type: int64}
`
const rulesB = `rules:
  - code: AMOUNT_NONNEGATIVE
    column: amount
    level: error
    checks: [{type: valid_range, min: 0}]
`

// Fixture B's oracle under schemaB and rulesB: three conversion failures.
var fixtureBCounts = counts{total: 5, pass: 2, error: 3}

const uniqueKey = `rules:
  - code: UNIQUE
    level: error
    column: key
    checks: [{type: is_unique}]
`
const fixtureC = `{"id":9007199254740993,"note":null}
{"id":9007199254740994}
{"id":9007199254740995,"note":""}
`

// fixtureCArray is fixtureC as a single JSON array document.
const fixtureCArray = `[{"id":9007199254740993,"note":null},{"id":9007199254740994},{"id":9007199254740995,"note":""}]`

const rulesC = `input:
  schema:
    columns:
      id: {type: int64, required: true, nullable: false}
      note: {type: string}
rules:
  - code: NOTE_PRESENT
    column: note
    level: error
    checks: [{type: is_present}]
  - code: NOTE_NONBLANK
    column: note
    level: warn
    checks: [{type: not_blank}]
`

// Fixture C's oracle under rulesC.
var fixtureCCounts = counts{total: 3, warn: 2, error: 1}

func TestE2E01FixtureA(t *testing.T) {
	for _, format := range []string{"csv", "parquet"} {
		t.Run(format, func(t *testing.T) {
			r := run(t, scenario{input: fixtureA, format: format, config: []string{rulesA}})
			if r.code != exitValidationFailed {
				t.Fatalf("validate fixture A as %s exit = %d, want %d; stderr=%s", format, r.code, exitValidationFailed, r.stderr)
			}
			if diff := cmp.Diff(fixtureACounts, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
				t.Errorf("validate fixture A as %s counts diff (-want +got):\n%s", format, diff)
			}
			if diff := cmp.Diff(fixtureAIssues, issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
				t.Errorf("validate fixture A as %s issues diff (-want +got):\n%s", format, diff)
			}
			wantRuleCounts := map[string]uint64{"ID_REQUIRED": 1, "AMOUNT_RANGE": 2, "COUNTRY_REQUIRED": 2}
			if !maps.Equal(r.summary.RuleCounts, wantRuleCounts) {
				t.Errorf("rule_counts = %v, want %v", r.summary.RuleCounts, wantRuleCounts)
			}
			for bucket, want := range map[string][]string{"pass": rowNumbers(1), "warn": rowNumbers(3), "error": rowNumbers(2, 4, 5)} {
				if diff := cmp.Diff(want, r.globalRows(t, bucket)); diff != "" {
					t.Errorf("validate fixture A as %s %s rows diff (-want +got):\n%s", format, bucket, diff)
				}
			}
			if r.summary.ReportVersion != 2 {
				t.Errorf("summary report_version = %d, want 2", r.summary.ReportVersion)
			}
			if got := r.manifest["status"]; got != r.summary.Status {
				t.Errorf("manifest status = %v, want summary status %q", got, r.summary.Status)
			}
			if err := metadataError(r.dir(t), r.manifest); err != nil {
				t.Errorf("validate fixture A manifest metadata: %v", err)
			}
			for _, issue := range r.issues {
				if issue.Source == "" || issue.SourceOrdinal == 0 || issue.Row == 0 || issue.GlobalRow == 0 {
					t.Errorf("row issue lacks 1-based provenance: %+v", issue)
				}
			}
		})
	}
}

func TestE2E02FixtureB(t *testing.T) {
	fixtureBScenario := scenario{input: fixtureB, config: []string{schemaB, rulesB}}
	r := run(t, fixtureBScenario)
	if r.code != exitValidationFailed {
		t.Fatalf("validate fixture B exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
	}
	if diff := cmp.Diff(fixtureBCounts, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
		t.Errorf("validate fixture B counts diff (-want +got):\n%s", diff)
	}
	wantIssues := []issueAt{{2, "INGEST_TYPE"}, {3, "INGEST_TYPE"}, {4, "INGEST_TYPE"}}
	if diff := cmp.Diff(wantIssues, issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
		t.Errorf("validate fixture B issues diff (-want +got):\n%s", diff)
	}
	for bucket, want := range map[string][]string{"pass": rowNumbers(1, 5), "warn": nil, "error": rowNumbers(2, 3, 4)} {
		if diff := cmp.Diff(want, r.globalRows(t, bucket)); diff != "" {
			t.Errorf("validate fixture B %s rows diff (-want +got):\n%s", bucket, diff)
		}
	}

	t.Run("conversion failures do not cascade into rules", func(t *testing.T) {
		if got := r.summary.Unevaluable["AMOUNT_NONNEGATIVE"]; got != 2 {
			t.Errorf("unevaluable[AMOUNT_NONNEGATIVE] = %d, want 2", got)
		}
		if got := r.summary.RuleCounts["AMOUNT_NONNEGATIVE"]; got != 0 {
			t.Errorf("rule_counts[AMOUNT_NONNEGATIVE] = %d, want 0", got)
		}
		var issueColumns []string
		for _, i := range r.issues {
			issueColumns = append(issueColumns, i.Column)
		}
		if want := []string{"amount", "amount", "units"}; !slices.Equal(issueColumns, want) {
			t.Errorf("ingestion issue columns = %q, want %q", issueColumns, want)
		}
	})
	t.Run("exact identifiers and decimals", func(t *testing.T) {
		pass := r.rows(t, "pass")
		if got, want := column(pass, "id"), []string{"0007", "0011"}; !slices.Equal(got, want) {
			t.Errorf("pass ids = %q, want %q", got, want)
		}
		// Row 0011's blank amount is null, published as \N.
		if got, want := column(pass, "amount"), []string{"12.30", `\N`}; !slices.Equal(got, want) {
			t.Errorf("pass amounts = %q, want %q", got, want)
		}
	})
	t.Run("rejects keep raw values", func(t *testing.T) {
		rejects := readRejects(t, r)
		want := []string{"oops", "1.001", "1.5"}
		if len(rejects) != len(want) {
			t.Fatalf("rejects = %d, want %d", len(rejects), len(want))
		}
		for i, raw := range want {
			if got := string(rejects[i]["raw"]); !strings.Contains(got, raw) {
				t.Errorf("reject %d raw = %s, want it to contain %q", i, got, raw)
			}
		}
	})
	t.Run("abort", func(t *testing.T) {
		aborted := run(t, scenario{
			input:  fixtureB,
			config: []string{"input: {on_type_error: abort}", schemaB, rulesB},
		})
		if aborted.code != exitOperational {
			t.Errorf("validate fixture B with abort exit = %d, want %d", aborted.code, exitOperational)
		}
	})
	t.Run("parquet exact values", func(t *testing.T) {
		parquetScenario := fixtureBScenario
		parquetScenario.format = "parquet"
		p := run(t, parquetScenario)
		if p.code != exitValidationFailed {
			t.Fatalf("validate fixture B as Parquet exit = %d, want %d; stderr=%s", p.code, exitValidationFailed, p.stderr)
		}
		for bucket, want := range map[string][]string{"pass": rowNumbers(1, 5), "warn": nil, "error": rowNumbers(2, 3, 4)} {
			if diff := cmp.Diff(want, p.globalRows(t, bucket)); diff != "" {
				t.Errorf("validate fixture B as Parquet %s rows diff (-want +got):\n%s", bucket, diff)
			}
		}
		want := []fixtureBDecimal{
			{ID: "0007", Type: "decimal(10,2)", Coefficient: "1230"},
			{ID: "0011", Type: "decimal(10,2)", Null: true},
		}
		if diff := cmp.Diff(want, fixtureBDecimals(t, p)); diff != "" {
			t.Errorf("validate fixture B as Parquet decimals diff (-want +got):\n%s", diff)
		}
	})
}

// readRejects decodes every record of the run's rejects stream.
func readRejects(t testing.TB, r *runResult) []map[string]json.RawMessage {
	t.Helper()
	var rejects []map[string]json.RawMessage
	for _, path := range files(t, filepath.Join(r.dir(t), "rejects", "*.ndjson")) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			var reject map[string]json.RawMessage
			if err := json.Unmarshal([]byte(line), &reject); err != nil {
				t.Fatal(err)
			}
			rejects = append(rejects, reject)
		}
	}
	return rejects
}

func TestE2E03FixtureC(t *testing.T) {
	cases := []struct {
		inputType, input string
	}{
		{inputType: "ndjson", input: fixtureC},
		{inputType: "json", input: fixtureCArray},
	}
	for _, c := range cases {
		t.Run(c.inputType, func(t *testing.T) {
			r := run(t, scenario{
				input:     c.input,
				inputType: c.inputType,
				config:    []string{rulesC},
			})
			if r.code != exitValidationFailed {
				t.Fatalf("validate fixture C as %s exit = %d, want %d; stderr=%s", c.inputType, r.code, exitValidationFailed, r.stderr)
			}
			if diff := cmp.Diff(fixtureCCounts, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
				t.Errorf("validate fixture C as %s counts diff (-want +got):\n%s", c.inputType, diff)
			}
			wantIssues := []issueAt{{1, "NOTE_NONBLANK"}, {2, "NOTE_PRESENT"}, {2, "NOTE_NONBLANK"}, {3, "NOTE_NONBLANK"}}
			if diff := cmp.Diff(wantIssues, issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
				t.Errorf("validate fixture C as %s issues diff (-want +got):\n%s", c.inputType, diff)
			}
			for bucket, want := range map[string][]string{"pass": nil, "warn": rowNumbers(1, 3), "error": rowNumbers(2)} {
				if diff := cmp.Diff(want, r.globalRows(t, bucket)); diff != "" {
					t.Errorf("validate fixture C as %s %s rows diff (-want +got):\n%s", c.inputType, bucket, diff)
				}
			}

			warn, failed := r.rows(t, "warn"), r.rows(t, "error")
			if got, want := column(warn, "id"), []string{"9007199254740993", "9007199254740995"}; !slices.Equal(got, want) {
				t.Errorf("warn ids = %q, want %q (int64 precision)", got, want)
			}
			if got, want := column(failed, "id"), []string{"9007199254740994"}; !slices.Equal(got, want) {
				t.Errorf("error ids = %q, want %q (int64 precision)", got, want)
			}
			// Explicit null and empty string stay distinct.
			if got, want := column(warn, "note"), []string{`\N`, ""}; !slices.Equal(got, want) {
				t.Errorf("warn notes = %q, want %q", got, want)
			}
			for code, n := range r.summary.IngestCounts {
				if n != 0 {
					t.Errorf("ingest_counts[%s] = %d, want 0", code, n)
				}
			}
		})
	}
}

func TestE2E08FixtureDUniqueness(t *testing.T) {
	root := t.TempDir()
	one := put(t, filepath.Join(root, "one.csv"), `key
A
B
`)
	two := put(t, filepath.Join(root, "two.csv"), `key
C
A
`)
	p := defaultPipeline(one, "csv", filepath.Join(root, "out"), "csv").with(t, uniqueKey)
	p.Source.URIs = append(p.Source.URIs, filepath.ToSlash(two))
	p.Input.NullValues = nulls("")
	p.Execution = executionSpec{BatchTarget: "64B", MaxRecordBytes: "1KiB"}
	r := runPipeline(t, root, p)
	if r.code != exitValidationFailed {
		t.Fatalf("validate cross-file uniqueness exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
	}
	if diff := cmp.Diff(counts{total: 4, pass: 2, error: 2}, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
		t.Errorf("validate cross-file uniqueness counts diff (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]issueAt{{1, "UNIQUE"}, {4, "UNIQUE"}}, issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
		t.Errorf("validate cross-file uniqueness issues diff (-want +got):\n%s", diff)
	}
	for bucket, want := range map[string][]string{"pass": rowNumbers(2, 3), "warn": nil, "error": rowNumbers(1, 4)} {
		if diff := cmp.Diff(want, r.globalRows(t, bucket)); diff != "" {
			t.Errorf("validate cross-file uniqueness %s rows diff (-want +got):\n%s", bucket, diff)
		}
	}

	failed := r.rows(t, "error")
	if got, want := column(failed, "__valet_source_ordinal"), []string{"1", "2"}; !slices.Equal(got, want) {
		t.Errorf("error source ordinals = %q, want %q", got, want)
	}
	if got, want := column(failed, "__valet_row"), []string{"1", "2"}; !slices.Equal(got, want) {
		t.Errorf("error rows within source = %q, want %q", got, want)
	}

	t.Run("null", func(t *testing.T) {
		r := run(t, scenario{
			input: `key
NULL
A
`,
			config: []string{"input: {null_values: ['NULL']}", uniqueKey},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate null uniqueness exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if diff := cmp.Diff([]issueAt{{1, "UNIQUE"}}, issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
			t.Errorf("validate null uniqueness issues diff (-want +got):\n%s", diff)
		}
	})
}

func TestE2E09FixtureDReferenceTuples(t *testing.T) {
	root := t.TempDir()
	ref := put(t, filepath.Join(root, "ref.csv"), `country,region
US,CA
NULL,x
`)
	input := put(t, filepath.Join(root, "data.csv"), `country,region
US,CA
CA,US
,x
NULL,x
`)
	p := defaultPipeline(input, "csv", filepath.Join(root, "output"), "csv")
	p.Input.NullValues = nulls("NULL")
	p.Execution = executionSpec{}
	p.with(t, fragment(t, obj{"rules": []obj{{
		"code":  "MEMBER",
		"level": "error",
		"checks": []obj{{
			"type":              "referential_integrity_composite",
			"columns":           []string{"country", "region"},
			"reference_file":    filepath.ToSlash(ref),
			"reference_columns": []string{"country", "region"},
		}},
	}}}))
	r := runPipeline(t, root, p)
	if r.code != exitValidationFailed {
		t.Fatalf("validate reference tuples exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
	}
	if diff := cmp.Diff(counts{total: 4, pass: 2, error: 2}, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
		t.Errorf("validate reference tuples counts diff (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]issueAt{{2, "MEMBER"}, {3, "MEMBER"}}, issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
		t.Errorf("validate reference tuples issues diff (-want +got):\n%s", diff)
	}
	for bucket, want := range map[string][]string{"pass": rowNumbers(1, 4), "warn": nil, "error": rowNumbers(2, 3)} {
		if diff := cmp.Diff(want, r.globalRows(t, bucket)); diff != "" {
			t.Errorf("validate reference tuples %s rows diff (-want +got):\n%s", bucket, diff)
		}
	}
}

func TestE2E11FixtureE(t *testing.T) {
	const amountSchema = "input: {schema: {columns: {amount: {type: int64}}}}"
	const headerOnly = "amount\n"
	t.Run("empty dataset fails dataset rule", func(t *testing.T) {
		r := run(t, scenario{
			input: headerOnly,
			config: []string{
				amountSchema,
				"dataset_rules: [{code: DATASET_NOT_EMPTY, metric: row_count, min: 1, level: error}]",
			},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate empty dataset rule exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if diff := cmp.Diff(counts{}, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
			t.Errorf("validate empty dataset counts diff (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff([]issueAt{{0, "DATASET_NOT_EMPTY"}}, issuesOf(r), cmp.AllowUnexported(issueAt{})); diff != "" {
			t.Errorf("validate empty dataset issues diff (-want +got):\n%s", diff)
		}
		if r.summary.Validation != "fail" {
			t.Errorf("validation = %q, want fail", r.summary.Validation)
		}
	})
	t.Run("empty dataset without dataset rules passes", func(t *testing.T) {
		r := run(t, scenario{input: headerOnly, config: []string{amountSchema}})
		if r.code != exitOK {
			t.Errorf("validate empty dataset without dataset rules exit = %d, want %d", r.code, exitOK)
		}
	})
	t.Run("empty file is operational failure", func(t *testing.T) {
		r := run(t, scenario{input: "", config: []string{amountSchema}})
		if r.code != exitOperational {
			t.Errorf("validate empty file exit = %d, want %d", r.code, exitOperational)
		}
	})
	t.Run("conversion fraction above max fails", func(t *testing.T) {
		r := run(t, scenario{
			input: fixtureB,
			config: []string{
				schemaB,
				rulesB,
				"dataset_rules: [{code: PARSE_RATE, level: error, metric: conversion_error_fraction, column: amount, max: 0.3}]",
			},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate conversion fraction exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if diff := cmp.Diff(fixtureBCounts, countsOf(r), cmp.AllowUnexported(counts{})); diff != "" {
			t.Errorf("validate conversion fraction counts diff (-want +got):\n%s", diff)
		}
		var codes []string
		for _, i := range r.summary.DatasetIssues {
			codes = append(codes, i.Code)
		}
		if want := []string{"PARSE_RATE"}; !slices.Equal(codes, want) {
			t.Errorf("dataset issue codes = %q, want %q", codes, want)
		}
	})
	// Fixture B's amount column has 2 conversion errors in 5 rows: 0.4.
	bounds := []struct{ name, bound string }{
		{name: "min equal to observed passes", bound: "min: 0.4"},
		{name: "max equal to observed passes", bound: "max: 0.4"},
		{name: "equals observed passes", bound: "equals: 0.4"},
	}
	for _, c := range bounds {
		t.Run(c.name, func(t *testing.T) {
			rule := fmt.Sprintf(
				"dataset_rules: [{code: BOUND, level: error, metric: conversion_error_fraction, column: amount, %s}]", c.bound)
			r := run(t, scenario{input: fixtureB, config: []string{schemaB, rulesB, rule}})
			if r.code != exitValidationFailed {
				t.Fatalf("validate %s exit = %d, want %d; stderr=%s", c.name, r.code, exitValidationFailed, r.stderr)
			}
			if len(r.summary.DatasetIssues) != 0 {
				t.Errorf("dataset issues = %v, want none (inclusive bound)", r.summary.DatasetIssues)
			}
		})
	}
	t.Run("metric without observations fails", func(t *testing.T) {
		r := run(t, scenario{
			input: headerOnly,
			config: []string{
				amountSchema,
				"dataset_rules: [{code: NO_DATA, level: error, metric: avg, column: amount, min: 0}]",
			},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate metric without observations exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if len(r.summary.DatasetResults) != 1 {
			t.Fatalf("dataset results = %+v, want one", r.summary.DatasetResults)
		}
		got := r.summary.DatasetResults[0]
		if got.Status != "fail" {
			t.Errorf("status = %q, want fail", got.Status)
		}
		if got.Value != nil {
			t.Errorf("value = %v, want null", got.Value)
		}
		if got.Reason != "NO_OBSERVATIONS" {
			t.Errorf("reason = %q, want NO_OBSERVATIONS", got.Reason)
		}
	})
}
