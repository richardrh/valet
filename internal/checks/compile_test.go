package checks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/source"
)

// compile opens the fixture dataset and compiles cfg against it; the program
// is closed before the fixture's leak check runs.
func compile(t *testing.T, files map[string]string, cfg *config.Config, logger *slog.Logger, schemaIssueSink func(data.Issue) error) (*Program, *source.Dataset) {
	t.Helper()
	ds, store, budget := datasetFixture(t, files, cfg)
	p, err := Compile(context.Background(), cfg, ds, store, budget, logger, schemaIssueSink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p, ds
}

// referenceParams points a referential check at column key of file.
func referenceParams(file string) map[string]any {
	return map[string]any{"reference_file": file, "reference_column": "key"}
}

// schemaMatchesInt64 is the check that column x casts to int64.
func schemaMatchesInt64() config.Check {
	return config.Check{Type: "schema_matches", Params: map[string]any{"columns": map[string]any{"x": "int64"}}}
}

func TestUniquenessAndReferenceMembership(t *testing.T) {
	nullInput := config.Input{NullValues: []string{"NULL"}}
	tests := []struct {
		name  string
		files map[string]string
		cfg   *config.Config
		want  []string
	}{
		{
			name: "cross-file uniqueness fails every occurrence; composite reference is ordered with distinct null",
			files: map[string]string{
				"a.csv": `x,y
a,b
x,y
`,
				"b.csv": `x,y
a,b
b,a
NULL,
`,
				"ref.csv": `a,b
a,b
x,y
NULL,
`,
			},
			cfg: &config.Config{
				Input:  nullInput,
				Source: config.Source{URIs: []string{"a.csv", "b.csv"}},
				Rules: []config.Rule{
					rule("UNIQUE", "is_unique", nil),
					rule("UNIQUE_AGAIN", "is_unique", nil),
					{Code: "REF", Level: "error", Checks: []config.Check{{
						Type: "referential_integrity_composite",
						Params: map[string]any{
							"columns":           []any{"x", "y"},
							"reference_columns": []any{"a", "b"},
							"reference_file":    "ref.csv",
						},
					}}},
				},
			},
			want: []string{"error", "pass", "error", "error", "error"},
		},
		{
			name: "composite uniqueness is ordered",
			files: map[string]string{
				"a.csv": `x,y
a,b
b,a
`,
				"b.csv": `x,y
a,b
`,
			},
			cfg: &config.Config{
				Source: config.Source{URIs: []string{"a.csv", "b.csv"}},
				Rules: []config.Rule{{Code: "TUPLE", Level: "error", Checks: []config.Check{{
					Type:   "is_unique_composite",
					Params: map[string]any{"columns": []any{"x", "y"}},
				}}}},
			},
			want: []string{"error", "pass", "error"},
		},
		{
			name: "reference null and empty stay distinct",
			files: map[string]string{
				"input.csv": `x
NULL
""
key
`,
				"ref.csv": `key
NULL
key
`,
			},
			cfg: &config.Config{
				Input:  nullInput,
				Source: config.Source{URIs: []string{"input.csv"}},
				Rules: []config.Rule{
					rule("FIRST", "referential_integrity", referenceParams("ref.csv")),
					rule("SECOND", "referential_integrity", referenceParams("ref.csv")),
				},
			},
			want: []string{"pass", "error", "pass"},
		},
		{
			// " AbC" keeps its leading space: identifiers are not trimmed.
			name: "reference identifiers keep lexical content",
			files: map[string]string{
				"input.csv": `x
001
1
 AbC
abc
AbC
`,
				"ref.csv": `key
001
AbC
`,
			},
			cfg: &config.Config{
				Source: config.Source{URIs: []string{"input.csv"}},
				Rules:  []config.Rule{rule("IDENTIFIER", "referential_integrity", referenceParams("ref.csv"))},
			},
			want: []string{"pass", "error", "error", "error", "pass"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, ds := compile(t, test.files, test.cfg, nil, nil)
			if got := scan(t, p, ds); !slices.Equal(got, test.want) {
				t.Errorf("buckets = %v, want %v", got, test.want)
			}
		})
	}
}

func TestUniquenessCountsAndPasses(t *testing.T) {
	cfg := &config.Config{
		Source: config.Source{URIs: []string{"a.csv", "b.csv"}},
		Rules:  []config.Rule{rule("UNIQUE", "is_unique", nil), rule("UNIQUE_AGAIN", "is_unique", nil)},
	}
	files := map[string]string{
		"a.csv": `x
a
b
`,
		"b.csv": `x
a
`,
	}
	p, ds := compile(t, files, cfg, nil, nil)
	if got := p.Summary().Passes; got != 1 {
		t.Errorf("prepass passes = %d, want 1 (compatible rules share one index pass)", got)
	}
	scan(t, p, ds)
	got := countsFor(p.Summary().RuleCounts, "UNIQUE", "UNIQUE_AGAIN")
	if want := map[string]uint64{"UNIQUE": 2, "UNIQUE_AGAIN": 2}; !maps.Equal(got, want) {
		t.Errorf("rule failures = %v, want %v", got, want)
	}
}

// Float -0 and +0 are one key. Sources reject NaN at ingestion (a row_error
// conversion failure), so NaN rows are unevaluable for uniqueness and neither
// enter the index nor duplicate each other or other values.
func TestUniquenessFloatKeyPolicy(t *testing.T) {
	budget := resource.New(1 << 20)
	builder := array.NewFloat64Builder(budget.Allocator())
	builder.AppendValues([]float64{0, math.Copysign(0, -1), math.NaN(), math.NaN(), 1}, nil)
	column := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Float64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 5)
	column.Release()
	var encoded bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	w, err := pqarrow.NewFileWriter(schema, &encoded, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	err = w.Write(record)
	record.Release()
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Input: config.Input{
			Type:        "parquet",
			OnTypeError: "row_error",
			Schema:      &config.Schema{Order: []string{"x"}, Columns: map[string]config.Column{"x": {Type: "float64"}}},
		},
		Source: config.Source{URIs: []string{"x.parquet"}},
		Rules:  []config.Rule{rule("UNIQUE", "is_unique", nil)},
	}
	p, ds := compile(t, map[string]string{"x.parquet": encoded.String()}, cfg, nil, nil)
	if got, want := scan(t, p, ds), []string{"error", "error", "error", "error", "pass"}; !slices.Equal(got, want) {
		t.Errorf("buckets = %v, want %v", got, want)
	}
	s := p.Summary()
	if got := s.RuleCounts["UNIQUE"]; got != 2 {
		t.Errorf("uniqueness failures = %d, want 2 (-0 duplicates +0; NaN rows never duplicate)", got)
	}
	if got := s.Unevaluable["UNIQUE"]; got != 2 {
		t.Errorf("NaN rows unevaluable = %d, want 2", got)
	}
	ingest := uint64(0)
	for _, n := range s.IngestCounts {
		ingest += n
	}
	if ingest != 2 {
		t.Errorf("ingest issues = %d (%v), want two NaN conversion failures", ingest, s.IngestCounts)
	}
}

func TestLateCastabilityGateStreamsAndRejectsBeforeRows(t *testing.T) {
	cfg := &config.Config{
		Source:       config.Source{URIs: []string{"a.csv", "b.csv"}},
		Rules:        []config.Rule{{Code: "SCHEMA", Level: "error", Checks: []config.Check{schemaMatchesInt64()}}},
		DatasetRules: []config.DatasetRule{{Code: "ROWS", Metric: "row_count", Min: 1}},
	}
	files := map[string]string{
		"a.csv": `x
1
2
`,
		"b.csv": `x
invalid
invalid
`,
	}
	var issues []data.Issue
	p, _ := compile(t, files, cfg, nil, func(i data.Issue) error { issues = append(issues, i); return nil })
	s := p.Summary()
	type progress struct {
		status             string
		total, rowsScanned uint64
	}
	gotProgress := progress{status: s.Status, total: s.Total, rowsScanned: s.RowsScanned}
	if want := (progress{status: data.StatusSchemaRejected, total: 0, rowsScanned: 4}); gotProgress != want {
		t.Fatalf("progress = %+v, want %+v (rejected after scanning 4 rows)", gotProgress, want)
	}
	if len(s.SchemaIssues) != 1 {
		t.Errorf("summary schema issues = %+v, want one with count 2", s.SchemaIssues)
	} else if got := s.SchemaIssues[0].Count; got != 2 {
		t.Errorf("summary schema issue count = %d, want 2", got)
	}
	if err := p.Finalize(); err != nil {
		t.Fatal(err)
	}
	if s.Validation != data.ValidationFail {
		t.Errorf("validation = %s, want %s", s.Validation, data.ValidationFail)
	}
	if got := s.DatasetResults[0].Status; got != datasetNotEvaluated {
		t.Errorf("dataset rule status = %s, want %s", got, datasetNotEvaluated)
	}

	if len(issues) != 2 {
		t.Fatalf("streamed issues = %+v, want two located UNCASTABLE_VALUE issues", issues)
	}
	if got, want := []uint64{issues[0].Row, issues[1].Row}, []uint64{1, 2}; !slices.Equal(got, want) {
		t.Errorf("streamed issue rows = %v, want %v", got, want)
	}
	if got := issues[0].Column; got != "x" {
		t.Errorf("streamed issue column = %q, want x", got)
	}
	if got := issues[0].Reason; got != reasonUncastableValue {
		t.Errorf("streamed issue reason = %s, want %s", got, reasonUncastableValue)
	}
}

func TestDatasetWideOutcomes(t *testing.T) {
	schemaColumns := func(level string) config.Rule {
		return config.Rule{Code: "NAMES", Level: level, Checks: []config.Check{{
			Type:   "valid_schema_columns",
			Params: map[string]any{"reference_file": "names.csv", "reference_column": "name"},
		}}}
	}
	unexpectedColumn := map[string]string{
		"input.csv": `x,unexpected
one,two
three,four
`,
		"names.csv": `name
x
`,
	}
	tests := []struct {
		name           string
		files          map[string]string
		rule           config.Rule
		want           []string
		wantFailures   uint64
		wantValidation string
		// rowScoped outcomes must not also surface as schema issues.
		rowScoped bool
	}{
		{
			name: "warning castability gate",
			files: map[string]string{"input.csv": `x
1
bad
`},
			rule:           config.Rule{Code: "SCHEMA", Level: "warn", Checks: []config.Check{schemaMatchesInt64()}},
			want:           []string{"warn", "warn"},
			wantFailures:   2,
			wantValidation: data.ValidationPass,
		},
		{
			name:           "warning schema column reference",
			files:          unexpectedColumn,
			rule:           schemaColumns("warn"),
			want:           []string{"warn", "warn"},
			wantFailures:   2,
			wantValidation: data.ValidationPass,
			rowScoped:      true,
		},
		{
			name:           "error schema column reference is not a gate",
			files:          unexpectedColumn,
			rule:           schemaColumns("error"),
			want:           []string{"error", "error"},
			wantFailures:   2,
			wantValidation: data.ValidationFail,
			rowScoped:      true,
		},
		{
			name: "schema column reference satisfied",
			files: map[string]string{
				"input.csv": `x
one
`,
				"names.csv": `name
x
y
`,
			},
			rule:           schemaColumns("error"),
			want:           []string{"pass"},
			wantFailures:   0,
			wantValidation: data.ValidationPass,
			rowScoped:      true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{Source: config.Source{URIs: []string{"input.csv"}}, Rules: []config.Rule{test.rule}}
			p, ds := compile(t, test.files, cfg, nil, nil)
			if got := p.Summary().Status; got != data.StatusCompleted {
				t.Fatalf("status = %s, want %s", got, data.StatusCompleted)
			}
			if got := scan(t, p, ds); !slices.Equal(got, test.want) {
				t.Errorf("buckets = %v, want %v", got, test.want)
			}
			if err := p.Finalize(); err != nil {
				t.Fatal(err)
			}
			s := p.Summary()
			if got := s.RuleCounts[test.rule.Code]; got != test.wantFailures {
				t.Errorf("rule failures = %d, want %d", got, test.wantFailures)
			}
			if s.Validation != test.wantValidation {
				t.Errorf("validation = %s, want %s", s.Validation, test.wantValidation)
			}
			if test.rowScoped && len(s.SchemaIssues) != 0 {
				t.Errorf("schema issues = %+v, want none for a row-scoped outcome", s.SchemaIssues)
			}
		})
	}
}

func TestTypedReferenceUsesCanonicalText(t *testing.T) {
	reference := func(code, column, file string) config.Rule {
		return config.Rule{Code: code, Level: "error", Column: column, Checks: []config.Check{{
			Type:   "referential_integrity",
			Params: referenceParams(file),
		}}}
	}
	cfg := &config.Config{
		Source: config.Source{URIs: []string{"input.csv"}},
		Input: config.Input{Schema: &config.Schema{
			ExtraFields: "error",
			Order:       []string{"amount", "date", "id", "blob"},
			Columns: map[string]config.Column{
				"amount": {Type: "decimal", Precision: 18, Scale: 2},
				"date":   {Type: "date", Formats: []string{"%d/%m/%Y"}},
				"id":     {Type: "int64"},
				"blob":   {Type: "binary"},
			},
		}},
		Rules: []config.Rule{
			reference("AMOUNT", "amount", "amounts.csv"),
			reference("DATE", "date", "dates.csv"),
			reference("ID", "id", "ids.csv"),
			reference("BLOB", "blob", "blobs.csv"),
		},
	}
	p, ds := compile(t, map[string]string{
		"input.csv": `amount,date,id,blob
1.2,31/01/2026,001,QQ==
1.21,01/02/2026,002,Qg==
`,
		"amounts.csv": "key\n1.20\n",
		"dates.csv":   "key\n2026-01-31\n",
		"ids.csv":     "key\n1\n",
		"blobs.csv":   "key\nQQ==\n",
	}, cfg, nil, nil)
	if got, want := scan(t, p, ds), []string{"pass", "error"}; !slices.Equal(got, want) {
		t.Fatalf("buckets = %v, want %v", got, want)
	}
	for _, code := range []string{"AMOUNT", "DATE", "ID", "BLOB"} {
		if got := p.Summary().RuleCounts[code]; got != 1 {
			t.Errorf("%s failures = %d, want 1", code, got)
		}
	}
}

func TestDescribeReportsScopesAndPasses(t *testing.T) {
	cfg := &config.Config{Source: config.Source{URIs: []string{"input.csv"}}, Rules: []config.Rule{
		rule("ROW", "not_null", nil),
		rule("UNIQUE", "is_unique", nil),
		rule("REF", "referential_integrity", referenceParams("ref.csv")),
		{Code: "GATE", Level: "warn", Checks: []config.Check{{
			Type:   "required_columns",
			Params: map[string]any{"columns": []any{"x"}},
		}}},
		{Code: "NOOP", Level: "error"},
	}}
	ds, store, budget := datasetFixture(t, map[string]string{"input.csv": "x\na\n", "ref.csv": "key\na\n"}, cfg)
	d, err := Describe(context.Background(), cfg, ds, store, budget)
	if err != nil {
		t.Fatal(err)
	}
	if d.RequiredPasses != 2 {
		t.Errorf("required passes = %d, want 2 (prepass + validation)", d.RequiredPasses)
	}
	want := []RuleDescription{
		{Code: "ROW", Scopes: []string{"row"}},
		{Code: "UNIQUE", Scopes: []string{"global"}},
		{Code: "REF", Scopes: []string{"reference"}},
		{Code: "GATE", Scopes: []string{"schema"}},
		{Code: "NOOP", Scopes: []string{}, NoOp: true},
	}
	if !reflect.DeepEqual(d.Rules, want) {
		t.Errorf("rules = %+v, want %+v", d.Rules, want)
	}
	wantState := []string{"UNIQUE: exact duplicate keys", "REF: projected reference keys"}
	if !slices.Equal(d.GlobalState, wantState) {
		t.Errorf("global state = %q, want %q", d.GlobalState, wantState)
	}
}

func TestCompileLogsPhases(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg := &config.Config{Source: config.Source{URIs: []string{"input.csv"}}, Rules: []config.Rule{
		rule("UNIQUE", "is_unique", nil),
		rule("FIRST", "referential_integrity", referenceParams("ref.csv")),
		rule("SECOND", "referential_integrity", referenceParams("ref.csv")),
	}}
	files := map[string]string{
		"input.csv": `x
a
b
`,
		"ref.csv": `key
a
b
c
`,
	}
	compile(t, files, cfg, logger, nil)
	counts := map[string]int{}
	var reference map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		phase := fmt.Sprint(record["phase"])
		counts[phase]++
		if phase == "reference" {
			reference = record
		}
	}
	if got := counts["compile"]; got != 1 {
		t.Errorf("compile events = %d, want 1 (all events %v)", got, counts)
	}
	if got := counts["prepass"]; got != 1 {
		t.Errorf("prepass events = %d, want 1 (all events %v)", got, counts)
	}
	if got := counts["reference"]; got != 1 {
		t.Errorf("reference loads = %d, want 1 (compatible references share an index)", got)
	}
	if got := reference["rows"]; got != float64(3) {
		t.Errorf("reference log rows = %v, want 3", got)
	}
	if got := reference["phase"]; got != "reference" {
		t.Errorf("reference log phase = %v, want reference", got)
	}
}

func TestBudgetExhaustionIsTypedAndReleases(t *testing.T) {
	var rows strings.Builder
	rows.WriteString("x\n")
	for i := range 2000 {
		fmt.Fprintf(&rows, "value-%d\n", i)
	}
	cfg := &config.Config{
		Source: config.Source{URIs: []string{"input.csv"}},
		Rules:  []config.Rule{rule("UNIQUE", "is_unique", nil)},
	}
	ds, store, _ := datasetFixture(t, map[string]string{"input.csv": rows.String()}, cfg)
	small := resource.New(64 << 10)
	_, err := Compile(context.Background(), cfg, ds, store, small, nil, nil)
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("compile error = %v, want a *resource.ResourceLimitError", err)
	}
	if got := small.Used(); got != 0 {
		t.Errorf("budget used after failed compile = %d bytes, want 0", got)
	}
}

func TestSchemaIssueSinkFailureReleasesCompilerState(t *testing.T) {
	cfg := &config.Config{
		Source: config.Source{URIs: []string{"a.csv"}},
		Rules:  []config.Rule{{Code: "S", Level: "error", Checks: []config.Check{schemaMatchesInt64()}}},
	}
	ds, store, budget := datasetFixture(t, map[string]string{"a.csv": "x\nbad\n"}, cfg)
	before := budget.Used()
	sentinel := errors.New("sink failure")
	_, err := Compile(context.Background(), cfg, ds, store, budget, nil, func(data.Issue) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("compile error = %v, want %v", err, sentinel)
	}
	if after := budget.Used(); after != before {
		t.Errorf("budget used after failed compile = %d bytes, want %d (as before)", after, before)
	}
}

func TestProgramReleasesToZeroOnClose(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := stringBatch(t, budget, []string{"x"}, [][]string{{"a", "b"}}, nil)
	defer batch.Release()
	used := budget.Used()
	cfg := &config.Config{Rules: []config.Rule{
		rule("PATTERN", "matches_pattern", map[string]any{"pattern": "a+"}),
		rule("VALUES", "valid_values", map[string]any{"values": []any{"a"}}),
	}}
	p, err := bindProgramErr(cfg, batch.Record.Schema(), budget)
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatal(err)
	}
	if got := budget.Used(); got <= used {
		t.Fatalf("budget used with program = %d bytes, want more than the batch's %d", got, used)
	}
	result.Release()
	p.Close()
	if got := budget.Used(); got != used {
		t.Errorf("budget used after Close = %d bytes, want the batch's %d", got, used)
	}
}

// A second Close must be a no-op: releasing the retained index twice would
// underflow the budget and panic.
func TestProgramCloseIsIdempotent(t *testing.T) {
	cfg := &config.Config{
		Source: config.Source{URIs: []string{"input.csv"}},
		Rules:  []config.Rule{rule("UNIQUE", "is_unique", nil)},
	}
	files := map[string]string{"input.csv": `x
a
b
`}
	ds, store, budget := datasetFixture(t, files, cfg)
	before := budget.Used()
	p, err := Compile(context.Background(), cfg, ds, store, budget, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := budget.Used(); got <= before {
		t.Fatalf("budget used with compiled uniqueness index = %d bytes, want more than %d", got, before)
	}
	for _, call := range []string{"first", "second"} {
		if err := p.Close(); err != nil {
			t.Fatalf("%s Close error = %v, want nil", call, err)
		}
		if got := budget.Used(); got != before {
			t.Errorf("budget used after %s Close = %d bytes, want %d (as before Compile)", call, got, before)
		}
	}
}

func TestAcceptedAliasesAndDateDirectivesCompile(t *testing.T) {
	cfg := &config.Config{Rules: []config.Rule{{
		Code: "ALIASES", Level: "error", Column: "x",
		Checks: []config.Check{
			{Type: "type_check", Params: map[string]any{"expected_type": "integer"}},
			{Type: "date_format", Params: map[string]any{
				"formats": []any{"%F", "%D", "%R", "%T", "%h", "%Y-%m-%dT%H:%M:%S%:z"},
			}},
		},
	}}}
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.BinaryTypes.String}}, nil)
	program, err := bindProgramErr(cfg, schema, resource.New(16<<20))
	if err != nil {
		t.Fatal(err)
	}
	program.Close()
}

func TestDescribeResolvesKnownWarningGates(t *testing.T) {
	cfg := &config.Config{Rules: []config.Rule{{
		Code: "REQUIRED", Level: "warn",
		Checks: []config.Check{{Type: "required_columns", Params: map[string]any{"columns": []any{"missing"}}}},
	}}}
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.BinaryTypes.String}}, nil)
	dataset := source.NewGenerated(schema, nil)
	description, err := Describe(context.Background(), cfg, dataset, nil, resource.New(16<<20))
	if err != nil {
		t.Fatal(err)
	}
	if len(description.SchemaIssues) != 1 {
		t.Fatalf("schema issues = %+v, want one", description.SchemaIssues)
	}
	if got := description.SchemaIssues[0].Code; got != "REQUIRED" {
		t.Errorf("schema issue code = %s, want REQUIRED", got)
	}
}

func TestDateFormatCompilationIsBudgeted(t *testing.T) {
	formats := make([]any, 1000)
	for i := range formats {
		formats[i] = "%Y-" + strings.Repeat("x", 100)
	}
	cfg := &config.Config{Rules: []config.Rule{{
		Code: "DATES", Level: "error", Column: "x",
		Checks: []config.Check{{Type: "date_format", Params: map[string]any{"formats": formats}}},
	}}}
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.BinaryTypes.String}}, nil)
	program, err := bindProgramErr(cfg, schema, resource.New(64<<10))
	if program != nil {
		program.Close()
	}
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("compile error = %v, want a *resource.ResourceLimitError", err)
	}
}
