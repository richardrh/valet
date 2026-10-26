package checks

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

// rowCase is one single-row input to a rule and the outcome expected for it.
type rowCase struct {
	value string
	state data.State
	want  string
}

// assertRowOutcomes evaluates each case as its own row of string column x and
// compares the outcome of rule code with the expected one.
func assertRowOutcomes(t *testing.T, cfg *config.Config, code string, rows []rowCase) {
	t.Helper()
	values := make([]string, len(rows))
	want := make([]string, len(rows))
	for i, row := range rows {
		values[i], want[i] = row.value, row.want
	}
	if got := rowOutcomes(t, cfg, code, rows); !slices.Equal(got, want) {
		t.Errorf("outcomes for values %q = %v, want %v", values, got, want)
	}
}

func TestRowCheckOutcomes(t *testing.T) {
	tests := []struct {
		name   string
		typ    string
		params map[string]any
		rows   []rowCase
	}{
		{
			name: "not_null",
			typ:  "not_null",
			rows: []rowCase{{"ok", V, pass}, {"", N, fail}, {"", M, fail}, {"", I, pass}},
		},
		{
			name: "is_present",
			typ:  "is_present",
			rows: []rowCase{{"ok", V, pass}, {"", N, pass}, {"", M, fail}, {"", I, pass}},
		},
		{
			name: "not_blank unicode whitespace",
			typ:  "not_blank",
			rows: []rowCase{{"\u2003", V, fail}, {"é", V, pass}, {"", N, fail}, {"", I, unevaluable}},
		},
		{
			name:   "valid_length code points",
			typ:    "valid_length",
			params: map[string]any{"length": 2},
			rows:   []rowCase{{"é界", V, pass}, {"abc", V, fail}, {"", N, fail}},
		},
		{
			name:   "min_length",
			typ:    "min_length",
			params: map[string]any{"min_length": 2},
			rows:   []rowCase{{"界", V, fail}, {"é界", V, pass}, {"", N, fail}},
		},
		{
			name:   "max_length",
			typ:    "max_length",
			params: map[string]any{"max_length": 1},
			rows:   []rowCase{{"界", V, pass}, {"é界", V, fail}, {"", N, fail}},
		},
		{
			name:   "valid_range non-numeric and non-finite violate",
			typ:    "valid_range",
			params: map[string]any{"min": 1, "max": 1},
			rows: []rowCase{
				{"1", V, pass},
				{"2", V, fail},
				{"NaN", V, fail},
				{"Infinity", V, fail},
				{"true", V, fail},
				{"abc", V, fail},
				{"", N, pass},
				{"", I, unevaluable},
			},
		},
		{
			name:   "valid_range exclusive bounds",
			typ:    "valid_range",
			params: map[string]any{"min": 0, "max": 10, "exclusive_min": true, "exclusive_max": true},
			rows: []rowCase{
				{"0", V, fail},
				{"0.1", V, pass},
				{"10", V, fail},
				{"9.9", V, pass},
				{"", N, pass},
			},
		},
		{
			name:   "equals assertion",
			typ:    "equals",
			params: map[string]any{"value": "bad"},
			rows:   []rowCase{{"bad", V, pass}, {"good", V, fail}, {"", N, fail}},
		},
		{
			name:   "not_equals assertion",
			typ:    "not_equals",
			params: map[string]any{"value": "good"},
			rows:   []rowCase{{"good", V, fail}, {"bad", V, pass}, {"", N, pass}},
		},
		{
			name:   "valid_values",
			typ:    "valid_values",
			params: map[string]any{"values": []any{"good"}},
			rows:   []rowCase{{"good", V, pass}, {"bad", V, fail}, {"", N, pass}},
		},
		{
			name:   "matches_pattern searches",
			typ:    "matches_pattern",
			params: map[string]any{"pattern": "ABC"},
			rows:   []rowCase{{"prefix-ABC-suffix", V, pass}, {"bad", V, fail}, {"", N, fail}},
		},
		{
			name:   "type_check int32",
			typ:    "type_check",
			params: map[string]any{"expected_type": "int32"},
			rows:   []rowCase{{"2147483647", V, pass}, {"2147483648", V, fail}, {"1.0", V, fail}, {"", N, pass}},
		},
		{
			name:   "type_check int64 bounds",
			typ:    "type_check",
			params: map[string]any{"expected_type": "int64"},
			rows: []rowCase{
				{"9223372036854775807", V, pass},
				{"9223372036854775808", V, fail},
				{"-9223372036854775808", V, pass},
			},
		},
		{
			name:   "type_check string null fails",
			typ:    "type_check",
			params: map[string]any{"expected_type": "string"},
			rows:   []rowCase{{"x", V, pass}, {"", N, fail}},
		},
		{
			name:   "date_format",
			typ:    "date_format",
			params: map[string]any{"formats": []any{"%Y-%m-%d", "%d/%m/%Y"}},
			rows:   []rowCase{{"29/02/2024", V, pass}, {"29/02/2023", V, fail}, {"", N, pass}},
		},
		{
			name:   "timestamp_window inclusive",
			typ:    "timestamp_window",
			params: map[string]any{"min": "2026-01-01T00:00:00Z", "max": "2026-01-01T00:00:00Z"},
			rows: []rowCase{
				{"2026-01-01T01:00:00+01:00", V, pass},
				{"2025-12-31T23:59:59Z", V, fail},
				{"", N, pass},
				{"not a time", V, fail},
			},
		},
		{
			name:   "timestamp_window zone",
			typ:    "timestamp_window",
			params: map[string]any{"min": "2026-01-01T00:00:00", "timezone": "UTC"},
			rows:   []rowCase{{"2026-01-01T00:00:00", V, pass}, {"2025-12-31T23:59:59", V, fail}},
		},
		{
			name:   "timestamp_window rejects nonexistent local time",
			typ:    "timestamp_window",
			params: map[string]any{"min": "2026-01-01T00:00:00Z", "timezone": "America/New_York"},
			rows: []rowCase{
				{"2026-03-08T02:30:00", V, fail},
				{"2026-11-01T01:30:00", V, fail},
				{"2026-06-01T12:00:00", V, pass},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{Rules: []config.Rule{rule("R", test.typ, test.params)}}
			assertRowOutcomes(t, cfg, "R", test.rows)
		})
	}
}

// Numeric literals against string-typed cells parse the cell as an exact
// decimal; a cell that is not a number is unevaluable (counted), never a
// silent mismatch. String literals always compare as exact text.
func TestStringColumnNumericLiteralPolicy(t *testing.T) {
	ten := json.Number("10")
	allEqual := config.Rule{Code: "R", Level: "error", Checks: []config.Check{{
		Type:   "all_equal",
		Params: map[string]any{"columns": []any{"x"}, "value": ten},
	}}}
	tests := []struct {
		name string
		rule config.Rule
		rows []rowCase
	}{
		{
			name: "equals numeric",
			rule: rule("R", "equals", map[string]any{"value": ten}),
			rows: []rowCase{{"10", V, pass}, {"10.0", V, pass}, {"abc", V, unevaluable}, {"", N, fail}},
		},
		{
			name: "equals integer literal",
			rule: rule("R", "equals", map[string]any{"value": 10}),
			rows: []rowCase{{"10", V, pass}, {"1e1", V, pass}, {"abc", V, unevaluable}},
		},
		{
			name: "equals string literal stays text",
			rule: rule("R", "equals", map[string]any{"value": "10"}),
			rows: []rowCase{{"10", V, pass}, {"10.0", V, fail}, {"abc", V, fail}},
		},
		{
			name: "not_equals numeric",
			rule: rule("R", "not_equals", map[string]any{"value": ten}),
			rows: []rowCase{
				{"10", V, fail},
				{"10.0", V, fail},
				{"11", V, pass},
				{"abc", V, unevaluable},
				{"", N, pass},
			},
		},
		{
			name: "valid_values numeric",
			rule: rule("R", "valid_values", map[string]any{"values": []any{ten}}),
			rows: []rowCase{
				{"10", V, pass},
				{"10.00", V, pass},
				{"11", V, fail},
				{"abc", V, unevaluable},
				{"", N, pass},
			},
		},
		{
			name: "valid_values text match wins",
			rule: rule("R", "valid_values", map[string]any{"values": []any{"abc", ten}}),
			rows: []rowCase{{"abc", V, pass}, {"10.0", V, pass}, {"xyz", V, unevaluable}},
		},
		{
			name: "all_equal numeric",
			rule: allEqual,
			rows: []rowCase{{"10.0", V, pass}, {"11", V, fail}, {"abc", V, unevaluable}},
		},
		{
			name: "when comparison",
			rule: whenRule(config.Condition{Column: "x", Operator: "ge", Value: ten}),
			rows: []rowCase{
				{"10", V, pass},
				{"10.0", V, fail},
				{"5", V, notApplicable},
				{"abc", V, unevaluable},
				{"", N, notApplicable},
			},
		},
		{
			name: "when in",
			rule: whenRule(config.Condition{Column: "x", Operator: "in", Value: []any{ten}}),
			rows: []rowCase{{"10", V, pass}, {"10.0", V, fail}, {"11", V, notApplicable}, {"abc", V, unevaluable}},
		},
		{
			name: "when string literal stays text",
			rule: whenRule(config.Condition{Column: "x", Operator: "eq", Value: "10"}),
			rows: []rowCase{{"10", V, pass}, {"10.0", V, notApplicable}, {"abc", V, notApplicable}},
		},
		{
			name: "when not of unparsable stays unknown",
			rule: whenRule(config.Condition{Not: &config.Condition{Column: "x", Operator: "lt", Value: ten}}),
			rows: []rowCase{{"10", V, pass}, {"5", V, notApplicable}, {"abc", V, unevaluable}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertRowOutcomes(t, &config.Config{Rules: []config.Rule{test.rule}}, "R", test.rows)
		})
	}
}

// whenRule fails values longer than three runes on applicable rows.
func whenRule(when config.Condition) config.Rule {
	r := rule("R", "max_length", map[string]any{"max_length": 3})
	r.When = &when
	return r
}

func TestRulesCombineChecksAndCountWarnings(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := stringBatch(t, budget,
		[]string{"x", "y"},
		[][]string{{"bad", "bad", ""}, {"bad", "good", "bad"}},
		map[string][]data.State{"x": {V, V, N}})
	defer batch.Release()
	notGood := config.Check{Type: "not_equals", Params: map[string]any{"value": "good"}}
	cfg := &config.Config{Rules: []config.Rule{
		{Code: "ALL", Level: "error", Checks: []config.Check{{
			Type:   "all_equal",
			Params: map[string]any{"columns": []any{"x", "y"}, "value": "bad"},
		}}},
		{Code: "NULL", Level: "error", Checks: []config.Check{{
			Type:   "has_null_keys",
			Params: map[string]any{"columns": []any{"x", "y"}},
		}}},
		{Code: "WARN", Level: "warn", Column: "y", Checks: []config.Check{notGood, notGood}},
	}}
	p := bindProgram(t, cfg, batch.Record.Schema(), budget)
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if got, want := bucketNames(result.Buckets), []string{"pass", "error", "error"}; !slices.Equal(got, want) {
		t.Fatalf("buckets = %v, want %v", got, want)
	}
	if got := p.Summary().RuleCounts["WARN"]; got != 1 {
		t.Errorf("warning failures on error rows = %d, want 1 (one per row, not per check)", got)
	}
	if got := len(result.Issues); got != 4 {
		t.Errorf("issues = %d, want 4", got)
	}
	s := p.Summary()
	got := bucketCounts{errored: s.Errored, warned: s.Warned, passed: s.Passed}
	if want := (bucketCounts{errored: 2, warned: 0, passed: 1}); got != want {
		t.Errorf("bucket counts = %+v, want %+v", got, want)
	}
}

func TestIssueColumnUsesResolvedFailingOperand(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := stringBatch(t, budget, []string{"x", "y", "z"}, [][]string{{"pass"}, {"fail"}, {"fail"}}, nil)
	defer batch.Release()
	equalsPass := func(column string) config.Check {
		return config.Check{Type: "equals", Params: map[string]any{"column": column, "value": "pass"}}
	}
	tests := []struct {
		name       string
		checks     []config.Check
		wantColumn string
	}{
		{name: "single operand names the overriding column", checks: []config.Check{equalsPass("y")}, wantColumn: "y"},
		{
			name:       "multiple failing operands name no column",
			checks:     []config.Check{equalsPass("y"), equalsPass("z")},
			wantColumn: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{Rules: []config.Rule{{Code: "R", Level: "error", Column: "x", Checks: test.checks}}}
			result, err := bindProgram(t, cfg, batch.Record.Schema(), budget).Evaluate(batch)
			if err != nil {
				t.Fatal(err)
			}
			defer result.Release()
			if len(result.Issues) != 1 {
				t.Fatalf("issues = %+v, want exactly one", result.Issues)
			}
			if got := result.Issues[0].Column; got != test.wantColumn {
				t.Errorf("issue column = %q, want %q", got, test.wantColumn)
			}
		})
	}
}

func TestConditionalThreeValuedLogicAndDenominators(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := stringBatch(t, budget,
		[]string{"x", "active"},
		[][]string{{"", "", "", ""}, {"yes", "no", "", ""}},
		map[string][]data.State{"active": {V, V, N, I}})
	defer batch.Release()
	batch.Ingest = []data.Issue{{
		Location: batch.Locations[3],
		Code:     "INGEST_TYPE",
		Severity: "error",
		Origin:   data.OriginIngest,
		Scope:    data.ScopeRow,
		Column:   "active",
	}}
	activeYes := config.Condition{Column: "active", Operator: "eq", Value: "yes"}
	required := rule("REQUIRED", "not_blank", nil)
	required.When = &activeYes
	anyRule := rule("ANY", "not_blank", nil)
	anyRule.When = &config.Condition{Any: []config.Condition{activeYes, {Column: "x", Operator: "eq", Value: ""}}}
	allRule := rule("ALL", "not_blank", nil)
	allRule.When = &config.Condition{All: []config.Condition{activeYes, {Column: "x", Operator: "eq", Value: "nonempty"}}}
	cfg := &config.Config{
		Rules: []config.Rule{required, anyRule, allRule},
		DatasetRules: []config.DatasetRule{
			{Code: "RATE", Level: "error", Metric: "rule_failure_fraction", RuleCode: "REQUIRED", Max: 0.5},
		},
	}
	p := bindProgram(t, cfg, batch.Record.Schema(), budget)
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if got, want := bucketNames(result.Buckets), []string{"error", "error", "error", "error"}; !slices.Equal(got, want) {
		t.Fatalf("buckets = %v, want %v", got, want)
	}
	s := p.Summary()
	// true OR unknown is true: ANY applies to every row.
	if got := s.RuleCounts["ANY"]; got != 4 {
		t.Errorf("ANY failures = %d, want 4", got)
	}
	if got := s.Unevaluable["ANY"]; got != 0 {
		t.Errorf("ANY unevaluable = %d, want 0", got)
	}
	// false AND unknown is false: ALL never applies.
	if got := s.NotApplicable["ALL"]; got != 4 {
		t.Errorf("ALL not applicable = %d, want 4", got)
	}
	if got := s.Unevaluable["ALL"]; got != 0 {
		t.Errorf("ALL unevaluable = %d, want 0", got)
	}
	if err = p.Finalize(); err != nil {
		t.Fatal(err)
	}
	type denominator struct{ numerator, denominator, notApplicable, unevaluable uint64 }
	d := s.DatasetResults[0]
	got := denominator{
		numerator:     d.Numerator,
		denominator:   d.Denominator,
		notApplicable: d.NotApplicable,
		unevaluable:   d.Unevaluable,
	}
	if want := (denominator{numerator: 1, denominator: 1, notApplicable: 2, unevaluable: 1}); got != want {
		t.Errorf("conditional denominator = %+v, want %+v", got, want)
	}
}

func TestInvalidOperandDoesNotCascade(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := stringBatch(t, budget, []string{"x"}, [][]string{{""}}, map[string][]data.State{"x": {I}})
	defer batch.Release()
	batch.Ingest = []data.Issue{{
		Location: batch.Locations[0],
		Code:     "INGEST_TYPE",
		Severity: "error",
		Origin:   data.OriginIngest,
		Scope:    data.ScopeRow,
		Column:   "x",
	}}
	cfg := &config.Config{Rules: []config.Rule{
		rule("RANGE", "valid_range", map[string]any{"min": 1}),
		rule("PRESENT", "is_present", nil),
	}}
	p := bindProgram(t, cfg, batch.Record.Schema(), budget)
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	s := p.Summary()
	if got := len(result.Issues); got != 1 {
		t.Errorf("issues = %+v, want only the ingest issue", result.Issues)
	}
	if got := s.IngestCounts["INGEST_TYPE"]; got != 1 {
		t.Errorf("INGEST_TYPE count = %d, want 1", got)
	}
	if got := s.Unevaluable["RANGE"]; got != 1 {
		t.Errorf("RANGE unevaluable = %d, want 1", got)
	}
	got, want := countsFor(s.RuleCounts, "RANGE", "PRESENT"), map[string]uint64{"RANGE": 0, "PRESENT": 0}
	if !maps.Equal(got, want) {
		t.Errorf("rule failures = %v, want %v (an invalid operand must not cascade)", got, want)
	}
}

func TestCompareColumnsUsesDeclaredDateContext(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := stringBatch(t, budget,
		[]string{"x", "y"},
		[][]string{{"31/01/2026", "01/02/2026"}, {"01/02/2026", "31/01/2026"}},
		nil)
	defer batch.Release()
	cfg := &config.Config{Rules: []config.Rule{
		{Code: "DATES", Level: "error", Checks: []config.Check{{
			Type:   "date_format",
			Params: map[string]any{"columns": []any{"x", "y"}, "format": "%d/%m/%Y"},
		}}},
		rule("ORDER", "compare_columns", map[string]any{"other_column": "y", "operator": "<"}),
	}}
	p := bindProgram(t, cfg, batch.Record.Schema(), budget)
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if got, want := bucketNames(result.Buckets), []string{"pass", "error"}; !slices.Equal(got, want) {
		t.Errorf("buckets = %v, want %v (dates compared as dates, not text)", got, want)
	}
}

func TestCompileRejectsInvalidRules(t *testing.T) {
	stringSchema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.BinaryTypes.String},
		{Name: "y", Type: arrow.BinaryTypes.String},
	}, nil)
	typedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64},
		{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	conditionalUnique := rule("UNIQUE", "is_unique", nil)
	conditionalUnique.When = &config.Condition{Column: "x", Operator: "eq", Value: 1}
	declaredSchema := config.Input{Schema: &config.Schema{}}
	tests := []struct {
		name   string
		cfg    *config.Config
		schema *arrow.Schema
	}{
		{
			name: "unsupported regexp syntax",
			cfg: &config.Config{Rules: []config.Rule{
				rule("RX", "matches_pattern", map[string]any{"pattern": "(?<=a)b"}),
			}},
			schema: stringSchema,
		},
		{
			name:   "unsupported date directive",
			cfg:    &config.Config{Rules: []config.Rule{rule("DATE", "date_format", map[string]any{"format": "%Q"})}},
			schema: stringSchema,
		},
		{
			name: "missing operand",
			cfg: &config.Config{Rules: []config.Rule{{Code: "MISSING", Level: "error", Checks: []config.Check{{
				Type:   "all_equal",
				Params: map[string]any{"columns": []any{"absent"}, "value": "x"},
			}}}}},
			schema: stringSchema,
		},
		{
			name:   "conditional uniqueness",
			cfg:    &config.Config{Rules: []config.Rule{conditionalUnique}},
			schema: typedSchema,
		},
		{
			name: "lossy mixed comparison",
			cfg: &config.Config{
				Input: declaredSchema,
				Rules: []config.Rule{
					rule("LOSSY", "compare_columns", map[string]any{"other_column": "y", "operator": "=="}),
				},
			},
			schema: typedSchema,
		},
		{
			name: "dataset bound beyond precision",
			cfg: &config.Config{DatasetRules: []config.DatasetRule{
				{Code: "ROWS", Metric: "row_count", Max: json.Number("1e100")},
			}},
			schema: stringSchema,
		},
		{
			name: "floating equals without tolerance",
			cfg: &config.Config{
				Input:        declaredSchema,
				DatasetRules: []config.DatasetRule{{Code: "SUM", Metric: "sum", Column: "y", Equals: 1}},
			},
			schema: typedSchema,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			budget := resource.New(16 << 20)
			p, err := bindProgramErr(test.cfg, test.schema, budget)
			if err == nil {
				p.Close()
				t.Fatal("compile error = nil, want an invalid-rule error")
			}
			if got := budget.Used(); got != 0 {
				t.Errorf("budget used after failed compile = %d bytes, want 0", got)
			}
		})
	}
}

func TestMetadataGatesPrecedeMissingRowOperands(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.BinaryTypes.String}}, nil)
	gate := config.Rule{Code: "GATE", Level: "error", Checks: []config.Check{{
		Type:   "required_columns",
		Params: map[string]any{"columns": []any{"absent"}},
	}}}
	strict := config.Rule{Code: "STRICT", Level: "error", Checks: []config.Check{{
		Type:   "schema_matches",
		Params: map[string]any{"columns": map[string]any{"x": "int64"}, "mode": "strict"},
	}}}
	tests := []struct {
		name   string
		rules  []config.Rule
		column string
		reason string
	}{
		{
			name:   "required column",
			rules:  []config.Rule{rule("ROW", "not_null", map[string]any{"column": "absent"}), gate},
			column: "absent",
			reason: reasonMissingColumn,
		},
		{name: "strict type", rules: []config.Rule{strict}, column: "x", reason: reasonTypeMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{
				Rules:        test.rules,
				DatasetRules: []config.DatasetRule{{Code: "COUNT", Metric: "row_count", Min: 1}},
			}
			p := bindProgram(t, cfg, schema, resource.New(16<<20))
			s := p.Summary()
			if s.Status != data.StatusSchemaRejected {
				t.Errorf("status = %s, want %s", s.Status, data.StatusSchemaRejected)
			}
			if len(s.SchemaIssues) != 1 {
				t.Fatalf("schema issues = %+v, want one schema rejection", s.SchemaIssues)
			}
			issue := s.SchemaIssues[0]
			got := data.Issue{Column: issue.Column, Reason: issue.Reason, Scope: issue.Scope}
			want := data.Issue{Column: test.column, Reason: test.reason, Scope: data.ScopeSchema}
			if got != want {
				t.Errorf("schema issue column/reason/scope = %q/%q/%q, want %q/%q/%q",
					got.Column, got.Reason, got.Scope, want.Column, want.Reason, want.Scope)
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
		})
	}
}

// Floating operands compare against literals rounded once to their width.
func TestFloatingLiteralsUseOperandPrecision(t *testing.T) {
	tenth := json.Number("0.1")
	conditional := func(op string, value any) config.Rule {
		r := rule("R", "equals", map[string]any{"value": 0})
		r.When = &config.Condition{Column: "x", Operator: op, Value: value}
		return r
	}
	tests := []struct {
		name         string
		rule         config.Rule
		wantFailures uint64
	}{
		{name: "equals", rule: rule("R", "equals", map[string]any{"value": tenth}), wantFailures: 0},
		{name: "not_equals", rule: rule("R", "not_equals", map[string]any{"value": tenth}), wantFailures: 1},
		{name: "valid_values", rule: rule("R", "valid_values", map[string]any{"values": []any{tenth}}), wantFailures: 0},
		{
			name:         "all_equal",
			rule:         rule("R", "all_equal", map[string]any{"columns": []any{"x"}, "value": tenth}),
			wantFailures: 0,
		},
		{name: "inclusive_max", rule: rule("R", "valid_range", map[string]any{"max": tenth}), wantFailures: 0},
		{name: "inclusive_min", rule: rule("R", "valid_range", map[string]any{"min": tenth}), wantFailures: 0},
		{
			name:         "string literal is not coerced",
			rule:         rule("R", "equals", map[string]any{"value": "0.1"}),
			wantFailures: 1,
		},
		{name: "when_eq", rule: conditional("eq", tenth), wantFailures: 1},
		{name: "when_in", rule: conditional("in", []any{tenth}), wantFailures: 1},
	}
	for _, typ := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
		for _, test := range tests {
			t.Run(typ.String()+"/"+test.name, func(t *testing.T) {
				budget := resource.New(16 << 20)
				batch := floatBatch(budget, typ, []float64{0.1})
				defer batch.Release()
				cfg := &config.Config{
					Input: config.Input{Schema: &config.Schema{
						Columns: map[string]config.Column{"x": {Type: typ.String()}},
					}},
					Rules: []config.Rule{test.rule},
				}
				p := bindProgram(t, cfg, batch.Record.Schema(), budget)
				result, err := p.Evaluate(batch)
				defer result.Release()
				if err != nil {
					t.Fatal(err)
				}
				if got := p.Summary().RuleCounts["R"]; got != test.wantFailures {
					t.Errorf("failures = %d, want %d", got, test.wantFailures)
				}
				if got := p.Summary().NotApplicable["R"]; got != 0 {
					t.Errorf("not applicable rows = %d, want 0 (the floating condition matches)", got)
				}
			})
		}
	}
}

func TestExactIntegerLiteralsKeepPrecision(t *testing.T) {
	budget := resource.New(16 << 20)
	builder := array.NewInt64Builder(budget.Allocator())
	builder.AppendValues([]int64{9007199254740992, 9007199254740993}, nil)
	column := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	batch := &data.Batch{Record: array.NewRecordBatch(schema, []arrow.Array{column}, 2)}
	column.Release()
	defer batch.Release()
	cfg := &config.Config{Input: config.Input{Schema: &config.Schema{}}, Rules: []config.Rule{
		rule("EQ", "equals", map[string]any{"value": json.Number("9007199254740993")}),
		rule("RANGE", "valid_range", map[string]any{"max": json.Number("9007199254740992")}),
	}}
	p := bindProgram(t, cfg, schema, budget)
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if got, want := bucketNames(result.Buckets), []string{"error", "error"}; !slices.Equal(got, want) {
		t.Fatalf("buckets = %v, want %v", got, want)
	}
	// EQ fails the first row and RANGE the second.
	got, want := countsFor(p.Summary().RuleCounts, "EQ", "RANGE"), map[string]uint64{"EQ": 1, "RANGE": 1}
	if !maps.Equal(got, want) {
		t.Errorf("rule failures = %v, want %v", got, want)
	}
}

func floatBatch(budget *resource.Budget, typ arrow.DataType, values []float64) *data.Batch {
	var column arrow.Array
	if typ.ID() == arrow.FLOAT32 {
		builder := array.NewFloat32Builder(budget.Allocator())
		for _, value := range values {
			builder.Append(float32(value))
		}
		column = builder.NewArray()
		builder.Release()
	} else {
		builder := array.NewFloat64Builder(budget.Allocator())
		builder.AppendValues(values, nil)
		column = builder.NewArray()
		builder.Release()
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: typ}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(values)))
	column.Release()
	return &data.Batch{Record: record}
}
