package checks

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

func int64Batch(budget *resource.Budget, values []int64) *data.Batch {
	builder := array.NewInt64Builder(budget.Allocator())
	builder.AppendValues(values, nil)
	column := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(values)))
	column.Release()
	return &data.Batch{Record: record}
}

// finalized evaluates batch (when non-nil) and finalizes.
func finalized(
	t *testing.T, cfg *config.Config, schema *arrow.Schema, budget *resource.Budget, batch *data.Batch,
) *data.Summary {
	t.Helper()
	p := bindProgram(t, cfg, schema, budget)
	if batch != nil {
		result, err := p.Evaluate(batch)
		if err != nil {
			t.Fatal(err)
		}
		result.Release()
	}
	if err := p.Finalize(); err != nil {
		t.Fatal(err)
	}
	return p.Summary()
}

func jsonText(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// datasetOutcome is the status, reason and value of one dataset rule result.
type datasetOutcome struct {
	status, reason string
	value          any
}

func outcomeOf(r data.DatasetResult) datasetOutcome {
	return datasetOutcome{status: r.Status, reason: r.Reason, value: r.Value}
}

func TestExactIntegerAggregates(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := int64Batch(budget, []int64{9007199254740993, 9007199254740994})
	defer batch.Release()
	cfg := &config.Config{
		Input: config.Input{Schema: &config.Schema{}},
		DatasetRules: []config.DatasetRule{
			{Code: "SUM", Metric: "sum", Column: "amount", Equals: "18014398509481987"},
			{Code: "AVG", Metric: "avg", Column: "amount", Min: "9007199254740993.5", Max: "9007199254740993.5"},
			{Code: "MIN", Metric: "min", Column: "amount", Min: "9007199254740993", Max: "9007199254740993"},
		},
		Stats: []config.Stat{{Code: "COUNT", Column: "amount", Aggregate: "count"}},
	}
	s := finalized(t, cfg, batch.Record.Schema(), budget, batch)
	want := map[string]string{
		"SUM": `"18014398509481987"`,
		"AVG": `{"sum":"18014398509481987","count":2}`,
		"MIN": `"9007199254740993"`,
	}
	for _, r := range s.DatasetResults {
		if r.Status != datasetPass {
			t.Errorf("%s status = %s, want %s (result %+v)", r.Code, r.Status, datasetPass, r)
		}
		if got := jsonText(t, r.Value); got != want[r.Code] {
			t.Errorf("%s value = %s, want %s", r.Code, got, want[r.Code])
		}
		if r.Contributing != 2 {
			t.Errorf("%s contributing = %d, want 2", r.Code, r.Contributing)
		}
	}
	if got := s.Stats["COUNT"]; got != uint64(2) {
		t.Errorf("COUNT stat = %v (%T), want uint64 2", got, got)
	}
	if s.Validation != data.ValidationPass {
		t.Errorf("validation = %s, want %s", s.Validation, data.ValidationPass)
	}
}

func TestUndefinedAndCountMetrics(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64}}, nil)
	cfg := &config.Config{DatasetRules: []config.DatasetRule{
		{Code: "EMPTY", Level: "warn", Metric: "null_fraction", Column: "amount", Max: 1},
		{Code: "ROWS", Metric: "row_count", Equals: 0},
	}}
	s := finalized(t, cfg, schema, resource.New(16<<20), nil)
	empty := s.DatasetResults[0]
	if got, want := (datasetOutcome{status: empty.Status, reason: empty.Reason}),
		(datasetOutcome{status: datasetFail, reason: reasonNoObservations}); got != want {
		t.Errorf("fraction over zero rows status/reason = %+v, want %+v", got, want)
	}
	if got := s.DatasetResults[1].Status; got != datasetPass {
		t.Errorf("row_count status = %s, want %s", got, datasetPass)
	}
	if s.Validation != data.ValidationPass {
		t.Errorf("validation = %s, want %s (the failing rule is a warning)", s.Validation, data.ValidationPass)
	}
	if len(s.DatasetIssues) != 1 {
		t.Fatalf("dataset issues = %+v, want one warning", s.DatasetIssues)
	}
	issue := s.DatasetIssues[0]
	if issue.Origin != data.OriginDataset {
		t.Errorf("dataset issue origin = %s, want %s", issue.Origin, data.OriginDataset)
	}
	if issue.Severity != "warn" {
		t.Errorf("dataset issue severity = %s, want warn", issue.Severity)
	}
}

func TestErrorDatasetAssertionFailsValidation(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := int64Batch(budget, []int64{1, 2, 3})
	defer batch.Release()
	cfg := &config.Config{DatasetRules: []config.DatasetRule{{Code: "ROWS", Metric: "row_count", Max: 2}}}
	s := finalized(t, cfg, batch.Record.Schema(), budget, batch)
	got := outcomeOf(s.DatasetResults[0])
	if want := (datasetOutcome{status: datasetFail, reason: reasonAssertionFailed, value: uint64(3)}); got != want {
		t.Errorf("result = %+v, want %+v", got, want)
	}
	if s.Validation != data.ValidationFail {
		t.Errorf("validation = %s, want %s", s.Validation, data.ValidationFail)
	}
}

func TestDecimalAggregateKeepsScale(t *testing.T) {
	budget := resource.New(16 << 20)
	typ := &arrow.Decimal128Type{Precision: 38, Scale: 2}
	builder := array.NewDecimal128Builder(budget.Allocator(), typ)
	for _, text := range []string{"9007199254740993.01", "0.09"} {
		value, err := decimal128.FromString(text, 38, 2)
		if err != nil {
			t.Fatal(err)
		}
		builder.Append(value)
	}
	column := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: typ}}, nil)
	batch := &data.Batch{Record: array.NewRecordBatch(schema, []arrow.Array{column}, 2)}
	column.Release()
	defer batch.Release()
	cfg := &config.Config{
		Input: config.Input{Schema: &config.Schema{}},
		DatasetRules: []config.DatasetRule{
			{Code: "SUM", Metric: "sum", Column: "amount", Equals: json.Number("9007199254740993.10")},
		},
		Rules: []config.Rule{{Code: "EXACT", Level: "error", Column: "amount", Checks: []config.Check{{
			Type:   "equals",
			Params: map[string]any{"value": json.Number("9007199254740993.01")},
		}}}},
	}
	s := finalized(t, cfg, schema, budget, batch)
	sum := s.DatasetResults[0]
	if got, want := (datasetOutcome{status: sum.Status, value: sum.Value}),
		(datasetOutcome{status: datasetPass, value: "9007199254740993.10"}); got != want {
		t.Errorf("decimal sum = %+v, want %+v (scale-2 text)", got, want)
	}
	if got := s.RuleCounts["EXACT"]; got != 1 {
		t.Errorf("decimal literal comparison failures = %d, want 1 (only the 0.09 row differs)", got)
	}
}

// overflowBatch holds two float64 values whose sum overflows.
func overflowBatch(budget *resource.Budget) *data.Batch {
	return floatBatch(budget, arrow.PrimitiveTypes.Float64, []float64{1e308, 1e308})
}

// float64Input declares column x as float64.
func float64Input() config.Input {
	return config.Input{Schema: &config.Schema{Columns: map[string]config.Column{"x": {Type: "float64"}}}}
}

// Extrema must not fail because an unrequested sum would overflow.
func TestFloatAggregates(t *testing.T) {
	for _, metric := range []string{"min", "max"} {
		t.Run(metric, func(t *testing.T) {
			budget := resource.New(16 << 20)
			batch := overflowBatch(budget)
			defer batch.Release()
			cfg := &config.Config{
				Input:        float64Input(),
				DatasetRules: []config.DatasetRule{{Code: "R", Metric: metric, Column: "x", Min: 1}},
			}
			r := finalized(t, cfg, batch.Record.Schema(), budget, batch).DatasetResults[0]
			if r.Status != datasetPass {
				t.Errorf("status = %s, want %s", r.Status, datasetPass)
			}
			if r.Value != 1e308 {
				t.Errorf("value = %v, want %g", r.Value, 1e308)
			}
			if r.Contributing != 2 {
				t.Errorf("contributing = %d, want 2", r.Contributing)
			}
		})
	}
}

func TestFloatSumOverflowFailsEvaluation(t *testing.T) {
	for _, metric := range []string{"sum", "avg"} {
		t.Run(metric+" overflow", func(t *testing.T) {
			budget := resource.New(16 << 20)
			batch := overflowBatch(budget)
			defer batch.Release()
			cfg := &config.Config{Input: float64Input(), DatasetRules: []config.DatasetRule{
				{Code: "M", Metric: "max", Column: "x", Min: 1},
				{Code: "R", Metric: metric, Column: "x", Min: 1},
			}}
			p := bindProgram(t, cfg, batch.Record.Schema(), budget)
			result, err := p.Evaluate(batch)
			defer result.Release()
			if err == nil {
				t.Fatalf("evaluate error = nil, want an overflow error for the requested %s", metric)
			}
		})
	}
}

func TestFloatEqualsUsesTolerance(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := floatBatch(budget, arrow.PrimitiveTypes.Float64, []float64{0.1, 0.2})
	defer batch.Release()
	tolerance := 1e-9
	cfg := &config.Config{
		Input: config.Input{Schema: &config.Schema{}},
		DatasetRules: []config.DatasetRule{
			{Code: "NEAR", Metric: "sum", Column: "x", Equals: json.Number("0.3"), Tolerance: &tolerance},
		},
		Stats: []config.Stat{
			{Code: "TOTAL", Column: "x", Aggregate: "sum"},
			{Code: "MEAN", Column: "x", Aggregate: "avg"},
		},
	}
	s := finalized(t, cfg, batch.Record.Schema(), budget, batch)
	if got := s.DatasetResults[0].Status; got != datasetPass {
		t.Errorf("tolerant equals status = %s, want %s", got, datasetPass)
	}
	for code, want := range map[string]float64{"TOTAL": 0.3, "MEAN": 0.15} {
		got, ok := s.Stats[code].(float64)
		if !ok || math.Abs(got-want) > tolerance {
			t.Errorf("%s stat = %v, want float64 %g ± %g", code, s.Stats[code], want, tolerance)
		}
	}
}
