package checks

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

func TestIndexedMembershipKeepsCoercionAndNullSemantics(t *testing.T) {
	for _, numeric := range []bool{false, true} {
		values := make([]any, 8)
		for i := range values {
			if numeric {
				values[i] = int64(i + 1)
			} else {
				values[i] = string(rune('a' + i))
			}
		}
		budget := resource.New(16 << 20)
		batch := stringBatch(t, budget, []string{"x"}, [][]string{{"a", "01", "bad", "", "", ""}}, map[string][]data.State{"x": {V, V, V, N, M, I}})
		p := bindProgram(t, &config.Config{Rules: []config.Rule{rule("R", "valid_values", map[string]any{"values": values})}}, batch.Record.Schema(), budget)
		result, err := p.Evaluate(batch)
		if err != nil {
			t.Fatal(err)
		}
		want := []data.Bucket{data.BucketPass, data.BucketError, data.BucketError, data.BucketPass, data.BucketPass, data.BucketPass}
		failures, skipped := uint64(2), uint64(1)
		if numeric {
			want = []data.Bucket{data.BucketPass, data.BucketPass, data.BucketPass, data.BucketPass, data.BucketPass, data.BucketPass}
			failures, skipped = 0, 3 // unparsable strings remain unevaluable, not violations.
		}
		if !slices.Equal(result.Buckets, want) || p.Summary().RuleCounts["R"] != failures || p.Summary().Unevaluable["R"] != skipped {
			t.Fatalf("numeric=%t buckets=%v summary=%+v", numeric, result.Buckets, p.Summary())
		}
		result.Release()
		p.Close()
		batch.Release()
		if budget.Used() != 0 {
			t.Fatalf("membership budget leak: %d", budget.Used())
		}
	}
}

func TestIndexedFloatingMembershipUsesColumnWidth(t *testing.T) {
	for _, typ := range []arrow.DataType{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64} {
		budget := resource.New(16 << 20)
		batch := floatBatch(budget, typ, []float64{0.1, 0.9})
		values := []any{json.Number("0.1"), json.Number("0.2"), json.Number("0.3"), json.Number("0.4"), json.Number("0.5"), json.Number("0.6"), json.Number("0.7"), json.Number("0.8")}
		r := rule("VALUES", "valid_values", map[string]any{"values": values})
		when := rule("WHEN", "equals", map[string]any{"value": 0.9})
		when.When = &config.Condition{Column: "x", Operator: "in", Value: values}
		cfg := &config.Config{Input: config.Input{Schema: &config.Schema{Columns: map[string]config.Column{"x": {Type: typ.String()}}}}, Rules: []config.Rule{r, when}}
		p := bindProgram(t, cfg, batch.Record.Schema(), budget)
		result, err := p.Evaluate(batch)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(result.Buckets, []data.Bucket{data.BucketError, data.BucketError}) || !maps.Equal(p.Summary().RuleCounts, map[string]uint64{"VALUES": 1, "WHEN": 1}) || p.Summary().NotApplicable["WHEN"] != 1 {
			t.Fatalf("%s summary=%+v buckets=%v", typ, p.Summary(), result.Buckets)
		}
		result.Release()
		p.Close()
		batch.Release()
	}
}

func TestIndexedIntegerMembershipRetainsExactValues(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := int64Batch(budget, []int64{9007199254740992, 9007199254740993})
	defer batch.Release()
	values := []any{int64(9007199254740993), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8)}
	cfg := &config.Config{Input: config.Input{Schema: &config.Schema{}}, Rules: []config.Rule{{Code: "R", Column: "amount", Checks: []config.Check{{Type: "valid_values", Params: map[string]any{"values": values}}}}}}
	p := bindProgram(t, cfg, batch.Record.Schema(), budget)
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if !slices.Equal(result.Buckets, []data.Bucket{data.BucketError, data.BucketPass}) || p.Summary().RuleCounts["R"] != 1 {
		t.Fatalf("exact integer membership: %v %+v", result.Buckets, p.Summary())
	}
}
