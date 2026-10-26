package checks

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

func TestResultsOwnStorageAcrossBatchesAndClose(t *testing.T) {
	budget := resource.New(16 << 20)
	first := stringBatch(t, budget, []string{"x"}, [][]string{{"bad", "okay"}}, nil)
	second := stringBatch(t, budget, []string{"x"}, [][]string{{"okay", "bad"}}, nil)
	p := bindProgram(t, &config.Config{Rules: []config.Rule{rule("R", "equals", map[string]any{"value": "okay"})}}, first.Record.Schema(), budget)
	a, err := p.Evaluate(first)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	first.Release()
	b, err := p.Evaluate(second)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	second.Release()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.Buckets, []data.Bucket{data.BucketError, data.BucketPass}) || !slices.Equal(b.Buckets, []data.Bucket{data.BucketPass, data.BucketError}) {
		t.Fatalf("independent buckets = %v, %v", a.Buckets, b.Buckets)
	}
	if len(a.Issues) != 1 || a.Issues[0].Row != 1 || len(b.Issues) != 1 || b.Issues[0].Row != 2 {
		t.Fatalf("independent issues = %+v, %+v", a.Issues, b.Issues)
	}
	if budget.Used() == 0 {
		t.Fatal("outstanding results lost their reservations at Close")
	}
	a.Release()
	a.Release()
	b.Release()
	if got := budget.Used(); got != 0 {
		t.Fatalf("released budget = %d, want 0", got)
	}
}

func TestResultIssueDensityPreservesOrderAndCounts(t *testing.T) {
	for _, failures := range []int{0, 1, 65} {
		t.Run(fmt.Sprint(failures), func(t *testing.T) {
			budget := resource.New(16 << 20)
			values := make([]string, 65)
			for i := range values {
				values[i] = "okay"
				if i < failures {
					values[i] = "bad"
				}
			}
			batch := stringBatch(t, budget, []string{"x"}, [][]string{values}, nil)
			defer batch.Release()
			cfg := &config.Config{}
			for i := range 4 {
				cfg.Rules = append(cfg.Rules, rule(fmt.Sprint(i), "equals", map[string]any{"value": "okay"}))
			}
			p := bindProgram(t, cfg, batch.Record.Schema(), budget)
			for range 2 {
				result, err := p.Evaluate(batch)
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Issues) != failures*4 {
					t.Fatalf("issues = %d, want %d", len(result.Issues), failures*4)
				}
				for i, issue := range result.Issues {
					if issue.Row != uint64(i/4+1) || issue.Code != fmt.Sprint(i%4) || issue.Column != "x" {
						t.Fatalf("issue %d = %+v", i, issue)
					}
				}
				result.Release()
			}
			if err := p.Finalize(); err != nil {
				t.Fatal(err)
			}
			s := p.Summary()
			if s.Total != 130 || s.Errored != uint64(2*failures) || s.Passed != uint64(130-2*failures) {
				t.Fatalf("counts = %+v", s)
			}
			for _, r := range p.rules {
				if r.evaluated != 130 || s.RuleCounts[r.code] != uint64(2*failures) {
					t.Fatalf("rule %s evaluated=%d failures=%d", r.code, r.evaluated, s.RuleCounts[r.code])
				}
			}
		})
	}
}

func TestSparseResultsFitBudgetAndDenseGrowthIsCharged(t *testing.T) {
	values := make([]string, 4096)
	for i := range values {
		values[i] = "okay"
	}
	batch := stringBatch(t, resource.New(16<<20), []string{"x"}, [][]string{values}, nil)
	defer batch.Release()
	cfg := &config.Config{}
	for i := range 4 {
		cfg.Rules = append(cfg.Rules, rule(fmt.Sprint(i), "equals", map[string]any{"value": "okay"}))
	}
	budget := resource.New(64 << 10)
	p := bindProgram(t, cfg, batch.Record.Schema(), budget)
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatalf("all-pass result should not reserve hypothetical issues: %v", err)
	}
	defer result.Release()
	if len(result.Issues) != 0 {
		t.Fatalf("issues = %+v", result.Issues)
	}
	remaining := budget.Limit() - budget.Used()
	if err := budget.Reserve(remaining, "test occupied budget"); err != nil {
		t.Fatal(err)
	}
	other, err := p.Evaluate(batch)
	other.Release()
	budget.Release(remaining)
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("outstanding result budget error = %v", err)
	}
	if result.Buckets[0] != data.BucketPass {
		t.Fatal("failed evaluation modified its predecessor")
	}
	result.Release()
	bad := stringBatch(t, resource.New(16<<20), []string{"x"}, [][]string{make([]string, 4096)}, nil)
	defer bad.Release()
	other, err = p.Evaluate(bad)
	other.Release()
	if !errors.As(err, &limit) {
		t.Fatalf("dense issue growth error = %v", err)
	}
	p.Close()
	if got := budget.Used(); got != 0 {
		t.Fatalf("budget after failed result and Close = %d", got)
	}
}

func TestReleaseMayOverlapEvaluationAndClose(t *testing.T) {
	budget := resource.New(16 << 20)
	batch := stringBatch(t, budget, []string{"x"}, [][]string{{"bad"}}, nil)
	defer batch.Release()
	before := budget.Used()
	p := bindProgram(t, &config.Config{Rules: []config.Rule{rule("R", "not_blank", nil)}}, batch.Record.Schema(), budget)
	var releases sync.WaitGroup
	for range 100 {
		result, err := p.Evaluate(batch)
		if err != nil {
			t.Fatal(err)
		}
		releases.Add(1)
		go func() { defer releases.Done(); result.Release(); result.Release() }()
	}
	p.Close()
	releases.Wait()
	if got := budget.Used(); got != before {
		t.Fatalf("budget after concurrent releases = %d, want %d", got, before)
	}
}

func TestSharedConditionsPreserveRulePrecedenceAndBatchStates(t *testing.T) {
	budget := resource.New(16 << 20)
	cfg := &config.Config{Rules: []config.Rule{
		{Code: "WARN", Level: "warn", When: &config.Condition{Column: "x", Operator: "eq", Value: "yes"}, Checks: []config.Check{{Type: "equals", Params: map[string]any{"column": "y", "value": "okay"}}}},
		{Code: "ERROR", Level: "error", When: &config.Condition{Column: "x", Operator: "eq", Value: "yes"}, Checks: []config.Check{
			{Type: "equals", Params: map[string]any{"column": "y", "value": "okay"}},
			{Type: "equals", Params: map[string]any{"column": "z", "value": 1}},
		}},
	}}
	batch := stringBatch(t, budget, []string{"x", "y", "z"}, [][]string{{"yes", "no", ""}, {"bad", "bad", "bad"}, {"not-number", "not-number", "not-number"}}, map[string][]data.State{"x": {V, V, I}})
	defer batch.Release()
	p := bindProgram(t, cfg, batch.Record.Schema(), budget)
	result, err := p.Evaluate(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(result.Buckets, []data.Bucket{data.BucketError, data.BucketPass, data.BucketPass}) {
		t.Fatalf("buckets = %v", result.Buckets)
	}
	if len(result.Issues) != 2 || result.Issues[0].Code != "WARN" || result.Issues[1].Code != "ERROR" || result.Issues[1].Column != "y" {
		t.Fatalf("precedence and attribution = %+v", result.Issues)
	}
	result.Release()
	second := stringBatch(t, budget, []string{"x", "y", "z"}, [][]string{{"no"}, {"bad"}, {"not-number"}}, nil)
	defer second.Release()
	result, err = p.Evaluate(second)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if len(result.Issues) != 0 {
		t.Fatalf("stale shared condition across batches: %+v", result.Issues)
	}
	s := p.Summary()
	if !maps.Equal(s.RuleCounts, map[string]uint64{"WARN": 1, "ERROR": 1}) || !maps.Equal(s.NotApplicable, map[string]uint64{"WARN": 2, "ERROR": 2}) || !maps.Equal(s.Unevaluable, map[string]uint64{"WARN": 1, "ERROR": 1}) {
		t.Fatalf("condition counters = %+v", s)
	}
	if !maps.Equal(s.RuleCoverage, map[string]data.RuleCoverage{"WARN": {ValueRows: 4}, "ERROR": {ValueRows: 4}}) {
		t.Fatalf("final-pass rule coverage excludes applicable or unevaluable rows: %+v", s.RuleCoverage)
	}
	if s.Coverage.ValueRows != 4 || s.Coverage.MetadataClaimedRows != 0 || s.Coverage.AccountedRows != 4 {
		t.Fatalf("decoded coverage = %+v", s.Coverage)
	}
}

func TestDescribeRejectsIngestionSchemaBeforeBindingRows(t *testing.T) {
	cfg := &config.Config{
		Source: config.Source{URIs: []string{"input.csv"}},
		Input:  config.Input{Schema: &config.Schema{ExtraFields: "error", Columns: map[string]config.Column{"x": {Type: "string", Required: true}}}},
		Rules:  []config.Rule{rule("UNBOUND", "not_null", map[string]any{"column": "absent"}), rule("REF", "referential_integrity", referenceParams("does-not-exist.csv"))},
	}
	ds, store, budget := datasetFixture(t, map[string]string{"input.csv": "other\nvalue\n"}, cfg)
	if len(ds.SchemaIssues()) == 0 {
		t.Fatal("fixture should reject ingestion schema")
	}
	description, err := Describe(context.Background(), cfg, ds, store, budget)
	if err != nil {
		t.Fatal(err)
	}
	if description.RequiredPasses != 0 || len(description.SchemaIssues) != len(ds.SchemaIssues()) {
		t.Fatalf("rejected description = %+v", description)
	}
}
