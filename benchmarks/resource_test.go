package benchmarks_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/testutil/batchgen"
)

func TestGeneratedEngineResourceWindows(t *testing.T) {
	// Both runs exceed the 16MiB budget in total input volume, while the live
	// working set stays bounded. Fixed-cardinality row-local state must not grow.
	const (
		budget          = 16 * mib
		smallRows       = 24000
		largeRows       = 96000
		fixedStateSlack = 1 * mib
	)
	window := func(rows uint64) measurement {
		t.Helper()
		o := batchgen.Options{
			Rows:         rows,
			BatchRows:    128,
			Seed:         42,
			StringWidth:  1024,
			Cardinality:  32,
			FailureEvery: 97,
			NullEvery:    41,
			LateFailure:  true,
			FileRows:     10000,
		}
		m, err := engine(context.Background(), engineRun{options: o, limit: budget, rules: numericRules()})
		if err != nil {
			t.Fatal(err)
		}
		if err := expectNumeric(m, o); err != nil {
			t.Fatal(err)
		}
		if m.LogicalBytes <= budget {
			t.Errorf("rows=%d logical bytes = %d, want > budget %d", rows, m.LogicalBytes, budget)
		}
		if m.ManagedPeak > budget {
			t.Errorf("rows=%d managed peak = %d, want <= budget %d", rows, m.ManagedPeak, budget)
		}
		if m.ManagedResidual != 0 {
			t.Errorf("rows=%d managed residual = %d, want 0", rows, m.ManagedResidual)
		}
		t.Logf("rows=%d logical=%d managed_peak=%d rss_peak=%d", m.Rows, m.LogicalBytes, m.ManagedPeak, m.PeakRSS)
		return m
	}

	small, large := window(smallRows), window(largeRows)
	if limit := small.ManagedPeak + fixedStateSlack; large.ManagedPeak > limit {
		t.Errorf("managed peak at %d rows = %d, want <= %d (peak at %d rows + %d slack)",
			largeRows, large.ManagedPeak, limit, smallRows, fixedStateSlack)
	}
}

func TestGeneratedEngineScenarios(t *testing.T) {
	const (
		rows      = 1025
		batchRows = 63
	)
	cases := []struct {
		name    string
		options batchgen.Options
	}{
		{name: "numeric", options: batchgen.Options{Rows: rows, BatchRows: batchRows, NumericOnly: true, FailureEvery: 97}},
		{
			name: "wide dense",
			options: batchgen.Options{
				Rows: rows, BatchRows: batchRows, StringWidth: 4096, FailureEvery: 2, NullEvery: 11,
			},
		},
		{name: "late", options: batchgen.Options{Rows: rows, BatchRows: batchRows, LateFailure: true}},
		{name: "empty", options: batchgen.Options{Rows: 0, BatchRows: batchRows}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, err := engine(context.Background(), engineRun{options: c.options, limit: 32 * mib, rules: numericRules()})
			if err != nil {
				t.Fatal(err)
			}
			if err := expectNumeric(m, c.options); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestGeneratedSlowSinkAndBoundaryDeterminism(t *testing.T) {
	const (
		smallBatch = 7
		largeBatch = 128
	)
	digest := func(batchRows int) string {
		t.Helper()
		o := batchgen.Options{Rows: 513, BatchRows: batchRows, Seed: 91, StringWidth: 100, FailureEvery: 7, FileRows: 61}
		m, err := engine(context.Background(), engineRun{
			options:   o,
			limit:     8 * mib,
			rules:     numericRules(),
			sinkDelay: time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := expectNumeric(m, o); err != nil {
			t.Fatal(err)
		}
		return m.Digest
	}

	small, large := digest(smallBatch), digest(largeBatch)
	if large != small {
		t.Errorf("digest with %d-row batches = %s, want %s (digest with %d-row batches)",
			largeBatch, large, small, smallBatch)
	}
}

func TestGeneratedGlobalBudgetAndDuplicates(t *testing.T) {
	const ruleUnique = "UNIQUE"
	rules := []config.Rule{{
		Code:   ruleUnique,
		Column: "id",
		Level:  "error",
		Checks: []config.Check{{Type: "is_unique"}},
	}}

	t.Run("all duplicate occurrences across batches and files", func(t *testing.T) {
		// Every 100th row repeats the previous id, so both rows of each of the
		// 10 pairs are flagged.
		const duplicateRows = 20
		o := batchgen.Options{Rows: 1000, BatchRows: 17, DuplicateEvery: 100, FileRows: 99, NumericOnly: true}
		m, err := engine(context.Background(), engineRun{options: o, limit: 16 * mib, rules: rules})
		if err != nil {
			t.Fatal(err)
		}
		want := outcome{rows: o.Rows, pass: o.Rows - duplicateRows, errored: duplicateRows}
		if got := outcomeOf(m); got != want {
			t.Errorf("outcome = %+v, want %+v", got, want)
		}
		if got := m.Rules[ruleUnique]; got != duplicateRows {
			t.Errorf("%s issues = %d, want %d", ruleUnique, got, duplicateRows)
		}
	})

	t.Run("bounded cardinality", func(t *testing.T) {
		// 32 distinct ids over 10000 rows: every row is a duplicate.
		o := batchgen.Options{Rows: 10000, BatchRows: 128, Cardinality: 32, NumericOnly: true}
		m, err := engine(context.Background(), engineRun{options: o, limit: 8 * mib, rules: rules})
		if err != nil {
			t.Fatal(err)
		}
		want := outcome{rows: o.Rows, errored: o.Rows}
		if got := outcomeOf(m); got != want {
			t.Errorf("outcome = %+v, want %+v", got, want)
		}
		if m.ManagedResidual != 0 {
			t.Errorf("managed residual = %d, want 0", m.ManagedResidual)
		}
	})

	t.Run("growing state refuses budget", func(t *testing.T) {
		const budget = 2 * mib
		o := batchgen.Options{Rows: 1000000, BatchRows: 64, NumericOnly: true}
		m, err := engine(context.Background(), engineRun{options: o, limit: budget, rules: rules})
		var resourceErr *resource.ResourceLimitError
		if !errors.As(err, &resourceErr) {
			t.Errorf("engine error = %v, want *ResourceLimitError", err)
		}
		if m.ManagedResidual != 0 {
			t.Errorf("managed residual = %d, want 0", m.ManagedResidual)
		}
		if m.ManagedPeak > budget {
			t.Errorf("managed peak = %d, want <= budget %d", m.ManagedPeak, budget)
		}
	})
}

func TestGeneratedCancellationAndOversize(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		m, err := engine(ctx, engineRun{
			options: batchgen.Options{Rows: 100, BatchRows: 10},
			limit:   8 * mib,
			rules:   numericRules(),
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("engine error = %v, want %v", err, context.Canceled)
		}
		if m.ManagedResidual != 0 {
			t.Errorf("managed residual = %d, want 0", m.ManagedResidual)
		}
	})

	t.Run("cancel with owned batch", func(t *testing.T) {
		const cancelAtRows = 128
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		m, err := engine(ctx, engineRun{
			options: batchgen.Options{Rows: 10000, BatchRows: 64, StringWidth: 1024},
			limit:   8 * mib,
			rules:   numericRules(),
			afterBatch: func(rows uint64) {
				if rows == cancelAtRows {
					cancel()
				}
			},
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("engine error = %v, want %v", err, context.Canceled)
		}
		if m.Rows != cancelAtRows {
			t.Errorf("rows = %d, want %d", m.Rows, cancelAtRows)
		}
		if m.ManagedResidual != 0 {
			t.Errorf("managed residual = %d, want 0", m.ManagedResidual)
		}
	})

	t.Run("oversize", func(t *testing.T) {
		// A single 8MiB string cannot fit a 1MiB budget.
		m, err := engine(context.Background(), engineRun{
			options: batchgen.Options{Rows: 1, BatchRows: 1, StringWidth: 8 * mib},
			limit:   1 * mib,
			rules:   numericRules(),
		})
		var resourceErr *resource.ResourceLimitError
		if !errors.As(err, &resourceErr) {
			t.Errorf("engine error = %v, want *ResourceLimitError", err)
		}
		if m.ManagedResidual != 0 {
			t.Errorf("managed residual = %d, want 0", m.ManagedResidual)
		}
	})
}

func TestGeneratedReferenceMembership(t *testing.T) {
	const (
		ruleReference = "REFERENCE"
		referenceIDs  = 32 // reference holds ids 0..31
		rows          = 1024
		// Generated ids cycle 0..63, so exactly half fall inside the reference.
		cardinality = 2 * referenceIDs
		outside     = rows / 2
	)
	path := filepath.Join(t.TempDir(), "reference.csv")
	var text strings.Builder
	text.WriteString("id\n")
	for i := range referenceIDs {
		fmt.Fprintf(&text, "%d\n", i)
	}
	if err := os.WriteFile(path, []byte(text.String()), 0600); err != nil {
		t.Fatal(err)
	}
	rules := []config.Rule{{
		Code:   ruleReference,
		Column: "id",
		Level:  "error",
		Checks: []config.Check{{
			Type:   "referential_integrity",
			Params: map[string]any{"reference_file": path, "reference_column": "id"},
		}},
	}}

	m, err := engine(context.Background(), engineRun{
		options: batchgen.Options{Rows: rows, BatchRows: 17, Cardinality: cardinality, NumericOnly: true},
		limit:   32 * mib,
		rules:   rules,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := outcome{rows: rows, pass: rows - outside, errored: outside}
	if got := outcomeOf(m); got != want {
		t.Errorf("outcome = %+v, want %+v", got, want)
	}
	if got := m.Rules[ruleReference]; got != outside {
		t.Errorf("%s issues = %d, want %d", ruleReference, got, outside)
	}
}

func BenchmarkEngine(b *testing.B) {
	for _, width := range []int{0, 1024} {
		b.Run(fmt.Sprintf("width=%d", width), func(b *testing.B) {
			o := batchgen.Options{
				Rows:         8192,
				BatchRows:    512,
				Seed:         42,
				StringWidth:  width,
				NumericOnly:  width == 0,
				FailureEvery: 97,
			}
			b.ReportAllocs()
			for range b.N {
				m, err := engine(context.Background(), engineRun{options: o, limit: 64 * mib, rules: numericRules()})
				if err != nil {
					b.Fatal(err)
				}
				if err := expectNumeric(m, o); err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(m.LogicalBytes))
				b.ReportMetric(float64(m.ManagedPeak), "managed-peak-B")
				b.ReportMetric(float64(m.PeakRSS), "rss-peak-B")
			}
		})
	}
}
