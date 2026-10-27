package checks

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
)

func projectedReference(code string, columns, referenceColumns []any) config.Rule {
	return config.Rule{Code: code, Checks: []config.Check{{Type: "referential_integrity_composite", Params: map[string]any{"columns": columns, "reference_columns": referenceColumns, "reference_file": "ref.csv"}}}}
}

func TestReferenceProjectionsShareValidatedSourceScan(t *testing.T) {
	cfg := &config.Config{Source: config.Source{URIs: []string{"input.csv"}}, Input: config.Input{NullValues: []string{"NULL"}}, Rules: []config.Rule{
		projectedReference("A", []any{"x"}, []any{"a"}),
		projectedReference("A_AGAIN", []any{"x"}, []any{"a"}),
		projectedReference("B", []any{"y"}, []any{"b"}),
		projectedReference("PAIR", []any{"x", "y"}, []any{"a", "b"}),
		projectedReference("REVERSED", []any{"y", "x"}, []any{"b", "a"}),
	}}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	p, ds := compile(t, map[string]string{
		"input.csv": "x,y\n001,alpha\nNULL,beta\n,empty\n001,beta\n",
		"ref.csv":   "a,b\n001,alpha\nNULL,beta\n,empty\n",
	}, cfg, logger, nil)
	if got := scan(t, p, ds); !slices.Equal(got, []string{"pass", "pass", "pass", "error"}) {
		t.Fatalf("projected membership buckets = %v", got)
	}
	for _, code := range []string{"A", "A_AGAIN", "B"} {
		if p.Summary().RuleCounts[code] != 0 {
			t.Fatalf("single projection %s failures = %d", code, p.Summary().RuleCounts[code])
		}
	}
	for _, code := range []string{"PAIR", "REVERSED"} {
		if p.Summary().RuleCounts[code] != 1 {
			t.Fatalf("ordered projection %s failures = %d", code, p.Summary().RuleCounts[code])
		}
	}
	loads := 0
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record["phase"] == "reference" {
			loads++
			if record["rows"] != float64(3) || record["projections"] != float64(4) {
				t.Fatalf("reference load = %+v", record)
			}
		}
	}
	if loads != 1 {
		t.Fatalf("source loads = %d, want 1", loads)
	}
}

func TestGroupedReferencesRejectMissingProjectionAndMalformedRows(t *testing.T) {
	for _, body := range []string{"a\none\n", "a,b\none,two\n\"unterminated"} {
		cfg := &config.Config{Source: config.Source{URIs: []string{"input.csv"}}, Rules: []config.Rule{
			projectedReference("A", []any{"x"}, []any{"a"}),
			projectedReference("B", []any{"x"}, []any{"b"}),
		}}
		ds, store, sourceBudget := datasetFixture(t, map[string]string{"input.csv": "x\none\n", "ref.csv": body}, cfg)
		// The shared store intentionally retains newly discovered identities
		// until Close. Isolate compiler/reference-dataset scratch from that
		// store-owned metadata so this assertion exercises failed-load cleanup.
		compilerBudget := resource.New(128 << 20)
		program, err := Compile(context.Background(), cfg, ds, store, compilerBudget, nil, nil)
		if program != nil {
			program.Close()
		}
		if err == nil {
			t.Fatalf("invalid reference %q accepted", body)
		}
		if got := compilerBudget.Used(); got != 0 {
			t.Fatalf("failed reference compiler budget = %d, want 0", got)
		}
		if err := ds.Close(); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if got := sourceBudget.Used(); got != 0 {
			t.Fatalf("source/storage budget after Close = %d, want 0", got)
		}
	}
}
