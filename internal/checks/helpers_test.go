package checks

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/source"
	"github.com/richardrh/valet/internal/storage"
)

// Row outcomes of one rule, as observed through summary counters.
const (
	pass          = "pass"
	fail          = "fail"
	unevaluable   = "unevaluable"
	notApplicable = "not_applicable"
)

// Cell states, abbreviated so state slices line up with the values they
// describe in case tables: V valid, N null, M missing column, I invalid
// (failed ingest conversion).
const (
	V = data.Valid
	N = data.Null
	M = data.Missing
	I = data.Invalid
)

// bucketCounts are a summary's per-bucket row counts.
type bucketCounts struct{ errored, warned, passed uint64 }

// countsFor projects counters onto keys, reading absent keys as zero.
func countsFor(counters map[string]uint64, keys ...string) map[string]uint64 {
	out := make(map[string]uint64, len(keys))
	for _, key := range keys {
		out[key] = counters[key]
	}
	return out
}

// stringBatch builds string columns; a non-Valid state stores null.
func stringBatch(
	t *testing.T, budget *resource.Budget, names []string, values [][]string, states map[string][]data.State,
) *data.Batch {
	t.Helper()
	fields := make([]arrow.Field, len(names))
	columns := make([]arrow.Array, len(names))
	for i, name := range names {
		fields[i] = arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}
		builder := array.NewStringBuilder(budget.Allocator())
		for row, value := range values[i] {
			if s := states[name]; s != nil && s[row] != data.Valid {
				builder.AppendNull()
			} else {
				builder.Append(value)
			}
		}
		columns[i] = builder.NewArray()
		builder.Release()
	}
	record := array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, int64(len(values[0])))
	for _, col := range columns {
		col.Release()
	}
	locations := make([]data.Location, len(values[0]))
	for row := range locations {
		locations[row] = data.Location{Source: "fixture", Row: uint64(row + 1), GlobalRow: uint64(row + 1)}
	}
	return &data.Batch{Record: record, Locations: locations, States: states}
}

// rule is an error-level rule on column x with one check.
func rule(code, typ string, params map[string]any) config.Rule {
	return config.Rule{
		Code:   code,
		Level:  "error",
		Column: "x",
		Checks: []config.Check{{Type: typ, Params: params}},
	}
}

// bindProgram compiles cfg against an in-memory schema: the Compile path
// without dataset replay or reference inputs.
func bindProgram(t *testing.T, cfg *config.Config, schema *arrow.Schema, budget *resource.Budget) *Program {
	t.Helper()
	p, err := bindProgramErr(cfg, schema, budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func bindProgramErr(cfg *config.Config, schema *arrow.Schema, budget *resource.Budget) (*Program, error) {
	p, _, err := newProgram(context.Background(), cfg, schema, budget, true, nil, nil)
	return p, err
}

// rowOutcomes evaluates each case as its own row of string column x and
// returns the outcome of rule code per row.
func rowOutcomes(t *testing.T, cfg *config.Config, code string, rows []rowCase) []string {
	t.Helper()
	budget := resource.New(16 << 20)
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	p := bindProgram(t, cfg, schema, budget)
	out := make([]string, len(rows))
	for i, row := range rows {
		batch := stringBatch(t, budget, []string{"x"}, [][]string{{row.value}}, map[string][]data.State{"x": {row.state}})
		s := p.Summary()
		failed, skipped, na := s.RuleCounts[code], s.Unevaluable[code], s.NotApplicable[code]
		result, err := p.Evaluate(batch)
		if err != nil {
			batch.Release()
			t.Fatalf("row %d: %v", i, err)
		}
		result.Release()
		batch.Release()
		switch {
		case s.RuleCounts[code] > failed:
			out[i] = fail
		case s.Unevaluable[code] > skipped:
			out[i] = unevaluable
		case s.NotApplicable[code] > na:
			out[i] = notApplicable
		default:
			out[i] = pass
		}
	}
	return out
}

// datasetFixture writes files into a temp dir, rewrites cfg's source and
// reference paths into it, and opens the dataset. Input type defaults to csv
// raw strings.
func datasetFixture(
	t *testing.T, files map[string]string, cfg *config.Config,
) (*source.Dataset, *storage.Store, *resource.Budget) {
	t.Helper()
	dir := t.TempDir()
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for i, path := range cfg.Source.URIs {
		cfg.Source.URIs[i] = filepath.Join(dir, path)
	}
	for ri := range cfg.Rules {
		for ci := range cfg.Rules[ri].Checks {
			m := cfg.Rules[ri].Checks[ci].Params
			if path, ok := m["reference_file"].(string); ok {
				m["reference_file"] = filepath.Join(dir, path)
			}
		}
	}
	cfg.Execution = config.Execution{
		MemoryBudget:   "128MiB",
		BatchTarget:    "32KiB",
		MaxRecordBytes: "64KiB",
		Workers:        1,
		MaxDepth:       64,
	}
	if cfg.Input.Type == "" {
		cfg.Input.Type = "csv"
		cfg.Input.Mode = "raw_strings"
	}
	cfg.Output.URI = filepath.Join(dir, "output")
	budget := resource.New(128 << 20)
	store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget, nil)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := source.New(context.Background(), cfg, store, budget, nil, false)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ds.Close()
		store.Close()
		if used := budget.Used(); used != 0 {
			t.Errorf("managed budget used after close = %d bytes, want 0", used)
		}
	})
	return ds, store, budget
}

// scan evaluates the whole dataset and returns the row buckets as strings.
func scan(t *testing.T, p *Program, ds *source.Dataset) []string {
	t.Helper()
	reader, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	buckets := []string{}
	for {
		batch, err := reader.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		result, err := p.Evaluate(batch)
		batch.Release()
		if err != nil {
			t.Fatal(err)
		}
		buckets = append(buckets, bucketNames(result.Buckets)...)
		result.Release()
	}
	return buckets
}

func bucketNames(buckets []data.Bucket) []string {
	out := make([]string, len(buckets))
	for i, b := range buckets {
		out[i] = b.String()
	}
	return out
}
