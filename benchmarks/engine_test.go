package benchmarks_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"time"

	"github.com/richardrh/valet/internal/checks"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
	"github.com/richardrh/valet/internal/testutil/batchgen"
	"github.com/richardrh/valet/internal/testutil/checksink"
	"github.com/richardrh/valet/internal/testutil/rss"
)

const (
	mib = 1 << 20

	engineWorkers = 1

	rssSampleInterval = 250 * time.Millisecond
	// RSS is only sampled once this much logical input has streamed, so the
	// sustained figure excludes process start-up.
	rssWarmupBytes = 32 * mib
	// Sustained RSS keeps a bounded ring of samples, never one entry per batch.
	rssSampleRing = 256

	ruleNonNegative = "NONNEGATIVE"
)

type measurement struct {
	LogicalBytes      uint64            `json:"logical_arrow_bytes"`
	Rows              uint64            `json:"rows"`
	AverageWidth      float64           `json:"average_width_bytes"`
	Schema            string            `json:"schema"`
	Counts            map[string]uint64 `json:"buckets"`
	Rules             map[string]uint64 `json:"rule_counts"`
	Digest            string            `json:"order_digest"`
	ManagedPeak       int64             `json:"managed_peak_bytes"`
	ManagedResidual   int64             `json:"managed_residual_bytes"`
	PeakRSS           uint64            `json:"process_peak_rss_bytes"`
	SustainedRSS      uint64            `json:"sustained_rss_bytes"`
	RSSIntervalMillis int64             `json:"rss_sampling_interval_ms"`
	RSSSamples        int               `json:"rss_samples"`
	DurationSeconds   float64           `json:"duration_seconds"`
	Runtime           string            `json:"runtime"`
	GOOS              string            `json:"goos"`
	GOARCH            string            `json:"goarch"`
	Dependencies      map[string]string `json:"dependencies"`
	Generator         batchgen.Options  `json:"generator"`
	ManagedLimit      int64             `json:"managed_limit_bytes"`
	CPUs              int               `json:"logical_cpus"`
	GOMAXPROCS        int               `json:"gomaxprocs"`
	Workers           int               `json:"validation_workers"`
	Hardware          string            `json:"hardware_label,omitempty"`
	GOGC              string            `json:"gogc_environment"`
	GOMEMLIMIT        string            `json:"gomemlimit_environment"`
}

// engineRun describes one streaming validation run over generated input.
type engineRun struct {
	options batchgen.Options
	// limit is the managed memory budget in bytes.
	limit int64
	rules []config.Rule
	// sinkDelay slows the sink per batch to simulate a slow consumer.
	sinkDelay time.Duration
	// afterBatch, if set, is called with the cumulative row count after each batch is written.
	afterBatch func(rows uint64)
}

// outcome is the per-bucket row tally of a run.
type outcome struct {
	rows, pass, warn, errored uint64
}

func outcomeOf(m measurement) outcome {
	return outcome{rows: m.Rows, pass: m.Counts["pass"], warn: m.Counts["warn"], errored: m.Counts["error"]}
}

func engine(ctx context.Context, run engineRun) (m measurement, err error) {
	budget := resource.New(run.limit)
	start := time.Now()
	m = newMeasurement(run)
	// This mirrors the app's typed resource boundary, not a catch-all panic mask.
	defer func() {
		if p := recover(); p != nil {
			if resourceErr, ok := p.(*resource.ResourceLimitError); ok {
				err = resourceErr
			} else {
				panic(p)
			}
		}
		m.ManagedPeak = budget.Peak()
		m.ManagedResidual = budget.Used()
		m.DurationSeconds = time.Since(start).Seconds()
		if err == nil && m.ManagedResidual != 0 {
			err = fmt.Errorf("managed reservations leaked: %d", m.ManagedResidual)
		}
	}()
	cfg := engineConfig(run)
	store, err := storage.New(ctx, config.S3{}, config.S3{}, budget)
	if err != nil {
		return m, err
	}
	defer store.Close()
	ds := run.options.Dataset(budget)
	defer ds.Close()
	m.Schema = ds.Schema().String()
	program, err := checks.Compile(ctx, cfg, ds, store, budget)
	if err != nil {
		return m, err
	}
	defer program.Close()
	reader, err := ds.Open(ctx)
	if err != nil {
		return m, err
	}
	defer reader.Close()
	sink := checksink.New()
	sink.Delay = run.sinkDelay

	samples := make([]uint64, rssSampleRing)
	sampleCount := 0
	nextSample := time.Now()
	for {
		batch, readErr := reader.Next(ctx)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return m, readErr
		}
		logical := batchgen.LogicalBytes(batch.Record)
		evaluateErr := func() error {
			defer batch.Release()
			result, evalErr := program.Evaluate(batch)
			if evalErr != nil {
				return evalErr
			}
			defer result.Release()
			if err := sink.Write(batch, result); err != nil {
				return err
			}
			if run.afterBatch != nil {
				run.afterBatch(sink.Rows)
			}
			return nil
		}()
		if evaluateErr != nil {
			return m, evaluateErr
		}
		m.LogicalBytes += logical
		m.Rows = sink.Rows
		if m.LogicalBytes >= rssWarmupBytes && !time.Now().Before(nextSample) {
			snapshot, sampleErr := rss.Read()
			if sampleErr != nil {
				return m, sampleErr
			}
			m.PeakRSS = max(m.PeakRSS, snapshot.Peak)
			samples[sampleCount%len(samples)] = snapshot.Current
			sampleCount++
			nextSample = time.Now().Add(rssSampleInterval)
		}
	}
	if err := program.Finalize(); err != nil {
		return m, err
	}
	summary := program.Summary()
	m.Rows = sink.Rows
	m.Counts = sink.Counts
	m.Rules = sink.Rules
	m.Digest = sink.Digest()
	if m.Rows > 0 {
		m.AverageWidth = float64(m.LogicalBytes) / float64(m.Rows)
	}
	summaryOutcome := outcome{rows: summary.Total, pass: summary.Passed, warn: summary.Warned, errored: summary.Errored}
	if sinkOutcome := outcomeOf(m); summaryOutcome != sinkOutcome {
		return m, fmt.Errorf("summary outcome = %+v, want sink outcome %+v", summaryOutcome, sinkOutcome)
	}
	m.RSSSamples = sampleCount
	if sampleCount > 0 {
		if sampleCount < len(samples) {
			samples = samples[:sampleCount]
		}
		slices.Sort(samples)
		m.SustainedRSS = samples[len(samples)/2]
	}
	snapshot, sampleErr := rss.Read()
	if sampleErr != nil {
		return m, sampleErr
	}
	m.PeakRSS = max(m.PeakRSS, snapshot.Peak)
	return m, nil
}

// newMeasurement records the environment a run executes in.
func newMeasurement(run engineRun) measurement {
	m := measurement{
		Runtime:           runtime.Version(),
		GOOS:              runtime.GOOS,
		GOARCH:            runtime.GOARCH,
		RSSIntervalMillis: rssSampleInterval.Milliseconds(),
		Dependencies:      map[string]string{},
		Generator:         run.options,
		ManagedLimit:      run.limit,
		CPUs:              runtime.NumCPU(),
		GOMAXPROCS:        runtime.GOMAXPROCS(0),
		Workers:           engineWorkers,
		Hardware:          os.Getenv("VALET_BENCH_HARDWARE"),
		GOGC:              os.Getenv("GOGC"),
		GOMEMLIMIT:        os.Getenv("GOMEMLIMIT"),
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dependency := range info.Deps {
			m.Dependencies[dependency.Path] = dependency.Version
		}
	}
	return m
}

func engineConfig(run engineRun) *config.Config {
	schema := &config.Schema{
		Columns: map[string]config.Column{"id": {Type: "uint64"}, "value": {Type: "int64"}},
		Order:   []string{"id", "value"},
	}
	if !run.options.NumericOnly {
		schema.Columns["text"] = config.Column{Type: "string"}
		schema.Order = append(schema.Order, "text")
	}
	return &config.Config{
		Input: config.Input{
			Type:              "csv",
			Mode:              "raw_strings",
			Delimiter:         ",",
			OnTypeError:       "row_error",
			InferSchemaLength: 1000000,
			NullValues:        []string{"NULL", "null", "N/A", "n/a", ""},
			Schema:            schema,
		},
		Execution: config.Execution{
			MemoryBudget:   fmt.Sprint(run.limit) + "B",
			BatchTarget:    "8MiB",
			MaxRecordBytes: "64KiB",
			MaxDepth:       64,
			Workers:        engineWorkers,
		},
		Rules: run.rules,
	}
}

// numericRules flags every row whose generated value is negative.
func numericRules() []config.Rule {
	return []config.Rule{{
		Code:   ruleNonNegative,
		Column: "value",
		Level:  "error",
		Checks: []config.Check{{Type: "valid_range", Params: map[string]any{"min": 0}}},
	}}
}

// expectNumeric checks m against an oracle computed independently from the
// generator options: every FailureEvery-th row fails, plus the final row when
// LateFailure is set and it is not already a failure.
func expectNumeric(m measurement, o batchgen.Options) error {
	failures := uint64(0)
	if o.FailureEvery > 0 {
		failures = o.Rows / o.FailureEvery
	}
	lastRowAlreadyFails := o.FailureEvery != 0 && o.Rows%o.FailureEvery == 0
	if o.LateFailure && o.Rows > 0 && !lastRowAlreadyFails {
		failures++
	}
	var errs []error
	want := outcome{rows: o.Rows, pass: o.Rows - failures, errored: failures}
	if got := outcomeOf(m); got != want {
		errs = append(errs, fmt.Errorf("numeric oracle outcome = %+v, want %+v", got, want))
	}
	if got := m.Rules[ruleNonNegative]; got != failures {
		errs = append(errs, fmt.Errorf("numeric oracle %s issues = %d, want %d", ruleNonNegative, got, failures))
	}
	return errors.Join(errs...)
}

// Preserve the public ownership type in compile-time test support interfaces.
var _ interface {
	Write(*data.Batch, *data.Result) error
} = (*checksink.Sink)(nil)
