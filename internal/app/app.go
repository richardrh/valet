// Package app assembles a single bounded validation run.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
	"unsafe"

	"github.com/richardrh/valet/internal/checks"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/sink"
	"github.com/richardrh/valet/internal/source"
	"github.com/richardrh/valet/internal/storage"
)

// Runtime carries process-scoped collaborators. main supplies the writers and
// version; cli installs Log from the logging flags. Log always writes to Stderr
// so Stdout stays reserved for summaries, plans and JSON.
type Runtime struct {
	Log     *slog.Logger
	Stdout  io.Writer
	Stderr  io.Writer
	Version string
	RunID   string
	// BeforeCommit receives the finalized summary before artifact commitment,
	// or before returning a completed stdout-only report.
	BeforeCommit func(*data.Summary) error
}

func (rt Runtime) logger() *slog.Logger {
	if rt.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return rt.Log
}

// Run validates and publishes one run. cfg must already be validated.
// Operational failures never publish a terminal manifest; validation failures
// are returned as a completed Summary. Cleanup failures after publication are
// logged and do not discard the summary.
func Run(ctx context.Context, rt Runtime, cfg *config.Config) (summary *data.Summary, err error) {
	started := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	log := rt.logger()
	var s *session
	var program *checks.Program
	var writer *sink.Writer
	var reader source.Reader
	completed := false
	defer func() {
		recovered := recover()
		cancel()
		var closeErr error
		if reader != nil {
			closeErr = errors.Join(closeErr, release(log, "input reader", reader.Close))
		}
		if writer != nil && !completed {
			log.Warn("aborting unpublished output", "phase", "output")
			closeErr = errors.Join(closeErr, release(log, "output abort", writer.Abort))
		}
		if program != nil {
			closeErr = errors.Join(closeErr, release(log, "checks", program.Close))
		}
		if s != nil {
			closeErr = errors.Join(closeErr, s.close())
		}
		recoverResource(recovered, &err)
		if err != nil {
			err = errors.Join(err, closeErr)
			summary = nil
		}
	}()
	if err = checkStart(ctx, cfg); err != nil {
		return nil, err
	}
	if cfg.Output.URI == "" && cfg.Output.Mode != "report" {
		return nil, errors.New("output.uri or --output is required for output.mode all or failures")
	}
	runID := rt.RunID
	if runID == "" {
		runID, err = NewRunID()
		if err != nil {
			return nil, err
		}
	}
	log = log.With("run_id", runID)
	if diagnostics.FromContext(ctx) == nil {
		log.Info("run started", "phase", "run", "version", rt.Version, "name", cfg.Name, "budget_bytes", cfg.BudgetBytes(), "uri", storage.LogURI(cfg.Output.URI))
	}
	s = &session{log: log, budget: resource.New(cfg.BudgetBytes())}
	if err = s.open(ctx, cfg, false); err != nil {
		return nil, err
	}
	// The pipeline treats a nil writer as report mode, so it must receive an
	// untyped nil rather than a nil *sink.Writer.
	var output batchWriter
	var issueSink func(data.Issue) error
	if cfg.Output.URI != "" {
		opened, openErr := sink.New(ctx, cfg, s.store, s.budget, runID, s.ds.Schema(), log)
		if openErr != nil {
			return nil, fmt.Errorf("open output: %w", openErr)
		}
		writer, output, issueSink = opened, opened, opened.WriteIssue
	}
	// checks logs compilation and preflight; app only logs steps it owns.
	compiled, err := checks.Compile(ctx, cfg, s.ds, s.store, s.budget, log, issueSink)
	if err != nil {
		return nil, fmt.Errorf("compile checks: %w", err)
	}
	program = compiled
	summary = program.Summary()
	summary.RunID = runID
	summary.RowsScanned, err = data.CheckedAdd(summary.RowsScanned, s.ds.InferenceRowsScanned())
	if err != nil {
		return nil, fmt.Errorf("count inference row visits: %w", err)
	}
	// Parquet group decisions are fixed before input identities are published
	// or a value pass opens. Off plans contain scans only and retain normal hashes.
	if cfg.Input.Type == "parquet" {
		if summary.Status == data.StatusSchemaRejected {
			scanned, skipped, total := uint64(0), uint64(0), uint64(0)
			summary.Coverage.RowGroupsScanned = &scanned
			summary.Coverage.RowGroupsStatisticsOnly = &skipped
			summary.Coverage.RowGroupsTotal = &total
			summary.Verification = "not_evaluated"
			summary.IngestionVerification = "not_evaluated"
		} else {
			groups, groupsErr := s.ds.ParquetGroups()
			if groupsErr != nil {
				return nil, fmt.Errorf("inspect Parquet groups: %w", groupsErr)
			}
			plan, planErr := program.PlanStatistics(groups)
			if planErr != nil {
				return nil, fmt.Errorf("plan Parquet statistics: %w", planErr)
			}
			applyErr := s.ds.ApplyStatisticsPlan(ctx, plan)
			plan.Release()
			if applyErr != nil {
				return nil, fmt.Errorf("apply Parquet statistics plan: %w", applyErr)
			}
		}
	}
	summary.Passes += s.ds.Passes()
	if writer != nil {
		if summary.Status != data.StatusSchemaRejected {
			err = writer.WriteInputs(s.ds.Objects())
		} else {
			objects := s.ds.Objects()
			var charge resource.Reservation
			charge, err = s.budget.Hold(int64(len(objects))*int64(unsafe.Sizeof(storage.Object{})), "schema rejected input descriptors")
			if err == nil {
				copied := make([]storage.Object, len(objects))
				copy(copied, objects)
				for i := range copied {
					copied[i].PayloadVerification = "not_verified"
					copied[i].IngestionVerification = "not_evaluated"
				}
				err = writer.WriteInputs(copied)
				charge.Release()
			}
		}
		if err != nil {
			return nil, fmt.Errorf("write input manifest: %w", err)
		}
	}
	if summary.Status != data.StatusSchemaRejected {
		phaseStarted := time.Now()
		log.Info("pass started", "phase", "validation")
		reader, err = s.ds.Open(ctx)
		if err != nil {
			return nil, fmt.Errorf("open validation pass: %w", err)
		}
		summary.Passes += 1 + s.ds.DecoderPassesPerOpen()
		rows, passErr := validatePass(ctx, cancel, cfg, reader, program, output, s.budget, log)
		if passErr != nil {
			return nil, passErr
		}
		closeErr := reader.Close()
		reader = nil
		if closeErr != nil {
			return nil, fmt.Errorf("close validation input: %w", closeErr)
		}
		log.Info("pass finished", "phase", "validation", "rows", rows, "duration_ms", since(phaseStarted))
	}
	if cfg.Input.Parquet.Statistics == "trusted" && cfg.Input.Type == "parquet" && summary.Status != data.StatusSchemaRejected {
		if err = s.ds.VerifyStatisticsInputs(ctx); err != nil {
			return nil, fmt.Errorf("verify selected input identity: %w", err)
		}
	}
	finalizeStarted := time.Now()
	if err = program.Finalize(); err != nil {
		return nil, fmt.Errorf("finalize checks: %w", err)
	}
	log.Info("checks finalized", "phase", "finalize", "duration_ms", since(finalizeStarted))
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if writer == nil {
		if summary.RowOutput == nil {
			summary.RowOutput = make(map[string]data.RowMaterialization, 4)
		}
		for _, kind := range []string{"pass", "warn", "error", "rejects"} {
			count := summary.RowOutput[kind]
			count.Policy = "omit_by_policy"
			summary.RowOutput[kind] = count
		}
		if summary.RowOutput["pass"].Omitted != summary.Passed || summary.RowOutput["warn"].Omitted != summary.Warned || summary.RowOutput["error"].Omitted != summary.Errored {
			return nil, errors.New("finalize report: summary counts do not match evaluated bucket rows")
		}
		total, addErr := data.CheckedAdd(summary.Passed, summary.Warned)
		if addErr == nil {
			total, addErr = data.CheckedAdd(total, summary.Errored)
		}
		if addErr != nil {
			return nil, fmt.Errorf("finalize report: %w", addErr)
		}
		if total != summary.Total || summary.Coverage.ValueRows != total {
			return nil, errors.New("finalize report: summary total does not match evaluated rows")
		}
		accounted, addErr := data.CheckedAdd(total, summary.Coverage.MetadataClaimedRows)
		if addErr != nil {
			return nil, fmt.Errorf("finalize report: %w", addErr)
		}
		if summary.Coverage.AccountedRows != accounted {
			return nil, errors.New("finalize report: summary accounted rows do not reconcile")
		}
		summary.OutputMode = "report"
		if summary.Status != data.StatusSchemaRejected && summary.Coverage.MetadataClaimedRows > 0 && total == 0 {
			summary.IngestionVerification = "not_evaluated"
		}
	}
	summary.DurationSeconds = time.Since(started).Seconds()
	summary.ManagedPeak = s.budget.Peak()
	if writer != nil {
		summary.ManifestURI, err = writer.Complete(summary, rt.BeforeCommit)
	} else if rt.BeforeCommit != nil {
		err = rt.BeforeCommit(summary)
	}
	if err != nil {
		return nil, fmt.Errorf("complete result: %w", err)
	}
	completed = true
	// sink logs the manifest URI; this is the run-level outcome.
	if diagnostics.FromContext(ctx) == nil {
		log.Info("run finished", "phase", "run", "status", summary.Status, "validation", summary.Validation, "rows", summary.Total, "duration_ms", since(started))
	}
	return summary, nil
}

// session holds the setup shared by Run and Plan.
type session struct {
	log    *slog.Logger
	budget *resource.Budget
	store  *storage.Store
	ds     *source.Dataset
}

func checkStart(ctx context.Context, cfg *config.Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cfg == nil {
		return errors.New("configuration is required")
	}
	return nil
}

// open fills the session with storage and the discovered dataset. Fields are
// set as each resource opens so close releases whatever was opened, including
// after a recovered resource-limit panic. source logs discovery itself.
func (s *session) open(ctx context.Context, cfg *config.Config, plan bool) (err error) {
	if recorder := diagnostics.FromContext(ctx); recorder != nil {
		if err = recorder.BindBudget(s.budget); err != nil {
			return err
		}
	}
	discovery := diagnostics.Start(ctx, diagnostics.Discover)
	defer func() { discovery.End(err) }()
	output := cfg.Source.S3
	if cfg.Output.URI != "" {
		output = cfg.Output.S3
	}
	store, err := storage.New(ctx, cfg.Source.S3, output, s.budget, s.log)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	s.store = store
	ds, err := source.New(ctx, cfg, s.store, s.budget, s.log, plan)
	if err != nil {
		return fmt.Errorf("discover input: %w", err)
	}
	s.ds = ds
	return nil
}

// close releases the session in reverse order of opening. Each failure is
// logged; the joined failures are returned.
func (s *session) close() error {
	var err error
	if s.ds != nil {
		err = errors.Join(err, release(s.log, "dataset", s.ds.Close))
	}
	if s.store != nil {
		err = errors.Join(err, release(s.log, "storage", s.store.Close))
	}
	s.log.Info("budget peak", "phase", "close", "bytes", s.budget.Peak(), "limit_bytes", s.budget.Limit())
	return err
}

func release(log *slog.Logger, name string, closer func() error) error {
	if err := closer(); err != nil {
		log.Error("close failed", "phase", name, "error", err)
		return fmt.Errorf("close %s: %w", name, err)
	}
	return nil
}

func since(started time.Time) int64 { return time.Since(started).Milliseconds() }

// recoverResource deliberately does not turn programming defects into ordinary
// operational failures. Only the budget allocator's documented panic is caught.
func recoverResource(value any, err *error) {
	if value == nil {
		return
	}
	if limit, ok := value.(*resource.ResourceLimitError); ok {
		*err = errors.Join(*err, limit)
		return
	}
	panic(value)
}

// NewRunID returns an unpredictable, path-safe correlation and publication ID.
func NewRunID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate run ID: %w", err)
	}
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(random[:]), nil
}
