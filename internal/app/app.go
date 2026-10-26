// Package app assembles a single bounded validation run.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/source"
	"github.com/richardrh/valet/internal/storage"
)

// Run validates and publishes one run with the production dependencies.
// cfg must already be validated.
func Run(ctx context.Context, rt Runtime, cfg *config.Config) (*data.Summary, error) {
	return Production().Run(ctx, rt, cfg)
}

// Run validates and publishes one run. Operational failures never publish a
// terminal manifest; validation failures are returned as a completed Summary.
// Cleanup failures after publication are logged and do not discard the summary.
func (d Deps) Run(ctx context.Context, rt Runtime, cfg *config.Config) (summary *data.Summary, err error) {
	started := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	log := rt.logger()
	var s *session
	var program Program
	var writer Writer
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
	s = newSession(log, cfg)
	if err = d.open(ctx, s, cfg, false); err != nil {
		return nil, err
	}
	var issueSink func(data.Issue) error
	if cfg.Output.URI != "" {
		writer, err = d.OpenSink(ctx, cfg, s.store, s.budget, runID, s.ds.Schema(), log)
		if err != nil {
			return nil, fmt.Errorf("open output: %w", err)
		}
		issueSink = writer.WriteIssue
	}
	// checks logs compilation and preflight; app only logs steps it owns.
	program, err = d.Compile(ctx, cfg, s.ds, s.store, s.budget, log, issueSink)
	if err != nil {
		return nil, fmt.Errorf("compile checks: %w", err)
	}
	summary = program.Summary()
	summary.RunID = runID
	if inferred, ok := s.ds.(interface{ InferenceRowsScanned() uint64 }); ok {
		summary.RowsScanned, err = data.CheckedAdd(summary.RowsScanned, inferred.InferenceRowsScanned())
		if err != nil {
			return nil, fmt.Errorf("count inference row visits: %w", err)
		}
	}
	if err = prepareStatistics(ctx, cfg, s.ds, program); err != nil {
		return nil, err
	}
	summary.Passes += s.ds.Passes()
	if err = writeRunInputs(writer, s.ds.Objects(), summary, s.budget); err != nil {
		return nil, fmt.Errorf("write input manifest: %w", err)
	}
	if summary.Status != data.StatusSchemaRejected {
		phaseStarted := time.Now()
		log.Info("pass started", "phase", "validation")
		reader, err = s.ds.Open(ctx)
		if err != nil {
			return nil, fmt.Errorf("open validation pass: %w", err)
		}
		summary.Passes += 1 + s.ds.DecoderPassesPerOpen()
		rows, passErr := validatePass(ctx, cancel, cfg, reader, program, writer, s.budget, log)
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
		ds, ok := s.ds.(statisticsDataset)
		if !ok {
			return nil, errors.New("source cannot revalidate selected Parquet input identities")
		}
		if err = ds.VerifyStatisticsInputs(ctx); err != nil {
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
		if err = finalizeReportSummary(summary); err != nil {
			return nil, fmt.Errorf("finalize report: %w", err)
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
	store  Store
	ds     Dataset
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

func newSession(log *slog.Logger, cfg *config.Config) *session {
	return &session{log: log, budget: resource.New(cfg.BudgetBytes())}
}

// open fills the session with storage and the discovered dataset. Fields are
// set as each resource opens so close releases whatever was opened, including
// after a recovered resource-limit panic. source logs discovery itself.
func (d Deps) open(ctx context.Context, s *session, cfg *config.Config, plan bool) (err error) {
	if recorder := diagnostics.FromContext(ctx); recorder != nil {
		if err = recorder.BindBudget(s.budget); err != nil {
			return err
		}
	}
	discovery := diagnostics.Start(ctx, diagnostics.Discover)
	defer func() { discovery.End(err) }()
	if s.store, err = d.OpenStorage(ctx, cfg, s.budget, s.log); err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	if s.ds, err = d.OpenSource(ctx, cfg, s.store, s.budget, s.log, plan); err != nil {
		return fmt.Errorf("discover input: %w", err)
	}
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
