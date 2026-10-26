package cli

import (
	"context"

	"github.com/richardrh/valet/internal/app"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
)

// commandDiagnostics outlives application cleanup and stdout presentation. That
// ordering makes an exit-3 event truthful about the already committed result.
type commandDiagnostics struct {
	recorder *diagnostics.Recorder
	scope    string
	config   *config.Config
	summary  *data.Summary
}

func beginDiagnostics(ctx context.Context, rt app.Runtime, opts *options) (context.Context, app.Runtime, *commandDiagnostics, error) {
	id, err := app.NewRunID()
	if err != nil {
		return ctx, rt, nil, err
	}
	recorder := diagnostics.New(opts.command)
	recorder.SetRunID(id)
	opts.recorder = recorder
	rt.RunID = id
	rt.Log = newLogger(rt.Stderr, opts)
	ctx = diagnostics.WithRecorder(ctx, recorder)
	scope := data.ScopeData
	if opts.command == "schema" {
		scope = data.ScopeSchema
	}
	if opts.command == "plan" {
		scope = data.ScopePlan
	}
	state := &commandDiagnostics{recorder: recorder, scope: scope}
	rt.Log.InfoContext(ctx, "run started", "event", "run.started", "version", rt.Version)
	return ctx, rt, state, nil
}

func (d *commandDiagnostics) finish(ctx context.Context, code int) {
	if d == nil {
		return
	}
	defer d.recorder.Close()
	terminal := diagnostics.Terminal{ExitCode: code, Scope: d.scope, CountersComplete: code == ExitOK || code == ExitValidation, Outcome: "completed"}
	if d.config != nil {
		terminal.OutputMode = d.config.Output.Mode
		terminal.TrustPolicy = "none"
		if d.config.Input.Parquet.Statistics == "trusted" {
			terminal.TrustPolicy = "producer_statistics"
		}
	}
	if s := d.summary; s != nil {
		terminal.ManifestCommitted = s.ManifestURI != ""
		terminal.OutputMode = s.OutputMode
		terminal.TrustPolicy = s.TrustPolicy
		terminal.ValueRows = s.Coverage.ValueRows
		terminal.MetadataClaimedRows = s.Coverage.MetadataClaimedRows
		terminal.AccountedRows = s.Coverage.AccountedRows
		terminal.RowsScanned = s.RowsScanned
		terminal.RowGroupsScanned = counterValue(s.Coverage.RowGroupsScanned)
		terminal.RowGroupsStatisticsOnly = counterValue(s.Coverage.RowGroupsStatisticsOnly)
		terminal.RowGroupsTotal = counterValue(s.Coverage.RowGroupsTotal)
	}
	switch {
	case code == ExitPresentation && terminal.ManifestCommitted:
		terminal.Outcome = "committed_presentation_failed"
		terminal.CountersComplete = true
	case code == ExitValidation:
		terminal.Outcome = "validation_failed"
	case code >= 128:
		terminal.Outcome = "canceled"
	case code != ExitOK:
		terminal.Outcome = "failed"
	}
	d.recorder.Finish(ctx, terminal)
}

func counterValue(n *uint64) uint64 {
	if n == nil {
		return 0
	}
	return *n
}
