package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/source"
	"github.com/richardrh/valet/internal/storage"
)

// Schema inspects only pinned Parquet metadata. It deliberately bypasses normal
// execution setup: no sink, rule compiler, reference index or prepass is opened.
// cfg must already be validated. The caller must Release the returned report.
func Schema(ctx context.Context, rt Runtime, cfg *config.Config) (report *source.SchemaReport, err error) {
	if err = checkStart(ctx, cfg); err != nil {
		return nil, err
	}
	if cfg.Input.Type != "parquet" {
		return nil, diagnostics.PublicError(fmt.Errorf("valet schema reads Parquet metadata only; for %s input, run valet plan --format pipeline to infer a schema", cfg.Input.Type))
	}
	budget := resource.New(cfg.BudgetBytes())
	if recorder := diagnostics.FromContext(ctx); recorder != nil {
		if err = recorder.BindBudget(budget); err != nil {
			return nil, err
		}
	}
	var store *storage.Store
	defer func() {
		recovered := recover()
		if store != nil {
			err = errors.Join(err, store.Close())
		}
		recoverResource(recovered, &err)
		if err != nil {
			report.Release()
			report = nil
		}
	}()
	// Output settings are irrelevant: metadata inspection must never initialize
	// output clients or publish a normal row-bucket/manifest result.
	store, err = storage.New(ctx, cfg.Source.S3, config.S3{}, budget, rt.logger())
	if err != nil {
		return nil, fmt.Errorf("open schema storage: %w", err)
	}
	inspection := diagnostics.Start(ctx, diagnostics.Metadata)
	report, err = source.InspectSchema(ctx, cfg, store, budget)
	inspection.End(err)
	if report != nil {
		report.RunID = rt.RunID
	}
	return report, err
}
