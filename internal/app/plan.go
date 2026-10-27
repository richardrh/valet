package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/richardrh/valet/internal/checks"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

type plannedField struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	Timezone  string `json:"timezone,omitempty"`
	Precision int32  `json:"precision,omitempty"`
	Scale     int32  `json:"scale,omitempty"`
}

// PlanDescription is the versioned JSON document printed by `valet plan`.
type PlanDescription struct {
	ReportVersion          int                 `json:"report_version"`
	RunID                  string              `json:"run_id"`
	Scope                  string              `json:"scope"`
	Validation             string              `json:"validation"`
	OutputMode             string              `json:"output_mode"`
	StatisticsPolicy       string              `json:"parquet_stats_policy"`
	StatisticsEvaluated    bool                `json:"statistics_shortcuts_evaluated"`
	Name                   string              `json:"name"`
	PipelineVersion        string              `json:"pipeline_version"`
	SourceCount            int                 `json:"source_count"`
	Format                 string              `json:"format"`
	InputMode              string              `json:"input_mode"`
	Schema                 []plannedField      `json:"schema"`
	SchemaIssues           []data.Issue        `json:"schema_issues"`
	SourcePasses           int                 `json:"source_passes"`
	RequiredPasses         int                 `json:"required_passes"`
	PassCountScope         string              `json:"pass_count_scope"`
	ReferenceLoadsIncluded bool                `json:"reference_loads_included"`
	DecoderPassesPerOpen   int                 `json:"decoder_passes_per_open"`
	Checks                 *checks.Description `json:"checks"`
	Capabilities           sourceCapabilities  `json:"source_capabilities"`
	MemoryBudget           int64               `json:"memory_budget_bytes"`
	BatchTarget            int64               `json:"batch_target_bytes"`
	Workers                int                 `json:"workers"`
	ManagedPeak            int64               `json:"managed_peak_bytes"`
	StateFitGuaranteed     bool                `json:"state_fit_guaranteed"`
}

type sourceCapabilities struct {
	Replayable         bool `json:"replayable"`
	DeterministicOrder bool `json:"deterministic_order"`
	IdentityPinned     bool `json:"identity_pinned"`
	Streaming          bool `json:"streaming"`
}

// Plan inspects source metadata/inference samples and binds rule definitions.
// It does not open a validation pass, load reference indexes or accumulate
// global state. StateFitGuaranteed is consequently always false. cfg must
// already be validated.
func Plan(ctx context.Context, rt Runtime, cfg *config.Config) (description *PlanDescription, err error) {
	ctx, cancel := context.WithCancel(ctx)
	log := rt.logger()
	var s *session
	defer func() {
		recovered := recover()
		cancel()
		var closeErr error
		if s != nil {
			closeErr = s.close()
		}
		recoverResource(recovered, &err)
		if err != nil {
			err = errors.Join(err, closeErr)
			description = nil
		}
	}()
	if err = checkStart(ctx, cfg); err != nil {
		return nil, err
	}
	s = &session{log: log, budget: resource.New(cfg.BudgetBytes())}
	if err = s.open(ctx, cfg, true); err != nil {
		return nil, err
	}
	started := time.Now()
	rules, err := checks.Describe(ctx, cfg, s.ds, s.store, s.budget)
	if err != nil {
		return nil, fmt.Errorf("describe checks: %w", err)
	}
	log.Info("checks described", "phase", "plan", "required_passes", rules.RequiredPasses, "duration_ms", since(started))
	schema := s.ds.Schema()
	fields := make([]plannedField, 0, schema.NumFields())
	for _, field := range schema.Fields() {
		typeName, ok := config.CanonicalType(field.Type.Name())
		if !ok {
			switch field.Type.ID() {
			case arrow.DATE64:
				typeName = "date"
			case arrow.LARGE_STRING:
				typeName = "string"
			case arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
				typeName = "binary"
			default:
				typeName = field.Type.Name()
			}
		}
		planned := plannedField{Name: field.Name, Type: typeName, Nullable: field.Nullable}
		switch typ := field.Type.(type) {
		case *arrow.TimestampType:
			planned.Timezone = typ.TimeZone
		case *arrow.Decimal128Type:
			planned.Precision, planned.Scale = typ.Precision, typ.Scale
		}
		fields = append(fields, planned)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	issues := s.ds.SchemaIssues()
	if issues == nil {
		issues = []data.Issue{}
	}
	return &PlanDescription{
		ReportVersion: data.ReportVersion, RunID: rt.RunID, Scope: data.ScopePlan, Validation: "not_evaluated",
		OutputMode: cfg.Output.Mode, StatisticsPolicy: cfg.Input.Parquet.Statistics,
		Name: cfg.Name, PipelineVersion: cfg.Version,
		SourceCount: len(s.ds.Objects()), Format: cfg.Input.Type, InputMode: cfg.Input.Mode,
		Schema: fields, SchemaIssues: issues, SourcePasses: s.ds.Passes(),
		RequiredPasses: s.ds.Passes() + rules.RequiredPasses, Checks: rules,
		PassCountScope: "main_dataset", ReferenceLoadsIncluded: false,
		DecoderPassesPerOpen: s.ds.DecoderPassesPerOpen(),
		Capabilities:         sourceCapabilities{Replayable: true, DeterministicOrder: true, IdentityPinned: true, Streaming: true},
		MemoryBudget:         s.budget.Limit(), BatchTarget: cfg.BatchBytes(), Workers: cfg.Execution.Workers,
		ManagedPeak: s.budget.Peak(), StateFitGuaranteed: false,
	}, nil
}
