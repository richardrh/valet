package source

//lint:file-ignore ST1005 Error strings here begin with the proper nouns Parquet or Arrow.

import (
	"context"
	"errors"

	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
)

var errParquetEvidence = errors.New("Parquet group evidence event")

// advance keeps observed group coordinates independent of footer-derived
// absolute prefixes. Evidence is a control event, never an Arrow record.
func (p *parquetInput) advance(ctx context.Context) bool {
	if !p.reader.Next(ctx) {
		return false
	}
	if group := p.reader.Group(); group != p.currentGroup {
		p.currentGroup = group
		coordinate := uint64(group)
		p.groupCoordinate = &coordinate
		p.groupRow = 0
	}
	p.evidence = p.reader.Evidence()
	return true
}

func (r *datasetReader) takeParquetEvidence(ctx context.Context, p *parquetInput) (*data.Batch, error) {
	evidence := p.evidence
	if evidence == nil {
		return nil, errors.New("missing Parquet group evidence")
	}
	if evidence.Action == "skip" {
		row, err := data.CheckedAdd(r.row, evidence.DeclaredRows)
		if err != nil {
			return nil, err
		}
		global, err := data.CheckedAdd(r.global, evidence.DeclaredRows)
		if err != nil {
			return nil, err
		}
		r.row, r.global = row, global
		if evidence.DeclaredRows != 0 {
			r.metadataPrefix = true
		}
	}
	if err := r.d.budget.Reserve(batchFixedBytes, "Parquet evidence control event"); err != nil {
		return nil, err
	}
	retained := r.d.statistics.Retain()
	if retained == nil {
		r.d.budget.Release(batchFixedBytes)
		return nil, errors.New("statistics evidence plan has been released")
	}
	var gate chan struct{}
	if r.d.cfg.Input.Parquet.Statistics == "trusted" {
		gate = make(chan struct{})
		r.awaitEvidence = gate
	}
	p.evidence = nil
	return &data.Batch{Evidence: evidence, Cleanup: func() {
		retained.Release()
		r.d.budget.Release(batchFixedBytes)
		if gate != nil {
			close(gate)
		}
	}}, nil
}

func (r *datasetReader) positionBasis() string {
	if r.metadataPrefix {
		return "metadata_prefix"
	}
	return "values"
}

// Decisions precede even fallback baseline hashing, not merely row decoding.
func (d *Dataset) logStatisticsDecision(ctx context.Context, evidence *data.GroupEvidence) {
	if d.cfg.Input.Parquet.Statistics != "trusted" {
		return
	}
	claim := "unknown"
	if evidence.Action == "skip" {
		claim = "pass"
	}
	diagnostics.RecordDecision(ctx, diagnostics.Decision{SourceOrdinal: evidence.SourceOrdinal, RowGroup: evidence.RowGroup, CheckIndex: -1, Claim: claim, Action: evidence.Action, Reason: evidence.Reason})
	for _, check := range evidence.Checks {
		d.log.DebugContext(ctx, "Parquet check evidence", "event", "parquet.stats_decision", "source_ordinal", evidence.SourceOrdinal, "row_group", evidence.RowGroup, "check_index", check.CheckIndex, "code", check.Code, "claim", check.Claim, "action", evidence.Action, "reason", check.Reason, "evidence_fields", check.Fields)
	}
}
