package sink

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/richardrh/valet/internal/data"
)

func addCount(dst *uint64, amount uint64, name string) error {
	value, err := data.CheckedAdd(*dst, amount)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*dst = value
	return nil
}

func (w *Writer) mode() string {
	if w.cfg.Output.Mode == "" {
		return "all"
	}
	return w.cfg.Output.Mode
}
func (w *Writer) emit(kind partKind) bool {
	switch w.mode() {
	case "report":
		return !kind.isRowKind() && kind != kindRejects
	case "failures":
		return kind != kindPass
	default:
		return true
	}
}

// WriteEvidence publishes one final group decision synchronously. Its caller
// retains ownership of the event and may release all borrowed data on return.
func (w *Writer) WriteEvidence(evidence *data.GroupEvidence) (err error) {
	defer w.fail(&err)
	if err = w.ready(); err != nil {
		return err
	}
	if evidence == nil {
		return errors.New("nil group evidence")
	}
	if w.cfg.Input.Parquet.Statistics != "trusted" {
		return errors.New("group evidence requires trusted statistics policy")
	}
	if evidence.SourceOrdinal == 0 {
		return errors.New("evidence source ordinal must be positive")
	}
	if w.evidenceWritten && (evidence.SourceOrdinal < w.evidenceSource || (evidence.SourceOrdinal == w.evidenceSource && evidence.RowGroup <= w.evidenceGroup)) {
		return errors.New("group evidence is duplicated or out of source/group order")
	}
	switch evidence.Action {
	case "skip":
		if evidence.DeclaredRows == 0 || w.mode() == "all" {
			return errors.New("invalid statistics-only group")
		}
		for _, check := range evidence.Checks {
			if check.Claim != "pass" {
				return errors.New("statistics-only group contains an unproven check")
			}
		}
		if err = addCount(&w.metadataRows, evidence.DeclaredRows, "metadata claimed rows"); err != nil {
			return err
		}
		if err = addCount(&w.groupsSkipped, 1, "statistics-only groups"); err != nil {
			return err
		}
	case "scan":
		if err = addCount(&w.groupsScanned, 1, "scanned groups"); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown evidence action %q", evidence.Action)
	}
	record := struct {
		ReportVersion int    `json:"report_version"`
		RunID         string `json:"run_id"`
		*data.GroupEvidence
	}{data.ReportVersion, w.runID, evidence}
	if err = w.writeLine(kindEvidence, record, jsonCapacity(reflect.ValueOf(record))); err != nil {
		return err
	}
	w.evidenceWritten, w.evidenceSource, w.evidenceGroup = true, evidence.SourceOrdinal, evidence.RowGroup
	return nil
}

func (w *Writer) reconcileSummary(summary *data.Summary) error {
	total := uint64(0)
	for kind := range partKind(numRowKinds) {
		emittedAndOmitted := w.counts[kind]
		if err := addCount(&emittedAndOmitted, w.omitted[kind], "materialized rows"); err != nil {
			return err
		}
		if emittedAndOmitted != w.evaluated[kind] {
			return fmt.Errorf("%s emitted/omitted rows do not reconcile", kind)
		}
		if err := addCount(&total, w.evaluated[kind], "evaluated total"); err != nil {
			return err
		}
	}
	if summary.Passed != w.evaluated[kindPass] || summary.Warned != w.evaluated[kindWarn] || summary.Errored != w.evaluated[kindError] || summary.Total != total {
		return errors.New("summary counts do not match evaluated bucket rows")
	}
	if summary.Coverage.MetadataClaimedRows != w.metadataRows {
		return errors.New("summary metadata claimed rows do not match evidence")
	}
	summary.Scope = data.ScopeData
	summary.OutputMode = w.mode()
	summary.TrustPolicy = "none"
	if w.cfg.Input.Parquet.Statistics == "trusted" {
		summary.TrustPolicy = "producer_statistics"
	}
	summary.Verification = "values"
	if summary.Coverage.MetadataClaimedRows > 0 {
		summary.Verification = "metadata_assisted"
	}
	if summary.Status == data.StatusSchemaRejected {
		summary.Verification = "not_evaluated"
		summary.IngestionVerification = "not_evaluated"
	}
	summary.Coverage.ValueRows = total
	summary.Coverage.AccountedRows = total
	if err := addCount(&summary.Coverage.AccountedRows, summary.Coverage.MetadataClaimedRows, "accounted rows"); err != nil {
		return err
	}
	if summary.Status != data.StatusSchemaRejected {
		summary.IngestionVerification = "full"
		if summary.Coverage.MetadataClaimedRows > 0 {
			summary.IngestionVerification = "partial"
			if total == 0 {
				summary.IngestionVerification = "not_evaluated"
			}
		}
	}
	if summary.ProvenancePositionBasis == "" {
		summary.ProvenancePositionBasis = "values"
	}
	if w.evidenceWritten {
		groups := w.groupsScanned
		if err := addCount(&groups, w.groupsSkipped, "row groups"); err != nil {
			return err
		}
		if summary.Coverage.RowGroupsScanned != nil && *summary.Coverage.RowGroupsScanned != w.groupsScanned {
			return errors.New("summary scanned groups do not match evidence")
		}
		if summary.Coverage.RowGroupsStatisticsOnly != nil && *summary.Coverage.RowGroupsStatisticsOnly != w.groupsSkipped {
			return errors.New("summary statistics-only groups do not match evidence")
		}
		if summary.Coverage.RowGroupsTotal != nil && *summary.Coverage.RowGroupsTotal != groups {
			return errors.New("summary total groups do not match evidence")
		}
		summary.Coverage.RowGroupsScanned = &w.groupsScanned
		summary.Coverage.RowGroupsStatisticsOnly = &w.groupsSkipped
		summary.Coverage.RowGroupsTotal = &groups
	}
	summary.RowOutput = make(map[string]data.RowMaterialization, 4)
	for kind := range partKind(numRowKinds) {
		policy := "emit"
		if !w.emit(kind) {
			policy = "omit_by_policy"
		}
		summary.RowOutput[kind.String()] = data.RowMaterialization{Policy: policy, Emitted: w.counts[kind], Omitted: w.omitted[kind]}
	}
	policy := "emit"
	if !w.emit(kindRejects) {
		policy = "omit_by_policy"
	}
	if w.rejectsEmitted > w.rejects {
		return errors.New("emitted rejects exceed evaluated rejects")
	}
	summary.RowOutput[kindRejects.String()] = data.RowMaterialization{Policy: policy, Emitted: w.rejectsEmitted, Omitted: w.rejects - w.rejectsEmitted}
	return nil
}

func (w *Writer) manifest(summary *data.Summary, artifacts, summaryRef Reference) manifestDocument {
	return manifestDocument{
		ReportVersion: data.ReportVersion, RunID: w.runID, Status: summary.Status, Validation: summary.Validation,
		Inputs: w.inputsRef, Rules: w.rulesRef, Artifacts: artifacts, Summary: summaryRef,
		Counts: map[string]uint64{"total": summary.Total, "pass": summary.Passed, "warn": summary.Warned, "error": summary.Errored},
		Scope:  summary.Scope, OutputMode: summary.OutputMode, Verification: summary.Verification, TrustPolicy: summary.TrustPolicy,
		IngestionVerification: summary.IngestionVerification, ProvenancePositionBasis: summary.ProvenancePositionBasis,
		Coverage: summary.Coverage, RuleCoverage: summary.RuleCoverage, RowOutput: summary.RowOutput,
	}
}
