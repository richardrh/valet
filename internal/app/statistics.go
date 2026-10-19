package app

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

type statisticsDataset interface {
	ParquetGroups() ([]data.ParquetGroup, error)
	ApplyStatisticsPlan(*data.StatisticsPlan) error
	VerifyStatisticsInputs(context.Context) error
}

type statisticsProgram interface {
	PlanStatistics([]data.ParquetGroup) (*data.StatisticsPlan, error)
	ApplyStatistics(*data.GroupEvidence) error
}

type evidenceWriter interface {
	WriteEvidence(*data.GroupEvidence) error
}

// prepareStatistics fixes group decisions before input identities are published
// or a value pass opens. Off plans contain scans only and retain normal hashes.
func prepareStatistics(cfg *config.Config, dataset Dataset, program Program) error {
	if cfg.Input.Type != "parquet" {
		return nil
	}
	summary := program.Summary()
	if summary.Status == data.StatusSchemaRejected {
		scanned, skipped, total := uint64(0), uint64(0), uint64(0)
		summary.Coverage.RowGroupsScanned = &scanned
		summary.Coverage.RowGroupsStatisticsOnly = &skipped
		summary.Coverage.RowGroupsTotal = &total
		summary.Verification = "not_evaluated"
		summary.IngestionVerification = "not_evaluated"
		return nil
	}
	ds, ok := dataset.(statisticsDataset)
	if !ok {
		return fmt.Errorf("Parquet source does not support group planning")
	}
	checks, ok := program.(statisticsProgram)
	if !ok {
		return fmt.Errorf("Parquet checks do not support group planning")
	}
	groups, err := ds.ParquetGroups()
	if err != nil {
		return fmt.Errorf("inspect Parquet groups: %w", err)
	}
	plan, err := checks.PlanStatistics(groups)
	if err != nil {
		return fmt.Errorf("plan Parquet statistics: %w", err)
	}
	defer plan.Release()
	if err = ds.ApplyStatisticsPlan(plan); err != nil {
		return fmt.Errorf("apply Parquet statistics plan: %w", err)
	}
	return nil
}

// applyGroupEvidence mutates checks only on the evaluator owner. The writer
// consumes the same event later in order and never retains its borrowed data.
func applyGroupEvidence(program Program, batch *data.Batch) error {
	if batch.Evidence == nil {
		return nil
	}
	checks, ok := program.(statisticsProgram)
	if !ok {
		return fmt.Errorf("checks cannot account for Parquet group evidence")
	}
	if err := checks.ApplyStatistics(batch.Evidence); err != nil {
		return fmt.Errorf("account Parquet group: %w", err)
	}
	return nil
}

func writeEvaluatedBatch(ctx context.Context, cfg *config.Config, writer Writer, batch *data.Batch, result *data.Result) error {
	if batch.Evidence != nil && cfg.Input.Parquet.Statistics == "trusted" {
		evidence, ok := writer.(evidenceWriter)
		if !ok {
			return fmt.Errorf("output cannot publish Parquet group evidence")
		}
		if err := evidence.WriteEvidence(batch.Evidence); err != nil {
			return fmt.Errorf("write Parquet evidence: %w", err)
		}
	}
	if batch.Record == nil {
		if batch.Evidence == nil {
			return fmt.Errorf("input batch has neither values nor evidence")
		}
		return nil
	}
	return writer.WriteBatch(batch, result)
}

func writeRunInputs(writer Writer, objects []storage.Object, summary *data.Summary, budget *resource.Budget) error {
	if summary.Status != data.StatusSchemaRejected {
		return writer.WriteInputs(objects)
	}
	charge, err := budget.Hold(int64(len(objects))*int64(unsafe.Sizeof(storage.Object{})), "schema rejected input descriptors")
	if err != nil {
		return err
	}
	defer charge.Release()
	copied := make([]storage.Object, len(objects))
	copy(copied, objects)
	for i := range copied {
		copied[i].PayloadVerification = "not_verified"
		copied[i].IngestionVerification = "not_evaluated"
	}
	return writer.WriteInputs(copied)
}
