package app

import (
	"errors"
	"fmt"

	"github.com/richardrh/valet/internal/data"
)

// accountReportBatch keeps only aggregate materialization counts on the evaluator
// owner. It neither retains row diagnostics nor borrows any batch storage.
func accountReportBatch(summary *data.Summary, batch *data.Batch, result *data.Result) error {
	if batch.Record == nil {
		return nil
	}
	n := int(batch.Record.NumRows())
	if result == nil || len(result.Buckets) != n || len(batch.Locations) != n {
		return errors.New("report row/provenance count mismatch")
	}
	if summary.RowOutput == nil {
		summary.RowOutput = make(map[string]data.RowMaterialization, 4)
	}
	add := func(kind string) error {
		count := summary.RowOutput[kind]
		var err error
		count.Omitted, err = data.CheckedAdd(count.Omitted, 1)
		if err != nil {
			return fmt.Errorf("report %s count: %w", kind, err)
		}
		count.Policy = "omit_by_policy"
		summary.RowOutput[kind] = count
		return nil
	}
	ingest := 0
	for row, bucket := range result.Buckets {
		if !bucket.Valid() {
			return fmt.Errorf("unknown row bucket %v", bucket)
		}
		if err := add(bucket.String()); err != nil {
			return err
		}
		reject := false
		for _, states := range batch.States {
			if row < len(states) && states[row] == data.Invalid {
				reject = true
				break
			}
		}
		globalRow := batch.Locations[row].GlobalRow
		for ingest < len(batch.Ingest) && batch.Ingest[ingest].GlobalRow <= globalRow {
			if batch.Ingest[ingest].GlobalRow == globalRow {
				reject = true
			}
			ingest++
		}
		if reject {
			if err := add("rejects"); err != nil {
				return err
			}
		}
	}
	return nil
}

func finalizeReportSummary(summary *data.Summary) error {
	if summary.RowOutput == nil {
		summary.RowOutput = make(map[string]data.RowMaterialization, 4)
	}
	for _, kind := range []string{"pass", "warn", "error", "rejects"} {
		count := summary.RowOutput[kind]
		count.Policy = "omit_by_policy"
		summary.RowOutput[kind] = count
	}
	if summary.RowOutput["pass"].Omitted != summary.Passed || summary.RowOutput["warn"].Omitted != summary.Warned || summary.RowOutput["error"].Omitted != summary.Errored {
		return errors.New("summary counts do not match evaluated bucket rows")
	}
	total, err := data.CheckedAdd(summary.Passed, summary.Warned)
	if err == nil {
		total, err = data.CheckedAdd(total, summary.Errored)
	}
	if err != nil {
		return err
	}
	if total != summary.Total || summary.Coverage.ValueRows != total {
		return errors.New("summary total does not match evaluated rows")
	}
	accounted, err := data.CheckedAdd(total, summary.Coverage.MetadataClaimedRows)
	if err != nil {
		return err
	}
	if summary.Coverage.AccountedRows != accounted {
		return errors.New("summary accounted rows do not reconcile")
	}
	summary.OutputMode = "report"
	if summary.Status != data.StatusSchemaRejected && summary.Coverage.MetadataClaimedRows > 0 && total == 0 {
		summary.IngestionVerification = "not_evaluated"
	}
	return nil
}
