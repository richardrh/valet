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
	add := func(kind string, n uint64) error {
		if n == 0 {
			return nil
		}
		count := summary.RowOutput[kind]
		var err error
		count.Omitted, err = data.CheckedAdd(count.Omitted, n)
		if err != nil {
			return fmt.Errorf("report %s count: %w", kind, err)
		}
		count.Policy = "omit_by_policy"
		summary.RowOutput[kind] = count
		return nil
	}
	// Per-batch tallies keep map updates and map iteration out of the row loop.
	states := make([][]data.State, 0, len(batch.States))
	for _, column := range batch.States {
		states = append(states, column)
	}
	var buckets [data.BucketError + 1]uint64
	var rejects uint64
	ingest := 0
	for row, bucket := range result.Buckets {
		if !bucket.Valid() {
			return fmt.Errorf("unknown row bucket %v", bucket)
		}
		buckets[bucket]++
		reject := false
		for _, column := range states {
			if row < len(column) && column[row] == data.Invalid {
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
			rejects++
		}
	}
	for bucket, n := range buckets {
		if err := add(data.Bucket(bucket).String(), n); err != nil {
			return err
		}
	}
	return add("rejects", rejects)
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
