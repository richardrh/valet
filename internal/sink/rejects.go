package sink

import (
	"encoding/json"
	"fmt"

	"github.com/richardrh/valet/internal/data"
)

const (
	rejectStateCharge      = 64  // one rejectState entry
	rejectDiagnosticCharge = 256 // one copied diagnostic entry
)

type rejectState struct {
	Column string     `json:"column"`
	State  data.State `json:"state"`
}
type rejectRecord struct {
	ReportVersion int    `json:"report_version"`
	RunID         string `json:"run_id"`
	data.Location
	Raw         json.RawMessage `json:"raw"`
	States      []rejectState   `json:"states"`
	Diagnostics []data.Issue    `json:"diagnostics"`
}

func (w *Writer) writeRejects(batch *data.Batch) error {
	// Native batches with no ingestion overrides cannot contain rejects. Rule
	// errors and warnings still produce their normal issues and bucket rows.
	if len(batch.Ingest) == 0 && len(batch.States) == 0 {
		return nil
	}
	// A bounded index of the current batch avoids scanning every ingestion issue
	// for every row. Linked positions preserve issue order without copying issues.
	charge := int64(len(batch.Ingest)) * rejectIndexEntryCharge
	if err := w.budget.Reserve(charge, "reject diagnostic index"); err != nil {
		return err
	}
	defer w.budget.Release(charge)
	heads := make(map[uint64]int, len(batch.Ingest))
	next := make([]int, len(batch.Ingest))
	for offset := range len(batch.Ingest) {
		i := len(batch.Ingest) - 1 - offset
		row := batch.Ingest[i].GlobalRow
		next[i] = heads[row]
		heads[row] = i + 1
	}
	for row, location := range batch.Locations {
		invalid := false
		for _, states := range batch.States {
			if row < len(states) && states[row] == data.Invalid {
				invalid = true
				break
			}
		}
		count := 0
		for pos := heads[location.GlobalRow]; pos != 0; pos = next[pos-1] {
			count++
			if batch.Ingest[pos-1].Code == "INGEST_TYPE" {
				invalid = true
			}
		}
		if !invalid && count == 0 {
			continue
		}
		if err := addCount(&w.rejects, 1, "raw rejects"); err != nil {
			return err
		}
		if !w.emit(kindRejects) {
			continue
		}
		if row >= len(batch.Raw) || len(batch.Raw[row]) == 0 {
			return fmt.Errorf("missing raw reject record at global row %d", location.GlobalRow)
		}
		rowCharge := int64(batch.Record.NumCols())*rejectStateCharge + int64(count)*rejectDiagnosticCharge
		if err := w.budget.Reserve(rowCharge, "reject field states"); err != nil {
			return err
		}
		record := rejectRecord{ReportVersion: data.ReportVersion, RunID: w.runID, Location: location, Raw: batch.Raw[row], States: make([]rejectState, 0, int(batch.Record.NumCols())), Diagnostics: make([]data.Issue, 0, count)}
		// Raw JSON is compacted and HTML-escaped by the encoder, so bound it like text.
		estimate := textBytes(len(location.Source)+len(w.runID)+len(record.Raw)) + recordOverhead(8)
		for column, field := range batch.Record.Schema().Fields() {
			state := data.Valid
			if states, ok := batch.States[field.Name]; ok {
				if row >= len(states) {
					w.budget.Release(rowCharge)
					return fmt.Errorf("state count mismatch for reject column %s", field.Name)
				}
				state = states[row]
			} else if batch.Record.Column(column).IsNull(row) {
				state = data.Null
			}
			if _, err := state.MarshalText(); err != nil {
				w.budget.Release(rowCharge)
				return fmt.Errorf("reject column %s: %w", field.Name, err)
			}
			record.States = append(record.States, rejectState{Column: field.Name, State: state})
			estimate += textBytes(len(field.Name)) + recordOverhead(2)
		}
		for pos := heads[location.GlobalRow]; pos != 0; pos = next[pos-1] {
			issue := &batch.Ingest[pos-1]
			record.Diagnostics = append(record.Diagnostics, *issue)
			estimate += issueBytes(issue)
		}
		err := w.writeLine(kindRejects, record, estimate)
		w.budget.Release(rowCharge)
		if err != nil {
			return err
		}
		if err := addCount(&w.rejectsEmitted, 1, "emitted raw rejects"); err != nil {
			return err
		}
	}
	return nil
}
