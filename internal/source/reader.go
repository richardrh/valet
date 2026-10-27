package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
)

// Batch accounting. Arrow builder buffers are charged by the budget's
// allocator; these constants cover the Go-side structures a batch retains.
const (
	// maxBatchRows caps rows per batch independently of batch_target.
	maxBatchRows = 65536
	// appendGrowth is the worst-case capacity overshoot of append-grown slices.
	appendGrowth = 2
	// minRowCapacity is the first row capacity of a batch with no history.
	minRowCapacity = 256
	locationBytes  = int64(unsafe.Sizeof(data.Location{}))
	issueBytes     = int64(unsafe.Sizeof(data.Issue{}))
	rawSlotBytes   = int64(unsafe.Sizeof(json.RawMessage(nil)))
	stateSliceHdr  = int64(unsafe.Sizeof([]data.State(nil)))
	// mapEntryBytes approximates one map slot plus its share of table overhead.
	mapEntryBytes = 64
	// builderStructBytes is the Go struct behind one Arrow field builder.
	builderStructBytes = 256
	// columnBatchBytes: the column's state slice (held both in the batch's
	// [][]data.State and in the published map), its map entry, its builder
	// struct, its mapping slot and its reordered cell.
	columnBatchBytes = 2*stateSliceHdr + mapEntryBytes + builderStructBytes + 8 + int64(unsafe.Sizeof(cell{}))
	// batchFixedBytes covers the batch, record builder and map headers.
	batchFixedBytes = 1024
	// decodedCellOverhead is added to each cell's text when measuring a batch
	// against batch_target.
	decodedCellOverhead = 16
)

func (d *Dataset) Open(ctx context.Context) (Reader, error) {
	if d.closed {
		return nil, errors.New("dataset is closed")
	}
	if d.generated != nil {
		return d.generated(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.establishPendingBaselines(ctx); err != nil {
		return nil, err
	}
	return &datasetReader{d: d, values: make([]cell, len(d.columns))}, nil
}

// OpenPreflight preserves conversion states for whole-dataset castability gates.
func (d *Dataset) OpenPreflight(ctx context.Context) (Reader, error) {
	reader, err := d.Open(ctx)
	if err == nil {
		if r, ok := reader.(*datasetReader); ok {
			r.preflight = true
		}
	}
	return reader, err
}

type datasetReader struct {
	d                 *Dataset
	input             lexicalInput
	object            int
	row, global       uint64
	closed, preflight bool
	metadataPrefix    bool
	awaitEvidence     <-chan struct{}

	// Source-field to column binding, computed once per opened input.
	bound    lexicalInput
	mapping  []int          // by source field position; nil for per-record names
	identity bool           // mapping[i] == i for every column
	dynamic  map[string]int // schema-less JSON: source name -> column
	values   []cell         // record cells in column order
	lastRows int            // rows in the previous batch, to size the next
}

func (r *datasetReader) Close() error {
	r.closed = true
	if r.input != nil {
		err := r.input.close()
		r.input = nil
		return err
	}
	return nil
}

func (r *datasetReader) open(ctx context.Context) (lexicalInput, error) {
	obj := r.d.objects[r.object]
	if r.d.kind != inputParquet {
		span := diagnostics.Start(ctx, diagnostics.Decode)
		input, err := openText(ctx, r.d, obj)
		span.End(err)
		return input, err
	}
	p, err := openParquet(ctx, r.d, obj, true)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (r *datasetReader) nextRecord(ctx context.Context) (lexicalRecord, error) {
	for {
		if err := ctx.Err(); err != nil {
			return lexicalRecord{}, err
		}
		if r.input == nil {
			if r.object >= len(r.d.objects) {
				return lexicalRecord{}, io.EOF
			}
			input, err := r.open(ctx)
			if err != nil {
				return lexicalRecord{}, err
			}
			r.input = input
			r.row = 0
		}
		if r.bound != r.input {
			if err := r.bind(r.input.names()); err != nil {
				return lexicalRecord{}, err
			}
			r.bound = r.input
		}
		rec, err := r.input.next(ctx)
		if errors.Is(err, io.EOF) {
			e := r.input.close()
			r.input = nil
			if e != nil {
				return lexicalRecord{}, e
			}
			r.object++
			continue
		}
		if errors.Is(err, errParquetEvidence) {
			return lexicalRecord{}, err
		}
		if err != nil {
			return lexicalRecord{}, fmt.Errorf("source %d row %d: %w", r.object+1, r.row+1, err)
		}
		row, err := data.CheckedAdd(r.row, 1)
		if err != nil {
			return lexicalRecord{}, err
		}
		global, err := data.CheckedAdd(r.global, 1)
		if err != nil {
			return lexicalRecord{}, err
		}
		r.row, r.global = row, global
		return rec, nil
	}
}

// bind maps fixed source names to columns once per input; nil names (schema-
// less JSON) are resolved per name through a cache instead.
func (r *datasetReader) bind(names []string) error {
	d := r.d
	r.mapping, r.identity = nil, false
	if names == nil {
		if r.dynamic == nil {
			r.dynamic = make(map[string]int, len(d.columns))
		}
		return nil
	}
	r.mapping = make([]int, len(names))
	r.identity = len(names) == len(d.columns)
	taken := make([]bool, len(d.columns))
	for i, name := range names {
		index := d.index(d.rename(name))
		r.mapping[i] = index
		r.identity = r.identity && index == i
		if index < 0 {
			continue
		}
		if taken[index] {
			return errors.New("ambiguous renamed input fields")
		}
		taken[index] = true
	}
	return nil
}

// arrange returns rec's cells in column order.
func (r *datasetReader) arrange(rec *lexicalRecord) ([]cell, error) {
	if r.identity {
		return rec.values, nil
	}
	clear(r.values)
	for j, name := range rec.names {
		if r.mapping != nil {
			if index := r.mapping[j]; index >= 0 {
				r.values[index] = rec.values[j]
			}
			continue
		}
		index, ok := r.dynamic[name]
		if !ok {
			index = r.d.index(r.d.rename(name))
			if index < 0 {
				return nil, errors.New("JSON field is incompatible with frozen schema")
			}
			r.dynamic[name] = index
		}
		if r.values[index].present {
			return nil, errors.New("ambiguous renamed input fields")
		}
		r.values[index] = rec.values[j]
	}
	return r.values, nil
}

// lexicalBatch accumulates one batch from lexical records.
type lexicalBatch struct {
	r       *datasetReader
	batch   *data.Batch
	builder *array.RecordBuilder
	states  [][]data.State
	charge  int64
	decoded int64
}

func (b *lexicalBatch) reserve(n int64, phase string) error {
	if err := b.r.d.budget.Reserve(n, phase); err != nil {
		return err
	}
	b.charge += n
	return nil
}

// ensureRow makes room for one more row in the row-indexed slices, reserving
// any added capacity before allocating it. A batch starts at the previous
// batch's size, so steady-state batches never re-grow.
func (b *lexicalBatch) ensureRow() error {
	rows := len(b.batch.Locations)
	if rows < cap(b.batch.Locations) {
		return nil
	}
	grow := max(rows, b.r.lastRows, minRowCapacity)
	grow = min(grow, maxBatchRows-rows)
	// The retained cost of one row of capacity: its location, reject slot and
	// one state byte per column.
	if err := b.reserve(int64(grow)*(locationBytes+rawSlotBytes+int64(len(b.states))), "source batch rows"); err != nil {
		return err
	}
	capacity := rows + grow
	b.batch.Locations = append(make([]data.Location, 0, capacity), b.batch.Locations...)
	b.batch.Raw = append(make([]json.RawMessage, 0, capacity), b.batch.Raw...)
	for i := range b.states {
		b.states[i] = append(make([]data.State, 0, capacity), b.states[i]...)
	}
	return nil
}

// issue records a row-level ingestion issue; owned is the size of strings
// the issue retains beyond shared column names.
func (b *lexicalBatch) issue(loc data.Location, code, column, reason string, owned int) error {
	if err := b.reserve(appendGrowth*issueBytes+int64(owned), "source ingestion issue"); err != nil {
		return err
	}
	b.batch.Ingest = append(b.batch.Ingest, data.Issue{
		Location: loc,
		Code:     code,
		Severity: "error",
		Origin:   data.OriginIngest,
		Scope:    data.ScopeRow,
		Column:   column,
		Message:  reason,
		Reason:   reason,
	})
	return nil
}

func (r *datasetReader) Next(ctx context.Context) (batch *data.Batch, err error) {
	if r.closed {
		return nil, io.EOF
	}
	d := r.d
	if r.awaitEvidence != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.awaitEvidence:
			r.awaitEvidence = nil
		}
	}
	if p, ok := r.input.(*parquetInput); ok && p.evidence != nil {
		return r.takeParquetEvidence(ctx, p)
	}
	if d.kind == inputParquet {
		if b, handled, err := r.nextNativeParquet(ctx); handled {
			return b, err
		}
	}
	if d.kind != inputParquet {
		// One span per batch: per-record clock reads cost more than decoding.
		// For text inputs it covers parsing and converting cells together.
		span := diagnostics.Start(ctx, diagnostics.Decode)
		defer func() {
			if errors.Is(err, io.EOF) {
				span.End(nil)
			} else {
				span.End(err)
			}
			if err == nil && batch != nil {
				diagnostics.AddRows(ctx, diagnostics.Decode, uint64(len(batch.Locations)))
			}
		}()
	}
	b := &lexicalBatch{r: r, batch: &data.Batch{}}
	success := false
	defer func() {
		if !success {
			b.batch.Release()
			d.budget.Release(b.charge)
			if closeErr := r.Close(); closeErr != nil && (err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				err = closeErr
			}
		}
	}()
	if err := b.reserve(int64(len(d.columns))*columnBatchBytes+batchFixedBytes, "source batch schema state"); err != nil {
		return nil, err
	}
	b.states = make([][]data.State, len(d.columns))
	b.builder = array.NewRecordBuilder(d.budget.Allocator(), d.schema)
	defer b.builder.Release()
	for len(b.batch.Locations) < maxBatchRows && (len(b.batch.Locations) == 0 || b.decoded < d.batchTarget) {
		rec, err := r.nextRecord(ctx)
		if errors.Is(err, io.EOF) {
			if len(b.batch.Locations) == 0 {
				return nil, io.EOF
			}
			break
		}
		if errors.Is(err, errParquetEvidence) {
			if len(b.batch.Locations) == 0 {
				event, eventErr := r.takeParquetEvidence(ctx, r.input.(*parquetInput))
				if eventErr != nil {
					return nil, eventErr
				}
				b.batch.Release()
				d.budget.Release(b.charge)
				b.charge = 0
				success = true
				return event, nil
			}
			break
		}
		if err != nil {
			return nil, err
		}
		if err = r.appendRecord(b, &rec); err != nil {
			return nil, err
		}
	}
	rows := int64(len(b.batch.Locations))
	if len(d.columns) == 0 {
		b.batch.Record = array.NewRecordBatch(d.schema, nil, rows)
	} else {
		b.batch.Record = b.builder.NewRecordBatch()
	}
	b.batch.States = make(map[string][]data.State, len(d.columns))
	for i, f := range d.columns {
		b.batch.States[f.name] = b.states[i]
	}
	charge := b.charge
	b.batch.Cleanup = func() { d.budget.Release(charge) }
	r.lastRows = len(b.batch.Locations)
	success = true
	return b.batch, nil
}

// appendRecord converts one record into the batch: presence/null/conversion
// state per column, ingestion issues, and a reject copy only for rows with an
// ingestion issue.
func (r *datasetReader) appendRecord(b *lexicalBatch, rec *lexicalRecord) error {
	d := r.d
	if err := b.ensureRow(); err != nil {
		return err
	}
	loc := data.Location{
		Source:        d.objects[r.object].URI,
		SourceOrdinal: uint64(r.object + 1),
		Row:           r.row,
		GlobalRow:     r.global,
		Line:          rec.line,
	}
	if p, ok := r.input.(*parquetInput); ok {
		loc.RowGroup = p.groupCoordinate
		loc.RowInGroup = p.groupRow
		loc.PositionBasis = r.positionBasis()
	}
	b.batch.Locations = append(b.batch.Locations, loc)
	if rec.malformed != nil {
		return r.appendMalformed(b, loc, rec)
	}
	values, err := r.arrange(rec)
	if err != nil {
		return err
	}
	hasIssue := false
	for i, f := range d.columns {
		v := values[i]
		state := data.Valid
		code, reason, owned := "", "", 0
		switch {
		case !v.present:
			state = data.Missing
			if f.options.Required {
				code, reason = "INGEST_MISSING_FIELD", "required field is missing"
			}
		case d.isNull(v, f.options):
			state = data.Null
			if f.options.Nullable != nil && !*f.options.Nullable {
				code, reason = "INGEST_NULL", "null is forbidden"
			}
		default:
			if e := appendCell(b.builder.Field(i), f.options, v); e != nil {
				if d.cfg.Input.Schema == nil || (d.cfg.Input.OnTypeError == "abort" && !r.preflight) {
					return fmt.Errorf("source %d row %d column %s: %w", r.object+1, r.row, f.name, e)
				}
				state = data.Invalid
				code, reason = "INGEST_TYPE", e.Error()
				owned = len(reason)
			}
		}
		if code != "" {
			hasIssue = true
			if err = b.issue(loc, code, f.name, reason, owned); err != nil {
				return err
			}
		}
		if state != data.Valid {
			b.builder.Field(i).AppendNull()
		}
		b.states[i] = append(b.states[i], state)
		b.decoded += int64(len(v.text) + decodedCellOverhead)
	}
	if d.cfg.Input.Schema != nil && !d.ignoreExtras() {
		for _, name := range rec.extras {
			hasIssue = true
			if err = b.issue(loc, "INGEST_EXTRA_FIELD", name, "undeclared JSON field", len(name)); err != nil {
				return err
			}
		}
	}
	if !hasIssue {
		b.batch.Raw = append(b.batch.Raw, nil)
		return nil
	}
	return r.appendReject(b, rec)
}

// appendMalformed records a CSV row whose field count differs from the
// header: every column is invalid, one INGEST_MALFORMED issue explains why,
// and the original fields go to rejects.
func (r *datasetReader) appendMalformed(b *lexicalBatch, loc data.Location, rec *lexicalRecord) error {
	for i := range r.d.columns {
		b.builder.Field(i).AppendNull()
		b.states[i] = append(b.states[i], data.Invalid)
	}
	for _, field := range rec.malformed {
		b.decoded += int64(len(field) + decodedCellOverhead)
	}
	reason := diagnostics.FieldCountMismatch(len(rec.malformed), len(rec.names))
	if err := b.issue(loc, "INGEST_MALFORMED", "", reason, len(reason)); err != nil {
		return err
	}
	return r.appendReject(b, rec)
}

// appendReject keeps an owned copy of the record for reject output.
func (r *datasetReader) appendReject(b *lexicalBatch, rec *lexicalRecord) error {
	bound := rec.rawSizeBound()
	if err := b.reserve(bound, "source reject record"); err != nil {
		return err
	}
	raw := rec.rejectRaw()
	// Keep only the retained capacity charged; the bound covered encoding.
	if unused := bound - int64(cap(raw)); unused > 0 {
		r.d.budget.Release(unused)
		b.charge -= unused
	}
	b.batch.Raw = append(b.batch.Raw, raw)
	return nil
}
