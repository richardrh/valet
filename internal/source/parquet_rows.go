package source

//lint:file-ignore ST1005 Error strings here begin with the proper nouns Parquet or Arrow.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
)

func parquetArrowProperties(d *Dataset) pqarrow.ArrowReadProperties {
	rows := min(int64(maxParquetBatchRows), max(1, d.batchTarget/parquetRowEstimateBytes))
	return pqarrow.ArrowReadProperties{Parallel: false, BatchSize: rows}
}

func newParquetFile(p *parquetInput, input parquet.ReaderAtSeeker, meta *metadata.FileMetaData) (*file.Reader, error) {
	props := parquet.NewReaderProperties(p.d.budget.Allocator())
	props.BufferedStreamEnabled = true
	props.BufferSize = parquetReadBufferBytes
	props.MaxCompressedPageSize = max(p.d.recordLimit, p.d.batchTarget)
	props.MaxUncompressedPageSize = props.MaxCompressedPageSize
	return file.NewParquetReader(input, file.WithReadProps(props), file.WithMetadata(meta))
}

// normalizedGroupMetadata clones only structures whose physical locations change.
// The physical schema, logical annotations and key/value metadata stay immutable.
func normalizedGroupMetadata(original *metadata.FileMetaData, group int) (*metadata.FileMetaData, error) {
	out := *original
	physical := *original.FileMetaData
	out.FileMetaData = &physical
	physical.RowGroups = slices.Clone(original.RowGroups[group : group+1])
	rg := *physical.RowGroups[0]
	physical.RowGroups[0] = &rg
	rg.Columns = slices.Clone(rg.Columns)
	rg.FileOffset, rg.TotalCompressedSize, rg.Ordinal = nil, nil, nil
	physical.NumRows = rg.NumRows
	for i, originalColumn := range rg.Columns {
		if originalColumn == nil || originalColumn.MetaData == nil {
			return nil, errors.New("missing Parquet column chunk metadata")
		}
		column := *originalColumn
		rg.Columns[i] = &column
		chunk := *column.MetaData
		column.MetaData = &chunk
		column.FileOffset = 0
		column.OffsetIndexOffset, column.OffsetIndexLength = nil, nil
		column.ColumnIndexOffset, column.ColumnIndexLength = nil, nil
		chunk.Codec = 0 // UNCOMPRESSED
		chunk.IndexPageOffset, chunk.DictionaryPageOffset = nil, nil
		chunk.BloomFilterOffset, chunk.BloomFilterLength = nil, nil
	}
	return &out, nil
}

type parquetRecordResult struct {
	record   arrow.RecordBatch
	err      error
	end      bool
	evidence *data.GroupEvidence
}

type parquetWorkerFailure struct {
	group int
	err   error
}

// Each worker owns one row-group stream and at most one unpublished Arrow batch.
// Round-robin assignment plus unbuffered worker channels bounds out-of-order work
// independently of file size and preserves group, object and row provenance.
type orderedParquetRecords struct {
	cancel    context.CancelFunc
	wait      sync.WaitGroup
	inputs    []chan parquetRecordResult
	groups    int
	group     int
	cur       arrow.RecordBatch
	err       error
	charge    resource.Reservation
	closed    bool
	failures  []parquetWorkerFailure // one writer per worker; read only after join
	evidence  *data.GroupEvidence
	awaiting  int // worker waiting for the next demand after an evidence event
	demand    []chan struct{}
	decisions []data.GroupEvidence
	gated     bool
	announced bool
}

func newOrderedParquetRecords(ctx context.Context, input *parquetInput) (*orderedParquetRecords, error) {
	groups := input.file.NumRowGroups()
	workers := min(32, max(1, input.d.cfg.Execution.Workers), max(1, groups))
	// Admission uses the largest single group, never the entire file. This is
	// not a reservation: every live buffer and typed decoder is charged where
	// allocated, including overlapping old/new capacities during growth.
	var maximum int64
	zstd := false
	var totalRows int64
	for group := range groups {
		raw := input.file.MetaData().RowGroups[group]
		if raw == nil {
			return nil, errors.New("missing Parquet row-group metadata")
		}
		for _, column := range raw.Columns {
			if column == nil || column.MetaData == nil {
				return nil, errors.New("missing Parquet column chunk metadata")
			}
		}
		rg := input.file.MetaData().RowGroup(group)
		rows := rg.NumRows()
		if rows < 0 || rows > math.MaxInt64-totalRows {
			return nil, errors.New("invalid or overflowing Parquet row-group row count")
		}
		totalRows += rows
		if rg.NumColumns() != len(input.header) {
			return nil, errors.New("row-group column count differs from schema")
		}
		n, compressed := rg.TotalByteSize(), rg.TotalCompressedSize()
		if n < 0 || compressed < 0 || n > (math.MaxInt64-compressed)/4 {
			return nil, errors.New("invalid Parquet row-group size")
		}
		maximum = max(maximum, n*4+compressed)
		for column := range rg.NumColumns() {
			chunk, err := rg.ColumnChunk(column)
			if err != nil {
				return nil, err
			}
			if chunk.NumValues() != rows {
				return nil, errors.New("flat Parquet chunk count differs from row-group count")
			}
			zstd = zstd || chunk.Compression() == compress.Codecs.Zstd
		}
	}
	if input.file.NumRows() < 0 || input.file.NumRows() != totalRows {
		return nil, errors.New("Parquet file row count differs from row-group counts")
	}
	pageLimit := max(input.d.recordLimit, input.d.batchTarget)
	if pageLimit > math.MaxInt64/4 || maximum > math.MaxInt64-4*pageLimit-resource.ParquetCodecWorkspace {
		return nil, errors.New("Parquet decoder limit overflows accounting")
	}
	estimate := max(int64(1), maximum+4*pageLimit+metadataFixedBytes)
	if zstd {
		estimate += resource.ParquetCodecWorkspace
	}
	available := max(int64(0), input.d.budget.Limit()-input.d.budget.Used())
	workers = min(workers, int(max(int64(1), available/estimate)))
	charge, err := input.d.budget.Hold(int64(workers)*512+512, "Parquet row-group scheduler")
	if err != nil {
		return nil, err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	r := &orderedParquetRecords{cancel: cancel, groups: groups, inputs: make([]chan parquetRecordResult, workers), failures: make([]parquetWorkerFailure, workers), charge: charge, awaiting: -1}
	if input.prepared != nil {
		r.decisions = input.prepared.decisions
	}
	r.gated = r.decisions != nil && input.d.cfg.Input.Parquet.Statistics == "trusted"
	if r.gated {
		r.demand = make([]chan struct{}, workers)
		for worker := range workers {
			r.demand[worker] = make(chan struct{})
		}
	}
	for worker := range workers {
		r.inputs[worker] = make(chan parquetRecordResult)
		r.wait.Add(1)
		go r.runWorker(workerCtx, input, worker)
	}
	return r, nil
}

func (r *orderedParquetRecords) send(ctx context.Context, worker int, result parquetRecordResult) bool {
	select {
	case r.inputs[worker] <- result:
		return true
	case <-ctx.Done():
		if result.record != nil {
			result.record.Release()
		}
		return false
	}
}

// The Arrow manifest, buffered file reader and its pool are prepared once per
// worker. Only that worker's normalized row-group metadata and byte view change,
// and only after the preceding RecordReader has released all decoder state.
type parquetRowGroupWorker struct {
	input    *parquetInput
	bytes    bytes.Reader
	metadata *metadata.FileMetaData
	file     *file.Reader
	arrow    *pqarrow.FileReader
}

func (r *orderedParquetRecords) runWorker(ctx context.Context, original *parquetInput, worker int) {
	defer r.wait.Done()
	defer close(r.inputs[worker])
	p := &parquetInput{d: original.d, handle: original.handle, file: original.file, schema: original.schema, header: original.header}
	state := &parquetRowGroupWorker{input: p}
	defer func() {
		cleanup := &parquetInput{d: p.d, file: state.file}
		_ = cleanup.close()
		state.bytes.Reset(nil)
		// The shared original reader and pinned handle are closed only after all
		// workers join. This worker owns only verifier buffers and codec state.
		p.handle, p.file = nil, nil
		_ = p.close()
	}()
	prepared := false
	for group := worker; group < r.groups; group += len(r.inputs) {
		if ctx.Err() != nil {
			return
		}
		if r.gated {
			if !r.send(ctx, worker, parquetRecordResult{evidence: &r.decisions[group]}) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-r.demand[worker]:
			}
			if r.decisions[group].Action == "skip" {
				if !r.send(ctx, worker, parquetRecordResult{end: true}) {
					return
				}
				continue
			}
		}
		if !prepared {
			if err := p.reserve(int64(len(p.header))*1024+metadataFixedBytes, "Parquet row-group worker state"); err != nil {
				r.failures[worker] = parquetWorkerFailure{group: group, err: err}
				r.send(ctx, worker, parquetRecordResult{end: true, err: err})
				return
			}
			p.pageStates = make([]int64, len(p.header))
			p.dictStates = make([]int64, len(p.header))
			prepared = true
		}
		err := r.readGroup(ctx, state, group, worker)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			r.failures[worker] = parquetWorkerFailure{group: group, err: err}
		}
		if !r.send(ctx, worker, parquetRecordResult{end: true, err: err}) || err != nil {
			return
		}
	}
}

func (r *orderedParquetRecords) readGroup(ctx context.Context, state *parquetRowGroupWorker, group, worker int) (err error) {
	p := state.input
	state.bytes.Reset(nil)
	defer func() {
		if recovered := recover(); recovered != nil {
			if limit, ok := recovered.(*resource.ResourceLimitError); ok {
				err = limit
			} else {
				panic(recovered)
			}
		}
		p.bridge = nil
	}()
	p.guardGroup = group
	p.bridge, err = normalizedGroupMetadata(p.file.MetaData(), group)
	if err != nil {
		return err
	}
	// Four bytes ensure every dictionary/data offset is positive, including
	// the first column, just as in an ordinary Parquet file.
	if err = p.resizeBuffer(&p.verified, 4, "verified Parquet row-group stream", false); err != nil {
		return err
	}
	copy(p.verified, "PAR1")
	span := diagnostics.Start(ctx, diagnostics.Decode)
	err = p.validatePages(ctx)
	span.End(err)
	if err != nil {
		return err
	}
	p.bridge.SetSourceFileSize(int64(len(p.verified)))
	p.bridge.RowGroups[0].TotalByteSize = int64(len(p.verified)) - 4
	state.bytes.Reset(p.verified)
	if state.file == nil {
		state.metadata = p.bridge
		state.file, err = newParquetFile(p, &state.bytes, state.metadata)
		if err != nil {
			return err
		}
		state.arrow, err = pqarrow.NewFileReader(state.file, parquetArrowProperties(p.d), p.d.budget.Allocator())
		if err != nil {
			return err
		}
	} else {
		*state.metadata = *p.bridge
	}
	reader, err := state.arrow.GetRecordReader(ctx, nil, []int{0})
	if err != nil {
		return err
	}
	defer reader.Release()
	var rows int64
	for {
		span := diagnostics.Start(ctx, diagnostics.Decode)
		next := reader.Next()
		span.End(reader.Err())
		if !next {
			break
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		record := reader.RecordBatch()
		rows += record.NumRows()
		diagnostics.AddRows(ctx, diagnostics.Decode, uint64(record.NumRows()))
		if rows > p.bridge.NumRows {
			return errors.New("decoded Parquet rows exceed row-group count")
		}
		record.Retain()
		if !r.send(ctx, worker, parquetRecordResult{record: record}) {
			return ctx.Err()
		}
	}
	if err = reader.Err(); err != nil {
		return err
	}
	if rows != p.bridge.NumRows {
		return errors.New("decoded Parquet rows differ from row-group count")
	}
	return nil
}

func (r *orderedParquetRecords) Next(ctx context.Context) bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.closed || r.err != nil {
		return false
	}
	r.evidence = nil
	if r.awaiting >= 0 {
		select {
		case <-ctx.Done():
			r.err = ctx.Err()
			r.Release()
			return false
		case r.demand[r.awaiting] <- struct{}{}:
			r.awaiting = -1
		}
	}
	for r.group < r.groups {
		if r.decisions != nil && !r.gated && !r.announced {
			r.evidence = &r.decisions[r.group]
			r.announced = true
			return true
		}
		select {
		case <-ctx.Done():
			r.err = ctx.Err()
			r.Release()
			return false
		case result, ok := <-r.inputs[r.group%len(r.inputs)]:
			if !ok {
				r.err = fmt.Errorf("Parquet row-group worker stopped before group %d completed", r.group+1)
				r.Release()
				return false
			}
			if result.err != nil {
				r.err = result.err
				r.Release()
				return false
			}
			if result.evidence != nil {
				r.evidence = result.evidence
				r.awaiting = r.group % len(r.inputs)
				return true
			}
			if result.end {
				r.group++
				r.announced = false
				continue
			}
			r.cur = result.record
			return true
		}
	}
	r.Release()
	return false
}

func (r *orderedParquetRecords) RecordBatch() arrow.RecordBatch { return r.cur }
func (r *orderedParquetRecords) Err() error                     { return r.err }
func (r *orderedParquetRecords) Evidence() *data.GroupEvidence  { return r.evidence }
func (r *orderedParquetRecords) Group() int                     { return r.group }
func (r *orderedParquetRecords) Release() {
	if r.closed {
		return
	}
	r.closed = true
	r.cancel()
	r.wait.Wait()
	if r.err == nil || errors.Is(r.err, context.Canceled) || errors.Is(r.err, context.DeadlineExceeded) {
		first := r.groups
		for _, failure := range r.failures {
			if failure.err != nil && failure.group < first {
				first, r.err = failure.group, failure.err
			}
		}
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	for _, input := range r.inputs {
		for result := range input {
			if result.record != nil {
				result.record.Release()
			}
		}
	}
	r.charge.Release()
}
