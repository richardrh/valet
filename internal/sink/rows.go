package sink

import (
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

var provenanceNames = [...]string{"__valet_source", "__valet_source_ordinal", "__valet_row", "__valet_global_row"}

// partKind identifies one output part family. The row kinds share their values
// with data.Bucket.
type partKind int

const (
	kindPass partKind = iota
	kindWarn
	kindError
	kindIssues
	kindRejects
	kindEvidence
	numPartKinds
)

const numRowKinds = int(kindIssues)

var partKindNames = [numPartKinds]string{"pass", "warn", "error", "issues", "rejects", "evidence"}

func (k partKind) String() string  { return partKindNames[k] }
func (k partKind) isRowKind() bool { return k < kindIssues }

func kindFor(bucket data.Bucket) partKind { return partKind(bucket) }

// formatFor is the file format, and extension, of parts of kind.
func (w *Writer) formatFor(kind partKind) string {
	if kind.isRowKind() {
		return w.cfg.Output.Format
	}
	return "ndjson"
}

const maxRowGroups = 64
const maxGroupRows = 64 << 10
const maxSelectionRows = 1024

const (
	parquetBatchSize                = 256
	parquetDataPageSize             = 64 << 10
	dictionarySampleRows            = 128
	dictionarySampleUnique          = 32
	dictionarySampleCharge          = 4096
	dictionaryOptionsPerFieldCharge = 512  // closures plus old/new option slice overlap
	boundedStatisticsPerField       = 1024 // fixed-width extrema, null counters and encoder state
	boundedStatisticsFooterPerField = 512  // serialized statistics retained per group
)

// parquetWriterOptions are the run-wide Parquet writer settings. The allocator
// is appended per part because each part owns its encoder allocations.
func parquetWriterOptions() []parquet.WriterProperty {
	return []parquet.WriterProperty{
		parquet.WithDictionaryDefault(false),
		parquet.WithDictionaryPageSizeLimit(16 << 10),
		parquet.WithDictionaryFor(provenanceNames[0], true),
		parquet.WithDictionaryCostFallbackFor(provenanceNames[0], true),
		parquet.WithCompression(compress.Codecs.Snappy),
		// Statistics retain potentially large min/max strings outside Arrow allocation.
		parquet.WithStats(false),
		parquet.WithMaxRowGroupLength(maxGroupRows), parquet.WithBatchSize(parquetBatchSize), parquet.WithDataPageSize(parquetDataPageSize),
	}
}

func boundedStatisticsType(t arrow.DataType) bool {
	switch t.ID() {
	case arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64, arrow.DECIMAL128:
		return true
	default:
		return false
	}
}

func (w *Writer) statisticsFields() int {
	if w.cfg.Output.Parquet.Statistics != "bounded" {
		return 0
	}
	n := 0
	for _, field := range w.outputSchema.Fields() {
		if boundedStatisticsType(field.Type) {
			n++
		}
	}
	return n
}

type part struct {
	stream      *stream
	csv         *csv.Writer
	record      []string // reusable CSV row
	parquet     *pqarrow.FileWriter
	allocator   *encoderAllocator
	groups      int
	charge      int64
	groupRows   int
	groupBytes  int64
	groupCharge int64
}

// close flushes the encoder and publishes the stream, then returns every
// resource the part holds. On error the caller aborts.
func (p *part) close(budget *resource.Budget) error {
	if p.csv != nil {
		p.csv.Flush()
		if err := p.csv.Error(); err != nil {
			return err
		}
	}
	if p.parquet != nil {
		if err := p.parquet.Close(); err != nil {
			return err
		}
	}
	p.allocator.release()
	budget.Release(p.groupCharge)
	p.groupCharge = 0
	if err := p.stream.close(); err != nil {
		return err
	}
	budget.Release(p.charge)
	p.charge = 0
	return nil
}

// abort discards the stream and returns every resource without encoding more.
func (p *part) abort(budget *resource.Budget) error {
	p.stream.discard = true
	p.allocator.release()
	budget.Release(p.groupCharge)
	p.groupCharge = 0
	err := p.stream.abort()
	budget.Release(p.charge)
	p.charge = 0
	return err
}

func provenanceSchema(schema *arrow.Schema) (*arrow.Schema, error) {
	fields := schema.Fields()
	for _, field := range fields {
		if strings.HasPrefix(field.Name, "__valet_") {
			return nil, fmt.Errorf("column %q uses reserved provenance prefix", field.Name)
		}
		switch field.Type.ID() {
		case arrow.NULL, arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64, arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.DECIMAL128:
		default:
			return nil, fmt.Errorf("unsupported output type %s for %s", field.Type, field.Name)
		}
	}
	fields = append(fields, arrow.Field{Name: provenanceNames[0], Type: arrow.BinaryTypes.String})
	for _, name := range provenanceNames[1:] {
		fields = append(fields, arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Uint64})
	}
	metadata := schema.Metadata()
	return arrow.NewSchema(fields, &metadata), nil
}
func (w *Writer) ensurePart(kind partKind, dictionaries ...parquet.WriterProperty) (*part, error) {
	if w.parts[kind] != nil {
		return w.parts[kind], nil
	}
	format := w.formatFor(kind)
	if err := addCount(&w.sequences[kind], 1, "part sequence"); err != nil {
		return nil, err
	}
	s, err := w.open(fmt.Sprintf("%s/part-%06d.%s", kind, w.sequences[kind], format))
	if err != nil {
		return nil, err
	}
	p := &part{stream: s}
	w.parts[kind] = p
	if !kind.isRowKind() {
		return p, nil
	}
	if format == "csv" {
		p.csv = csv.NewWriter(s)
		p.record = make([]string, w.outputSchema.NumFields())
		for i, field := range w.outputSchema.Fields() {
			p.record[i] = field.Name
		}
		if err = p.csv.Write(p.record); err != nil {
			return nil, err
		}
		return p, nil
	}
	// Non-Arrow encoder bookkeeping is bounded by columns and row groups.
	charge := int64(parquetEncoderBase + w.outputSchema.NumFields()*parquetEncoderPerField + w.statisticsFields()*boundedStatisticsPerField)
	if err = w.budget.Reserve(charge, "Parquet encoder metadata"); err != nil {
		return nil, err
	}
	p.charge = charge
	p.allocator = newEncoderAllocator(w.budget)
	options := append(w.parquetOptions[:len(w.parquetOptions):len(w.parquetOptions)], parquet.WithAllocator(p.allocator))
	options = append(options, dictionaries...)
	p.parquet, err = pqarrow.NewFileWriter(w.outputSchema, s, parquet.NewWriterProperties(options...), pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(p.allocator), pqarrow.WithStoreSchema()))
	if err != nil {
		return nil, err
	}
	return p, nil
}
func (w *Writer) closePart(kind partKind) error {
	p := w.parts[kind]
	if p == nil {
		return nil
	}
	if err := p.close(w.budget); err != nil {
		return err
	}
	ref := p.stream.ref()
	if err := w.artifacts.writeRecord(artifact{data.ReportVersion, ref, kind.String(), w.formatFor(kind)}, textBytes(len(ref.URI)+len(ref.SHA256))+recordOverhead(8)); err != nil {
		return err
	}
	if err := addCount(&w.artifacts.count, 1, "artifact index records"); err != nil {
		return err
	}
	if err := addCount(&w.artifactCounts[kind], ref.Count, "artifact records"); err != nil {
		return err
	}
	w.parts[kind] = nil
	w.logger.Info("output part published", slog.String("phase", "output"), slog.String("kind", kind.String()), slog.Any("uri", storage.LogURI(ref.URI)), slog.Uint64("rows", ref.Count), slog.Int64("bytes", ref.Size))
	return nil
}
func cellBytes(values arrow.Array, row int) int64 {
	if values.IsNull(row) {
		return 8
	}
	switch a := values.(type) {
	case *array.String:
		return int64(len(a.Value(row))) + 16
	case *array.LargeString:
		return int64(len(a.Value(row))) + 16
	case *array.Binary:
		return int64(len(a.Value(row))) + 16
	case *array.LargeBinary:
		return int64(len(a.Value(row))) + 16
	case *array.FixedSizeBinary:
		return int64(len(a.Value(row))) + 16
	default:
		return 64
	}
}
func rowBytes(batch *data.Batch, row int) int64 {
	n := int64(len(batch.Locations[row].Source) + 128)
	for _, column := range batch.Record.Columns() {
		n += cellBytes(column, row)
	}
	return n
}
func csvValue(values arrow.Array, row int) string {
	if values.IsNull(row) {
		return "\\N"
	}
	switch a := values.(type) {
	case *array.String:
		return escapeCSVString(a.Value(row))
	case *array.LargeString:
		return escapeCSVString(a.Value(row))
	case *array.Binary:
		return base64.StdEncoding.EncodeToString(a.Value(row))
	case *array.LargeBinary:
		return base64.StdEncoding.EncodeToString(a.Value(row))
	case *array.FixedSizeBinary:
		return base64.StdEncoding.EncodeToString(a.Value(row))
	case *array.Timestamp:
		return a.Value(row).ToTime(a.DataType().(*arrow.TimestampType).Unit).Format(time.RFC3339Nano)
	case *array.Date32:
		return a.Value(row).ToTime().Format("2006-01-02")
	case *array.Date64:
		return a.Value(row).ToTime().Format("2006-01-02")
	case *array.Decimal128:
		return a.Value(row).ToString(a.DataType().(*arrow.Decimal128Type).Scale)
	default:
		return values.ValueStr(row)
	}
}
func escapeCSVString(value string) string { return strings.ReplaceAll(value, "\\", "\\\\") }
func (w *Writer) writeRows(batch *data.Batch, result *data.Result) error {
	if w.cfg.Output.Format == "parquet" {
		return w.writeParquet(batch, result)
	}
	for row, bucket := range result.Buckets {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		kind := kindFor(bucket)
		if !w.emit(kind) {
			continue
		}
		if p := w.parts[kind]; p != nil && p.stream.size >= w.partBytes {
			if err := w.closePart(kind); err != nil {
				return err
			}
		}
		p, err := w.ensurePart(kind)
		if err != nil {
			return err
		}
		if err = w.writeCSVRow(p, batch, row); err != nil {
			return err
		}
		if err = addCount(&p.stream.count, 1, "part rows"); err != nil {
			return err
		}
		if err = addCount(&w.counts[kind], 1, "emitted rows"); err != nil {
			return err
		}
	}
	// Flush without closing to observe errors before releasing the source batch.
	for _, p := range w.parts[:numRowKinds] {
		if p != nil && p.csv != nil {
			p.csv.Flush()
			if err := p.csv.Error(); err != nil {
				return err
			}
		}
	}
	return nil
}
func (w *Writer) writeCSVRow(p *part, batch *data.Batch, row int) error {
	charge := 2*rowBytes(batch, row) + int64(w.outputSchema.NumFields()*csvRowPerField) + csvRowBase
	if err := w.budget.Reserve(charge, "CSV output row"); err != nil {
		return err
	}
	defer w.budget.Release(charge)
	record := p.record
	for i, column := range batch.Record.Columns() {
		record[i] = csvValue(column, row)
	}
	offset := int(batch.Record.NumCols())
	location := batch.Locations[row]
	record[offset] = escapeCSVString(location.Source)
	record[offset+1] = strconv.FormatUint(location.SourceOrdinal, 10)
	record[offset+2] = strconv.FormatUint(location.Row, 10)
	record[offset+3] = strconv.FormatUint(location.GlobalRow, 10)
	err := p.csv.Write(record)
	// Drop cell references so the scratch row does not pin batch-derived strings.
	clear(record)
	return err
}
func (w *Writer) writeParquet(batch *data.Batch, result *data.Result) error {
	if w.mode() == "report" {
		return nil
	}
	// Only one bounded selection is held at a time. WriteBuffered encodes it
	// synchronously into the part-owned page buffers; no source batch or result
	// is retained after this call, even while a row group spans batches.
	if err := w.budget.Reserve(selectionCharge, "Parquet row selection"); err != nil {
		return err
	}
	defer w.budget.Release(selectionCharge)
	rows := make([]int, 0, maxSelectionRows)
	for kind := range partKind(numRowKinds) {
		if !w.emit(kind) {
			continue
		}
		var size int64
		for row, bucket := range result.Buckets {
			if kindFor(bucket) != kind {
				continue
			}
			amount := rowBytes(batch, row)
			if len(rows) > 0 && (len(rows) == maxSelectionRows || size+amount > w.groupTarget) {
				if err := w.writeGroup(kind, batch, rows, size); err != nil {
					return err
				}
				rows = rows[:0]
				size = 0
			}
			rows = append(rows, row)
			size += amount
		}
		if len(rows) > 0 {
			if err := w.writeGroup(kind, batch, rows, size); err != nil {
				return err
			}
			rows = rows[:0]
		}
	}
	return nil
}
func (w *Writer) writeGroup(kind partKind, batch *data.Batch, rows []int, size int64) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if p := w.parts[kind]; p != nil {
		newGroup := p.groupRows+len(rows) > maxGroupRows || p.groupBytes+size > w.groupTarget
		// Buffered bytes have not necessarily reached stream.Write yet. Include
		// them when deciding to roll a part, independently of the footer cap.
		if p.stream.size+p.groupBytes >= w.partBytes || (newGroup && p.groups >= maxRowGroups) {
			if err := w.closePart(kind); err != nil {
				return err
			}
		}
	}
	p := w.parts[kind]
	var err error
	if p == nil {
		// The probe owns one reused bounded map plus per-column property
		// closures and append-growth overlap. Hold both through construction,
		// until the part's encoder metadata reservation owns retained properties.
		probeCharge := int64(dictionarySampleCharge) + int64(batch.Record.NumCols())*dictionaryOptionsPerFieldCharge
		if err = w.budget.Reserve(probeCharge, "Parquet dictionary sampling"); err != nil {
			return err
		}
		func() {
			defer w.budget.Release(probeCharge)
			p, err = w.ensurePart(kind, sampledDictionaries(batch, rows)...)
		}()
		if err != nil {
			return err
		}
	}
	if p.groupRows == 0 || p.groupRows+len(rows) > maxGroupRows || p.groupBytes+size > w.groupTarget {
		charge := int64(w.outputSchema.NumFields()*rowGroupMetadataPerCol + w.statisticsFields()*boundedStatisticsFooterPerField)
		if err = w.budget.Reserve(charge, "Parquet row-group metadata"); err != nil {
			return err
		}
		p.charge += charge // footer metadata lives until the part closes
		if p.groupRows > 0 {
			// This flushes the previous group before releasing its non-Arrow
			// charge. The allocator separately tracks all live encoded pages.
			if err = p.parquet.NewBufferedRowGroupChecked(); err != nil {
				return err
			}
			w.budget.Release(p.groupCharge)
			p.groupCharge = 0
		}
		if statisticsCharge := int64(w.statisticsFields() * boundedStatisticsPerField); statisticsCharge > 0 {
			if err = w.budget.Reserve(statisticsCharge, "Parquet bounded group statistics"); err != nil {
				return err
			}
			p.groupCharge += statisticsCharge
		}
		p.groupRows, p.groupBytes = 0, 0
		p.groups++
	}
	// Compression scratch and dictionary bookkeeping can allocate outside the
	// Arrow allocator. Hold this conservative bound for the entire active group,
	// independently of part/footer metadata and tracked encoder/page buffers.
	charge := 2 * size
	if err = w.budget.Reserve(charge, "Parquet buffered group working set"); err != nil {
		return err
	}
	p.groupCharge += charge
	selectionAllocator := newEncoderAllocator(w.budget)
	defer selectionAllocator.release()
	rec, err := w.selectRows(batch, rows, selectionAllocator)
	if err != nil {
		return err
	}
	defer rec.Release()
	if err = p.parquet.WriteBuffered(rec); err != nil {
		return err
	}
	p.groupRows += len(rows)
	p.groupBytes += size
	if err = addCount(&p.stream.count, uint64(len(rows)), "part rows"); err != nil {
		return err
	}
	if err = addCount(&w.counts[kind], uint64(len(rows)), "emitted rows"); err != nil {
		return err
	}
	return nil
}

// sampledDictionaries enables bounded categorical encoding from actual values,
// not user column names. A later distribution change still falls back at the
// dictionary page limit. Very wide IDs are rejected before any string hashing.
func sampledDictionaries(batch *data.Batch, rows []int) []parquet.WriterProperty {
	if len(rows) < 16 {
		return nil
	}
	rows = rows[:min(len(rows), dictionarySampleRows)]
	seen := make(map[string]struct{}, dictionarySampleUnique)
	var options []parquet.WriterProperty
	for index := range int(batch.Record.NumCols()) {
		field := batch.Record.Schema().Field(index)
		if field.Type.ID() != arrow.STRING && field.Type.ID() != arrow.LARGE_STRING {
			continue
		}
		clear(seen)
		values := batch.Record.Column(index)
		text := values.(interface{ Value(int) string })
		nonNull, bytes := 0, 0
		eligible := true
		for _, row := range rows {
			if values.IsNull(row) {
				continue
			}
			nonNull++
			value := text.Value(row)
			if len(value) > 256 {
				eligible = false
				break
			}
			if _, exists := seen[value]; exists {
				continue
			}
			bytes += len(value) + 4
			if len(seen) == dictionarySampleUnique || bytes > 16<<10 {
				eligible = false
				break
			}
			seen[value] = struct{}{}
		}
		if eligible && nonNull >= 16 && len(seen)*4 <= nonNull {
			options = append(options, parquet.WithDictionaryFor(field.Name, true), parquet.WithDictionaryCostFallbackFor(field.Name, true))
		}
	}
	return options
}
func (w *Writer) selectRows(batch *data.Batch, rows []int, mem memory.Allocator) (arrow.RecordBatch, error) {
	contiguous := true
	for i := 1; i < len(rows); i++ {
		if rows[i] != rows[0]+i {
			contiguous = false
			break
		}
	}
	var selected arrow.Array
	if !contiguous {
		indices := array.NewUint64Builder(mem)
		defer indices.Release()
		indices.Reserve(len(rows))
		for _, row := range rows {
			indices.Append(uint64(row))
		}
		selected = indices.NewArray()
		defer selected.Release()
	}
	columns := make([]arrow.Array, 0, w.outputSchema.NumFields())
	defer func() {
		for _, column := range columns {
			column.Release()
		}
	}()
	ctx := exec.WithAllocator(w.ctx, mem)
	for _, column := range batch.Record.Columns() {
		if contiguous {
			columns = append(columns, array.NewSlice(column, int64(rows[0]), int64(rows[0]+len(rows))))
		} else {
			taken, err := compute.TakeArray(ctx, column, selected)
			if err != nil {
				return nil, err
			}
			columns = append(columns, taken)
		}
	}
	sources := array.NewStringBuilder(mem)
	defer sources.Release()
	sources.Reserve(len(rows))
	sourceBytes := 0
	for _, row := range rows {
		sourceBytes += len(batch.Locations[row].Source)
	}
	// writeGroup already accounts for these bytes; reserve once instead of
	// repeatedly growing and copying the provenance string payload.
	sources.ReserveData(sourceBytes)
	for _, row := range rows {
		sources.Append(batch.Locations[row].Source)
	}
	columns = append(columns, sources.NewArray())
	for field := range 3 {
		builder := array.NewUint64Builder(mem)
		func() {
			defer builder.Release()
			builder.Reserve(len(rows))
			for _, row := range rows {
				location := batch.Locations[row]
				switch field {
				case 0:
					builder.Append(location.SourceOrdinal)
				case 1:
					builder.Append(location.Row)
				case 2:
					builder.Append(location.GlobalRow)
				}
			}
			columns = append(columns, builder.NewArray())
		}()
	}
	return array.NewRecordBatch(w.outputSchema, columns, int64(len(rows))), nil
}
