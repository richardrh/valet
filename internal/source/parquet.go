package source

//lint:file-ignore ST1005 Error strings here begin with the proper nouns Parquet or Arrow.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// Parquet decoder accounting.
const (
	// parquetFooterBytes is the 4-byte metadata length plus the "PAR1" magic;
	// a file also starts with "PAR1", hence the 12-byte minimum size.
	parquetFooterBytes  = 8
	parquetMinFileBytes = 12
	// metadataExpansion bounds arrow-go's decoded FileMetaData objects per
	// encoded footer byte; metadataFixedBytes covers the reader itself.
	metadataExpansion  = 256
	metadataFixedBytes = 64 << 10
	// parquetReadBufferBytes is the buffered column-chunk stream size.
	parquetReadBufferBytes = 4096
	// maxParquetBatchRows caps rows per Arrow batch; the batch target is
	// divided by parquetRowEstimateBytes to size smaller batches.
	maxParquetBatchRows     = 1024
	parquetRowEstimateBytes = 4096
)

type parquetPreparation struct {
	metadata                   *metadata.FileMetaData
	schema                     *arrow.Schema
	header                     []string
	footerSHA256               string
	footerOffset, footerLength int64
	decisions                  []data.GroupEvidence
	selective                  bool
	baseline                   bool
}

type parquetInput struct {
	d               *Dataset
	handle          storage.ReadAtCloser
	file            *file.Reader
	reader          *orderedParquetRecords
	schema          *arrow.Schema
	header          []string
	values          []cell // reused row cells
	record          arrow.RecordBatch
	row             int
	charge          int64
	prepared        *parquetPreparation
	currentGroup    int
	groupRow        uint64
	groupCoordinate *uint64
	evidence        *data.GroupEvidence

	// Each active row-group worker exclusively owns these buffers and codec.
	// Their high-water capacities remain charged until the worker closes.
	decoder                       *resource.ParquetDecoder
	compressed                    []byte
	decompressed                  []byte
	verified                      []byte
	guardGroup                    int
	pageStates                    []int64
	dictStates                    []int64
	bridge                        *metadata.FileMetaData
	nativeIndices                 []int
	nativeBound, nativeCompatible bool
}

func (p *parquetInput) names() []string { return p.header }
func (p *parquetInput) reserve(n int64, phase string) error {
	if n < 0 {
		return errors.New("invalid Parquet allocation size")
	}
	if e := p.d.budget.Reserve(n, phase); e != nil {
		return e
	}
	p.charge += n
	return nil
}
func openParquet(ctx context.Context, d *Dataset, obj storage.Object, rows bool) (out *parquetInput, err error) {
	resource.ConfigureParquetCodecs()
	if !rows {
		span := diagnostics.Start(ctx, diagnostics.Metadata)
		defer func() { span.End(err) }()
	}
	p := &parquetInput{d: d, currentGroup: -1}
	complete := false
	defer func() {
		if !complete {
			_ = p.close()
		}
	}()
	prepared := d.parquet[obj.URI]
	mode := storage.ReadVerified
	switch {
	case !rows && (d.plan || d.statisticsPending):
		mode = storage.ReadMetadata
	case !rows:
		mode = storage.ReadMetadataVerified
	case prepared != nil && prepared.selective:
		mode = storage.ReadSelective
	}
	p.handle, err = d.store.OpenAt(ctx, obj, mode)
	if err != nil {
		return nil, err
	}
	p.prepared = prepared
	if prepared == nil {
		if obj.Size < parquetMinFileBytes {
			return nil, errors.New("Parquet file is too short")
		}
		var footer [parquetFooterBytes]byte
		if _, err = p.handle.ReadAt(footer[:], obj.Size-parquetFooterBytes); err != nil {
			return nil, err
		}
		if string(footer[4:]) != "PAR1" {
			return nil, errors.New("unsupported or invalid Parquet footer")
		}
		size := int64(binary.LittleEndian.Uint32(footer[:4]))
		if size > obj.Size-parquetMinFileBytes {
			return nil, errors.New("invalid Parquet footer length")
		}
		if err = p.reserve(size*metadataExpansion+metadataFixedBytes, "Parquet metadata"); err != nil {
			return nil, err
		}
		metadataBytes := make([]byte, int(size))
		if _, err = p.handle.ReadAt(metadataBytes, obj.Size-parquetFooterBytes-size); err != nil {
			return nil, err
		}
		if err = validateCompact(metadataBytes); err != nil {
			return nil, fmt.Errorf("invalid Parquet metadata: %w", err)
		}
		var header [4]byte
		if _, err = p.handle.ReadAt(header[:], 0); err != nil {
			return nil, err
		}
		if string(header[:]) != "PAR1" {
			return nil, errors.New("unsupported or invalid Parquet header")
		}
		if err = validateEmbeddedSchemas(metadataBytes, func(n int64) error {
			return p.reserve(n, "embedded Arrow schema")
		}); err != nil {
			return nil, err
		}
		meta, e := metadata.NewFileMetaData(metadataBytes, nil)
		if e != nil {
			return nil, fmt.Errorf("parquet: could not read footer: %w", e)
		}
		meta.SetSourceFileSize(obj.Size)
		p.file, err = newParquetFile(p, io.NewSectionReader(p.handle, 0, obj.Size), meta)
		if err != nil {
			return nil, err
		}
		ar, e := pqarrow.NewFileReader(p.file, parquetArrowProperties(d), d.budget.Allocator())
		if e != nil {
			return nil, e
		}
		p.schema, err = ar.Schema()
		if err != nil {
			return nil, err
		}
		for _, f := range p.schema.Fields() {
			if _, e := optionsForArrow(f.Type); e != nil {
				return nil, e
			}
			p.header = append(p.header, f.Name)
		}
		if err = validateNames(p.header); err != nil {
			return nil, err
		}
		if d.cfg.Input.Schema == nil && len(d.columns) > 0 {
			for _, f := range p.schema.Fields() {
				index := d.index(d.rename(f.Name))
				if index >= 0 && !arrow.TypeEqual(d.columns[index].typ, f.Type) {
					if err = d.addIssue(d.rename(f.Name), "incompatible Parquet physical schema"); err != nil {
						return nil, err
					}
				}
			}
		}
		if err = p.reserve(512+int64(len(p.header))*64, "Parquet preparation cache"); err != nil {
			return nil, err
		}
		if err = validateParquetStructure(meta, p.schema, obj.Size-parquetFooterBytes-size); err != nil {
			return nil, err
		}
		digest := sha256.Sum256(metadataBytes)
		prepared = &parquetPreparation{metadata: meta, schema: p.schema, header: p.header,
			footerSHA256: hex.EncodeToString(digest[:]), footerOffset: obj.Size - parquetFooterBytes - size,
			footerLength: size, baseline: !d.plan && !d.statisticsPending}
		if err = d.store.SetParquetMetadataRange(obj, prepared.footerOffset); err != nil {
			return nil, err
		}
		if d.parquet == nil {
			d.parquet = make(map[string]*parquetPreparation)
		}
		d.parquet[obj.URI] = prepared
		p.prepared = prepared
		// Preparation is immutable and shared across every replay and worker.
		d.retained += p.charge
		p.charge = 0
	} else {
		p.schema, p.header = prepared.schema, prepared.header
		p.file, err = newParquetFile(p, io.NewSectionReader(p.handle, 0, obj.Size), prepared.metadata)
		if err != nil {
			return nil, err
		}
	}
	if rows {
		reader, openErr := newOrderedParquetRecords(ctx, p)
		if openErr != nil {
			return nil, openErr
		}
		p.reader = reader
	}
	complete = true
	return p, nil
}
func (p *parquetInput) close() error {
	var err error
	if p.reader != nil {
		p.reader.Release()
		err = p.reader.Err()
		p.reader = nil
	}
	if p.decoder != nil {
		p.decoder.Close()
		p.decoder = nil
	}
	p.compressed, p.decompressed, p.verified = nil, nil, nil
	p.pageStates, p.dictStates = nil, nil
	p.values, p.nativeIndices = nil, nil
	p.record = nil
	if p.file != nil {
		pool := p.file.BufferPool()
		pool.New = nil
		for {
			v := pool.Get()
			if v == nil {
				break
			}
			if b, ok := v.(*memory.Buffer); ok {
				runtime.SetFinalizer(b, nil)
				b.Release()
			}
		}
		p.file = nil
	}
	if p.handle != nil {
		if closeErr := p.handle.Close(); err == nil {
			err = closeErr
		}
		p.handle = nil
	}
	p.d.budget.Release(p.charge)
	p.charge = 0
	return err
}
func (p *parquetInput) next(ctx context.Context) (lexicalRecord, error) {
	if e := ctx.Err(); e != nil {
		return lexicalRecord{}, e
	}
	for p.record == nil || p.row >= int(p.record.NumRows()) {
		if !p.advance(ctx) {
			if e := p.reader.Err(); e != nil {
				return lexicalRecord{}, e
			}
			return lexicalRecord{}, io.EOF
		}
		if p.evidence != nil {
			return lexicalRecord{}, errParquetEvidence
		}
		p.record = p.reader.RecordBatch()
		p.row = 0
	}
	if p.values == nil {
		if p.d.recordLimit > (math.MaxInt64-int64(len(p.header))*64)/4 {
			return lexicalRecord{}, errors.New("Parquet lexical scratch size overflows accounting")
		}
		if err := p.reserve(p.d.recordLimit*4+int64(len(p.header))*64, "Parquet lexical row scratch"); err != nil {
			return lexicalRecord{}, err
		}
		p.values = make([]cell, len(p.header))
	}
	if err := validateParquetRowWidth(p.record, p.row, p.d.recordLimit); err != nil {
		return lexicalRecord{}, err
	}
	for i := range p.header {
		p.values[i] = parquetCell(p.record.Column(i), p.row)
	}
	p.row++
	p.groupRow++
	// The reject representation is encoded from names/values only when the
	// row has an ingestion issue (datasetReader.appendRecord).
	return lexicalRecord{names: p.header, values: p.values}, nil
}

// parquetCellOverhead is the per-cell allowance counted against
// max_record_bytes besides name and text, matching the cell representation.
const parquetCellOverhead = 32

func parquetValueWidth(column arrow.Array, row int) int64 {
	if column.IsNull(row) {
		return 0
	}
	switch values := column.(type) {
	case *array.String:
		return int64(values.ValueLen(row))
	case *array.LargeString:
		return int64(values.ValueLen(row))
	case *array.Binary:
		return int64(values.ValueLen(row))
	case *array.LargeBinary:
		return int64(values.ValueLen(row))
	case *array.FixedSizeBinary:
		return int64(len(values.Value(row)))
	case *array.Dictionary:
		return parquetValueWidth(values.Dictionary(), values.GetValueIndex(row))
	}
	if fixed, ok := column.DataType().(arrow.FixedWidthDataType); ok {
		return int64(max(1, fixed.Bytes()))
	}
	return int64(len(column.ValueStr(row)))
}

// parquetCell renders one Arrow value lexically for the shared converter.
func parquetCell(a arrow.Array, row int) cell {
	if a.IsNull(row) {
		return cell{present: true, kind: kindNull}
	}
	v := cell{present: true, kind: kindString}
	switch x := a.(type) {
	case *array.Binary:
		v.kind, v.text = kindBytes, string(x.Value(row))
	case *array.FixedSizeBinary:
		v.kind, v.text = kindBytes, string(x.Value(row))
	case *array.Boolean:
		v.kind, v.text = kindBool, x.ValueStr(row)
	case *array.Timestamp:
		switch x.DataType().(*arrow.TimestampType).Unit {
		case arrow.Second:
			v.kind = kindTimestampSecond
		case arrow.Millisecond:
			v.kind = kindTimestampMillisecond
		case arrow.Microsecond:
			v.kind = kindTimestampMicrosecond
		case arrow.Nanosecond:
			v.kind = kindTimestampNanosecond
		}
		v.text = strconv.FormatInt(int64(x.Value(row)), 10)
	case *array.Date32:
		v.kind = kindDate32
		v.text = strconv.FormatInt(int64(x.Value(row)), 10)
	case *array.Date64:
		v.kind = kindDate64
		v.text = strconv.FormatInt(int64(x.Value(row)), 10)
	default:
		v.text = a.ValueStr(row)
		id := a.DataType().ID()
		if arrow.IsInteger(id) || arrow.IsFloating(id) || id == arrow.DECIMAL128 {
			v.kind = kindNumber
		}
	}
	return v
}

func optionsForArrow(t arrow.DataType) (config.Column, error) {
	c := config.Column{Type: t.Name()}
	switch t.ID() {
	case arrow.NULL:
		c.Type = "null"
	case arrow.BOOL:
		c.Type = "bool"
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64, arrow.FLOAT32, arrow.FLOAT64:
	case arrow.STRING:
		c.Type = "string"
	case arrow.BINARY, arrow.FIXED_SIZE_BINARY:
		c.Type = "binary"
	case arrow.DATE32, arrow.DATE64:
		c.Type = "date"
	case arrow.TIMESTAMP:
		c.Type = "timestamp"
		c.Timezone = t.(*arrow.TimestampType).TimeZone
	case arrow.DECIMAL128:
		dt := t.(*arrow.Decimal128Type)
		c.Type = "decimal"
		c.Precision = dt.Precision
		c.Scale = dt.Scale
	default:
		return c, fmt.Errorf("unsupported or nested Parquet type %s", t)
	}
	return c, nil
}
