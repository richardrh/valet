package source

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/richardrh/valet/internal/resource"
)

// Parquet PageType enum values.
const (
	pageData       = 0 // DATA_PAGE
	pageIndex      = 1 // INDEX_PAGE
	pageDictionary = 2 // DICTIONARY_PAGE
	pageDataV2     = 3 // DATA_PAGE_V2
)

// PageHeader and sub-header Thrift field IDs from parquet.thrift.
const (
	pageTypeField         = 1 // PageHeader.type
	pageUncompressedField = 2 // PageHeader.uncompressed_page_size
	pageCompressedField   = 3 // PageHeader.compressed_page_size
	pageCRCField          = 4 // PageHeader.crc (over the original compressed body)
	pageDataHeaderField   = 5 // PageHeader.data_page_header
	pageDictHeaderField   = 7 // PageHeader.dictionary_page_header
	pageDataV2HeaderField = 8 // PageHeader.data_page_header_v2
	pageHeaderFieldCount  = 10

	dataValuesField     = 1 // DataPageHeader/DictionaryPageHeader/DataPageHeaderV2.num_values
	dataEncodingField   = 2 // DataPageHeader/DictionaryPageHeader.encoding
	dataDefEncodingFld  = 3 // DataPageHeader.definition_level_encoding
	dataRepEncodingFld  = 4 // DataPageHeader.repetition_level_encoding
	v2NullsField        = 2 // DataPageHeaderV2.num_nulls
	v2RowsField         = 3 // DataPageHeaderV2.num_rows
	v2EncodingField     = 4 // DataPageHeaderV2.encoding
	v2DefBytesField     = 5 // DataPageHeaderV2.definition_levels_byte_length
	v2RepBytesField     = 6 // DataPageHeaderV2.repetition_levels_byte_length
	v2CompressedField   = 7 // DataPageHeaderV2.is_compressed
	subHeaderFieldCount = 10
)

// Budget estimates for the page verifier and Arrow's typed page decoders.
const (
	// maxPageHeaderBytes caps one encoded Thrift page header independently of
	// record/batch limits. Real headers are hundreds of bytes even with
	// statistics; this only stops a header from streaming unbounded data.
	maxPageHeaderBytes = 16 << 20
	// pageChunkBufferBytes is the bufio size used to read one column chunk.
	pageChunkBufferBytes = 4096
	// pageDecoderFixedBytes is the fixed working set of one Arrow page or
	// dictionary decoder (level decoders, scratch buffers) and the slack added
	// to the verifier's own compressed+uncompressed buffers.
	pageDecoderFixedBytes = 65536
	// pageHeaderExpansion approximates Arrow's in-memory cost per encoded
	// header byte (generated Thrift structs plus copied statistics).
	pageHeaderExpansion = 256
	// pageValueStateBytes is Arrow's per-declared-value decoder state.
	pageValueStateBytes = 64
	// pageUncompressedFactor scales the uncompressed page size to cover the
	// decompressed body plus decoded output buffers.
	pageUncompressedFactor = 4
	// dictValueStateBytes is the retained per-entry cost of a decoded dictionary.
	dictValueStateBytes = 128
	// dictUncompressedFactor scales a dictionary page's body for its retained copy.
	dictUncompressedFactor = 2
)

type guardedPageHeader struct {
	kind, compressed, uncompressed, values, encoding int64
	definitionEncoding, repetitionEncoding           int64
	nulls, rows, definitionBytes, repetitionBytes    int64
	compressedV2                                     bool
	crc                                              int64
	hasCRC                                           bool
	bodyKind                                         int
}

func readGuardedPageHeader(ctx context.Context, r io.Reader, limit int) (guardedPageHeader, int, error) {
	h := guardedPageHeader{
		kind: -1, compressed: -1, uncompressed: -1, values: -1, encoding: -1,
		definitionEncoding: -1, repetitionEncoding: -1, compressedV2: true,
	}
	c := newStreamCompactReader(ctx, r, limit)
	seen := [pageHeaderFieldCount]bool{}
	integer := func(k thrift.TType) (int64, error) {
		v, e := c.integer(k)
		if e == nil && (v < math.MinInt32 || v > math.MaxInt32) {
			return 0, errors.New("Parquet page integer exceeds int32")
		}
		return v, e
	}
	e := c.fields(func(id int, k thrift.TType) error {
		if id < len(seen) {
			if seen[id] {
				return errors.New("duplicate Parquet page header field")
			}
			seen[id] = true
		}
		var e error
		switch id {
		case pageTypeField:
			h.kind, e = integer(k)
		case pageUncompressedField:
			h.uncompressed, e = integer(k)
		case pageCompressedField:
			h.compressed, e = integer(k)
		case pageCRCField:
			h.crc, e = integer(k)
			h.hasCRC = true
		case pageDataHeaderField, pageDictHeaderField, pageDataV2HeaderField:
			if k != thrift.STRUCT || h.bodyKind != 0 {
				return errors.New("invalid Parquet page subheader")
			}
			h.bodyKind = id
			innerSeen := [subHeaderFieldCount]bool{}
			e = c.fields(func(field int, kind thrift.TType) error {
				if field < len(innerSeen) {
					if innerSeen[field] {
						return errors.New("duplicate Parquet data header field")
					}
					innerSeen[field] = true
				}
				if id == pageDataV2HeaderField && field == v2CompressedField {
					if kind != thrift.BOOL {
						return errors.New("invalid Parquet page compression flag")
					}
					var e error
					h.compressedV2, e = c.boolean(kind)
					return e
				}
				value := h.subHeaderField(id, field)
				if value == nil {
					return c.skip(kind, 2)
				}
				v, e := integer(kind)
				*value = v
				return e
			})
		default:
			e = c.skip(k, 1)
		}
		return e
	})
	if e != nil {
		return h, c.pos(), e
	}
	if h.kind < pageData || h.kind > pageDataV2 || h.compressed < 0 || h.uncompressed < 0 {
		return h, c.pos(), errors.New("invalid Parquet page dimensions")
	}
	mismatch := (h.kind == pageData && h.bodyKind != pageDataHeaderField) ||
		(h.kind == pageDictionary && h.bodyKind != pageDictHeaderField) ||
		(h.kind == pageDataV2 && h.bodyKind != pageDataV2HeaderField)
	if mismatch {
		return h, c.pos(), errors.New("Parquet page type/subheader mismatch")
	}
	return h, c.pos(), nil
}

// subHeaderField maps an integer field of the given sub-header to its slot,
// or nil when the field is not tracked.
func (h *guardedPageHeader) subHeaderField(subHeader, field int) *int64 {
	switch subHeader {
	case pageDataHeaderField:
		switch field {
		case dataValuesField:
			return &h.values
		case dataEncodingField:
			return &h.encoding
		case dataDefEncodingFld:
			return &h.definitionEncoding
		case dataRepEncodingFld:
			return &h.repetitionEncoding
		}
	case pageDictHeaderField:
		switch field {
		case dataValuesField:
			return &h.values
		case dataEncodingField:
			return &h.encoding
		}
	case pageDataV2HeaderField:
		switch field {
		case dataValuesField:
			return &h.values
		case v2NullsField:
			return &h.nulls
		case v2RowsField:
			return &h.rows
		case v2EncodingField:
			return &h.encoding
		case v2DefBytesField:
			return &h.definitionBytes
		case v2RepBytesField:
			return &h.repetitionBytes
		}
	}
	return nil
}

// validatePages verifies one row group before any typed decoder can consume it.
// The verified uncompressed pages are retained in a bounded worker-owned stream;
// Arrow consumes that stream without rereading or decompressing original pages.
func (p *parquetInput) validatePages(ctx context.Context, fileSize int64) error {
	pageLimit := max(p.d.recordLimit, p.d.batchTarget)
	headerLimit := min(pageLimit, int64(maxPageHeaderBytes))
	pageStates, dictStates := p.pageStates, p.dictStates
	for group := p.guardGroup; group < p.guardGroup+1; group++ {
		rg := p.file.MetaData().RowGroup(group)
		if rg.NumRows() < 0 {
			return errors.New("negative Parquet row-group row count")
		}
		if rg.NumColumns() != len(p.header) {
			return errors.New("row-group column count differs from schema")
		}
		for column := range rg.NumColumns() {
			chunk, e := rg.ColumnChunk(column)
			if e != nil {
				return e
			}
			normalized := p.bridge.RowGroups[0].Columns[column].MetaData
			normalizedStart := int64(len(p.verified))
			normalized.DataPageOffset = normalizedStart
			normalized.DictionaryPageOffset = nil
			descriptor := p.file.MetaData().Schema.Column(column)
			if chunk.Type() != descriptor.PhysicalType() {
				return errors.New("Parquet chunk physical type differs from schema")
			}
			if chunk.FilePath() != "" {
				return errors.New("external Parquet column chunks are unsupported")
			}
			if chunk.CryptoMetadata() != nil {
				return errors.New("encrypted Parquet column chunks are unsupported")
			}
			codec := chunk.Compression()
			if codec != compress.Codecs.Uncompressed && codec != compress.Codecs.Snappy && codec != compress.Codecs.Zstd {
				return fmt.Errorf("unsupported Parquet codec %s", codec)
			}
			if codec == compress.Codecs.Zstd && p.decoder == nil {
				if e = p.reserve(resource.ParquetCodecWorkspace, "Parquet Zstd workspace"); e != nil {
					return e
				}
				p.decoder, e = resource.NewParquetDecoder()
				if e != nil {
					return e
				}
			}
			if descriptor.MaxRepetitionLevel() != 0 || descriptor.MaxDefinitionLevel() > 1 {
				return errors.New("nested Parquet levels are unsupported")
			}
			if chunk.NumValues() != rg.NumRows() {
				return errors.New("flat Parquet chunk count differs from row-group count")
			}
			start := chunk.DataPageOffset()
			dictionaryOffset := chunk.DictionaryPageOffset()
			if dictionaryOffset > 0 {
				if start == 0 && chunk.NumValues() == 0 {
					// Empty dictionary-encoded chunks may contain only a
					// dictionary page and use zero for the absent data page.
					start = dictionaryOffset
				} else {
					start = min(start, dictionaryOffset)
				}
			}
			size := chunk.TotalCompressedSize()
			if size == 0 && chunk.NumValues() == 0 {
				continue
			}
			if start < 4 || size < 0 || start > fileSize || size > fileSize-start {
				return errors.New("Parquet column chunk lies outside the object")
			}
			section := io.NewSectionReader(p.handle, start, size)
			input := bufio.NewReaderSize(section, pageChunkBufferBytes)
			remaining := size
			var total, dictCount int64
			dictCount = -1
			ensurePage := func(n int64) error {
				if n < 0 {
					return errors.New("Parquet page state accounting overflow")
				}
				if n > pageStates[column] {
					if e := p.reserve(n-pageStates[column], "Parquet verified page decoder state"); e != nil {
						return e
					}
					pageStates[column] = n
				}
				return nil
			}
			ensureDict := func(n int64) error {
				if n < 0 {
					return errors.New("Parquet dictionary state accounting overflow")
				}
				if n > dictStates[column] {
					if e := p.reserve(n-dictStates[column], "Parquet verified dictionary state"); e != nil {
						return e
					}
					dictStates[column] = n
				}
				return nil
			}
			for remaining > 0 {
				if e = ctx.Err(); e != nil {
					return e
				}
				h, headerBytes, e := readGuardedPageHeader(ctx, input, int(min(remaining, headerLimit)))
				if e != nil {
					return fmt.Errorf("invalid Parquet page header: %w", e)
				}
				remaining -= int64(headerBytes)
				if h.compressed > pageLimit || h.uncompressed > pageLimit {
					return &resource.ResourceLimitError{
						Phase:     "Parquet page declared size",
						Limit:     p.d.budget.Limit(),
						Used:      p.d.budget.Used(),
						Requested: max(h.compressed, h.uncompressed),
					}
				}
				if h.compressed > remaining {
					return errors.New("Parquet page exceeds column chunk boundary")
				}
				headerState := int64(headerBytes)*pageHeaderExpansion + pageDecoderFixedBytes
				if e = ensurePage(headerState); e != nil {
					return e
				}
				if h.kind == pageIndex {
					if e = p.readCompressed(input, h); e != nil {
						return e
					}
					remaining -= h.compressed
					continue
				}
				if chunk.DataPageOffset() == 0 && h.kind != pageDictionary {
					return errors.New("Parquet data page has no declared offset")
				}
				if h.values < 0 || h.encoding < 0 {
					return errors.New("negative Parquet page count or encoding")
				}
				encoding := parquet.Encoding(h.encoding)
				if h.kind == pageDictionary {
					if dictCount >= 0 || total != 0 {
						return errors.New("misplaced or duplicate dictionary page")
					}
					plain := encoding == parquet.Encodings.Plain || encoding == parquet.Encodings.PlainDict
					if chunk.Type() == parquet.Types.Boolean || !plain {
						return errors.New("unsupported dictionary type or encoding")
					}
				} else {
					if h.values > chunk.NumValues()-total {
						return errors.New("page values exceed chunk count")
					}
					if e = validatePageEncoding(chunk.Type(), encoding); e != nil {
						return e
					}
				}
				base := h.values*pageValueStateBytes + h.uncompressed*pageUncompressedFactor + headerState
				extraState := int64(0)
				ensure := func(extra int64) error {
					if e := ctx.Err(); e != nil {
						return e
					}
					if extra < 0 || extra > math.MaxInt64-base {
						return errors.New("Parquet decoder state overflow")
					}
					extraState = max(extraState, extra)
					return ensurePage(base + extraState)
				}
				if e = ensure(0); e != nil {
					return e
				}
				body := pageBody{
					header:        h,
					codec:         chunk.Compression(),
					physical:      chunk.Type(),
					fixedWidth:    descriptor.TypeLength(),
					maxDefinition: descriptor.MaxDefinitionLevel(),
					dictCount:     dictCount,
					ensure:        ensure,
				}
				pageStart := int64(len(p.verified))
				if h.kind == pageDictionary {
					normalized.DictionaryPageOffset = &pageStart
				} else if total == 0 {
					normalized.DataPageOffset = pageStart
				}
				if e = p.validatePageBody(input, body); e != nil {
					return e
				}
				remaining -= h.compressed
				if h.kind == pageDictionary {
					dictCount = h.values
					dictState := h.values*dictValueStateBytes + h.uncompressed*dictUncompressedFactor + pageDecoderFixedBytes
					if e = ensureDict(dictState); e != nil {
						return e
					}
				} else {
					total += h.values
				}
			}
			if total != chunk.NumValues() {
				return errors.New("Parquet page counts do not match column metadata")
			}
			normalized.TotalCompressedSize = int64(len(p.verified)) - normalizedStart
			normalized.TotalUncompressedSize = normalized.TotalCompressedSize
		}
	}
	return nil
}

// pageBody describes one page whose header has been verified and whose
// compressed payload is next on the column chunk reader.
type pageBody struct {
	header        guardedPageHeader
	codec         compress.Compression
	physical      parquet.Type
	fixedWidth    int
	maxDefinition int16
	// dictCount is the verified dictionary cardinality, or -1 before a dictionary page.
	dictCount int64
	// ensure reserves extra decoder state beyond the page's base charge.
	ensure func(int64) error
}

func (p *parquetInput) validatePageBody(r io.Reader, body pageBody) error {
	h := body.header
	if e := p.readCompressed(r, h); e != nil {
		return e
	}
	compressed := p.compressed
	if e := p.resizeBuffer(&p.decompressed, int(h.uncompressed), "Parquet verifier decompressed buffer", false); e != nil {
		return e
	}
	data := p.decompressed
	levels := int64(0)
	if h.kind == pageDataV2 {
		if h.definitionBytes < 0 || h.repetitionBytes != 0 || h.nulls < 0 || h.nulls > h.values || h.rows != h.values {
			return errors.New("invalid flat Parquet V2 level counts")
		}
		levels = h.definitionBytes
		if levels > h.compressed || levels > h.uncompressed {
			return errors.New("Parquet V2 levels exceed page size")
		}
		copy(data[:levels], compressed[:levels])
	}
	if h.kind == pageDataV2 && !h.compressedV2 {
		if h.compressed != h.uncompressed {
			return errors.New("uncompressed V2 page size mismatch")
		}
		copy(data, compressed)
	} else {
		var decoded []byte
		var e error
		if p.decoder != nil {
			decoded, e = p.decoder.Decode(body.codec, data[levels:], compressed[levels:])
		} else {
			codec, codecErr := compress.GetCodec(body.codec)
			if codecErr != nil {
				return codecErr
			}
			decoded, e = compress.Decode(codec, data[levels:], compressed[levels:])
		}
		if e != nil {
			return fmt.Errorf("invalid Parquet compressed page: %w", e)
		}
		if len(decoded) != len(data)-int(levels) {
			return errors.New("decompressed Parquet page size mismatch")
		}
	}
	if h.kind == pageDictionary {
		if e := validatePlain(data, h.values, body.physical, body.fixedWidth, p.d.recordLimit); e != nil {
			return e
		}
		return p.appendVerifiedPage(h, data)
	}
	values := h.values
	offset := int64(0)
	if h.kind == pageData {
		repetition := parquet.Encoding(h.repetitionEncoding)
		if repetition != parquet.Encodings.RLE && repetition != parquet.Encodings.BitPacked {
			return errors.New("unsupported repetition-level encoding")
		}
		if body.maxDefinition > 0 {
			switch parquet.Encoding(h.definitionEncoding) {
			case parquet.Encodings.RLE:
				if len(data) < 4 {
					return io.ErrUnexpectedEOF
				}
				n := int64(binary.LittleEndian.Uint32(data))
				if n > int64(len(data)-4) {
					return errors.New("definition levels exceed page payload")
				}
				ones, e := scanHybrid(data[4:4+n], 1, h.values, 1)
				if e != nil {
					return e
				}
				values = ones
				offset = 4 + n
			case parquet.Encodings.BitPacked:
				n := (h.values + 7) / 8
				if n > int64(len(data)) {
					return io.ErrUnexpectedEOF
				}
				values = 0
				for i := range h.values {
					values += int64(packedValue(data[:n], i, 1))
				}
				offset = n
			default:
				return errors.New("unsupported definition-level encoding")
			}
		}
	} else {
		offset = levels
		if body.maxDefinition == 0 {
			if levels != 0 || h.nulls != 0 {
				return errors.New("required V2 field contains null levels")
			}
		} else {
			ones, e := scanHybrid(data[:levels], 1, h.values, 1)
			if e != nil {
				return e
			}
			values = ones
			if h.values-values != h.nulls {
				return errors.New("V2 null count differs from levels")
			}
		}
	}
	if e := validateEncodedValues(encodedValues{
		data:      data[offset:],
		count:     values,
		physical:  body.physical,
		encoding:  parquet.Encoding(h.encoding),
		fixed:     body.fixedWidth,
		dictCount: body.dictCount,
		maxRecord: p.d.recordLimit,
		ensure:    body.ensure,
	}); e != nil {
		return e
	}
	return p.appendVerifiedPage(h, data)
}

// resizeBuffer reserves both old and replacement capacities before growth.
// Buffers never escape the worker, so subsequent pages reuse the high-water mark.
func (p *parquetInput) resizeBuffer(buffer *[]byte, size int, phase string, preserve bool) error {
	if size < 0 {
		return errors.New("Parquet buffer size overflows int")
	}
	if size <= cap(*buffer) {
		*buffer = (*buffer)[:size]
		return nil
	}
	if err := p.reserve(int64(size), phase); err != nil {
		return err
	}
	next := make([]byte, size)
	if preserve {
		copy(next, *buffer)
	}
	old := int64(cap(*buffer))
	*buffer = next
	p.d.budget.Release(old)
	p.charge -= old
	return nil
}

func (p *parquetInput) readCompressed(r io.Reader, h guardedPageHeader) error {
	if err := p.resizeBuffer(&p.compressed, int(h.compressed), "Parquet verifier compressed buffer", false); err != nil {
		return err
	}
	if _, err := io.ReadFull(r, p.compressed); err != nil {
		return err
	}
	if h.hasCRC && crc32.ChecksumIEEE(p.compressed) != uint32(h.crc) {
		return errors.New("Parquet page CRC mismatch")
	}
	return nil
}

// appendVerifiedPage writes only fields needed for sequential decoding. Original
// CRCs are verified before normalization; statistics and indexes are deliberately
// absent because their offsets/checksums describe the original compressed stream.
func (p *parquetInput) appendVerifiedPage(h guardedPageHeader, data []byte) error {
	var scratch [96]byte
	header := scratch[:0]
	integer := func(field byte, value int64) {
		header = append(header, field)
		header = binary.AppendVarint(header, value)
	}
	integer(0x15, h.kind)
	integer(0x15, int64(len(data)))
	integer(0x15, int64(len(data)))
	switch h.kind {
	case pageData:
		header = append(header, 0x2c)
		integer(0x15, h.values)
		integer(0x15, h.encoding)
		integer(0x15, h.definitionEncoding)
		integer(0x15, h.repetitionEncoding)
	case pageDictionary:
		header = append(header, 0x4c)
		integer(0x15, h.values)
		integer(0x15, h.encoding)
	case pageDataV2:
		header = append(header, 0x5c)
		integer(0x15, h.values)
		integer(0x15, h.nulls)
		integer(0x15, h.rows)
		integer(0x15, h.encoding)
		integer(0x15, h.definitionBytes)
		integer(0x15, h.repetitionBytes)
		header = append(header, 0x12) // is_compressed = false
	default:
		return errors.New("unsupported verified Parquet page type")
	}
	header = append(header, 0, 0) // subheader and PageHeader STOP
	start := len(p.verified)
	if len(header) > math.MaxInt-start || len(data) > math.MaxInt-start-len(header) {
		return errors.New("verified Parquet stream size overflow")
	}
	size := start + len(header) + len(data)
	if size > cap(p.verified) {
		capacity := max(size, cap(p.verified)+cap(p.verified)/2)
		if err := p.resizeBuffer(&p.verified, capacity, "verified Parquet row-group stream", true); err != nil {
			return err
		}
	}
	p.verified = p.verified[:size]
	copy(p.verified[start:], header)
	copy(p.verified[start+len(header):], data)
	return nil
}
