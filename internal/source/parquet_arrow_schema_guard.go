package source

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/apache/thrift/lib/go/thrift"
)

// KeyValue Thrift field IDs from parquet.thrift.
const (
	keyValueKeyField   = 1 // KeyValue.key
	keyValueValueField = 2 // KeyValue.value
)

// arrowSchemaKey is the Parquet key/value entry pqarrow uses for the
// base64-encoded Arrow IPC schema message.
const arrowSchemaKey = "ARROW:schema"

// Budget estimates for the embedded schema verifier and pqarrow's decoding.
const (
	// arrowSchemaDecodeFactor covers the decoded schema bytes plus pqarrow's
	// copies while reconstructing the Arrow schema.
	arrowSchemaDecodeFactor = 4
	// arrowSchemaFixedBytes is the fixed overhead per embedded schema.
	arrowSchemaFixedBytes = 1024
	// arrowIPCMessageFactor covers the IPC metadata plus its parsed form.
	arrowIPCMessageFactor = 2
	// flatbufferTableBytes is the parsed cost charged per FlatBuffer table.
	flatbufferTableBytes = 256
	// flatbufferVectorEntryBytes is the minimum parsed cost per vector entry.
	flatbufferVectorEntryBytes = 32
	// flatbufferStringFactor covers a string's bytes plus its Go copy.
	flatbufferStringFactor = 2
)

// FlatBuffer wire layout (see flatbuffers/internal docs).
const (
	// uoffsetBytes is the width of a FlatBuffer uoffset_t / soffset_t.
	uoffsetBytes = 4
	// vtableHeaderBytes holds the vtable size and object size (two uint16s).
	vtableHeaderBytes = 4
	// vtableEntryBytes is the width of one vtable field offset.
	vtableEntryBytes = 2
	// tableSoffsetBytes is the leading soffset to the vtable in each table.
	tableSoffsetBytes = 4
)

// Scalar widths of Arrow schema fields.
const (
	widthBool  = 1
	widthUint8 = 1
	widthInt16 = 2
	widthInt32 = 4
	widthInt64 = 8
)

// IPC encapsulation (Arrow columnar format, "Encapsulated message format").
const (
	ipcContinuation       = math.MaxUint32 // 0xFFFFFFFF continuation marker
	ipcLegacyPrefixBytes  = 4              // pre-0.15 int32 length prefix
	ipcContinuationPrefix = 8              // marker + int32 length
	ipcMinMetadataBytes   = 4
	ipcMaxMetadataVersion = 4 // MetadataVersion.V5
	ipcHeaderSchema       = 1 // MessageHeader.Schema
)

// Arrow Message.fbs / Schema.fbs vtable slots.
const (
	messageVersionSlot    = 0 // Message.version: short
	messageHeaderTypeSlot = 1 // Message.header_type: ubyte
	messageHeaderSlot     = 2 // Message.header: table
	messageBodySlot       = 3 // Message.bodyLength: long
	messageMetadataSlot   = 4 // Message.custom_metadata: [KeyValue]

	schemaEndiannessSlot = 0 // Schema.endianness: short
	schemaFieldsSlot     = 1 // Schema.fields: [Field]
	schemaMetadataSlot   = 2 // Schema.custom_metadata: [KeyValue]
	schemaFeaturesSlot   = 3 // Schema.features: [long]

	fieldNameSlot       = 0 // Field.name: string
	fieldNullableSlot   = 1 // Field.nullable: bool
	fieldTypeTypeSlot   = 2 // Field.type_type: ubyte
	fieldTypeSlot       = 3 // Field.type: table
	fieldDictionarySlot = 4 // Field.dictionary: DictionaryEncoding
	fieldChildrenSlot   = 5 // Field.children: [Field]
	fieldMetadataSlot   = 6 // Field.custom_metadata: [KeyValue]

	dictIDSlot        = 0 // DictionaryEncoding.id: long
	dictIndexTypeSlot = 1 // DictionaryEncoding.indexType: Int
	dictOrderedSlot   = 2 // DictionaryEncoding.isOrdered: bool
	dictKindSlot      = 3 // DictionaryEncoding.dictionaryKind: short

	keyValueKeySlot   = 0 // KeyValue.key: string
	keyValueValueSlot = 1 // KeyValue.value: string
)

// Arrow Schema.fbs Type union discriminants.
const (
	arrowTypeNull            = 1
	arrowTypeInt             = 2
	arrowTypeFloatingPoint   = 3
	arrowTypeDecimal         = 7
	arrowTypeDate            = 8
	arrowTypeTime            = 9
	arrowTypeTimestamp       = 10
	arrowTypeInterval        = 11
	arrowTypeList            = 12
	arrowTypeStruct          = 13
	arrowTypeUnion           = 14
	arrowTypeFixedSizeBinary = 15
	arrowTypeFixedSizeList   = 16
	arrowTypeMap             = 17
	arrowTypeDuration        = 18
	arrowTypeLargeList       = 21
	arrowTypeRunEndEncoded   = 22
	arrowTypeListView        = 25
	arrowTypeLargeListView   = 26
	arrowTypeMax             = arrowTypeLargeListView
)

// Arrow type-table slots and enum bounds.
const (
	intBitWidthSlot      = 0 // Int.bitWidth: int
	intSignedSlot        = 1 // Int.is_signed: bool
	floatPrecisionSlot   = 0 // FloatingPoint.precision: short (HALF..DOUBLE)
	floatPrecisionMax    = 2
	decimalPrecisionSlot = 0 // Decimal.precision: int
	decimalScaleSlot     = 1 // Decimal.scale: int
	decimalBitWidthSlot  = 2 // Decimal.bitWidth: int, default 128
	decimalDefaultWidth  = 128
	dateUnitSlot         = 0 // Date.unit: short (DAY, MILLISECOND)
	timeUnitSlot         = 0 // Time.unit: short, default MILLISECOND
	timeBitWidthSlot     = 1 // Time.bitWidth: int, default 32
	timeDefaultWidth     = 32
	timestampUnitSlot    = 0 // Timestamp.unit: short
	timestampZoneSlot    = 1 // Timestamp.timezone: string
	intervalUnitSlot     = 0 // Interval.unit: short (YEAR_MONTH..MONTH_DAY_NANO)
	intervalUnitMax      = 2
	unionModeSlot        = 0 // Union.mode: short (Sparse, Dense)
	unionTypeIDsSlot     = 1 // Union.typeIds: [int]
	maxUnionTypeCodes    = 128
	fixedWidthSlot       = 0 // FixedSizeBinary.byteWidth / FixedSizeList.listSize: int
	mapKeysSortedSlot    = 0 // Map.keysSorted: bool
	durationUnitSlot     = 0 // Duration.unit: short, default MILLISECOND
	timeUnitSecond       = 0 // TimeUnit.SECOND
	timeUnitMillisecond  = 1 // TimeUnit.MILLISECOND
	timeUnitMicrosecond  = 2 // TimeUnit.MICROSECOND
	timeUnitNanosecond   = 3 // TimeUnit.NANOSECOND
)

// arrowFieldMaxDepth bounds nesting of embedded Arrow fields; it matches the
// Thrift metadata nesting bound.
const arrowFieldMaxDepth = metadataMaxDepth

// validateEmbeddedSchemas preserves ARROW:schema metadata, but checks framing,
// all reachable FlatBuffer ranges, and expansion costs before pqarrow sees it.
func validateEmbeddedSchemas(raw []byte, reserve func(int64) error) error {
	c := newRawCompactReader(raw)
	seen := false
	return c.fields(func(id int, kind thrift.TType) error {
		if id != fileMetaKeyValueField {
			return c.skip(kind, 1)
		}
		if kind != thrift.LIST {
			return errors.New("Parquet key/value metadata must be a list")
		}
		elem, count, e := c.ReadListBegin(c.ctx)
		if e != nil {
			return e
		}
		if elem != thrift.STRUCT {
			return errors.New("invalid Parquet key/value count")
		}
		for range count {
			key, value, e := c.keyValue()
			if e != nil {
				return e
			}
			if !bytes.Equal(key, []byte(arrowSchemaKey)) {
				continue
			}
			if seen {
				return errors.New("duplicate ARROW:schema metadata")
			}
			seen = true
			if e = validateEmbeddedSchema(value, reserve); e != nil {
				return e
			}
		}
		return c.ReadListEnd(c.ctx)
	})
}

// keyValue reads one KeyValue struct, returning sub-slices of the footer.
func (c *compactReader) keyValue() (key, value []byte, err error) {
	seenKey, seenValue := false, false
	err = c.fields(func(field int, k thrift.TType) error {
		if field != keyValueKeyField && field != keyValueValueField {
			return c.skip(k, 2)
		}
		if k != thrift.STRING {
			return errors.New("invalid Parquet key/value string")
		}
		s, e := c.rawBinary()
		if e != nil {
			return e
		}
		if field == keyValueKeyField {
			if seenKey {
				return errors.New("duplicate metadata key field")
			}
			seenKey = true
			key = s
		} else {
			if seenValue {
				return errors.New("duplicate metadata value field")
			}
			seenValue = true
			value = s
		}
		return nil
	})
	return key, value, err
}
func validateEmbeddedSchema(value []byte, reserve func(int64) error) error {
	bound := max(base64.StdEncoding.DecodedLen(len(value)), base64.RawStdEncoding.DecodedLen(len(value)))
	if e := reserve(int64(bound)*arrowSchemaDecodeFactor + arrowSchemaFixedBytes); e != nil {
		return e
	}
	decoded := make([]byte, bound)
	n, e := base64.StdEncoding.Decode(decoded, value)
	if e != nil {
		n, e = base64.RawStdEncoding.Decode(decoded, value)
	}
	if e != nil {
		return errors.New("invalid base64 ARROW:schema")
	}
	if e = validateArrowIPC(decoded[:n], reserve); e != nil {
		return fmt.Errorf("invalid embedded Arrow schema: %w", e)
	}
	return nil
}

type guardedFlatbuffer struct {
	data    []byte
	reserve func(int64) error
}
type guardedTable struct {
	buffer                                   *guardedFlatbuffer
	position, vtable, vtableSize, objectSize int
}

func (b *guardedFlatbuffer) rangeOK(pos, size int) bool {
	return pos >= 0 && size >= 0 && pos <= len(b.data) && size <= len(b.data)-pos
}
func (b *guardedFlatbuffer) reference(pos int) (int, error) {
	if !b.rangeOK(pos, uoffsetBytes) {
		return 0, errors.New("FlatBuffer offset outside buffer")
	}
	offset := uint64(binary.LittleEndian.Uint32(b.data[pos:]))
	if offset < uoffsetBytes || offset > uint64(len(b.data)-pos) {
		return 0, errors.New("FlatBuffer reference outside buffer")
	}
	return pos + int(offset), nil
}
func (b *guardedFlatbuffer) table(pos int) (guardedTable, error) {
	t := guardedTable{buffer: b, position: pos}
	if !b.rangeOK(pos, tableSoffsetBytes) {
		return t, errors.New("FlatBuffer table outside buffer")
	}
	distance := int64(int32(binary.LittleEndian.Uint32(b.data[pos:])))
	vt := int64(pos) - distance
	if vt < 0 || vt > int64(len(b.data)-vtableHeaderBytes) {
		return t, errors.New("FlatBuffer vtable outside buffer")
	}
	t.vtable = int(vt)
	t.vtableSize = int(binary.LittleEndian.Uint16(b.data[t.vtable:]))
	t.objectSize = int(binary.LittleEndian.Uint16(b.data[t.vtable+vtableEntryBytes:]))
	validVtable := t.vtableSize >= vtableHeaderBytes && t.vtableSize%vtableEntryBytes == 0
	validObject := t.objectSize >= tableSoffsetBytes
	if !validVtable || !validObject || !b.rangeOK(t.vtable, t.vtableSize) || !b.rangeOK(pos, t.objectSize) {
		return t, errors.New("invalid FlatBuffer table dimensions")
	}
	for i := vtableHeaderBytes; i < t.vtableSize; i += vtableEntryBytes {
		offset := int(binary.LittleEndian.Uint16(b.data[t.vtable+i:]))
		if offset != 0 && (offset < tableSoffsetBytes || offset >= t.objectSize) {
			return t, errors.New("FlatBuffer field outside object")
		}
	}
	if e := b.reserve(flatbufferTableBytes); e != nil {
		return t, e
	}
	return t, nil
}
func (t guardedTable) field(slot, width int) (int, error) {
	entry := vtableHeaderBytes + slot*vtableEntryBytes
	if entry >= t.vtableSize {
		return 0, nil
	}
	offset := int(binary.LittleEndian.Uint16(t.buffer.data[t.vtable+entry:]))
	if offset == 0 {
		return 0, nil
	}
	if offset < tableSoffsetBytes || width > t.objectSize-offset || !t.buffer.rangeOK(t.position+offset, width) {
		return 0, errors.New("FlatBuffer field width exceeds object")
	}
	return t.position + offset, nil
}
func (t guardedTable) scalar(slot, width int, def int64) (int64, error) {
	p, e := t.field(slot, width)
	if e != nil || p == 0 {
		return def, e
	}
	data := t.buffer.data[p:]
	switch width {
	case widthUint8:
		return int64(data[0]), nil
	case widthInt16:
		return int64(int16(binary.LittleEndian.Uint16(data))), nil
	case widthInt32:
		return int64(int32(binary.LittleEndian.Uint32(data))), nil
	case widthInt64:
		return int64(binary.LittleEndian.Uint64(data)), nil
	}
	return 0, errors.New("invalid scalar width")
}
func (t guardedTable) reference(slot int) (int, error) {
	p, e := t.field(slot, uoffsetBytes)
	if e != nil || p == 0 {
		return 0, e
	}
	return t.buffer.reference(p)
}
func (t guardedTable) vector(slot, width int) (int, int, error) {
	pos, e := t.reference(slot)
	if e != nil || pos == 0 {
		return 0, 0, e
	}
	if !t.buffer.rangeOK(pos, uoffsetBytes) {
		return 0, 0, errors.New("FlatBuffer vector header outside buffer")
	}
	count := uint64(binary.LittleEndian.Uint32(t.buffer.data[pos:]))
	start := pos + uoffsetBytes
	if count > uint64(len(t.buffer.data)-start)/uint64(width) {
		return 0, 0, errors.New("FlatBuffer vector count exceeds payload")
	}
	if e = t.buffer.reserve(int64(count) * int64(max(width, flatbufferVectorEntryBytes))); e != nil {
		return 0, 0, e
	}
	return start, int(count), nil
}
func (t guardedTable) string(slot int) error {
	start, n, e := t.vector(slot, widthUint8)
	if e != nil || start == 0 {
		return e
	}
	if !t.buffer.rangeOK(start, n+1) || t.buffer.data[start+n] != 0 {
		return errors.New("unterminated FlatBuffer string")
	}
	return t.buffer.reserve(int64(n) * flatbufferStringFactor)
}
func (t guardedTable) metadata(slot int) error {
	start, n, e := t.vector(slot, uoffsetBytes)
	if e != nil {
		return e
	}
	for i := range n {
		pos, e := t.buffer.reference(start + i*uoffsetBytes)
		if e != nil {
			return e
		}
		kv, e := t.buffer.table(pos)
		if e != nil {
			return e
		}
		if e = kv.string(keyValueKeySlot); e != nil {
			return e
		}
		if e = kv.string(keyValueValueSlot); e != nil {
			return e
		}
	}
	return nil
}
func validateArrowIPC(raw []byte, reserve func(int64) error) error {
	if len(raw) < ipcLegacyPrefixBytes {
		return errors.New("truncated IPC framing")
	}
	size := uint64(binary.LittleEndian.Uint32(raw))
	prefix := ipcLegacyPrefixBytes
	if size == ipcContinuation {
		if len(raw) < ipcContinuationPrefix {
			return errors.New("truncated IPC continuation")
		}
		size = uint64(binary.LittleEndian.Uint32(raw[ipcLegacyPrefixBytes:]))
		prefix = ipcContinuationPrefix
	}
	if size < ipcMinMetadataBytes || size > math.MaxInt32 || size > uint64(len(raw)-prefix) {
		return errors.New("IPC metadata length exceeds actual bytes")
	}
	if e := reserve(int64(size)*arrowIPCMessageFactor + arrowSchemaFixedBytes); e != nil {
		return e
	}
	b := guardedFlatbuffer{data: raw[prefix : prefix+int(size)], reserve: reserve}
	root, e := b.reference(0)
	if e != nil {
		return e
	}
	message, e := b.table(root)
	if e != nil {
		return e
	}
	version, e := message.scalar(messageVersionSlot, widthInt16, 0)
	if e != nil || version < 0 || version > ipcMaxMetadataVersion {
		return errors.New("unsupported IPC metadata version")
	}
	header, e := message.scalar(messageHeaderTypeSlot, widthUint8, 0)
	if e != nil || header != ipcHeaderSchema {
		return errors.New("embedded IPC message is not a schema")
	}
	body, e := message.scalar(messageBodySlot, widthInt64, 0)
	if e != nil || body != 0 {
		return errors.New("embedded IPC schema must have no body")
	}
	if e = message.metadata(messageMetadataSlot); e != nil {
		return e
	}
	schemaPos, e := message.reference(messageHeaderSlot)
	if e != nil || schemaPos == 0 {
		return errors.New("IPC schema header missing")
	}
	schema, e := b.table(schemaPos)
	if e != nil {
		return e
	}
	endian, e := schema.scalar(schemaEndiannessSlot, widthInt16, 0)
	if e != nil || endian < 0 || endian > 1 {
		return errors.New("invalid Arrow endianness")
	}
	if _, _, e = schema.vector(schemaFeaturesSlot, widthInt64); e != nil {
		return e
	}
	if e = schema.metadata(schemaMetadataSlot); e != nil {
		return e
	}
	start, count, e := schema.vector(schemaFieldsSlot, uoffsetBytes)
	if e != nil {
		return e
	}
	for i := range count {
		pos, e := b.reference(start + i*uoffsetBytes)
		if e != nil {
			return e
		}
		if _, e = b.arrowField(pos, 1); e != nil {
			return e
		}
	}
	return nil
}
func (b *guardedFlatbuffer) arrowField(pos, depth int) (int64, error) {
	if depth > arrowFieldMaxDepth {
		return 0, errors.New("embedded Arrow field nesting exceeds limit")
	}
	field, e := b.table(pos)
	if e != nil {
		return 0, e
	}
	if e = field.string(fieldNameSlot); e != nil {
		return 0, e
	}
	nullable, e := field.scalar(fieldNullableSlot, widthBool, 0)
	if e != nil || nullable > 1 {
		return 0, errors.New("invalid Arrow nullable flag")
	}
	kind, e := field.scalar(fieldTypeTypeSlot, widthUint8, 0)
	if e != nil || kind < arrowTypeNull || kind > arrowTypeMax {
		return 0, errors.New("unsupported Arrow type union")
	}
	typePos, e := field.reference(fieldTypeSlot)
	if e != nil || typePos == 0 {
		return 0, errors.New("Arrow type union has no value")
	}
	typ, e := b.table(typePos)
	if e != nil {
		return 0, e
	}
	start, children, e := field.vector(fieldChildrenSlot, uoffsetBytes)
	if e != nil {
		return 0, e
	}
	switch kind {
	case arrowTypeList, arrowTypeFixedSizeList, arrowTypeMap, arrowTypeLargeList, arrowTypeListView, arrowTypeLargeListView:
		if children != 1 {
			return 0, errors.New("Arrow list/map must have one child")
		}
	case arrowTypeRunEndEncoded:
		if children != 2 {
			return 0, errors.New("Arrow run-end encoding must have two children")
		}
	case arrowTypeStruct, arrowTypeUnion:
	default:
		if children != 0 {
			return 0, errors.New("Arrow scalar has child fields")
		}
	}
	for i := range children {
		child, e := b.reference(start + i*uoffsetBytes)
		if e != nil {
			return 0, e
		}
		if _, e = b.arrowField(child, depth+1); e != nil {
			return 0, e
		}
	}
	if e = validateArrowType(typ, kind, children); e != nil {
		return 0, e
	}
	if e = field.metadata(fieldMetadataSlot); e != nil {
		return 0, e
	}
	dictionary, e := field.reference(fieldDictionarySlot)
	if e != nil {
		return 0, e
	}
	if dictionary != 0 {
		if e = b.dictionaryEncoding(dictionary); e != nil {
			return 0, e
		}
	}
	return kind, nil
}
func (b *guardedFlatbuffer) dictionaryEncoding(pos int) error {
	dict, e := b.table(pos)
	if e != nil {
		return e
	}
	id, e := dict.scalar(dictIDSlot, widthInt64, 0)
	if e != nil || id < 0 {
		return errors.New("invalid Arrow dictionary ID")
	}
	ordered, e := dict.scalar(dictOrderedSlot, widthBool, 0)
	if e != nil || ordered > 1 {
		return errors.New("invalid dictionary ordering")
	}
	if _, e = dict.scalar(dictKindSlot, widthInt16, 0); e != nil {
		return e
	}
	index, e := dict.reference(dictIndexTypeSlot)
	if e != nil || index == 0 {
		return errors.New("dictionary index type missing")
	}
	integer, e := b.table(index)
	if e != nil {
		return e
	}
	return validateArrowType(integer, arrowTypeInt, 0)
}
func validateArrowType(t guardedTable, kind int64, children int) error {
	scalar := func(slot, width int, def int64, min, max int64) (int64, error) {
		v, e := t.scalar(slot, width, def)
		if e != nil {
			return 0, e
		}
		if v < min || v > max {
			return 0, errors.New("Arrow type parameter out of range")
		}
		return v, nil
	}
	switch kind {
	case arrowTypeInt:
		width, e := scalar(intBitWidthSlot, widthInt32, 0, 8, 64)
		if e != nil {
			return e
		}
		if width != 8 && width != 16 && width != 32 && width != 64 {
			return errors.New("invalid Arrow integer width")
		}
		_, e = scalar(intSignedSlot, widthBool, 0, 0, 1)
		return e
	case arrowTypeFloatingPoint:
		_, e := scalar(floatPrecisionSlot, widthInt16, 0, 0, floatPrecisionMax)
		return e
	case arrowTypeDecimal:
		width, e := t.scalar(decimalBitWidthSlot, widthInt32, decimalDefaultWidth)
		if e != nil {
			return e
		}
		maximum := int64(0)
		switch width {
		case 32:
			maximum = 9
		case 64:
			maximum = 18
		case 128:
			maximum = 38
		case 256:
			maximum = 76
		default:
			return errors.New("invalid Arrow decimal width")
		}
		precision, e := scalar(decimalPrecisionSlot, widthInt32, 0, 1, maximum)
		if e != nil {
			return e
		}
		_, e = scalar(decimalScaleSlot, widthInt32, 0, -maximum, precision)
		return e
	case arrowTypeDate:
		_, e := scalar(dateUnitSlot, widthInt16, 0, 0, 1)
		return e
	case arrowTypeTime:
		unit, e := scalar(timeUnitSlot, widthInt16, timeUnitMillisecond, timeUnitSecond, timeUnitNanosecond)
		if e != nil {
			return e
		}
		width, e := t.scalar(timeBitWidthSlot, widthInt32, timeDefaultWidth)
		if e != nil {
			return e
		}
		if (unit <= timeUnitMillisecond && width != 32) || (unit >= timeUnitMicrosecond && width != 64) {
			return errors.New("invalid Arrow time width/unit")
		}
		return nil
	case arrowTypeTimestamp:
		if _, e := scalar(timestampUnitSlot, widthInt16, timeUnitSecond, timeUnitSecond, timeUnitNanosecond); e != nil {
			return e
		}
		return t.string(timestampZoneSlot)
	case arrowTypeInterval:
		_, e := scalar(intervalUnitSlot, widthInt16, 0, 0, intervalUnitMax)
		return e
	case arrowTypeUnion:
		if _, e := scalar(unionModeSlot, widthInt16, 0, 0, 1); e != nil {
			return e
		}
		start, n, e := t.vector(unionTypeIDsSlot, widthInt32)
		if e != nil {
			return e
		}
		if children > maxUnionTypeCodes || (n != 0 && n != children) {
			return errors.New("invalid Arrow union child/type-code count")
		}
		seen := [maxUnionTypeCodes]bool{}
		for i := range n {
			id := int64(int32(binary.LittleEndian.Uint32(t.buffer.data[start+i*widthInt32:])))
			if id < 0 || id >= maxUnionTypeCodes || seen[id] {
				return errors.New("invalid or duplicate Arrow union code")
			}
			seen[id] = true
		}
		return nil
	case arrowTypeFixedSizeBinary, arrowTypeFixedSizeList:
		// FixedSizeBinary.byteWidth and FixedSizeList.listSize share slot 0.
		_, e := scalar(fixedWidthSlot, widthInt32, 0, 1, math.MaxInt32)
		return e
	case arrowTypeMap:
		_, e := scalar(mapKeysSortedSlot, widthBool, 0, 0, 1)
		return e
	case arrowTypeDuration:
		_, e := scalar(durationUnitSlot, widthInt16, timeUnitMillisecond, timeUnitSecond, timeUnitNanosecond)
		return e
	}
	return nil
}
