package source

//lint:file-ignore ST1005 Error strings here begin with the proper nouns Parquet or Arrow.

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/thrift/lib/go/thrift"
)

// metadataMaxDepth bounds nesting of Thrift structs and collections in Parquet
// footers and page headers. Parquet's own schema never nests deeper than a
// handful of levels; the limit only stops hostile recursion.
const metadataMaxDepth = thrift.DEFAULT_RECURSION_DEPTH

// Thrift field IDs from parquet.thrift.
const (
	fileMetaSchemaField   = 2 // FileMetaData.schema
	fileMetaKeyValueField = 5 // FileMetaData.key_value_metadata

	schemaTypeField        = 1  // SchemaElement.type
	schemaTypeLengthField  = 2  // SchemaElement.type_length
	schemaRepetitionField  = 3  // SchemaElement.repetition_type
	schemaNumChildrenField = 5  // SchemaElement.num_children
	schemaLogicalTypeField = 10 // SchemaElement.logicalType
	schemaFieldCount       = 11 // one past the highest SchemaElement field ID tracked for duplicates
)

// Parquet repetition enum values accepted for flat leaves.
const (
	repetitionRequired = 0
	repetitionOptional = 1
)

// LogicalType union member IDs from parquet.thrift.
const (
	logicalString    = 1
	logicalEnum      = 4
	logicalDecimal   = 5
	logicalDate      = 6
	logicalTime      = 7
	logicalTimestamp = 8
	logicalInteger   = 10
	logicalUnknown   = 11
	logicalJSON      = 12
	logicalBSON      = 13
	logicalUUID      = 14
)

// Field IDs inside individual logical annotations.
const (
	decimalScaleField     = 1 // DecimalType.scale
	decimalPrecisionField = 2 // DecimalType.precision
	temporalUTCField      = 1 // TimeType/TimestampType.isAdjustedToUTC
	temporalUnitField     = 2 // TimeType/TimestampType.unit
	timeUnitMillis        = 1 // TimeUnit.MILLIS
	timeUnitNanos         = 3 // TimeUnit.NANOS
	intBitWidthField      = 1 // IntType.bitWidth
	intSignedField        = 2 // IntType.isSigned
	maxDecimalPrecision   = 38
)

var (
	errReadOnlyTransport   = errors.New("Parquet metadata transport is read-only")
	errCollectionTooLarge  = errors.New("Parquet metadata collection exceeds encoded bytes")
	errMetadataStringBytes = errors.New("Parquet metadata string exceeds encoded bytes")
	errMetadataBoolean     = errors.New("invalid Parquet metadata boolean")
	errMetadataUUID        = errors.New("UUID is not a Parquet metadata type")
)

// validateCompact bounds every Thrift length before generated metadata readers
// allocate collections. Compact protocol counts must be supported by actual
// encoded bytes, not just a small footer containing an enormous claimed count.
func validateCompact(raw []byte) error {
	c := newRawCompactReader(raw)
	if e := c.skip(thrift.STRUCT, 0); e != nil {
		return e
	}
	if c.trans.remaining() != 0 {
		return errors.New("trailing Parquet metadata bytes")
	}
	// A second walk checks the flat schema shape the readers support.
	c = newRawCompactReader(raw)
	seenSchema := false
	return c.fields(func(id int, kind thrift.TType) error {
		if id != fileMetaSchemaField {
			return c.skip(kind, 1)
		}
		if seenSchema {
			return errors.New("duplicate Parquet schema metadata")
		}
		seenSchema = true
		if kind != thrift.LIST {
			return errors.New("Parquet schema must be a list")
		}
		elem, n, e := c.ReadListBegin(c.ctx)
		if e != nil {
			return e
		}
		if elem != thrift.STRUCT || n == 0 {
			return errors.New("invalid Parquet schema element count")
		}
		count := uint64(n)
		for i := range count {
			physical, children, repetition, width := int64(-1), int64(0), int64(-1), int64(0)
			hasType := false
			seen := [schemaFieldCount]bool{}
			e = c.fields(func(field int, k thrift.TType) error {
				if field < len(seen) {
					if seen[field] {
						return errors.New("duplicate Parquet schema field")
					}
					seen[field] = true
				}
				var e error
				switch field {
				case schemaTypeField:
					hasType = true
					physical, e = c.integer(k)
				case schemaTypeLengthField:
					width, e = c.integer(k)
				case schemaRepetitionField:
					repetition, e = c.integer(k)
				case schemaNumChildrenField:
					children, e = c.integer(k)
				case schemaLogicalTypeField:
					e = c.logicalType(k)
				default:
					e = c.skip(k, 2)
				}
				return e
			})
			if e != nil {
				return e
			}
			if children < 0 || uint64(children) > count-i-1 {
				return errors.New("Parquet schema child count exceeds remaining elements")
			}
			if i == 0 {
				if hasType || uint64(children) != count-1 {
					return errors.New("invalid Parquet root child count")
				}
				continue
			}
			if children != 0 || !hasType {
				return errors.New("nested Parquet schema is unsupported")
			}
			if physical < int64(parquet.Types.Boolean) || physical > int64(parquet.Types.FixedLenByteArray) {
				return errors.New("invalid Parquet physical type")
			}
			if repetition != repetitionRequired && repetition != repetitionOptional {
				return errors.New("repeated or invalid Parquet leaf is unsupported")
			}
			if physical == int64(parquet.Types.FixedLenByteArray) && (width <= 0 || width > math.MaxInt32) {
				return errors.New("invalid Parquet fixed binary width")
			}
		}
		return c.ReadListEnd(c.ctx)
	})
}

// compactTransport is a read-only Thrift transport over either an in-memory
// footer or a byte-limited stream. It never buffers ahead, so the position is
// exactly the number of bytes the protocol consumed.
type compactTransport struct {
	raw    []byte
	stream io.Reader
	pos    int
	limit  int
}

var _ thrift.TRichTransport = (*compactTransport)(nil)
var _ thrift.TTransport = (*compactTransport)(nil)

func (t *compactTransport) remaining() int {
	if t.stream != nil {
		return t.limit - t.pos
	}
	return len(t.raw) - t.pos
}
func (t *compactTransport) Read(p []byte) (int, error) {
	n := min(len(p), t.remaining())
	if n <= 0 {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.ErrUnexpectedEOF
	}
	if t.stream != nil {
		read, e := io.ReadFull(t.stream, p[:n])
		t.pos += read
		return read, e
	}
	copy(p, t.raw[t.pos:t.pos+n])
	t.pos += n
	return n, nil
}
func (t *compactTransport) ReadByte() (byte, error) {
	if t.remaining() <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if t.stream != nil {
		var b [1]byte
		if _, e := io.ReadFull(t.stream, b[:]); e != nil {
			return 0, e
		}
		t.pos++
		return b[0], nil
	}
	v := t.raw[t.pos]
	t.pos++
	return v, nil
}

// discard advances past n bytes without materialising them.
func (t *compactTransport) discard(n int) error {
	if n > t.remaining() {
		return io.ErrUnexpectedEOF
	}
	if t.stream != nil {
		if _, e := io.CopyN(io.Discard, t.stream, int64(n)); e != nil {
			return e
		}
	}
	t.pos += n
	return nil
}
func (t *compactTransport) Write([]byte) (int, error)       { return 0, errReadOnlyTransport }
func (t *compactTransport) WriteByte(byte) error            { return errReadOnlyTransport }
func (t *compactTransport) WriteString(string) (int, error) { return 0, errReadOnlyTransport }
func (t *compactTransport) Flush(context.Context) error     { return nil }
func (t *compactTransport) RemainingBytes() uint64          { return uint64(max(t.remaining(), 0)) }
func (t *compactTransport) Open() error                     { return nil }
func (t *compactTransport) IsOpen() bool                    { return true }
func (t *compactTransport) Close() error                    { return nil }

// compactReader drives Thrift's compact protocol as a validation-only walker.
// Generic skipping uses thrift.Skip with a depth limit; the overrides below
// tighten the library for untrusted input:
//   - collection counts must fit in the bytes actually remaining;
//   - binary/string lengths are checked against remaining bytes and skipped
//     without allocation (ReadString/ReadBinary never materialise data; use
//     rawBinary for raw-backed slices);
//   - booleans inside collections must be encoded as compact true/false;
//   - UUID, which Parquet metadata never uses, is rejected.
type compactReader struct {
	*thrift.TCompactProtocol
	ctx         context.Context
	trans       *compactTransport
	pendingBool bool
}

func newRawCompactReader(raw []byte) *compactReader {
	return newCompactReader(context.Background(), &compactTransport{raw: raw})
}
func newStreamCompactReader(ctx context.Context, r io.Reader, limit int) *compactReader {
	return newCompactReader(ctx, &compactTransport{stream: r, limit: limit})
}
func newCompactReader(ctx context.Context, t *compactTransport) *compactReader {
	size := int32(min(max(t.remaining(), 0), math.MaxInt32))
	conf := &thrift.TConfiguration{MaxMessageSize: size, MaxFrameSize: size}
	return &compactReader{TCompactProtocol: thrift.NewTCompactProtocolConf(t, conf), ctx: ctx, trans: t}
}

// pos is the number of bytes consumed so far.
func (c *compactReader) pos() int { return c.trans.pos }

// fields visits each field of the next struct until its stop marker. The
// visitor must consume the field value (via skip or a typed read).
func (c *compactReader) fields(visit func(id int, kind thrift.TType) error) error {
	if _, e := c.ReadStructBegin(c.ctx); e != nil {
		return e
	}
	for {
		_, kind, id, e := c.ReadFieldBegin(c.ctx)
		if e != nil {
			return e
		}
		if kind == thrift.STOP {
			return c.ReadStructEnd(c.ctx)
		}
		if id <= 0 {
			return errors.New("invalid compact field ID")
		}
		if e = visit(int(id), kind); e != nil {
			return e
		}
	}
}

// skip consumes a value of kind found at the given nesting depth.
func (c *compactReader) skip(kind thrift.TType, depth int) error {
	return thrift.Skip(c.ctx, c, kind, metadataMaxDepth-depth+1)
}

// integer reads any compact integer field as a full 64-bit zigzag varint so
// out-of-range values are visible to callers instead of being truncated.
func (c *compactReader) integer(kind thrift.TType) (int64, error) {
	switch kind {
	case thrift.I16, thrift.I32, thrift.I64:
		return c.ReadI64(c.ctx)
	}
	return 0, errors.New("invalid integer metadata field type")
}
func (c *compactReader) boolean(kind thrift.TType) (bool, error) {
	if kind != thrift.BOOL {
		return false, errors.New("invalid boolean metadata field type")
	}
	return c.ReadBool(c.ctx)
}

// binaryLength reads a binary length prefix and bounds it by the bytes that
// actually remain, before any caller can allocate.
func (c *compactReader) binaryLength() (int, error) {
	n, e := binary.ReadUvarint(c.trans)
	if e != nil {
		return 0, e
	}
	if n > c.trans.RemainingBytes() {
		return 0, errMetadataStringBytes
	}
	return int(n), nil
}

// rawBinary returns the next binary value as a sub-slice of the raw footer.
func (c *compactReader) rawBinary() ([]byte, error) {
	if c.trans.stream != nil {
		return nil, errors.New("binary slices require an in-memory transport")
	}
	n, e := c.binaryLength()
	if e != nil {
		return nil, e
	}
	start := c.trans.pos
	c.trans.pos += n
	return c.trans.raw[start:c.trans.pos], nil
}
func (c *compactReader) skipBinary() error {
	n, e := c.binaryLength()
	if e != nil {
		return e
	}
	return c.trans.discard(n)
}

func (c *compactReader) ReadFieldBegin(ctx context.Context) (string, thrift.TType, int16, error) {
	name, kind, id, e := c.TCompactProtocol.ReadFieldBegin(ctx)
	c.pendingBool = e == nil && kind == thrift.BOOL
	return name, kind, id, e
}
func (c *compactReader) ReadBool(ctx context.Context) (bool, error) {
	if c.pendingBool {
		c.pendingBool = false
		return c.TCompactProtocol.ReadBool(ctx)
	}
	b, e := c.trans.ReadByte()
	if e != nil {
		return false, e
	}
	switch b {
	case thrift.COMPACT_BOOLEAN_TRUE:
		return true, nil
	case thrift.COMPACT_BOOLEAN_FALSE:
		return false, nil
	}
	return false, errMetadataBoolean
}
func (c *compactReader) ReadListBegin(ctx context.Context) (thrift.TType, int, error) {
	kind, n, e := c.TCompactProtocol.ReadListBegin(ctx)
	if e == nil && uint64(n) > c.trans.RemainingBytes() {
		e = errCollectionTooLarge
	}
	return kind, n, e
}
func (c *compactReader) ReadSetBegin(ctx context.Context) (thrift.TType, int, error) {
	return c.ReadListBegin(ctx)
}
func (c *compactReader) ReadMapBegin(ctx context.Context) (thrift.TType, thrift.TType, int, error) {
	key, value, n, e := c.TCompactProtocol.ReadMapBegin(ctx)
	if e == nil && uint64(n) > c.trans.RemainingBytes()/2 {
		e = errCollectionTooLarge
	}
	return key, value, n, e
}
func (c *compactReader) ReadString(context.Context) (string, error) { return "", c.skipBinary() }
func (c *compactReader) ReadBinary(context.Context) ([]byte, error) { return nil, c.skipBinary() }
func (c *compactReader) ReadUUID(context.Context) (thrift.Tuuid, error) {
	return thrift.Tuuid{}, errMetadataUUID
}

func (c *compactReader) logicalType(kind thrift.TType) error {
	if kind != thrift.STRUCT {
		return errors.New("logicalType must be a struct")
	}
	members := 0
	err := c.fields(func(id int, k thrift.TType) error {
		members++
		if members != 1 || k != thrift.STRUCT {
			return errors.New("logicalType must contain exactly one annotation")
		}
		switch id {
		case logicalString, logicalEnum, logicalDate, logicalUnknown, logicalJSON, logicalBSON, logicalUUID:
			return c.fields(func(_ int, k thrift.TType) error { return c.skip(k, 3) })
		case logicalDecimal:
			scale, precision := int64(-1), int64(-1)
			seenScale, seenPrecision := false, false
			e := c.fields(func(field int, k thrift.TType) error {
				var target *int64
				var seen *bool
				switch field {
				case decimalScaleField:
					target, seen = &scale, &seenScale
				case decimalPrecisionField:
					target, seen = &precision, &seenPrecision
				default:
					return c.skip(k, 3)
				}
				if *seen || k != thrift.I32 {
					return errors.New("invalid decimal annotation field")
				}
				*seen = true
				v, e := c.integer(k)
				*target = v
				return e
			})
			if e != nil {
				return e
			}
			if scale < 0 || precision < 1 || precision > maxDecimalPrecision || scale > precision {
				return errors.New("unsupported decimal precision or scale")
			}
			return nil
		case logicalTime, logicalTimestamp:
			seenUTC, seenUnit := false, false
			e := c.fields(func(field int, k thrift.TType) error {
				switch field {
				case temporalUTCField:
					if seenUTC || k != thrift.BOOL {
						return errors.New("invalid temporal UTC annotation")
					}
					seenUTC = true
					_, e := c.boolean(k)
					return e
				case temporalUnitField:
					if seenUnit || k != thrift.STRUCT {
						return errors.New("invalid temporal unit annotation")
					}
					seenUnit = true
					units := 0
					e := c.fields(func(unit int, k thrift.TType) error {
						units++
						if units != 1 || unit < timeUnitMillis || unit > timeUnitNanos || k != thrift.STRUCT {
							return errors.New("time unit must contain one of MILLIS, MICROS, NANOS")
						}
						return c.fields(func(_ int, k thrift.TType) error { return c.skip(k, 4) })
					})
					if e != nil {
						return e
					}
					if units != 1 {
						return errors.New("empty temporal time unit")
					}
					return nil
				default:
					return c.skip(k, 3)
				}
			})
			if e != nil {
				return e
			}
			if !seenUTC || !seenUnit {
				return errors.New("temporal annotation lacks required fields")
			}
			return nil
		case logicalInteger:
			seenWidth, seenSigned := false, false
			e := c.fields(func(field int, k thrift.TType) error {
				switch field {
				case intBitWidthField:
					if seenWidth || k != thrift.BYTE {
						return errors.New("invalid integer bit-width annotation")
					}
					seenWidth = true
					width, e := c.ReadByte(c.ctx)
					if e != nil {
						return e
					}
					if width != 8 && width != 16 && width != 32 && width != 64 {
						return errors.New("unsupported integer annotation width")
					}
					return nil
				case intSignedField:
					if seenSigned || k != thrift.BOOL {
						return errors.New("invalid integer signedness annotation")
					}
					seenSigned = true
					_, e := c.boolean(k)
					return e
				default:
					return c.skip(k, 3)
				}
			})
			if e != nil {
				return e
			}
			if !seenWidth || !seenSigned {
				return errors.New("integer annotation lacks required fields")
			}
			return nil
		default:
			return errors.New("unsupported nested or extended Parquet logical annotation")
		}
	})
	if err != nil {
		return err
	}
	if members != 1 {
		return errors.New("empty Parquet logicalType union")
	}
	return nil
}
