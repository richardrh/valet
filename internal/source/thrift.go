package source

//lint:file-ignore ST1005 Error strings here begin with the proper nouns Parquet or Arrow.

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/apache/thrift/lib/go/thrift"
)

// metadataMaxDepth bounds nesting of Thrift structs and collections in Parquet
// footers and page headers. Parquet's own schema never nests deeper than a
// handful of levels; the limit only stops hostile recursion.
const metadataMaxDepth = thrift.DEFAULT_RECURSION_DEPTH

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
	return validateSchemaFooter(raw)
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
