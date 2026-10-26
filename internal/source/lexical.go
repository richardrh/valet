package source

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// Fixed decoder state charged when a text object is opened.
const (
	// gzipStateBytes covers compress/flate's 32 KiB history window plus its
	// Huffman tables and the gzip.Reader itself.
	gzipStateBytes = 64 << 10
	// textReadBufferBytes is the bufio.Reader placed under encoding/csv (csv
	// reuses a *bufio.Reader of at least its default 4 KiB size).
	textReadBufferBytes = 4096
	// jsonDecoderFixedBytes covers the jsontext.Decoder struct, its token and
	// name stacks for the configured nesting depth, and its 4 KiB read chunk.
	jsonDecoderFixedBytes = 16 << 10
)

// Record-proportional decoder state, charged at the high-water record size
// because the decoders keep buffer capacity between records.
const (
	// csvRecordCopies: encoding/csv keeps a line buffer (rawBuffer, for lines
	// longer than its bufio buffer) and an unquoted-field buffer
	// (recordBuffer), each up to twice the record after append growth, plus the
	// single string that backs every returned field.
	csvRecordCopies = 5
	// csvDecoderFieldBytes covers encoding/csv's fieldIndexes (8),
	// fieldPositions (16), and the returned string header (16). Retained
	// headers and converted cells are charged separately after discovery.
	csvDecoderFieldBytes  = 40
	csvRetainedFieldBytes = 64
	// jsonBufferCopies: jsontext's internal buffer (doubles to hold the
	// longest token) and the raw-record tape (record plus read-ahead).
	jsonBufferCopies = 4
	// jsonNameIndexFactor bounds jsontext's duplicate-name index: each member
	// costs an 8-byte offset and, past 64 names, a map slot plus string copy
	// (~56 bytes), for a minimal 7-byte member such as "ab":0,.
	jsonNameIndexFactor = 8
	// jsonDecodedCopies covers decoded name and cell strings of one record.
	jsonDecodedCopies = 1
	jsonRecordCopies  = jsonBufferCopies + jsonNameIndexFactor + jsonDecodedCopies
)

// lexicalRecord is one decoded source record. Its slices and raw bytes are
// owned by the producing input and stay valid only until the next call to next.
type lexicalRecord struct {
	names, sourceNames []string
	values             []cell
	// raw holds the verbatim JSON record; nil for CSV and Parquet, whose
	// reject representation is encoded on demand from names and values.
	raw    jsontext.Value
	extras []string
	line   uint64
	// malformed holds the original fields of a CSV record whose field count
	// differs from the header; values is then empty.
	malformed []string
}

// rawSizeBound is an upper bound for rejectRaw's output size, so the budget can
// be charged before encoding.
func (rec *lexicalRecord) rawSizeBound() int64 {
	if rec.raw != nil {
		return int64(len(rec.raw))
	}
	// Quoting expands a byte to at most six (\u00XX); base64 expands less.
	n := int64(2)
	if rec.malformed != nil {
		for _, field := range rec.malformed {
			n += 6*int64(len(field)) + 3
		}
		return n
	}
	for i, name := range rec.names {
		n += 6*int64(len(name)+len(rec.values[i].text)) + 8
	}
	return n
}

// rejectRaw returns an owned copy of the record for reject output.
func (rec *lexicalRecord) rejectRaw() json.RawMessage {
	if rec.raw != nil {
		return json.RawMessage(bytes.Clone(rec.raw))
	}
	if rec.malformed != nil {
		// Fields cannot be matched to the header, so keep them in order.
		out := []byte{'['}
		for i, field := range rec.malformed {
			if i > 0 {
				out = append(out, ',')
			}
			out = appendJSONString(out, field)
		}
		return append(out, ']')
	}
	// Grow by append so the retained capacity tracks the encoded size; the
	// caller charges cap(result) after reserving rawSizeBound for encoding.
	out := []byte{'{'}
	for i, name := range rec.names {
		if i > 0 {
			out = append(out, ',')
		}
		out = appendJSONString(out, name)
		out = append(out, ':')
		v := rec.values[i]
		switch v.kind {
		case kindNull:
			out = append(out, "null"...)
		case kindBool, kindNumber:
			if jsontext.Value(v.text).IsValid() {
				out = append(out, v.text...)
			} else {
				out = appendJSONString(out, v.text)
			}
		case kindBytes:
			out = appendJSONString(out, base64.StdEncoding.EncodeToString([]byte(v.text)))
		default:
			out = appendJSONString(out, v.text)
		}
	}
	return append(out, '}')
}

// appendJSONString quotes s. Invalid UTF-8 (possible only in Parquet values
// that are being rejected for it) becomes U+FFFD; the reported error only
// signals that replacement, so it is deliberately dropped.
func appendJSONString(dst []byte, s string) []byte {
	dst, _ = jsontext.AppendQuote(dst, s)
	return dst
}

type lexicalInput interface {
	next(context.Context) (lexicalRecord, error)
	close() error
	// names returns the source field names shared by every record, or nil
	// when each record carries its own (schema-less JSON).
	names() []string
}

// highWater is a budget reservation that only grows; it backs decoder buffers
// whose capacity persists at the largest record seen.
type highWater struct {
	budget *resource.Budget
	phase  string
	held   int64
}

func (h *highWater) growTo(n int64) error {
	if n <= h.held {
		return nil
	}
	if err := h.budget.Reserve(n-h.held, h.phase); err != nil {
		return err
	}
	h.held = n
	return nil
}
func (h *highWater) release() {
	h.budget.Release(h.held)
	h.held = 0
}

func decoderWorkspace(bytes, copies, items, itemBytes int64) (int64, error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	if bytes < 0 || copies < 0 || items < 0 || itemBytes < 0 ||
		(items != 0 && itemBytes > maxInt64/items) {
		return 0, errors.New("decoder workspace size overflows")
	}
	itemTotal := items * itemBytes
	if copies != 0 && bytes > (maxInt64-itemTotal)/copies {
		return 0, errors.New("decoder workspace size overflows")
	}
	return bytes*copies + itemTotal, nil
}

// recordLimiter sits between the decompressed stream and a library decoder.
// It refuses to deliver bytes more than limit+1 past the start of the current
// record, so an oversized record fails before the decoder buffers it. When
// tape is enabled it also retains the delivered bytes from the current record
// onwards so the verbatim JSON record and its line number can be recovered
// from decoder offsets.
type recordLimiter struct {
	r         io.Reader
	limit     int64
	base      int64 // stream offset where the current record starts
	delivered int64 // total bytes handed to the decoder
	exceeded  bool
	reserve   func(int64) error

	keepTape bool
	tape     []byte // delivered bytes from tapeBase
	tapeBase int64
	tapeLine uint64 // 1-based line number at tapeBase
}

func (l *recordLimiter) Read(p []byte) (int, error) {
	allowed := l.base + l.limit + 1 - l.delivered
	if allowed <= 0 {
		l.exceeded = true
		return 0, l.inputError()
	}
	if int64(len(p)) > allowed {
		p = p[:allowed]
	}
	if l.reserve != nil {
		next := l.delivered + int64(len(p)) - l.base
		if err := l.reserve(next); err != nil {
			return 0, err
		}
	}
	n, err := l.r.Read(p)
	l.delivered += int64(n)
	if l.keepTape {
		l.tape = append(l.tape, p[:n]...)
	}
	return n, err
}

// inputError reports the oversized record at its starting byte offset.
func (l *recordLimiter) inputError() *diagnostics.InputError {
	return &diagnostics.InputError{Reason: diagnostics.ReasonRecordTooLarge, Offset: l.base, Limit: l.limit}
}

// startRecord moves the record start to offset and drops tape bytes before it.
func (l *recordLimiter) startRecord(offset int64) {
	l.base = offset
	if !l.keepTape || offset <= l.tapeBase {
		return
	}
	drop := offset - l.tapeBase
	l.tapeLine += uint64(bytes.Count(l.tape[:drop], []byte{'\n'}))
	l.tape = l.tape[:copy(l.tape, l.tape[drop:])]
	l.tapeBase = offset
}

// span returns retained tape bytes [from, to); both offsets must be retained.
func (l *recordLimiter) span(from, to int64) []byte {
	return l.tape[from-l.tapeBase : to-l.tapeBase]
}

// lineAt returns the 1-based line of a retained offset.
func (l *recordLimiter) lineAt(offset int64) uint64 {
	return l.tapeLine + uint64(bytes.Count(l.span(l.tapeBase, offset), []byte{'\n'}))
}

// lineOf is lineAt for an offset that may be outside the retained tape.
func (l *recordLimiter) lineOf(offset int64) (uint64, bool) {
	if !l.keepTape || offset < l.tapeBase || offset > l.tapeBase+int64(len(l.tape)) {
		return 0, false
	}
	return l.lineAt(offset), true
}

// textStream is an opened, decompressed object with the record limiter on
// top. Its fixed charge is released by close.
type textStream struct {
	uri     string // object URI, for locating malformed input
	raw     io.ReadCloser
	zipped  *gzip.Reader
	limiter *recordLimiter
	input   io.Reader // decompressed bytes under the limiter
	budget  *resource.Budget
	fixed   int64
}

// openTextStream opens obj, charging fixed bytes plus gzip state if needed.
func openTextStream(ctx context.Context, d *Dataset, obj storage.Object, fixed int64, keepTape bool) (*textStream, error) {
	stream, err := d.store.Open(ctx, obj)
	if err != nil {
		return nil, err
	}
	return newTextStream(d, obj, stream, fixed, keepTape)
}

// newTextStream takes ownership of stream, closing it on failure.
func newTextStream(d *Dataset, obj storage.Object, stream io.ReadCloser, fixed int64, keepTape bool) (*textStream, error) {
	s := &textStream{uri: obj.URI, raw: stream, input: stream, budget: d.budget}
	gzipped := strings.HasSuffix(strings.ToLower(obj.URI), ".gz")
	if gzipped {
		fixed += gzipStateBytes
	}
	if err := d.budget.Reserve(fixed, "source decoder"); err != nil {
		_ = stream.Close()
		return nil, err
	}
	s.fixed = fixed
	if gzipped {
		z, err := gzip.NewReader(stream)
		if err != nil {
			_ = s.close()
			return nil, fmt.Errorf("gzip header: %w", err)
		}
		s.zipped = z
		s.input = z
	}
	s.limiter = &recordLimiter{r: s.input, limit: d.recordLimit, keepTape: keepTape, tapeLine: 1}
	return s, nil
}

func (s *textStream) close() error {
	var err error
	if s.zipped != nil {
		err = s.zipped.Close()
		s.zipped = nil
	}
	if s.raw != nil {
		err = errors.Join(err, s.raw.Close())
		s.raw = nil
	}
	s.budget.Release(s.fixed)
	s.fixed = 0
	if s.limiter != nil {
		s.limiter.tape = nil
	}
	return err
}

func (s *textStream) drainIdentity(ctx context.Context) error {
	const drainBufferBytes int64 = 32 << 10
	if err := s.budget.Reserve(drainBufferBytes, "source identity drain"); err != nil {
		return err
	}
	defer s.budget.Release(drainBufferBytes)
	buffer := make([]byte, drainBufferBytes)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := s.input.Read(buffer)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// openText opens obj with the lexical decoder for the dataset's input kind.
// Failures return an untyped nil so callers never retain a typed-nil input.
func openText(ctx context.Context, d *Dataset, obj storage.Object) (lexicalInput, error) {
	if d.kind == inputCSV {
		c, err := openCSV(ctx, d, obj, d.csvOptions())
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	j, err := openJSON(ctx, d, obj)
	if err != nil {
		return nil, err
	}
	return j, nil
}

func validateNames(names []string) error {
	seen := make(map[string]bool, len(names))
	for _, s := range names {
		if !utf8.ValidString(s) {
			return errors.New("invalid UTF-8 column name")
		}
		if seen[s] {
			return fmt.Errorf("duplicate input column %q", s)
		}
		seen[s] = true
	}
	return nil
}

// skipBOM drops an initial UTF-8 byte order mark and reports its length.
func skipBOM(br *bufio.Reader) int64 {
	if b, err := br.Peek(3); err == nil && b[0] == 0xef && b[1] == 0xbb && b[2] == 0xbf {
		_, _ = br.Discard(3)
		return 3
	}
	return 0
}
