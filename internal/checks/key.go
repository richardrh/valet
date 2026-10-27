package checks

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"

	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

// Key encodings. Both are run-local and never persisted.
//
// Value keys (uniqueness) concatenate, per component, the valueKind byte and
// the native payload: fixed-width integers/bits, or a length-prefixed string.
// Encoding native bits avoids float intermediaries and per-cell formatting.
// Float -0 encodes as +0; NaN has no key (see valueKey).
//
// Reference keys concatenate, per component, the data.State byte and the
// length-prefixed canonical text (empty for null), so null and empty text stay
// distinct and component boundaries are unambiguous.
const (
	// lengthPrefixBytes is the uint64 length before variable-size payloads.
	lengthPrefixBytes = 8
	// referenceKeyHeader is the state byte plus length prefix.
	referenceKeyHeader = 1 + lengthPrefixBytes
	// valueKeyHeader is the kind byte plus the largest fixed payload
	// (decimal: 4-byte scale and two 8-byte words).
	valueKeyHeader = 1 + 4 + 8 + 8
)

func appendExactKey(dst []byte, v scalar) []byte {
	dst = append(dst, byte(v.kind))
	switch v.kind {
	case valString, valBinary:
		dst = binary.LittleEndian.AppendUint64(dst, uint64(len(v.s)))
		return append(dst, v.s...)
	case valInt:
		return binary.LittleEndian.AppendUint64(dst, uint64(v.i))
	case valBool:
		if v.b {
			return append(dst, 1)
		}
		return append(dst, 0)
	case valUint:
		return binary.LittleEndian.AppendUint64(dst, v.u)
	case valFloat:
		bits := math.Float64bits(v.f)
		if v.f == 0 {
			bits = 0
		}
		return binary.LittleEndian.AppendUint64(dst, bits)
	case valTime:
		dst = binary.LittleEndian.AppendUint64(dst, uint64(v.i))
		return binary.LittleEndian.AppendUint64(dst, v.u)
	case valDecimal:
		dst = binary.LittleEndian.AppendUint32(dst, uint32(v.scale))
		dst = binary.LittleEndian.AppendUint64(dst, v.decimal[0])
		return binary.LittleEndian.AppendUint64(dst, v.decimal[1])
	}
	return dst
}

func appendReferenceKey(dst []byte, state data.State, text string) []byte {
	dst = append(dst, byte(state))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(len(text)))
	return append(dst, text...)
}

// indexName identifies a shareable index; parts are length-prefixed so
// distinct part lists never collide.
func indexName(prefix string, parts ...string) string {
	var b strings.Builder
	b.WriteString(prefix)
	for _, p := range parts {
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(len(p)))
		b.WriteByte(':')
		b.WriteString(p)
	}
	return b.String()
}

// keyIndex is an exact key set with occurrence saturation at two.
type keyIndex struct {
	values   map[string]uint8
	budget   *resource.Budget
	reserved int64
}

// add records key, copying it. With duplicates, a repeated key saturates at 2.
func (k *keyIndex) add(key []byte, duplicates bool) error {
	if n, ok := k.values[string(key)]; ok {
		if duplicates && n < 2 {
			k.values[string(key)] = 2
		}
		return nil
	}
	// Reservations never shrink until the index is discarded.
	n := int64(len(key))*indexBytesPerKeyByte + indexBytesPerEntry
	if err := k.budget.Reserve(n, "check index"); err != nil {
		return err
	}
	k.reserved += n
	k.values[string(key)] = 1
	return nil
}

// count returns occurrences of key (0, 1 or 2 meaning "more than one").
func (k *keyIndex) count(key []byte) uint8 { return k.values[string(key)] }

func (k *keyIndex) close() { k.budget.Release(k.reserved); k.reserved = 0; k.values = nil }

// keyBuffer is the program's single reusable key scratch, charged to the
// budget as it grows. Every key built into it aliases the buffer and is valid
// only until the next build: keyIndex.add copies what it retains and lookups
// retain nothing, so no caller may hold a key across builds.
type keyBuffer struct {
	buf      []byte
	reserved int64
	budget   *resource.Budget
}

// reset empties the buffer, keeping capacity.
func (k *keyBuffer) reset() { k.buf = k.buf[:0] }

// ensure grows capacity to at least n bytes.
func (k *keyBuffer) ensure(n int) error {
	if n <= cap(k.buf) {
		return nil
	}
	size := max(n, 2*cap(k.buf), minKeyBufferBytes)
	if err := k.budget.Reserve(int64(size), "key scratch growth"); err != nil {
		return err
	}
	next := make([]byte, len(k.buf), size)
	copy(next, k.buf)
	k.budget.Release(k.reserved)
	k.reserved = int64(size)
	k.buf = next
	return nil
}

func (k *keyBuffer) release() {
	k.budget.Release(k.reserved)
	k.reserved = 0
	k.buf = nil
}

// keyStatus says why a key was or was not built.
type keyStatus uint8

const (
	keyBuilt keyStatus = iota
	// keyAbsent: a component is null/missing (or, for value keys, invalid).
	keyAbsent
	// keyNaN: a float component is NaN, which has no identity; uniqueness of
	// such a row is unevaluable rather than equal or distinct. Sources already
	// reject NaN as an invalid conversion; this guards batches from any other
	// producer.
	keyNaN
)

// valueKey builds the exact uniqueness key of columns at row into the shared
// buffer. Only Valid components have keys.
func (p *Program) valueKey(b *data.Batch, columns []operand, row int) ([]byte, keyStatus, error) {
	p.keys.reset()
	for _, col := range columns {
		if col.state(row) != data.Valid {
			return nil, keyAbsent, nil
		}
		v := col.value(row)
		if v.kind == valFloat && math.IsNaN(v.f) {
			return nil, keyNaN, nil
		}
		if err := p.keys.ensure(len(p.keys.buf) + len(v.s) + valueKeyHeader + lengthPrefixBytes); err != nil {
			return nil, keyBuilt, err
		}
		p.keys.buf = appendExactKey(p.keys.buf, v)
	}
	return p.keys.buf, keyBuilt, nil
}

// referenceKey builds the canonical-text membership key of columns at row into
// the shared buffer. Null components participate; missing/invalid do not.
func (p *Program) referenceKey(b *data.Batch, columns []operand, row int) ([]byte, keyStatus, error) {
	p.keys.reset()
	for _, col := range columns {
		state := col.state(row)
		if state == data.Invalid || state == data.Missing {
			return nil, keyAbsent, nil
		}
		text := ""
		if state == data.Valid {
			text = col.value(row).text()
		}
		if err := p.referenceText(state, text, len(p.keys.buf)); err != nil {
			return nil, keyBuilt, err
		}
	}
	return p.keys.buf, keyBuilt, nil
}

// referenceText appends one reference component to the shared buffer after
// the first used bytes.
func (p *Program) referenceText(state data.State, text string, used int) error {
	if err := p.keys.ensure(used + len(text) + referenceKeyHeader); err != nil {
		return err
	}
	p.keys.buf = appendReferenceKey(p.keys.buf[:used], state, text)
	return nil
}
