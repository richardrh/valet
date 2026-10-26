package source

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/apache/arrow-go/v18/parquet"
)

func validatePageEncoding(physical parquet.Type, encoding parquet.Encoding) error {
	allowed := encoding == parquet.Encodings.Plain
	if encoding == parquet.Encodings.PlainDict || encoding == parquet.Encodings.RLEDict {
		allowed = physical != parquet.Types.Boolean
	}
	switch physical {
	case parquet.Types.Boolean:
		allowed = allowed || encoding == parquet.Encodings.RLE
	case parquet.Types.Int32, parquet.Types.Int64:
		allowed = allowed || encoding == parquet.Encodings.DeltaBinaryPacked || encoding == parquet.Encodings.ByteStreamSplit
	case parquet.Types.Float, parquet.Types.Double, parquet.Types.FixedLenByteArray:
		allowed = allowed || encoding == parquet.Encodings.ByteStreamSplit
	case parquet.Types.ByteArray:
		allowed = allowed || encoding == parquet.Encodings.DeltaLengthByteArray || encoding == parquet.Encodings.DeltaByteArray
	case parquet.Types.Int96:
	default:
		allowed = false
	}
	if !allowed {
		return fmt.Errorf("unsupported Parquet type/encoding combination %s/%s", physical, encoding)
	}
	return nil
}
func physicalWidth(physical parquet.Type, fixed int) int64 {
	switch physical {
	case parquet.Types.Int32, parquet.Types.Float:
		return 4
	case parquet.Types.Int64, parquet.Types.Double:
		return 8
	case parquet.Types.Int96:
		return 12
	case parquet.Types.FixedLenByteArray:
		return int64(fixed)
	}
	return 0
}
func validatePlain(data []byte, count int64, physical parquet.Type, fixed int, maxRecord int64) error {
	if count < 0 {
		return errors.New("negative Parquet value count")
	}
	if physical == parquet.Types.Boolean {
		if (count+7)/8 != int64(len(data)) {
			return errors.New("boolean payload does not match declared count")
		}
		return nil
	}
	if physical == parquet.Types.ByteArray {
		offset := 0
		for range count {
			if len(data)-offset < 4 {
				return io.ErrUnexpectedEOF
			}
			n := int64(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if n > maxRecord || n > int64(len(data)-offset) {
				return errors.New("binary payload length exceeds available data or record limit")
			}
			offset += int(n)
		}
		if offset != len(data) {
			return errors.New("binary payload does not match declared count")
		}
		return nil
	}
	width := physicalWidth(physical, fixed)
	if width <= 0 || width > maxRecord || count > int64(len(data))/width || count*width != int64(len(data)) {
		return errors.New("fixed-width payload does not match declared count")
	}
	return nil
}
func packedValue(data []byte, index int64, width uint) uint64 {
	if width == 0 {
		return 0
	}
	bit := uint64(index) * uint64(width)
	offset := int(bit / 8)
	shift := uint(bit % 8)
	value := uint64(data[offset]) >> shift
	available := uint(8) - shift
	for available < width {
		offset++
		value |= uint64(data[offset]) << available
		available += 8
	}
	if width < 64 {
		value &= (uint64(1) << width) - 1
	}
	return value
}

// Parquet DELTA_BINARY_PACKED layout rules and decoder state estimates.
const (
	// deltaBlockMultiple: block sizes must be a multiple of 128 values.
	deltaBlockMultiple = 128
	// deltaMiniblockMultiple: values per miniblock must be a multiple of 32.
	deltaMiniblockMultiple = 32
	// deltaValueStateBytes is Arrow's decoder state per value of one miniblock.
	deltaValueStateBytes = 16
	// deltaWidthStateBytes is Arrow's decoder state per miniblock bit width.
	deltaWidthStateBytes = 2
	// deltaDecoderFixedBytes is the fixed working set of one delta decoder.
	deltaDecoderFixedBytes = 65536
	// deltaContextCheckInterval is how many expanded values pass between
	// cancellation/budget checks while visiting a miniblock.
	deltaContextCheckInterval = 4096
	// deltaPrefixStateBytes is the per-value cost of DELTA_BYTE_ARRAY prefix
	// lengths and Arrow's offsets.
	deltaPrefixStateBytes = 8
)

// uvarint decodes an unsigned LEB128 integer at data[*pos:] and advances pos.
func uvarint(data []byte, pos *int) (uint64, error) {
	v, n := binary.Uvarint(data[*pos:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, errors.New("overflowing Parquet varint")
	}
	*pos += n
	return v, nil
}

// zigzagVarint decodes a zigzag LEB128 integer at data[*pos:] and advances pos.
func zigzagVarint(data []byte, pos *int) (int64, error) {
	v, n := binary.Varint(data[*pos:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, errors.New("overflowing Parquet varint")
	}
	*pos += n
	return v, nil
}

// scanHybrid validates RLE/bit-packed runs without expanding repeated runs.
func scanHybrid(data []byte, width uint, count, maxValue int64) (ones int64, err error) {
	if width > 32 || count < 0 || maxValue < 0 {
		return 0, errors.New("invalid hybrid encoding parameters")
	}
	pos := 0
	remaining := count
	for remaining > 0 {
		header, e := uvarint(data, &pos)
		if e != nil {
			return 0, e
		}
		if header == 0 {
			return 0, errors.New("empty hybrid run")
		}
		if header&1 == 0 {
			run := header >> 1
			if run > math.MaxInt64 {
				return 0, errors.New("hybrid run overflows count")
			}
			bytes := int((width + 7) / 8)
			if bytes > len(data)-pos {
				return 0, io.ErrUnexpectedEOF
			}
			var value uint64
			for i := range bytes {
				value |= uint64(data[pos+i]) << uint(i*8)
			}
			pos += bytes
			if value > uint64(maxValue) {
				return 0, errors.New("hybrid value exceeds verified cardinality")
			}
			used := min(remaining, int64(run))
			if value == 1 {
				ones += used
			}
			remaining -= used
		} else {
			groups := header >> 1
			if groups > math.MaxInt64/8 {
				return 0, errors.New("hybrid run overflows count")
			}
			values := int64(groups) * 8
			if width > 0 && groups > uint64(len(data)-pos)/uint64(width) {
				return 0, io.ErrUnexpectedEOF
			}
			bytes := groups * uint64(width)
			used := min(remaining, values)
			payload := data[pos : pos+int(bytes)]
			for i := range used {
				value := packedValue(payload, i, width)
				if value > uint64(maxValue) {
					return 0, errors.New("dictionary index exceeds verified cardinality")
				}
				if value == 1 {
					ones++
				}
			}
			pos += int(bytes)
			remaining -= used
		}
	}
	if pos != len(data) {
		return 0, errors.New("trailing hybrid encoded data")
	}
	return ones, nil
}

// scanDelta never allocates a miniblock. The reservation callback runs before
// the Arrow decoder can allocate/expand its declared miniblock or count arrays.
func scanDelta(data []byte, count int64, width int, ensure func(int64) error, visit func(int64, int64) error) (int, error) {
	pos := 0
	block, e := uvarint(data, &pos)
	if e != nil {
		return 0, e
	}
	minis, e := uvarint(data, &pos)
	if e != nil {
		return 0, e
	}
	total, e := uvarint(data, &pos)
	if e != nil {
		return 0, e
	}
	first, e := zigzagVarint(data, &pos)
	if e != nil {
		return 0, e
	}
	validBlock := block != 0 && block <= math.MaxUint32 && block%deltaBlockMultiple == 0
	if !validBlock || minis == 0 || minis > math.MaxUint32 || block%minis != 0 {
		return 0, errors.New("invalid delta block/miniblock dimensions")
	}
	perMini := block / minis
	if perMini%deltaMiniblockMultiple != 0 {
		return 0, errors.New("invalid delta miniblock width")
	}
	if total != uint64(count) || count < 0 || total > math.MaxUint32 {
		return 0, errors.New("delta count differs from verified non-null count")
	}
	state := int64(perMini)*deltaValueStateBytes + int64(minis)*deltaWidthStateBytes + deltaDecoderFixedBytes
	if e = ensure(state); e != nil {
		return 0, e
	}
	if width == 32 && (first < math.MinInt32 || first > math.MaxInt32) {
		return 0, errors.New("delta initial value outside INT32")
	}
	if count == 0 {
		return pos, nil
	}
	if visit != nil {
		if e = visit(0, first); e != nil {
			return 0, e
		}
	}
	produced := int64(1)
	last := first
	for produced < count {
		minimum, e := zigzagVarint(data, &pos)
		if e != nil {
			return 0, e
		}
		if width == 32 && (minimum < math.MinInt32 || minimum > math.MaxInt32) {
			return 0, errors.New("delta minimum outside INT32")
		}
		if minis > uint64(len(data)-pos) {
			return 0, io.ErrUnexpectedEOF
		}
		widths := data[pos : pos+int(minis)]
		pos += int(minis)
		for _, bits := range widths {
			if bits > byte(width) {
				return 0, errors.New("delta bit width exceeds physical type")
			}
		}
		for _, bits := range widths {
			if produced == count {
				break
			}
			size := perMini * uint64(bits) / 8
			if size > uint64(len(data)-pos) {
				return 0, io.ErrUnexpectedEOF
			}
			payload := data[pos : pos+int(size)]
			used := min(int64(perMini), count-produced)
			if visit == nil {
				produced += used
			} else {
				for i := range used {
					if i%deltaContextCheckInterval == 0 {
						if e = ensure(0); e != nil {
							return 0, e
						}
					}
					last += int64(packedValue(payload, i, uint(bits))) + minimum
					value := last
					if width == 32 {
						value = int64(int32(value))
					}
					if e = visit(produced, value); e != nil {
						return 0, e
					}
					produced++
				}
			}
			pos += int(size)
		}
	}
	return pos, nil
}

// encodedValues is one decompressed page payload after its definition levels.
type encodedValues struct {
	data []byte
	// count is the number of non-null values the payload must encode.
	count    int64
	physical parquet.Type
	encoding parquet.Encoding
	// fixed is the FIXED_LEN_BYTE_ARRAY width (ignored for other types).
	fixed int
	// dictCount is the verified dictionary cardinality, or <= 0 without one.
	dictCount int64
	// maxRecord bounds any single binary value.
	maxRecord int64
	// ensure reserves decoder state beyond the page's base charge.
	ensure func(int64) error
}

func validateEncodedValues(v encodedValues) error {
	if e := validatePageEncoding(v.physical, v.encoding); e != nil {
		return e
	}
	data, count, maxRecord, ensure := v.data, v.count, v.maxRecord, v.ensure
	if count == 0 && len(data) == 0 {
		return nil
	}
	switch v.encoding {
	case parquet.Encodings.Plain, parquet.Encodings.ByteStreamSplit:
		return validatePlain(data, count, v.physical, v.fixed, maxRecord)
	case parquet.Encodings.PlainDict, parquet.Encodings.RLEDict:
		if count == 0 && len(data) == 1 && data[0] <= 32 {
			return nil
		}
		if v.dictCount <= 0 || len(data) == 0 {
			return errors.New("dictionary data page has no verified dictionary")
		}
		_, e := scanHybrid(data[1:], uint(data[0]), count, v.dictCount-1)
		return e
	case parquet.Encodings.RLE:
		if len(data) < 4 {
			return io.ErrUnexpectedEOF
		}
		size := int(binary.LittleEndian.Uint32(data))
		if size != len(data)-4 {
			return errors.New("invalid boolean RLE length")
		}
		_, e := scanHybrid(data[4:], 1, count, 1)
		return e
	case parquet.Encodings.DeltaBinaryPacked:
		width := 64
		if v.physical == parquet.Types.Int32 {
			width = 32
		}
		used, e := scanDelta(data, count, width, ensure, nil)
		if e == nil && used != len(data) {
			return errors.New("trailing delta payload")
		}
		return e
	case parquet.Encodings.DeltaLengthByteArray:
		var sum int64
		used, e := scanDelta(data, count, 32, ensure, func(_ int64, n int64) error {
			if n < 0 || n > maxRecord || n > int64(len(data))-sum {
				return errors.New("invalid delta binary length")
			}
			sum += n
			return nil
		})
		if e != nil {
			return e
		}
		if sum != int64(len(data)-used) {
			return errors.New("delta lengths exceed actual payload")
		}
		return nil
	case parquet.Encodings.DeltaByteArray:
		return validateDeltaByteArray(data, count, maxRecord, ensure)
	}
	return errors.New("unsupported Parquet encoding")
}
func validateDeltaByteArray(data []byte, count, maxRecord int64, ensure func(int64) error) error {
	if e := ensure(count * deltaPrefixStateBytes); e != nil {
		return e
	}
	prefixes := make([]int32, int(count))
	used, e := scanDelta(data, count, 32, ensure, func(i, n int64) error {
		if n < 0 || n > maxRecord {
			return errors.New("invalid delta prefix length")
		}
		prefixes[i] = int32(n)
		return nil
	})
	if e != nil {
		return e
	}
	var previous, suffixBytes, expanded int64
	suffixUsed, e := scanDelta(data[used:], count, 32, ensure, func(i, n int64) error {
		prefix := int64(prefixes[i])
		if n < 0 || n > maxRecord || prefix > previous || prefix > maxRecord-n || n > int64(len(data))-suffixBytes {
			return errors.New("invalid delta prefix/suffix length")
		}
		if i == 0 && prefix != 0 {
			return errors.New("first delta prefix must be zero")
		}
		previous = prefix + n
		suffixBytes += n
		if n != 0 || i == 0 {
			if expanded > math.MaxInt64-previous {
				return errors.New("delta expansion overflow")
			}
			expanded += previous
			if e := ensure(expanded*2 + count*deltaPrefixStateBytes); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	if suffixBytes != int64(len(data)-used-suffixUsed) {
		return errors.New("delta suffixes exceed actual payload")
	}
	return nil
}
