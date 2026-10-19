package resource

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

var configureCodecs sync.Once

// ParquetCodecWorkspace is a conservative per-active-decoder reservation for
// zstd's bounded window and tables. The source reserves it separately from page
// input/output buffers. Decoders are synchronous and never retained globally.
const ParquetCodecWorkspace int64 = 32 << 20
const parquetMaxWindow uint64 = 8 << 20

// ConfigureParquetCodecs must precede every application Parquet reader/writer.
// Arrow's codec registry is process-global and not concurrency-safe to mutate;
// this function serializes the single installation before callers use it.
func ConfigureParquetCodecs() {
	configureCodecs.Do(func() {
		for _, kind := range []compress.Compression{compress.Codecs.Uncompressed, compress.Codecs.Snappy, compress.Codecs.Zstd} {
			original, err := compress.GetCodec(kind)
			if err != nil {
				panic(err)
			}
			compress.RegisterCodec(kind, boundedCodec{Codec: original, kind: kind})
		}
	})
}

type boundedCodec struct {
	compress.Codec
	kind compress.Compression
}

func (c boundedCodec) Encode(dst, src []byte) []byte {
	if c.kind == compress.Codecs.Snappy {
		return s2.EncodeSnappy(dst, src)
	}
	return c.Codec.Encode(dst, src)
}

func (c boundedCodec) EncodeLevel(dst, src []byte, level int) []byte {
	if c.kind == compress.Codecs.Snappy {
		return s2.EncodeSnappy(dst, src)
	}
	return c.Codec.EncodeLevel(dst, src, level)
}

func (c boundedCodec) Decode(dst, src []byte) []byte {
	result, err := c.DecodeWithError(dst, src)
	if err != nil {
		panic(err)
	}
	return result
}
func (c boundedCodec) DecodeWithError(dst, src []byte) ([]byte, error) {
	decoder := ParquetDecoder{}
	defer decoder.Close()
	return decoder.Decode(c.kind, dst, src)
}

// ParquetDecoder owns reusable decoder scratch. It is not safe for concurrent
// use. Its owner must retain a ParquetCodecWorkspace reservation until Close.
// Unlike the process-wide codec registry, its lifetime follows the reader.
type ParquetDecoder struct {
	zstd   *zstd.Decoder
	closed bool
}

// NewParquetDecoder creates a lazy, reader-owned bounded decoder.
func NewParquetDecoder() (*ParquetDecoder, error) {
	return &ParquetDecoder{}, nil
}

// Close releases retained codec scratch and may be called repeatedly.
func (d *ParquetDecoder) Close() {
	if d.zstd != nil {
		d.zstd.Close()
		d.zstd = nil
	}
	d.closed = true
}

// Decode writes only within dst's declared length, irrespective of its capacity.
func (d *ParquetDecoder) Decode(kind compress.Compression, dst, src []byte) ([]byte, error) {
	if d.closed {
		return nil, fmt.Errorf("parquet: decoder is closed")
	}
	switch kind {
	case compress.Codecs.Uncompressed:
		if len(src) > len(dst) {
			return nil, fmt.Errorf("parquet: uncompressed page exceeds declared size")
		}
		return dst[:copy(dst, src)], nil
	case compress.Codecs.Snappy:
		size, err := snappy.DecodedLen(src)
		if err != nil {
			return nil, fmt.Errorf("parquet: invalid snappy page: %w", err)
		}
		if size > len(dst) {
			return nil, fmt.Errorf("parquet: snappy decoded size %d exceeds declared page size %d", size, len(dst))
		}
		return snappy.Decode(dst[:size:size], src)
	case compress.Codecs.Zstd:
		// Limit capacity, not merely initial length: DecodeAll otherwise grows dst
		// according to attacker-controlled frame metadata outside Arrow accounting.
		// The window bound controls retained workspace independently of page
		// length. The output cap bounds each page, including concatenated frames
		// and frames with unknown content size. A generous MaxMemory permits
		// large valid pages without permitting destination growth.
		// Retained scratch remains charged to the owner's workspace reservation.
		if d.zstd == nil {
			decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(1<<63), zstd.WithDecoderMaxWindow(parquetMaxWindow), zstd.WithDecodeAllCapLimit(true))
			if err != nil {
				return nil, err
			}
			d.zstd = decoder
		}
		result, err := d.zstd.DecodeAll(src, dst[:0:len(dst)])
		if err != nil {
			return nil, fmt.Errorf("parquet: bounded zstd decode: %w", err)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("parquet: unsupported bounded codec %v", kind)
	}
}
