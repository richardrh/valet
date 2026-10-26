package resource_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/klauspost/compress/snappy"

	"github.com/richardrh/valet/internal/resource"
)

func TestParquetCodecsRespectDeclaredPageBuffer(t *testing.T) {
	resource.ConfigureParquetCodecs()
	payload := bytes.Repeat([]byte("a valid page with repeated text;"), 128)
	const undersizedPage = 8
	kinds := []compress.Compression{compress.Codecs.Uncompressed, compress.Codecs.Snappy, compress.Codecs.Zstd}
	for _, kind := range kinds {
		t.Run(kind.String(), func(t *testing.T) {
			codec, err := compress.GetCodec(kind)
			if err != nil {
				t.Fatal(err)
			}
			encoded := codec.Encode(nil, payload)

			if _, err := compress.Decode(codec, make([]byte, undersizedPage), encoded); err == nil {
				t.Errorf("decode into %d-byte page error = nil, want oversized-page refusal", undersizedPage)
			}

			dst := make([]byte, len(payload))
			decoded, err := compress.Decode(codec, dst, encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded, payload) {
				t.Errorf("decoded (%d bytes) differs from payload (%d bytes)", len(decoded), len(payload))
			}
			if len(decoded) == 0 || &decoded[0] != &dst[0] {
				t.Error("decoded does not alias supplied dst, want decode into supplied storage")
			}
		})
	}
}

func TestSnappyHugeDeclaredSizeRejectedBeforeAllocation(t *testing.T) {
	const (
		declaredSize = 1 << 30 // 1 GiB claimed in the snappy length header
		pageSize     = 64
	)
	resource.ConfigureParquetCodecs()
	codec, err := compress.GetCodec(compress.Codecs.Snappy)
	if err != nil {
		t.Fatal(err)
	}
	header := binary.AppendUvarint(nil, declaredSize)
	if _, err := compress.Decode(codec, make([]byte, pageSize), header); err == nil {
		t.Errorf("decode of %d-byte claim into %d-byte page error = nil, want refusal", declaredSize, pageSize)
	}
}

func TestSnappyRawBlockBoundaries(t *testing.T) {
	resource.ConfigureParquetCodecs()
	codec, err := compress.GetCodec(compress.Codecs.Snappy)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, 15, 16, 31, 32, 63, 64, 255, 256, 65535, 65536, 65537, 4 << 20} {
		for _, random := range []bool{false, true} {
			payload := make([]byte, size)
			if random {
				rand.New(rand.NewSource(int64(size))).Read(payload)
			} else {
				for i := range payload {
					payload[i] = byte(i % 7)
				}
			}
			for _, level := range []int{-1, 0, 1, 9} {
				dst := make([]byte, codec.CompressBound(int64(size)))
				var encoded []byte
				if level == -1 {
					encoded = codec.Encode(dst, payload)
				} else {
					encoded = codec.EncodeLevel(dst, payload, level)
				}
				if int64(len(encoded)) > codec.CompressBound(int64(size)) {
					t.Fatalf("size %d random %v level %d: encoded length exceeds bound", size, random, level)
				}
				// An independent standard raw-block decoder is the compatibility
				// contract, not the implementation's choice of encoder.
				decoded, err := snappy.Decode(nil, encoded)
				if err != nil || !bytes.Equal(decoded, payload) {
					t.Fatalf("size %d random %v level %d: standard decode failed: %v", size, random, level, err)
				}
			}
		}
	}
}

func TestScopedDecoderBoundsAndLifetime(t *testing.T) {
	resource.ConfigureParquetCodecs()
	decoder, err := resource.NewParquetDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	for _, kind := range []compress.Compression{compress.Codecs.Uncompressed, compress.Codecs.Snappy, compress.Codecs.Zstd} {
		codec, err := compress.GetCodec(kind)
		if err != nil {
			t.Fatal(err)
		}
		for _, size := range []int{0, 1, 1024, 9 << 20, 17} {
			payload := bytes.Repeat([]byte{'x'}, size)
			encoded := codec.Encode(nil, payload)
			dst := make([]byte, size, size+4096)
			got, err := decoder.Decode(kind, dst, encoded)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("%s size %d: decode = %v", kind, size, err)
			}
			if size > 0 {
				if _, err := decoder.Decode(kind, dst[:size-1], encoded); err == nil {
					t.Fatalf("%s size %d: accepted output beyond declared length despite spare capacity", kind, size)
				}
			}
		}
		if _, err := decoder.Decode(kind, make([]byte, 4), []byte{0xff, 0xff, 0xff, 0xff, 0xff}); err == nil {
			t.Fatalf("%s: malformed/oversized input accepted", kind)
		}
	}
	decoder.Close()
	decoder.Close()
	if _, err := decoder.Decode(compress.Codecs.Uncompressed, nil, nil); err == nil {
		t.Fatal("closed decoder accepted decode")
	}
}

func TestZstdConcatenatedFramesCannotEscapePageBound(t *testing.T) {
	resource.ConfigureParquetCodecs()
	codec, err := compress.GetCodec(compress.Codecs.Zstd)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("bounded"), 128)
	frame := codec.Encode(nil, payload)
	frames := append(append([]byte(nil), frame...), frame...)
	decoder, err := resource.NewParquetDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	dst := make([]byte, len(payload), len(payload)*4)
	if _, err := decoder.Decode(compress.Codecs.Zstd, dst, frames); err == nil {
		t.Fatal("concatenated frames escaped declared output length")
	}
	// A rejected frame must not poison the reader-owned decoder.
	got, err := decoder.Decode(compress.Codecs.Zstd, dst, frame)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("decode after rejected frame: %v", err)
	}
}

func TestSnappyDecodePanicsOnOversizedRawBlock(t *testing.T) {
	resource.ConfigureParquetCodecs()
	codec, err := compress.GetCodec(compress.Codecs.Snappy)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Decode accepted oversized raw block")
		}
	}()
	codec.Decode(make([]byte, 8, 4096), binary.AppendUvarint(nil, 1<<30))
}
