package source

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/richardrh/valet/internal/config"
)

// Footer fixtures below are Thrift compact protocol. A field header byte is
// (field-id delta << 4 | type); types used: 8 binary, 9 list, 11 map, 12 struct.

func TestParquetRejectsNegativeSchemaChildrenBeforeArrow(t *testing.T) {
	// FileMetaData.schema = [{name:"schema", num_children:-1}]. This test
	// invokes only the allocation-free guard, never Arrow's unsafe constructor.
	footer := []byte{0x29, 0x1c, 0x48, 0x06, 's', 'c', 'h', 'e', 'm', 'a', 0x15, 0x01, 0, 0}
	if err := validateCompact(footer); err == nil {
		t.Fatal("validateCompact error = nil, want negative schema child count rejected")
	}
}

// nestedStructs encodes a FileMetaData whose field 1 nests depth structs.
func nestedStructs(depth int) []byte {
	raw := bytes.Repeat([]byte{0x1c}, depth) // field delta 1, type struct
	return append(raw, make([]byte, depth+1)...)
}

func TestCompactMetadataStructuralGuards(t *testing.T) {
	uuidField := append([]byte{0x1d}, make([]byte, 17)...) // field 1 UUID, 16 bytes, stop
	for _, tc := range []struct {
		name   string
		footer []byte
		valid  bool
	}{
		// Field 1 list<bool> of one element; in collections 0x01 is true, 0x02 false.
		{name: "collection boolean true", footer: []byte{0x19, 0x11, 0x01, 0}, valid: true},
		{name: "collection boolean invalid byte", footer: []byte{0x19, 0x11, 0x03, 0}},
		{name: "trailing bytes", footer: []byte{0, 0}}, // stop, then one stray byte
		{name: "UUID field", footer: uuidField},
		{name: "nesting at limit", footer: nestedStructs(metadataMaxDepth), valid: true},
		{name: "nesting beyond limit", footer: nestedStructs(metadataMaxDepth + 1)},
		// Field 1 map of 127 binary->binary entries (0x88) with no entry bytes.
		{name: "map count beyond bytes", footer: []byte{0x1b, 0x7f, 0x88, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCompact(tc.footer)
			if (err == nil) != tc.valid {
				t.Fatalf("validateCompact(% x) = %v, want valid=%v", tc.footer, err, tc.valid)
			}
		})
	}
}

func TestParquetRejectsIntegralDeltaByteArrayBeforeArrow(t *testing.T) {
	if err := validatePageEncoding(parquet.Types.Int32, parquet.Encodings.DeltaByteArray); err == nil {
		t.Fatal("validatePageEncoding(INT32, DELTA_BYTE_ARRAY) = nil, want error")
	}
}

func TestParquetHugeDeltaMiniblockFailsBeforeExpansion(t *testing.T) {
	// Delta header: block size 2^28, 1 miniblock, 2 values, first value 0;
	// then min delta 0 and a zero bit-width byte.
	var encoded []byte
	for _, v := range []uint64{1 << 28, 1, 2, 0, 0} {
		encoded = binary.AppendUvarint(encoded, v)
	}
	encoded = append(encoded, 0)
	refused := errors.New("reservation refused")
	called := false
	reserve := func(n int64) error {
		called = true
		if n < 1<<30 {
			t.Fatalf("miniblock reservation = %d bytes, want >= %d", n, int64(1<<30))
		}
		return refused
	}
	expand := func(int64, int64) error {
		t.Fatal("values expanded before reservation")
		return nil
	}
	_, err := scanDelta(encoded, 2, 64, reserve, expand)
	if !called {
		t.Error("reserve called = false, want true")
	}
	if !errors.Is(err, refused) {
		t.Errorf("scanDelta error = %v, want %v", err, refused)
	}
}

func TestParquetDictionaryCardinalityPrecedesLookup(t *testing.T) {
	t.Run("huge dictionary count with tiny payload", func(t *testing.T) {
		if err := validatePlain(make([]byte, 8), 1<<28, parquet.Types.Int64, 0, 1<<20); err == nil {
			t.Fatal("validatePlain error = nil, want 2^28 values in 8 bytes rejected")
		}
	})
	t.Run("index beyond verified dictionary", func(t *testing.T) {
		// RLE run of one index 2^28-1; the verified dictionary contains one value.
		indexes := []byte{2, 0xff, 0xff, 0xff, 0x0f}
		if _, err := scanHybrid(indexes, 28, 1, 0); err == nil {
			t.Fatal("scanHybrid error = nil, want out-of-range dictionary index rejected")
		}
	})
}

func TestParquetDeltaValidatorAcceptsBoundedData(t *testing.T) {
	// block 128, one miniblock, two values, first 1, min delta 1, width 0.
	encoded := []byte{0x80, 1, 4, 2, 2, 2, 0, 0, 0, 0}
	var values []int64
	accept := func(int64) error { return nil }
	collect := func(_ int64, v int64) error {
		values = append(values, v)
		return nil
	}
	used, err := scanDelta(encoded, 2, 32, accept, collect)
	if err != nil {
		t.Fatalf("scanDelta error = %v, want nil", err)
	}
	if used != len(encoded) {
		t.Errorf("bytes consumed = %d, want %d", used, len(encoded))
	}
	if want := []int64{1, 2}; !slices.Equal(values, want) {
		t.Errorf("delta values = %v, want %v", values, want)
	}
}

func TestParquetEmptyDictionaryNeedsNoLookup(t *testing.T) {
	err := validateEncodedValues(encodedValues{
		data:      []byte{0}, // bit width 0 and no runs: no indexes to look up
		physical:  parquet.Types.Int64,
		encoding:  parquet.Encodings.RLEDict,
		maxRecord: 1 << 20,
		ensure:    func(int64) error { return nil },
	})
	if err != nil {
		t.Fatalf("validateEncodedValues error = %v, want nil", err)
	}
}

func TestParquetSourceRejectsIntegralDeltaByteArrayWithoutPanic(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	builder.Field(0).(*array.Int32Builder).Append(7)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	file := encodeParquet(t, record)

	// Change only the one-byte PLAIN encoding enum in the first page header,
	// which starts after the 4-byte "PAR1" magic. The page payload and all
	// offsets remain valid; the old decoder panicked on this physical INT32 /
	// DELTA_BYTE_ARRAY pairing.
	const magicLen = 4
	header := newRawCompactReader(file[magicLen:])
	encodingOffset := -1
	err := header.fields(func(id int, kind thrift.TType) error {
		if id != pageDataHeaderField {
			return header.skip(kind, 1)
		}
		return header.fields(func(field int, k thrift.TType) error {
			if field == dataEncodingField {
				encodingOffset = header.pos()
			}
			return header.skip(k, 2)
		})
	})
	if err != nil {
		t.Fatalf("parse fixture page header: %v", err)
	}
	if encodingOffset < 0 {
		t.Fatal("fixture page header has no data encoding field")
	}
	file[magicLen+encodingOffset] = byte(parquet.Encodings.DeltaByteArray) * 2 // zigzag

	ds := fixture{name: "bad-encoding.parquet", body: string(file), input: config.Input{Type: "parquet"}}.dataset(t)
	reader, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if batch, err := reader.Next(context.Background()); err == nil {
		batch.Release()
		t.Fatal("Next error = nil, want invalid physical encoding rejected before Arrow")
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
}

// boundedReserve returns a reservation callback that refuses once the total
// exceeds limit, so fuzzed inputs cannot exhaust memory.
func boundedReserve(limit int64) func(int64) error {
	var used int64
	return func(n int64) error {
		if n < 0 || n > limit-used {
			return errors.New("fuzz reservation limit")
		}
		used += n
		return nil
	}
}

// arrowSchemaMessage serialises schema as an encapsulated IPC schema message,
// the same framing pqarrow base64-encodes into ARROW:schema.
func arrowSchemaMessage(t testing.TB, schema *arrow.Schema) []byte {
	t.Helper()
	var stream bytes.Buffer
	w := ipc.NewWriter(&stream, ipc.WithSchema(schema))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return stream.Bytes()
}

func FuzzValidateArrowIPC(f *testing.F) {
	tz := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	dict := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}
	meta := arrow.NewMetadata([]string{"origin"}, []string{"fuzz"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: meta},
		{Name: "amount", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: true},
		{Name: "at", Type: tz},
		{Name: "day", Type: arrow.FixedWidthTypes.Date32},
		{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32)},
		{Name: "pair", Type: arrow.StructOf(arrow.Field{Name: "flag", Type: arrow.FixedWidthTypes.Boolean})},
		{Name: "attrs", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Float64)},
		{Name: "code", Type: dict},
	}, &meta)
	seed := arrowSchemaMessage(f, schema)
	if err := validateArrowIPC(seed, boundedReserve(64<<20)); err != nil {
		f.Fatalf("validateArrowIPC(genuine arrow-go schema) = %v, want nil", err)
	}
	f.Add(seed)
	f.Add(seed[4:]) // legacy framing without the continuation marker
	// Continuation marker followed by a zero length: an end-of-stream marker.
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		_ = validateArrowIPC(raw, boundedReserve(64<<20))
	})
}

func FuzzValidateCompact(f *testing.F) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	b := array.NewInt64Builder(memory.NewGoAllocator())
	b.AppendValues([]int64{1, 2}, []bool{true, false})
	values := b.NewArray()
	b.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 2)
	values.Release()
	defer record.Release()
	var file bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &file, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		f.Fatal(err)
	}
	if err = writer.Write(record); err != nil {
		f.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		f.Fatal(err)
	}
	// A Parquet file ends with the footer, its 4-byte little-endian length, then "PAR1".
	raw := file.Bytes()
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-8:]))
	footer := raw[len(raw)-8-footerLen : len(raw)-8]
	if err = validateCompact(footer); err != nil {
		f.Fatalf("validateCompact(genuine footer) = %v, want nil", err)
	}
	if err = validateEmbeddedSchemas(footer, boundedReserve(64<<20)); err != nil {
		f.Fatalf("validateEmbeddedSchemas(genuine ARROW:schema) = %v, want nil", err)
	}
	f.Add(footer)
	f.Add([]byte{0x19, 0xfc, 0x80, 0x80, 0x80, 0x80, 0x01, 0}) // field 1 list<struct> claiming 2^28 elements
	f.Add([]byte{0x18, 0xff, 0xff, 0xff, 0xff, 0x7f, 0})       // field 1 binary claiming 2^35-1 bytes
	f.Fuzz(func(t *testing.T, raw []byte) {
		_ = validateCompact(raw)
		_ = validateEmbeddedSchemas(raw, boundedReserve(64<<20))
	})
}
