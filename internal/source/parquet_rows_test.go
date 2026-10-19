package source

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

func encodeRowGroups(t *testing.T, first, count int, physical32 bool, options ...parquet.WriterProperty) []byte {
	t.Helper()
	idType := arrow.DataType(arrow.PrimitiveTypes.Int64)
	if physical32 {
		idType = arrow.PrimitiveTypes.Int32
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: idType},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	for row := range count {
		value := int64(first + row)
		if physical32 {
			builder.Field(0).(*array.Int32Builder).Append(int32(value))
		} else {
			builder.Field(0).(*array.Int64Builder).Append(value)
		}
		if value%7 == 0 {
			builder.Field(1).AppendNull()
		} else {
			builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("value-%d-α", value%3))
		}
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	options = append([]parquet.WriterProperty{parquet.WithMaxRowGroupLength(13), parquet.WithDataPageSize(128)}, options...)
	var encoded bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &encoded, parquet.NewWriterProperties(options...), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.Write(record); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func rowGroupDataset(t *testing.T, inputs [][]byte, budgetBytes int64, input config.Input) (*Dataset, *resource.Budget) {
	t.Helper()
	directory := t.TempDir()
	paths := make([]string, len(inputs))
	for i, encoded := range inputs {
		paths[i] = filepath.Join(directory, fmt.Sprintf("%02d.parquet", i))
		if err := os.WriteFile(paths[i], encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
	budget := resource.New(budgetBytes)
	store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget)
	if err != nil {
		t.Fatal(err)
	}
	input.Type = "parquet"
	cfg := &config.Config{Input: input, Source: config.Source{URIs: paths}, Execution: config.Execution{
		Workers: 4, BatchTarget: "16KiB", MaxRecordBytes: "1MiB",
	}}
	dataset, err := New(context.Background(), cfg, store, budget)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = dataset.Close()
		if err := store.Close(); err != nil {
			t.Error(err)
		}
		if used := budget.Used(); used != 0 {
			t.Errorf("source retained %d managed bytes after close", used)
		}
	})
	return dataset, budget
}

func TestParquetOrderedRowGroupsAcrossCodecsAndFiles(t *testing.T) {
	resource.ConfigureParquetCodecs()
	for _, codec := range []compress.Compression{compress.Codecs.Uncompressed, compress.Codecs.Snappy, compress.Codecs.Zstd} {
		for _, version := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
			for _, dictionary := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/v%d/dictionary-%t", codec, version, dictionary), func(t *testing.T) {
					const count = 57
					options := []parquet.WriterProperty{parquet.WithCompression(codec), parquet.WithDataPageVersion(version), parquet.WithDictionaryDefault(dictionary)}
					ds, _ := rowGroupDataset(t, [][]byte{
						encodeRowGroups(t, 1, count, false, options...),
						encodeRowGroups(t, count+1, count, false, options...),
					}, 192<<20, config.Input{})
					reader, err := ds.Open(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer reader.Close()
					seen := 0
					for {
						batch, err := reader.Next(context.Background())
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						ids, labels := batch.Record.Column(0).(*array.Int64), batch.Record.Column(1).(*array.String)
						for row, location := range batch.Locations {
							seen++
							ordinal, sourceRow := (seen-1)/count+1, (seen-1)%count+1
							if ids.Value(row) != int64(seen) || location.GlobalRow != uint64(seen) || location.SourceOrdinal != uint64(ordinal) || location.Row != uint64(sourceRow) || location.Source != ds.Objects()[ordinal-1].URI {
								t.Errorf("row %d: id=%d location=%+v", seen, ids.Value(row), location)
							}
							if labels.IsNull(row) != (seen%7 == 0) {
								t.Errorf("row %d null=%t", seen, labels.IsNull(row))
							} else if !labels.IsNull(row) && labels.Value(row) != fmt.Sprintf("value-%d-α", seen%3) {
								t.Errorf("row %d label=%q", seen, labels.Value(row))
							}
						}
						batch.Release()
					}
					if seen != count*2 {
						t.Errorf("rows=%d, want %d", seen, count*2)
					}
				})
			}
		}
	}
}

func TestParquetMixedPhysicalFallbackPreservesEveryRow(t *testing.T) {
	const count = 901
	input := config.Input{Schema: &config.Schema{Order: []string{"id", "label"}, Columns: map[string]config.Column{
		"id": {Type: "int64"}, "label": {Type: "string"},
	}}}
	ds, _ := rowGroupDataset(t, [][]byte{
		encodeRowGroups(t, 1, count, false),
		encodeRowGroups(t, count+1, count, true),
		encodeRowGroups(t, count*2+1, count, false),
	}, 128<<20, input)
	reader, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen := 0
	for {
		batch, err := reader.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		ids := batch.Record.Column(0).(*array.Int64)
		for row, location := range batch.Locations {
			seen++
			if ids.Value(row) != int64(seen) || location.GlobalRow != uint64(seen) || location.Row != uint64((seen-1)%count+1) || location.SourceOrdinal != uint64((seen-1)/count+1) {
				t.Errorf("row %d: id=%d location=%+v", seen, ids.Value(row), location)
			}
		}
		batch.Release()
	}
	if seen != count*3 {
		t.Errorf("rows=%d, want %d", seen, count*3)
	}
}

func TestParquetCancellationJoinsRowGroupWorkers(t *testing.T) {
	ds, budget := rowGroupDataset(t, [][]byte{encodeRowGroups(t, 1, 512, false)}, 128<<20, config.Input{})
	baseline := budget.Used()
	reader, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	batch, err := reader.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	batch.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if batch, err = reader.Next(ctx); !errors.Is(err, context.Canceled) {
		if batch != nil {
			batch.Release()
		}
		t.Fatalf("canceled Next error=%v", err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if used := budget.Used(); used != baseline {
		t.Errorf("after worker cancellation used=%d, before open=%d", used, baseline)
	}
}

func TestParquetBudgetRefusalReleasesWorkerState(t *testing.T) {
	ds, budget := rowGroupDataset(t, [][]byte{encodeRowGroups(t, 1, 64, false)}, 8<<20, config.Input{})
	baseline := budget.Used()
	charge, err := budget.Hold(budget.Limit()-baseline-(32<<10), "test competing live allocation")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	batch, err := reader.Next(context.Background())
	if batch != nil {
		batch.Release()
	}
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Errorf("Next error=%v, want resource limit", err)
	}
	_ = reader.Close()
	charge.Release()
	if used := budget.Used(); used != baseline {
		t.Errorf("after refused worker allocation used=%d, before open=%d", used, baseline)
	}
}

func TestParquetIgnoredColumnStillReceivesEncodingGuards(t *testing.T) {
	encoded := encodeRowGroups(t, 1, 13, false, parquet.WithDictionaryDefault(false))
	metadataReader, err := file.NewParquetReader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	defer metadataReader.Close()
	chunk, err := metadataReader.MetaData().RowGroup(0).ColumnChunk(1)
	if err != nil {
		t.Fatal(err)
	}
	start := int(chunk.DataPageOffset())
	header := newRawCompactReader(encoded[start:])
	encodingOffset := -1
	err = header.fields(func(id int, kind thrift.TType) error {
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
	if err != nil || encodingOffset < 0 {
		t.Fatalf("fixture page header: offset=%d error=%v", encodingOffset, err)
	}
	// RLE is not a valid BYTE_ARRAY value encoding, including ignored fields.
	encoded[start+encodingOffset] = byte(parquet.Encodings.RLE) * 2
	input := config.Input{Schema: &config.Schema{ExtraFields: "ignore", Order: []string{"id"}, Columns: map[string]config.Column{"id": {Type: "int64"}}}}
	ds, _ := rowGroupDataset(t, [][]byte{encoded}, 128<<20, input)
	if err := readErr(t, ds); err == nil {
		t.Fatal("malformed ignored column was accepted")
	}
}

func TestParquetDeclaredPageLimitIsResourceError(t *testing.T) {
	encoded := encodeRowGroups(t, 1, 4096, false,
		parquet.WithDictionaryDefault(false),
		parquet.WithMaxRowGroupLength(4096),
		parquet.WithDataPageSize(1<<20))
	ds := fixture{
		name: "oversized-page.parquet", body: string(encoded),
		input: config.Input{Type: "parquet"}, recordBytes: "1KiB",
	}.dataset(t)
	var limit *resource.ResourceLimitError
	if err := readErr(t, ds); !errors.As(err, &limit) {
		t.Fatalf("Next error=%v, want resource limit for declared page size", err)
	}
}

func TestParquetOriginalPageCRCIsVerifiedBeforeNormalization(t *testing.T) {
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	builder.Append(7)
	values := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{values}, 1)
	values.Release()
	original := encodeParquet(t, record)
	record.Release()
	reader, err := file.NewParquetReader(bytes.NewReader(original))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt-%t", corrupt), func(t *testing.T) {
			var payload [8]byte
			binary.LittleEndian.PutUint64(payload[:], 7)
			checksum := crc32.ChecksumIEEE(payload[:])
			if corrupt {
				checksum ^= 1
			}
			header := []byte{0x15, 0, 0x15, 16, 0x15, 16, 0x15}
			header = binary.AppendVarint(header, int64(int32(checksum)))
			header = append(header, 0x1c, 0x15, 2, 0x15, 0, 0x15, 6, 0x15, 6, 0, 0)
			meta := reader.MetaData()
			chunk := meta.RowGroups[0].Columns[0].MetaData
			column := meta.RowGroups[0].Columns[0]
			column.OffsetIndexOffset, column.OffsetIndexLength = nil, nil
			column.ColumnIndexOffset, column.ColumnIndexLength = nil, nil
			chunk.DictionaryPageOffset, chunk.IndexPageOffset = nil, nil
			chunk.BloomFilterOffset, chunk.BloomFilterLength = nil, nil
			chunk.DataPageOffset = 4
			chunk.TotalCompressedSize = int64(len(header) + len(payload))
			chunk.TotalUncompressedSize = chunk.TotalCompressedSize
			meta.RowGroups[0].TotalByteSize = chunk.TotalUncompressedSize
			footer, err := meta.Serialize(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			encoded := append([]byte("PAR1"), header...)
			encoded = append(encoded, payload[:]...)
			encoded = append(encoded, footer...)
			encoded = binary.LittleEndian.AppendUint32(encoded, uint32(len(footer)))
			encoded = append(encoded, "PAR1"...)
			ds, _ := rowGroupDataset(t, [][]byte{encoded}, 128<<20, config.Input{})
			err = readErr(t, ds)
			if corrupt && err == nil {
				t.Fatal("invalid original compressed-page CRC was accepted")
			}
			if !corrupt && err != nil {
				t.Fatalf("valid original compressed-page CRC rejected: %v", err)
			}
		})
	}
}

func rewriteParquetFooter(t *testing.T, encoded []byte, change func(*metadata.FileMetaData)) []byte {
	t.Helper()
	reader, err := file.NewParquetReader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	meta := reader.MetaData()
	change(meta)
	footer, err := meta.Serialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	originalSize := int(binary.LittleEndian.Uint32(encoded[len(encoded)-8:]))
	output := append([]byte(nil), encoded[:len(encoded)-8-originalSize]...)
	output = append(output, footer...)
	output = binary.LittleEndian.AppendUint32(output, uint32(len(footer)))
	return append(output, "PAR1"...)
}

func TestParquetRejectsInconsistentOrOverflowingMetadataCounts(t *testing.T) {
	original := encodeRowGroups(t, 1, 26, false)
	for _, tc := range []struct {
		name   string
		change func(*metadata.FileMetaData)
	}{
		{name: "file rows differ from groups", change: func(meta *metadata.FileMetaData) { meta.NumRows++ }},
		{name: "negative group count", change: func(meta *metadata.FileMetaData) { meta.RowGroups[0].NumRows = -1 }},
		{name: "chunk rows differ from group", change: func(meta *metadata.FileMetaData) { meta.RowGroups[0].Columns[0].MetaData.NumValues++ }},
		{name: "group prefix overflow", change: func(meta *metadata.FileMetaData) {
			meta.RowGroups[0].NumRows = 1<<63 - 1
			for _, column := range meta.RowGroups[0].Columns {
				column.MetaData.NumValues = 1<<63 - 1
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded := rewriteParquetFooter(t, original, tc.change)
			path := filepath.Join(t.TempDir(), "invalid.parquet")
			if err := os.WriteFile(path, encoded, 0600); err != nil {
				t.Fatal(err)
			}
			budget := resource.New(128 << 20)
			store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget)
			if err != nil {
				t.Fatal(err)
			}
			var ds *Dataset
			t.Cleanup(func() {
				if ds != nil {
					if err := ds.Close(); err != nil {
						t.Error(err)
					}
				}
				if err := store.Close(); err != nil {
					t.Error(err)
				}
				if used := budget.Used(); used != 0 {
					t.Errorf("source retained %d managed bytes after rejection", used)
				}
			})
			cfg := &config.Config{
				Input:     config.Input{Type: "parquet"},
				Source:    config.Source{URIs: []string{path}},
				Execution: config.Execution{Workers: 4, BatchTarget: "16KiB", MaxRecordBytes: "1MiB"},
			}
			ds, err = New(context.Background(), cfg, store, budget)
			if err == nil {
				reader, openErr := ds.Open(context.Background())
				err = openErr
				if err == nil {
					batch, readErr := reader.Next(context.Background())
					batch.Release()
					err = errors.Join(readErr, reader.Close())
				}
			}
			if err == nil {
				t.Fatal("inconsistent metadata counts were accepted")
			}
		})
	}
}

func TestParquetEmptyFilesAndGroupsPreserveSourceOrdinals(t *testing.T) {
	withEmptyGroup := rewriteParquetFooter(t, encodeRowGroups(t, 1, 26, false), func(meta *metadata.FileMetaData) {
		empty, err := normalizedGroupMetadata(meta, 0)
		if err != nil {
			t.Fatal(err)
		}
		group := empty.RowGroups[0]
		group.NumRows, group.TotalByteSize = 0, 0
		for _, column := range group.Columns {
			chunk := column.MetaData
			chunk.NumValues, chunk.TotalCompressedSize, chunk.TotalUncompressedSize = 0, 0, 0
			chunk.DataPageOffset = 4
		}
		meta.RowGroups = append(meta.RowGroups, group)
		copy(meta.RowGroups[2:], meta.RowGroups[1:len(meta.RowGroups)-1])
		meta.RowGroups[1] = group
	})
	empty := encodeRowGroups(t, 1, 0, false)
	ds, _ := rowGroupDataset(t, [][]byte{empty, withEmptyGroup, empty}, 128<<20, config.Input{})
	reader, err := ds.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen := 0
	for {
		batch, err := reader.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for row, location := range batch.Locations {
			seen++
			if batch.Record.Column(0).(*array.Int64).Value(row) != int64(seen) || location.SourceOrdinal != 2 || location.Row != uint64(seen) || location.GlobalRow != uint64(seen) {
				t.Errorf("row %d location=%+v", seen, location)
			}
		}
		batch.Release()
	}
	if seen != 26 {
		t.Errorf("rows=%d, want 26", seen)
	}
}

func TestParquetRetainedBatchSurvivesRowGroupReuseAndClose(t *testing.T) {
	for _, version := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			decimalType := &arrow.Decimal128Type{Precision: 12, Scale: 2}
			fields := []arrow.Field{
				{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
				{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
				{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
				{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
				{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
				{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
				{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
				{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
				{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
				{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
				{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
				{Name: "string", Type: arrow.BinaryTypes.String},
				{Name: "bytes", Type: arrow.BinaryTypes.Binary},
				{Name: "fixed", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}},
				{Name: "date32", Type: arrow.FixedWidthTypes.Date32},
				{Name: "date64", Type: arrow.FixedWidthTypes.Date64},
				{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
				{Name: "decimal", Type: decimalType},
			}
			if version == parquet.DataPageV1 {
				// Arrow's NULL-logical V2 writer derives num_nulls from disabled
				// statistics and emits an invalid zero count. Keep the source guard
				// strict; V1 supplies valid NULL ownership coverage.
				fields = append(fields, arrow.Field{Name: "null", Type: arrow.Null})
			}
			for i := range fields {
				fields[i].Nullable = true
			}
			schema := arrow.NewSchema(fields, nil)
			builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
			const rows = 113
			for row := range rows {
				for column, field := range fields {
					b := builder.Field(column)
					if row%5 == 0 || field.Type.ID() == arrow.NULL {
						b.AppendNull()
						continue
					}
					switch b := b.(type) {
					case *array.BooleanBuilder:
						b.Append(row%2 == 0)
					case *array.Int8Builder:
						b.Append(int8(row))
					case *array.Int16Builder:
						b.Append(int16(row * 17))
					case *array.Int32Builder:
						b.Append(int32(row * 12345))
					case *array.Int64Builder:
						b.Append(9007199254740993 + int64(row))
					case *array.Uint8Builder:
						b.Append(uint8(row))
					case *array.Uint16Builder:
						b.Append(uint16(row * 19))
					case *array.Uint32Builder:
						b.Append(uint32(row * 123456))
					case *array.Uint64Builder:
						b.Append(^uint64(0) - uint64(row))
					case *array.Float32Builder:
						b.Append(float32(row) + 0.25)
					case *array.Float64Builder:
						b.Append(float64(row) + 0.125)
					case *array.StringBuilder:
						b.Append(fmt.Sprintf("row-%d-α", row))
					case *array.BinaryBuilder:
						b.Append([]byte{byte(row), 0, 255})
					case *array.FixedSizeBinaryBuilder:
						b.Append([]byte{byte(row), byte(row >> 8), 255, 0})
					case *array.Date32Builder:
						b.Append(arrow.Date32(row - 60))
					case *array.Date64Builder:
						b.Append(arrow.Date64((int64(row) - 60) * 86400000))
					case *array.TimestampBuilder:
						b.Append(arrow.Timestamp(-123456789012345 + int64(row)))
					case *array.Decimal128Builder:
						b.Append(decimal128.FromI64(int64(row*100 + 7)))
					default:
						t.Fatalf("unsupported fixture builder %T", b)
					}
				}
			}
			original := builder.NewRecordBatch()
			builder.Release()
			defer original.Release()
			var encoded bytes.Buffer
			writer, err := pqarrow.NewFileWriter(schema, &encoded, parquet.NewWriterProperties(
				parquet.WithCompression(compress.Codecs.Snappy),
				parquet.WithDataPageVersion(version),
				parquet.WithDictionaryDefault(true),
				parquet.WithMaxRowGroupLength(13)), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
			if err != nil {
				t.Fatal(err)
			}
			if err = writer.Write(original); err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			ds, budget := rowGroupDataset(t, [][]byte{encoded.Bytes()}, 192<<20, config.Input{})
			baseline := budget.Used()
			reader, err := ds.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			retained, err := reader.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			seen := retained.Record.NumRows()
			for {
				batch, err := reader.Next(context.Background())
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					retained.Release()
					t.Fatal(err)
				}
				seen += batch.Record.NumRows()
				batch.Release()
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			if seen != rows {
				t.Errorf("rows=%d, want %d", seen, rows)
			}
			for column, field := range fields {
				var expected arrow.Array
				if field.Type.ID() == arrow.DATE64 {
					// Parquet DATE stores whole days and decodes as Date32.
					// Build independent expected buffers from original values;
					// never borrow a decoded buffer that reuse could corrupt.
					dates := original.Column(column).(*array.Date64)
					builder := array.NewDate32Builder(memory.NewGoAllocator())
					for row := range int(retained.Record.NumRows()) {
						if dates.IsNull(row) {
							builder.AppendNull()
						} else {
							builder.Append(arrow.Date32(int64(dates.Value(row)) / 86400000))
						}
					}
					expected = builder.NewArray()
					builder.Release()
				} else {
					expected = array.NewSlice(original.Column(column), 0, retained.Record.NumRows())
				}
				if !array.Equal(retained.Record.Column(column), expected) {
					t.Errorf("retained column %q changed after row-group reuse/close: got %s want %s", field.Name, retained.Record.Column(column), expected)
				}
				expected.Release()
			}
			retained.Release()
			if used := budget.Used(); used != baseline {
				t.Errorf("after retained batch release used=%d, before open=%d", used, baseline)
			}
		})
	}
}
