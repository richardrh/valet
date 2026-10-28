package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// Poison freed input buffers so a buffered encoder cannot accidentally borrow
// their bytes until a later batch or Complete.
type poisonAllocator struct{ memory.Allocator }

func (a poisonAllocator) Free(b []byte) {
	clear(b)
	a.Allocator.Free(b)
}

func nativeSinkBatch(mem memory.Allocator, schema *arrow.Schema, start, rows int) *data.Batch {
	builder := array.NewRecordBuilder(mem, schema)
	for i := -1; i <= rows; i++ {
		value := start + i
		builder.Field(0).(*array.StringBuilder).Append(strconv.Itoa(value) + strings.Repeat("x", 512))
		status := "ready"
		if value%2 != 0 {
			status = "done"
		}
		if value%3 == 0 {
			// Include very wide categorical values without changing the schema.
			status = strconv.Itoa(value) + strings.Repeat("s", 512)
		}
		if value%7 == 0 {
			builder.Field(1).(*array.StringBuilder).AppendNull()
		} else {
			builder.Field(1).(*array.StringBuilder).Append(status)
		}
		builder.Field(2).(*array.Int64Builder).Append(math.MaxInt64 - int64(value))
	}
	full := builder.NewRecordBatch()
	builder.Release()
	record := full.NewSlice(1, int64(rows+1))
	full.Release()
	batch := &data.Batch{Record: record}
	for row := range rows {
		value := start + row
		batch.Locations = append(batch.Locations, data.Location{
			Source:        "native-" + strconv.Itoa(value/200) + ".parquet",
			SourceOrdinal: uint64(value/200 + 1),
			Row:           uint64(value%200 + 1),
			GlobalRow:     uint64(9007199254740993) + uint64(value),
		})
	}
	return batch
}

func TestBufferedParquetOwnsValuesAcrossNativeAndMixedBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	store := newFakeStore()
	budget := resource.New(32 << 20)
	w := startWriter(t, testConfig(t, "parquet"), store, budget, schema)
	defer w.Abort()
	mem := memory.NewCheckedAllocator(poisonAllocator{memory.DefaultAllocator})
	var expected [numRowKinds][]int
	summary := data.NewSummary()
	for batchIndex := range 4 {
		batch := nativeSinkBatch(mem, schema, batchIndex*200, 200)
		result := &data.Result{Buckets: make([]data.Bucket, 200)}
		for row := range result.Buckets {
			if batchIndex%2 != 0 {
				result.Buckets[row] = data.Bucket(row % numRowKinds)
			}
			kind := int(result.Buckets[row])
			expected[kind] = append(expected[kind], batchIndex*200+row)
		}
		err := w.WriteBatch(batch, result)
		batch.Release()
		result.Release()
		mem.AssertSize(t, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	summary.Total = 800
	summary.Passed, summary.Warned, summary.Errored = uint64(len(expected[0])), uint64(len(expected[1])), uint64(len(expected[2]))
	if _, err := w.Complete(summary); err != nil {
		t.Fatal(err)
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("managed bytes after Complete = %d, want 0", got)
	}
	for kind := range partKind(numRowKinds) {
		body := store.object(t, w, kind.String()+"/part-000001.parquet")
		reader, err := file.NewParquetReader(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		if got := reader.NumRowGroups(); got != 1 {
			t.Errorf("%s groups = %d, want one cross-batch group", kind, got)
		}
		ar, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 37}, memory.DefaultAllocator)
		if err != nil {
			t.Fatal(err)
		}
		records, err := ar.GetRecordReader(context.Background(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer records.Release()
		seen := 0
		for records.Next() {
			rec := records.RecordBatch()
			for row := range int(rec.NumRows()) {
				if seen >= len(expected[kind]) {
					t.Fatalf("%s produced extra row", kind)
				}
				value := expected[kind][seen]
				seen++
				if got, want := rec.Column(0).(*array.String).Value(row), strconv.Itoa(value)+strings.Repeat("x", 512); got != want {
					t.Fatalf("%s row %d id changed", kind, seen)
				}
				status := rec.Column(1).(*array.String)
				if status.IsNull(row) != (value%7 == 0) {
					t.Fatalf("%s row %d status validity changed", kind, seen)
				}
				if value%7 != 0 {
					want := "ready"
					if value%2 != 0 {
						want = "done"
					}
					if value%3 == 0 {
						want = strconv.Itoa(value) + strings.Repeat("s", 512)
					}
					if got := status.Value(row); got != want {
						t.Fatalf("%s row %d status changed", kind, seen)
					}
				}
				if got, want := rec.Column(2).(*array.Int64).Value(row), math.MaxInt64-int64(value); got != want {
					t.Fatalf("%s row %d amount = %d, want %d", kind, seen, got, want)
				}
				if got, want := rec.Column(3).(*array.String).Value(row), "native-"+strconv.Itoa(value/200)+".parquet"; got != want {
					t.Fatalf("%s row %d source = %q, want %q", kind, seen, got, want)
				}
				for column, want := range []uint64{uint64(value/200 + 1), uint64(value%200 + 1), uint64(9007199254740993) + uint64(value)} {
					if got := rec.Column(column + 4).(*array.Uint64).Value(row); got != want {
						t.Fatalf("%s row %d provenance column %d = %d, want %d", kind, seen, column, got, want)
					}
				}
			}
		}
		if err := records.Err(); err != nil {
			t.Fatal(err)
		}
		if seen != len(expected[kind]) {
			t.Errorf("%s rows = %d, want %d", kind, seen, len(expected[kind]))
		}
	}
}

func TestBufferedParquetSmallPartsPreserveOrder(t *testing.T) {
	cfg := testConfig(t, "parquet")
	cfg.Output.PartSize = "1B"
	store := newFakeStore()
	budget := resource.New(16 << 20)
	batch, result := passOnlyFixture(t)
	defer batch.Release()
	defer result.Release()
	w := startWriter(t, cfg, store, budget, batch.Record.Schema())
	defer w.Abort()
	for range 2 {
		if err := w.WriteBatch(batch, result); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Complete(passedSummary(6)); err != nil {
		t.Fatal(err)
	}
	artifacts := decodeLines(t, store.object(t, w, "artifacts.ndjson"))
	if len(artifacts) != 6 {
		t.Fatalf("artifacts = %d, want six bounded one-row parts", len(artifacts))
	}
	for i, artifact := range artifacts {
		var uri string
		var count uint64
		unmarshalField(t, artifact, "uri", &uri)
		unmarshalField(t, artifact, "count", &count)
		if count != 1 {
			t.Fatalf("part %d rows = %d, want 1", i, count)
		}
		reader, err := file.NewParquetReader(bytes.NewReader(store.objects[uri]))
		if err != nil {
			t.Fatal(err)
		}
		ar, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 16}, memory.DefaultAllocator)
		if err != nil {
			reader.Close()
			t.Fatal(err)
		}
		records, err := ar.GetRecordReader(context.Background(), nil, nil)
		if err != nil {
			reader.Close()
			t.Fatal(err)
		}
		if !records.Next() {
			t.Fatal("missing row in small part")
		}
		if got, want := records.RecordBatch().Column(6).(*array.Uint64).Value(0), batch.Locations[i%3].GlobalRow; got != want {
			t.Errorf("part %d global row = %d, want %d", i, got, want)
		}
		records.Release()
		reader.Close()
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("managed bytes after Complete = %d, want 0", got)
	}
}

func TestNativeRejectFastPathDoesNotSkipOverridesOrIngestion(t *testing.T) {
	for _, tc := range []struct {
		name                string
		states, ingest, raw bool
		wantReject          bool
		wantError           bool
	}{
		{name: "native"},
		{name: "state_override", states: true, raw: true, wantReject: true},
		{name: "ingestion", ingest: true, raw: true, wantReject: true},
		{name: "missing_raw", states: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch, result, summary := fixture(t)
			defer batch.Release()
			defer result.Release()
			if !tc.states {
				batch.States = nil
			}
			if !tc.ingest {
				batch.Ingest = nil
			}
			if !tc.raw {
				batch.Raw = nil
			}
			store := newFakeStore()
			budget := resource.New(16 << 20)
			w := startWriter(t, testConfig(t, "parquet"), store, budget, batch.Record.Schema())
			defer w.Abort()
			err := w.WriteBatch(batch, result)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "missing raw reject record") {
					t.Fatalf("WriteBatch err = %v, want missing raw rejection", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := w.Complete(summary); err != nil {
					t.Fatal(err)
				}
				body, ok := store.objects[storage.Join(w.root, "rejects/part-000001.ndjson")]
				if ok != tc.wantReject {
					t.Fatalf("reject artifact present = %v, want %v", ok, tc.wantReject)
				}
				if ok && len(decodeLines(t, body)) != 1 {
					t.Fatal("reject artifact did not contain exactly one row")
				}
			}
			if got := budget.Used(); got != 0 {
				t.Errorf("managed bytes after terminal operation = %d, want 0", got)
			}
		})
	}
}

func TestBufferedParquetCancellationReleasesActiveGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newFakeStore()
	budget := resource.New(16 << 20)
	batch, result := passOnlyFixture(t)
	defer batch.Release()
	defer result.Release()
	w, err := New(ctx, testConfig(t, "parquet"), store, budget, "run", batch.Record.Schema(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := w.WriteInputs(nil); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBatch(batch, result); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := w.Complete(passedSummary(3)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Complete err = %v, want context cancellation", err)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("managed bytes after cancellation = %d, want 0", got)
	}
	if len(store.active) != 0 {
		t.Errorf("open outputs after cancellation = %d, want 0", len(store.active))
	}
	if _, ok := store.objects[storage.Join(w.root, "manifest.json")]; ok {
		t.Fatal("manifest published after cancellation")
	}
}

type completedDigestStore struct {
	*fakeStore
	failDigest bool
}

func (s *completedDigestStore) Create(ctx context.Context, uri string) (storage.Writer, error) {
	raw, err := s.fakeStore.Create(ctx, uri)
	if err != nil {
		return nil, err
	}
	return &completedDigestOutput{fakeOutput: raw.(*fakeOutput), store: s}, nil
}

type completedDigestOutput struct {
	*fakeOutput
	store *completedDigestStore
}

func (o *completedDigestOutput) CompletedDigest() (int64, [32]byte, bool) {
	if !o.closed || o.aborted || o.store.failDigest {
		return 0, [32]byte{}, false
	}
	return int64(o.Len()), sha256.Sum256(o.Bytes()), true
}

func TestCompletedStorageDigestControlsPublication(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(strconv.FormatBool(unavailable), func(t *testing.T) {
			store := &completedDigestStore{fakeStore: newFakeStore()}
			budget := resource.New(16 << 20)
			batch, result := passOnlyFixture(t)
			defer batch.Release()
			defer result.Release()
			w, err := New(context.Background(), testConfig(t, "parquet"), store, budget, "run", batch.Record.Schema(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			if err := w.WriteInputs(nil); err != nil {
				t.Fatal(err)
			}
			if err := w.WriteBatch(batch, result); err != nil {
				t.Fatal(err)
			}
			store.failDigest = unavailable
			_, err = w.Complete(passedSummary(3))
			if unavailable {
				if err == nil {
					t.Fatal("Complete succeeded without completed digest")
				}
				if _, ok := store.objects[storage.Join(w.root, "manifest.json")]; ok {
					t.Fatal("manifest published without completed digest")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				artifacts := decodeLines(t, store.object(t, w, "artifacts.ndjson"))
				if len(artifacts) != 1 {
					t.Fatalf("artifacts = %d, want 1", len(artifacts))
				}
				var uri, digest string
				var size int64
				unmarshalField(t, artifacts[0], "uri", &uri)
				unmarshalField(t, artifacts[0], "sha256", &digest)
				unmarshalField(t, artifacts[0], "size", &size)
				sum := sha256.Sum256(store.objects[uri])
				if size != int64(len(store.objects[uri])) || digest != hex.EncodeToString(sum[:]) {
					t.Fatal("artifact reference does not match completed storage bytes")
				}
			}
			if got := budget.Used(); got != 0 {
				t.Errorf("managed bytes after terminal operation = %d, want 0", got)
			}
		})
	}
}

func TestSampledCategoricalDictionariesPreserveChangingDistributions(t *testing.T) {
	for _, categoryType := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		t.Run(categoryType.Name(), func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "wide_identifier", Type: arrow.BinaryTypes.String},
				{Name: "renamed_category", Type: categoryType},
			}, nil)
			store := newFakeStore()
			budget := resource.New(16 << 20)
			w := startWriter(t, testConfig(t, "parquet"), store, budget, schema)
			defer w.Abort()
			mem := memory.NewCheckedAllocator(poisonAllocator{memory.DefaultAllocator})
			want := make([]string, 0, 400)
			for batchIndex := range 2 {
				builder := array.NewRecordBuilder(mem, schema)
				batch := &data.Batch{}
				result := &data.Result{Buckets: make([]data.Bucket, 200)}
				for row := range 200 {
					value := batchIndex*200 + row
					builder.Field(0).(*array.StringBuilder).Append(strconv.Itoa(value) + strings.Repeat("i", 512))
					category := "same"
					if batchIndex != 0 {
						// The initial low-cardinality sample must not prevent
						// bounded fallback when later values become large/unique.
						category = strconv.Itoa(value) + strings.Repeat("c", 512)
					}
					want = append(want, category)
					switch categoryType.ID() {
					case arrow.STRING:
						builder.Field(1).(*array.StringBuilder).Append(category)
					case arrow.LARGE_STRING:
						builder.Field(1).(*array.LargeStringBuilder).Append(category)
					}
					batch.Locations = append(batch.Locations, data.Location{
						Source: "native.parquet", SourceOrdinal: 1,
						Row: uint64(value + 1), GlobalRow: uint64(value + 1),
					})
				}
				batch.Record = builder.NewRecordBatch()
				builder.Release()
				err := w.WriteBatch(batch, result)
				batch.Release()
				result.Release()
				mem.AssertSize(t, 0)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.Complete(passedSummary(400)); err != nil {
				t.Fatal(err)
			}
			if got := budget.Used(); got != 0 {
				t.Fatalf("managed bytes after Complete = %d, want 0", got)
			}
			reader, err := file.NewParquetReader(bytes.NewReader(store.object(t, w, "pass/part-000001.parquet")))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			metadata := reader.MetaData().RowGroup(0)
			for column, wantDictionary := range []bool{false, true} {
				chunk, err := metadata.ColumnChunk(column)
				if err != nil {
					t.Fatal(err)
				}
				if got := chunk.HasDictionaryPage(); got != wantDictionary {
					t.Errorf("column %d dictionary = %v, want %v", column, got, wantDictionary)
				}
			}
			ar, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 37}, memory.DefaultAllocator)
			if err != nil {
				t.Fatal(err)
			}
			records, err := ar.GetRecordReader(context.Background(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer records.Release()
			seen := 0
			for records.Next() {
				record := records.RecordBatch()
				for row := range int(record.NumRows()) {
					if seen >= len(want) {
						t.Fatal("extra output row")
					}
					got := record.Column(1).(interface{ Value(int) string }).Value(row)
					if got != want[seen] {
						t.Fatalf("category row %d changed", seen)
					}
					if got := record.Column(0).(*array.String).Value(row); got != strconv.Itoa(seen)+strings.Repeat("i", 512) {
						t.Fatalf("identifier row %d changed", seen)
					}
					seen++
				}
			}
			if err := records.Err(); err != nil {
				t.Fatal(err)
			}
			if seen != len(want) {
				t.Errorf("output rows = %d, want %d", seen, len(want))
			}
		})
	}
}

func TestCategoricalPropertyConstructionRespectsWideSchemaBudget(t *testing.T) {
	const columns = 256
	fields := make([]arrow.Field, columns)
	for column := range fields {
		fields[column] = arrow.Field{Name: "category_" + strconv.Itoa(column), Type: arrow.BinaryTypes.String}
	}
	schema := arrow.NewSchema(fields, nil)
	budget := resource.New(16 << 20)
	store := newFakeStore()
	w := startWriter(t, testConfig(t, "parquet"), store, budget, schema)
	defer w.Abort()
	builder := array.NewRecordBuilder(budget.Allocator(), schema)
	for column := range fields {
		values := builder.Field(column).(*array.StringBuilder)
		for range 16 {
			values.Append("same")
		}
	}
	batch := &data.Batch{Record: builder.NewRecordBatch()}
	builder.Release()
	defer batch.Release()
	result := &data.Result{Buckets: make([]data.Bucket, 16)}
	defer result.Release()
	for row := range 16 {
		batch.Locations = append(batch.Locations, data.Location{
			Source: "native.parquet", SourceOrdinal: 1,
			Row: uint64(row + 1), GlobalRow: uint64(row + 1),
		})
	}
	// Leave one byte less than the complete property-construction working set.
	// The sampler must refuse before allocating a column-proportional options
	// list, rather than relying on the later encoder-construction reservation.
	probeCharge := int64(dictionarySampleCharge + columns*dictionaryOptionsPerFieldCharge)
	held := budget.Limit() - budget.Used() - int64(selectionCharge) - probeCharge + 1
	if err := budget.Reserve(held, "test pressure"); err != nil {
		t.Fatal(err)
	}
	defer budget.Release(held)
	err := w.WriteBatch(batch, result)
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) || limit.Requested != probeCharge {
		t.Fatalf("WriteBatch err = %v, want refusal of %d-byte dictionary construction", err, probeCharge)
	}
	batch.Release()
	if got := budget.Used(); got != held {
		t.Errorf("managed bytes after failed write = %d, want only held pressure %d", got, held)
	}
	if len(store.active) != 0 {
		t.Errorf("open outputs after failed write = %d, want 0", len(store.active))
	}
	if _, ok := store.objects[storage.Join(w.root, "manifest.json")]; ok {
		t.Fatal("manifest published after property-construction resource failure")
	}
}
