package source

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// A plan whose application fails (here: cancelled while establishing input
// baselines) must leave the dataset without a plan, and must honour the
// caller's context rather than one captured when the dataset was opened.
func TestFailedStatisticsPlanLeavesNoPlanApplied(t *testing.T) {
	resource.ConfigureParquetCodecs()
	b := array.NewInt64Builder(memory.NewGoAllocator())
	b.AppendValues([]int64{1, 2, 3}, nil)
	values := b.NewArray()
	b.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
	values.Release()
	path := nativeFixture(t, record)
	record.Release()

	budget := resource.New(128 << 20)
	store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		// Trusted statistics defer input baselines until the plan is applied.
		Input:     config.Input{Type: "parquet", Parquet: config.InputParquet{Statistics: "trusted"}},
		Source:    config.Source{URIs: []string{path}},
		Execution: config.Execution{BatchTarget: "16KiB", MaxRecordBytes: "1MiB"},
	}
	ds, err := New(context.Background(), cfg, store, budget, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ds.Close()
		if err := store.Close(); err != nil {
			t.Error(err)
		}
		if n := budget.Used(); n != 0 {
			t.Errorf("leaked %d managed bytes", n)
		}
	})
	groups, err := ds.ParquetGroups()
	if err != nil {
		t.Fatal(err)
	}
	decisions := make([]data.GroupEvidence, len(groups))
	for i, g := range groups {
		decisions[i] = data.GroupEvidence{
			SourceOrdinal: g.SourceOrdinal, RowGroup: g.RowGroup, DeclaredRows: g.DeclaredRows, Source: g.Source,
			FooterSHA256: g.FooterSHA256, FooterOffset: g.FooterOffset, FooterLength: g.FooterLength, Action: "scan",
		}
	}
	plan := data.NewStatisticsPlan(decisions, nil)
	defer plan.Release()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ds.ApplyStatisticsPlan(cancelled, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("ApplyStatisticsPlan with a cancelled context = %v, want context.Canceled", err)
	}
	if err := ds.ApplyStatisticsPlan(context.Background(), plan); err != nil {
		t.Fatalf("ApplyStatisticsPlan after a failed attempt = %v, want the plan applied", err)
	}
}
