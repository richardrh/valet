package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

func TestPipelineOverlapsEvaluationAndOrderedWriteWithoutEarlyRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := runConfig(t, "rows.csv", "out")
	cfg.Execution.Workers = 3
	budget := resource.New(cfg.BudgetBytes())
	var released [6]atomic.Bool
	var resultsReleased [6]atomic.Bool
	var live atomic.Int64
	var peak int64
	secondEvaluated := make(chan struct{})
	next := 0
	reader := &stageReader{next: func(context.Context) (*data.Batch, error) {
		if next == len(released) {
			return nil, io.EOF
		}
		i := next
		next++
		peak = max(peak, live.Add(1))
		return stageBatch(budget, int64(i), func() {
			released[i].Store(true)
			live.Add(-1)
		}), nil
	}}
	program := &stageProgram{evaluate: func(batch *data.Batch) (*data.Result, error) {
		i := stageValue(batch)
		if i == 1 {
			close(secondEvaluated)
		}
		return &data.Result{
			Buckets: []data.Bucket{data.BucketPass},
			Cleanup: func() { resultsReleased[i].Store(true) },
		}, nil
	}}
	var order []int64
	writer := &stageWriter{write: func(batch *data.Batch, result *data.Result) error {
		value := stageValue(batch)
		if value == 0 {
			select {
			case <-secondEvaluated:
			case <-ctx.Done():
				return errors.New("writer did not overlap the next evaluation")
			}
		}
		if released[value].Load() || resultsReleased[value].Load() {
			return errors.New("batch or result released before writer finished")
		}
		if len(result.Buckets) != 1 || result.Buckets[0] != data.BucketPass {
			return errors.New("writer received stale result scratch")
		}
		order = append(order, value)
		return nil
	}}
	rows, err := validatePass(ctx, cancel, cfg, reader, program, writer, budget, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if rows != int64(len(released)) || !slices.Equal(order, []int64{0, 1, 2, 3, 4, 5}) {
		t.Fatalf("rows/order = %d/%v", rows, order)
	}
	for i := range released {
		if !released[i].Load() || !resultsReleased[i].Load() {
			t.Errorf("batch or result %d was not released", i)
		}
	}
	if peak > int64(cfg.Execution.Workers) || live.Load() != 0 {
		t.Errorf("peak/live in-flight batches = %d/%d", peak, live.Load())
	}
	if budget.Used() != 0 {
		t.Errorf("managed bytes after join = %d, want 0", budget.Used())
	}
}

func TestPipelineWriterFailureCancelsAndJoinsPrefetch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := runConfig(t, "rows.csv", "out")
	cfg.Execution.Workers = 3
	budget := resource.New(cfg.BudgetBytes())
	writerErr := errors.New("output failed")
	readingThird := make(chan struct{})
	var active atomic.Bool
	var evaluated, resultsReleased atomic.Int64
	var released atomic.Int64
	next := 0
	reader := &stageReader{next: func(ctx context.Context) (*data.Batch, error) {
		next++
		if next == 3 {
			active.Store(true)
			defer active.Store(false)
			close(readingThird)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return stageBatch(budget, int64(next), func() { released.Add(1) }), nil
	}}
	program := &stageProgram{evaluate: func(*data.Batch) (*data.Result, error) {
		evaluated.Add(1)
		return &data.Result{
			Buckets: []data.Bucket{data.BucketPass},
			Cleanup: func() { resultsReleased.Add(1) },
		}, nil
	}}
	writer := &stageWriter{write: func(*data.Batch, *data.Result) error {
		select {
		case <-readingThird:
			return writerErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	_, err := validatePass(ctx, cancel, cfg, reader, program, writer, budget, slog.New(slog.DiscardHandler))
	if !errors.Is(err, writerErr) {
		t.Fatalf("error = %v, want original writer error", err)
	}
	if active.Load() || released.Load() != 2 || budget.Used() != 0 {
		t.Fatalf("after return: reader active=%v, released=%d, managed=%d", active.Load(), released.Load(), budget.Used())
	}
	if resultsReleased.Load() != evaluated.Load() {
		t.Fatalf("released results=%d, evaluated=%d", resultsReleased.Load(), evaluated.Load())
	}
}

func TestPipelineEvaluationFailureJoinsBlockedRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := runConfig(t, "rows.csv", "out")
	cfg.Execution.Workers = 2
	budget := resource.New(cfg.BudgetBytes())
	evalErr := errors.New("evaluation failed")
	readingSecond := make(chan struct{})
	var active atomic.Bool
	var released atomic.Bool
	first := true
	reader := &stageReader{next: func(ctx context.Context) (*data.Batch, error) {
		if first {
			first = false
			return stageBatch(budget, 0, func() { released.Store(true) }), nil
		}
		active.Store(true)
		defer active.Store(false)
		close(readingSecond)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	program := &stageProgram{evaluate: func(*data.Batch) (*data.Result, error) {
		select {
		case <-readingSecond:
			return nil, evalErr
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	writer := &stageWriter{write: func(*data.Batch, *data.Result) error {
		return errors.New("unevaluated batch reached writer")
	}}
	_, err := validatePass(ctx, cancel, cfg, reader, program, writer, budget, slog.New(slog.DiscardHandler))
	if !errors.Is(err, evalErr) {
		t.Fatalf("error = %v, want original evaluation error", err)
	}
	if active.Load() || !released.Load() || budget.Used() != 0 {
		t.Fatalf("after return: reader active=%v, released=%v, managed=%d", active.Load(), released.Load(), budget.Used())
	}
}

func TestPipelineResourcePanicJoinsBeforeReturning(t *testing.T) {
	cfg := runConfig(t, "rows.csv", "out")
	cfg.Execution.Workers = 2
	budget := resource.New(cfg.BudgetBytes())
	limit := &resource.ResourceLimitError{Phase: "writer", Limit: 10, Used: 8, Requested: 4}
	var released atomic.Int64
	next := 0
	reader := &stageReader{next: func(context.Context) (*data.Batch, error) {
		if next == 2 {
			return nil, io.EOF
		}
		next++
		return stageBatch(budget, int64(next), func() { released.Add(1) }), nil
	}}
	program := &stageProgram{evaluate: func(*data.Batch) (*data.Result, error) {
		return &data.Result{Buckets: []data.Bucket{data.BucketPass}}, nil
	}}
	writer := &stageWriter{write: func(*data.Batch, *data.Result) error { panic(limit) }}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := validatePass(ctx, cancel, cfg, reader, program, writer, budget, slog.New(slog.DiscardHandler))
	if !errors.Is(err, limit) {
		t.Fatalf("error = %v, want resource panic", err)
	}
	if released.Load() != int64(next) || budget.Used() != 0 {
		t.Fatalf("released=%d read=%d managed=%d", released.Load(), next, budget.Used())
	}
}

func TestPipelineSingleWorkerAndTightBudgetUseSerialOwnership(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			cfg := runConfig(t, "rows.csv", "out")
			cfg.Execution.Workers = workers
			budget := resource.New(1 << 20)
			var live atomic.Int64
			next := 0
			reader := &stageReader{next: func(context.Context) (*data.Batch, error) {
				if live.Load() != 0 {
					return nil, errors.New("tight-budget run prefetched a second batch")
				}
				if next == 3 {
					return nil, io.EOF
				}
				next++
				live.Add(1)
				return stageBatch(budget, int64(next), func() { live.Add(-1) }), nil
			}}
			program := &stageProgram{evaluate: func(*data.Batch) (*data.Result, error) {
				return &data.Result{Buckets: []data.Bucket{data.BucketPass}}, nil
			}}
			writer := &stageWriter{write: func(*data.Batch, *data.Result) error { return nil }}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rows, err := validatePass(ctx, cancel, cfg, reader, program, writer, budget, slog.New(slog.DiscardHandler))
			if err != nil || rows != 3 || live.Load() != 0 || budget.Used() != 0 {
				t.Fatalf("rows=%d error=%v live=%d managed=%d", rows, err, live.Load(), budget.Used())
			}
		})
	}
}

type stageReader struct {
	next func(context.Context) (*data.Batch, error)
}

func (r *stageReader) Next(ctx context.Context) (*data.Batch, error) { return r.next(ctx) }
func (r *stageReader) Close() error                                  { return nil }

type stageProgram struct {
	evaluate func(*data.Batch) (*data.Result, error)
}

func (p *stageProgram) Summary() *data.Summary                           { return nil }
func (p *stageProgram) Evaluate(batch *data.Batch) (*data.Result, error) { return p.evaluate(batch) }
func (p *stageProgram) ApplyStatistics(*data.GroupEvidence) error        { return nil }

type stageWriter struct {
	write func(*data.Batch, *data.Result) error
}

func (w *stageWriter) WriteBatch(batch *data.Batch, result *data.Result) error {
	return w.write(batch, result)
}
func (w *stageWriter) WriteEvidence(*data.GroupEvidence) error { return nil }

func stageBatch(budget *resource.Budget, value int64, cleanup func()) *data.Batch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(budget.Allocator(), schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(value)
	return &data.Batch{Record: builder.NewRecordBatch(), Cleanup: cleanup}
}

func stageValue(batch *data.Batch) int64 {
	return batch.Record.Column(0).(*array.Int64).Value(0)
}

func TestPipelineCancellationAndProgrammingPanicJoinRead(t *testing.T) {
	for _, panicWriter := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic%v", panicWriter), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := runConfig(t, "rows.csv", "out")
			cfg.Execution.Workers = 2
			budget := resource.New(cfg.BudgetBytes())
			readingSecond := make(chan struct{})
			var active, released, resultReleased atomic.Bool
			first := true
			reader := &stageReader{next: func(ctx context.Context) (*data.Batch, error) {
				if first {
					first = false
					return stageBatch(budget, 0, func() { released.Store(true) }), nil
				}
				active.Store(true)
				defer active.Store(false)
				close(readingSecond)
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			program := &stageProgram{evaluate: func(*data.Batch) (*data.Result, error) {
				return &data.Result{
					Buckets: []data.Bucket{data.BucketPass},
					Cleanup: func() { resultReleased.Store(true) },
				}, nil
			}}
			defect := errors.New("programming defect")
			writer := &stageWriter{write: func(*data.Batch, *data.Result) error {
				select {
				case <-readingSecond:
				case <-ctx.Done():
					return ctx.Err()
				}
				if panicWriter {
					panic(defect)
				}
				cancel()
				return ctx.Err()
			}}
			var err error
			var panicValue any
			func() {
				defer func() { panicValue = recover() }()
				_, err = validatePass(ctx, cancel, cfg, reader, program, writer, budget, slog.New(slog.DiscardHandler))
			}()
			if panicWriter {
				if panicValue != defect {
					t.Fatalf("panic = %v, want original programming defect", panicValue)
				}
			} else if panicValue != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("panic/error = %v/%v, want cancellation", panicValue, err)
			}
			if active.Load() || !released.Load() || !resultReleased.Load() || budget.Used() != 0 {
				t.Fatalf("reader active=%v batch/result released=%v/%v managed=%d", active.Load(), released.Load(), resultReleased.Load(), budget.Used())
			}
		})
	}
}
