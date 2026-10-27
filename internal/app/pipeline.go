package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/util"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/source"
)

// evaluator and batchWriter are the pass's only seams: *checks.Program and
// *sink.Writer in production, and stage fakes in tests that need faults real
// inputs cannot produce on demand. A nil batchWriter means report mode.
type evaluator interface {
	Summary() *data.Summary
	Evaluate(*data.Batch) (*data.Result, error)
	ApplyStatistics(*data.GroupEvidence) error
}

// WriteBatch is called in input order by one goroutine. It must finish using
// borrowed batch/result data before returning, or retain its own accounted copy.
type batchWriter interface {
	WriteBatch(*data.Batch, *data.Result) error
	WriteEvidence(*data.GroupEvidence) error
}

// validatePass keeps stateful evaluation on its caller and gives Next and
// WriteBatch one owner each. Handoffs transfer ownership, not borrowed scratch.
// Every stage is joined before the caller may close or publish anything.
func validatePass(ctx context.Context, cancelRun context.CancelFunc, cfg *config.Config, reader source.Reader, program evaluator, writer batchWriter, budget *resource.Budget, log *slog.Logger) (int64, error) {
	inflight := cfg.Execution.Workers
	// Leave space for builders, result growth, decoder workspaces and output.
	// This is an admission bound, not a second charge for already-accounted
	// payloads. The allocator still enforces the exact live-byte budget.
	batchBytes := max(cfg.BatchBytes(), cfg.RecordBytes(), 1)
	available := max(budget.Limit()-budget.Used(), 0)
	if slots := available / batchBytes / 4; int64(inflight) > slots {
		inflight = int(slots)
	}
	debug := log.Enabled(ctx, slog.LevelDebug)
	if inflight < 2 {
		return validateSerial(ctx, cfg, reader, program, writer, budget, log, debug)
	}
	// Channels are unbuffered; this charge covers their headers, stage state
	// and the fixed number of ownership slots. Payloads own their own charges.
	charge, err := budget.Hold(1024+int64(inflight)*64, "validation pipeline")
	if err != nil {
		return 0, err
	}
	defer charge.Release()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	state := &passState{cancel: func() {
		cancel()
		// Compile and Open may retain the run context as well as honoring the
		// context passed to Next. Interrupt that work before joining stages.
		cancelRun()
	}}
	reads := make(chan *data.Batch)
	writes := make(chan evaluatedBatch)
	// A slot is acquired before Next, and held through the last writer use.
	// Thus decoding in progress also counts against the total in-flight bound.
	slots := make(chan struct{}, inflight)
	var stages sync.WaitGroup
	stages.Add(2)
	go func() {
		defer stages.Done()
		defer close(reads)
		defer state.capture()
		var pending *data.Batch
		held := false
		defer func() {
			pending.Release()
			if held {
				<-slots
			}
		}()
		for {
			queued := time.Now()
			select {
			case slots <- struct{}{}:
				held = true
			case <-ctx.Done():
				return
			}
			diagnostics.AddWait(ctx, diagnostics.Decode, time.Since(queued))
			if ctx.Err() != nil {
				return
			}
			var readErr error
			pending, readErr = reader.Next(ctx)
			if errors.Is(readErr, io.EOF) {
				return
			}
			if readErr != nil {
				state.fail(fmt.Errorf("read validation input: %w", readErr))
				return
			}
			handoff := time.Now()
			select {
			case reads <- pending:
				pending, held = nil, false
			case <-ctx.Done():
				return
			}
			diagnostics.AddWait(ctx, diagnostics.Decode, time.Since(handoff))
		}
	}()
	var rows int64 // written by the writer, read only after stages.Wait.
	go func() {
		defer stages.Done()
		defer state.capture()
		var pending evaluatedBatch
		defer func() { pending.release(slots) }()
		for next := range writes {
			pending = next
			if ctx.Err() == nil {
				if writeErr := writeEvaluatedBatch(ctx, cfg, writer, pending.batch, pending.result); writeErr != nil {
					state.fail(fmt.Errorf("write output batch: %w", writeErr))
				} else {
					if pending.batch.Record != nil {
						rows += pending.batch.Record.NumRows()
						if debug {
							log.DebugContext(ctx, "batch validated", "phase", "validation", "rows", pending.batch.Record.NumRows(), "bytes", util.TotalRecordSize(pending.batch.Record), "managed_bytes", budget.Used())
						}
					}
				}
			}
			pending.release(slots)
		}
	}()
	// Catch at the stage boundary so even a programming panic first cancels,
	// drains and joins the other owners; it is re-panicked after that cleanup.
	func() {
		defer state.capture()
		var pending evaluatedBatch
		defer func() { pending.release(slots) }()
		for batch := range reads {
			pending.batch = batch
			if ctx.Err() != nil {
				return
			}
			if evidenceErr := applyGroupEvidence(program, batch); evidenceErr != nil {
				state.fail(evidenceErr)
				return
			}
			if batch.Record != nil {
				var evalErr error
				evaluation := diagnostics.Start(ctx, diagnostics.Evaluate)
				pending.result, evalErr = program.Evaluate(batch)
				evaluation.End(evalErr)
				if evalErr != nil {
					state.fail(fmt.Errorf("evaluate checks: %w", evalErr))
					return
				}
				diagnostics.AddRows(ctx, diagnostics.Evaluate, uint64(batch.Record.NumRows()))
			} else if batch.Evidence == nil {
				state.fail(errors.New("input batch has neither values nor evidence"))
				return
			}
			if writer == nil && pending.result != nil {
				if reportErr := accountReportBatch(program.Summary(), batch, pending.result); reportErr != nil {
					state.fail(reportErr)
					return
				}
			}
			queued := time.Now()
			select {
			case writes <- pending:
				pending = evaluatedBatch{}
			case <-ctx.Done():
				return
			}
			diagnostics.AddWait(ctx, diagnostics.Evaluate, time.Since(queued))
		}
	}()
	close(writes)
	stages.Wait()
	// A canceled reader may have completed a handoff just as evaluation
	// stopped. No resources can be closed until all such ownership is drained.
	for batch := range reads {
		batch.Release()
		<-slots
	}
	if state.panicValue != nil {
		panic(state.panicValue)
	}
	if state.err != nil {
		return rows, state.err
	}
	return rows, ctx.Err()
}

type evaluatedBatch struct {
	batch  *data.Batch
	result *data.Result
}

func (b *evaluatedBatch) release(slots chan struct{}) {
	if b.batch == nil {
		return
	}
	b.result.Release()
	b.batch.Release()
	*b = evaluatedBatch{}
	if slots != nil {
		<-slots
	}
}

// passState preserves the first operational failure rather than the secondary
// cancellation it induces in another stage. Only the stage boundary catches
// panics; programming defects are never converted into ordinary errors.
type passState struct {
	mu         sync.Mutex
	cancel     context.CancelFunc
	err        error
	panicValue any
}

func (s *passState) fail(err error) {
	s.mu.Lock()
	canceled := errors.Is(s.err, context.Canceled) || errors.Is(s.err, context.DeadlineExceeded)
	if s.err == nil || (canceled && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
		s.err = err
	}
	s.mu.Unlock()
	s.cancel()
}

func (s *passState) capture() {
	value := recover()
	if value == nil {
		return
	}
	if limit, ok := value.(*resource.ResourceLimitError); ok {
		s.fail(limit)
		return
	}
	s.mu.Lock()
	if s.panicValue == nil {
		s.panicValue = value
	}
	s.mu.Unlock()
	s.cancel()
}

func validateSerial(ctx context.Context, cfg *config.Config, reader source.Reader, program evaluator, writer batchWriter, budget *resource.Budget, log *slog.Logger, debug bool) (int64, error) {
	var rows int64
	for {
		if err := ctx.Err(); err != nil {
			return rows, err
		}
		batch, err := reader.Next(ctx)
		if err != nil {
			batch.Release()
			if errors.Is(err, io.EOF) {
				return rows, nil
			}
			return rows, fmt.Errorf("read validation input: %w", err)
		}
		// The closure scopes the releases to this batch, including on a
		// recovered resource-limit panic.
		count, err := func() (int64, error) {
			defer batch.Release()
			if err := applyGroupEvidence(program, batch); err != nil {
				return 0, err
			}
			var result *data.Result
			if batch.Record != nil {
				evaluation := diagnostics.Start(ctx, diagnostics.Evaluate)
				var err error
				result, err = program.Evaluate(batch)
				evaluation.End(err)
				defer result.Release()
				if err != nil {
					return 0, fmt.Errorf("evaluate checks: %w", err)
				}
				diagnostics.AddRows(ctx, diagnostics.Evaluate, uint64(batch.Record.NumRows()))
				if writer == nil {
					if err := accountReportBatch(program.Summary(), batch, result); err != nil {
						return 0, err
					}
				}
			}
			if err := writeEvaluatedBatch(ctx, cfg, writer, batch, result); err != nil {
				return 0, fmt.Errorf("write output batch: %w", err)
			}
			if batch.Record == nil {
				return 0, nil
			}
			if debug {
				log.DebugContext(ctx, "batch validated", "phase", "validation", "rows", batch.Record.NumRows(), "bytes", util.TotalRecordSize(batch.Record), "managed_bytes", budget.Used())
			}
			return batch.Record.NumRows(), nil
		}()
		if err != nil {
			return rows, err
		}
		rows += count
	}
}

// applyGroupEvidence mutates checks only on the evaluator owner. The writer
// consumes the same event later in order and never retains its borrowed data.
func applyGroupEvidence(program evaluator, batch *data.Batch) error {
	if batch.Evidence == nil {
		return nil
	}
	if err := program.ApplyStatistics(batch.Evidence); err != nil {
		return fmt.Errorf("account Parquet group: %w", err)
	}
	return nil
}

func writeEvaluatedBatch(ctx context.Context, cfg *config.Config, writer batchWriter, batch *data.Batch, result *data.Result) error {
	if writer == nil {
		return nil
	}
	if batch.Evidence != nil && cfg.Input.Parquet.Statistics == "trusted" {
		if err := writer.WriteEvidence(batch.Evidence); err != nil {
			return fmt.Errorf("write Parquet evidence: %w", err)
		}
	}
	if batch.Record == nil {
		if batch.Evidence == nil {
			return fmt.Errorf("input batch has neither values nor evidence")
		}
		return nil
	}
	return writer.WriteBatch(batch, result)
}

// accountReportBatch keeps only aggregate materialization counts on the evaluator
// owner. It neither retains row diagnostics nor borrows any batch storage.
func accountReportBatch(summary *data.Summary, batch *data.Batch, result *data.Result) error {
	if batch.Record == nil {
		return nil
	}
	n := int(batch.Record.NumRows())
	if result == nil || len(result.Buckets) != n || len(batch.Locations) != n {
		return errors.New("report row/provenance count mismatch")
	}
	if summary.RowOutput == nil {
		summary.RowOutput = make(map[string]data.RowMaterialization, 4)
	}
	add := func(kind string, n uint64) error {
		if n == 0 {
			return nil
		}
		count := summary.RowOutput[kind]
		var err error
		count.Omitted, err = data.CheckedAdd(count.Omitted, n)
		if err != nil {
			return fmt.Errorf("report %s count: %w", kind, err)
		}
		count.Policy = "omit_by_policy"
		summary.RowOutput[kind] = count
		return nil
	}
	// Per-batch tallies keep map updates and map iteration out of the row loop.
	states := make([][]data.State, 0, len(batch.States))
	for _, column := range batch.States {
		states = append(states, column)
	}
	var buckets [data.BucketError + 1]uint64
	var rejects uint64
	ingest := 0
	for row, bucket := range result.Buckets {
		if !bucket.Valid() {
			return fmt.Errorf("unknown row bucket %v", bucket)
		}
		buckets[bucket]++
		reject := false
		for _, column := range states {
			if row < len(column) && column[row] == data.Invalid {
				reject = true
				break
			}
		}
		globalRow := batch.Locations[row].GlobalRow
		for ingest < len(batch.Ingest) && batch.Ingest[ingest].GlobalRow <= globalRow {
			if batch.Ingest[ingest].GlobalRow == globalRow {
				reject = true
			}
			ingest++
		}
		if reject {
			rejects++
		}
	}
	for bucket, n := range buckets {
		if err := add(data.Bucket(bucket).String(), n); err != nil {
			return err
		}
	}
	return add("rejects", rejects)
}
