// Package diagnostics provides bounded, context-bound operational accounting.
// Durations are monotonic operation time (including I/O), never CPU time.
package diagnostics

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/richardrh/valet/internal/resource"
)

// Phase identifies one of the fixed aggregation buckets. Nested and concurrent
// work may legitimately sum to more than elapsed wall time.
type Phase uint8

const (
	Discover Phase = iota
	Metadata
	Reference
	Prepass
	InputHash
	Decode
	Evaluate
	Encode
	Publish
	Present
	phaseCount
)

var phaseNames = [phaseCount]string{"discover", "metadata", "reference", "prepass", "input_hash", "decode", "evaluate", "encode", "publish", "present"}

// Counter identifies actual backend bytes, not cache hits or wire retransmits.
type Counter uint8

const (
	SourceMetadataBytesRead Counter = iota
	SourcePayloadBytesRead
	OutputBytesWritten
	counterCount
)

type phaseTotals struct {
	work, wait, rows, bytes uint64
	performed, failed       bool
}

type reasonTotal struct {
	reason [64]byte
	n      uint8
	count  uint64
}

// Recorder retains only fixed phase/counter/reason arrays. It never retains a
// decision, group, row, literal or statistics bound.
type Recorder struct {
	mu                                        sync.Mutex
	started                                   time.Time
	command, runID                            string
	seq                                       uint64
	budget                                    *resource.Budget
	held                                      int64
	closed, finished, trust, overflow         bool
	phases                                    [phaseCount]phaseTotals
	counters                                  [counterCount]uint64
	committed                                 uint64
	decisions, skipped, scanned, reasonsOther uint64
	reasons                                   [32]reasonTotal
	log                                       *slog.Logger
	handler                                   *handlerState
}

// New starts elapsed accounting. SetRunID before emitting run-scoped records.
func New(command string) *Recorder {
	return &Recorder{started: time.Now(), command: clipped(command, 64)}
}

type contextKey struct{}

func WithRecorder(ctx context.Context, r *Recorder) context.Context {
	return context.WithValue(ctx, contextKey{}, r)
}

func FromContext(ctx context.Context) *Recorder {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(contextKey{}).(*Recorder)
	return r
}

func (r *Recorder) SetRunID(id string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runID = clipped(id, 128)
}

// BindBudget charges the entire retained recorder and logging workspace before
// source work begins. It is idempotent for the same budget.
func (r *Recorder) BindBudget(b *resource.Budget) error {
	if r == nil || b == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.budget == b {
		return nil
	}
	if r.budget != nil {
		return &resource.ResourceLimitError{Phase: "diagnostics budget already bound", Limit: b.Limit()}
	}
	n := int64(unsafe.Sizeof(Recorder{})) + int64(unsafe.Sizeof(handlerState{})) + 1024
	if err := b.Reserve(n, "diagnostics"); err != nil {
		return err
	}
	r.budget, r.held = b, n
	return nil
}

// Close releases the fixed charge after terminal logging. No run work may remain.
func (r *Recorder) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	if r.budget != nil {
		r.budget.Release(r.held)
	}
	r.held = 0
}

func (r *Recorder) add(dst *uint64, n uint64) {
	if math.MaxUint64-*dst < n {
		r.overflow = true
		return
	}
	*dst += n
}

// Span must be ended exactly once, after the operation (and its I/O) finishes.
// Queue admission belongs in AddWait and must occur outside this span.
type Span struct {
	recorder *Recorder
	phase    Phase
	started  time.Time
}

func Start(ctx context.Context, phase Phase) Span {
	r := FromContext(ctx)
	if r == nil || phase >= phaseCount {
		return Span{}
	}
	return Span{r, phase, time.Now()}
}

func (s Span) End(err error) {
	if s.recorder == nil {
		return
	}
	n := time.Since(s.started)
	r := s.recorder
	r.mu.Lock()
	defer r.mu.Unlock()
	p := &r.phases[s.phase]
	p.performed = true
	p.failed = p.failed || err != nil
	if n > 0 {
		r.add(&p.work, uint64(n))
	}
}

func AddWait(ctx context.Context, phase Phase, wait time.Duration) {
	r := FromContext(ctx)
	if r == nil || phase >= phaseCount || wait <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(&r.phases[phase].wait, uint64(wait))
}

func AddRows(ctx context.Context, phase Phase, rows uint64) {
	r := FromContext(ctx)
	if r == nil || phase >= phaseCount {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(&r.phases[phase].rows, rows)
}

func AddBytes(ctx context.Context, phase Phase, bytes uint64) {
	r := FromContext(ctx)
	if r == nil || phase >= phaseCount {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(&r.phases[phase].bytes, bytes)
}

func AddCounter(ctx context.Context, counter Counter, bytes uint64) {
	r := FromContext(ctx)
	if r == nil || counter >= counterCount {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(&r.counters[counter], bytes)
}

func SetResultBytesCommitted(ctx context.Context, bytes uint64) {
	if r := FromContext(ctx); r != nil {
		r.SetResultBytesCommitted(bytes)
	}
}

func (r *Recorder) SetResultBytesCommitted(bytes uint64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.committed = bytes
}

// Decision contains identity and evidence-field names only, never bound values.
// Record one group-level decision; individual checks may be emitted separately
// by the caller's debug logger without incrementing group counts.
type Decision struct {
	SourceOrdinal, RowGroup     uint64
	CheckIndex                  int
	Code, Claim, Action, Reason string
	Fields                      []string
}

func RecordDecision(ctx context.Context, d Decision) {
	r := FromContext(ctx)
	if r == nil {
		return
	}
	r.mu.Lock()
	r.add(&r.decisions, 1)
	if d.Action == "skip" {
		r.add(&r.skipped, 1)
	} else {
		r.add(&r.scanned, 1)
	}
	reason := clipped(d.Reason, 64)
	stored := false
	for i := range r.reasons {
		entry := &r.reasons[i]
		if entry.n == 0 || string(entry.reason[:entry.n]) == reason {
			if entry.n == 0 {
				entry.n = uint8(copy(entry.reason[:], reason))
			}
			r.add(&entry.count, 1)
			stored = true
			break
		}
	}
	if !stored {
		r.add(&r.reasonsOther, 1)
	}
	warn := d.Action == "skip" && !r.trust
	r.trust = r.trust || warn
	log := r.log
	r.mu.Unlock()
	if log == nil {
		return
	}
	if warn {
		log.WarnContext(ctx, "Producer statistics replace value checks; skipped payload is unverified", "event", "validation.trust", "trust_policy", "producer_statistics", "ingestion_verification", "partial")
	}
	if log.Enabled(ctx, slog.LevelDebug) {
		// The handler encodes only canonical field names into a bounded array.
		log.DebugContext(ctx, "Parquet statistics decision", "event", "parquet.stats_decision", "source_ordinal", d.SourceOrdinal, "row_group", d.RowGroup, "check_index", d.CheckIndex, "code", d.Code, "claim", d.Claim, "action", d.Action, "reason", reason, "evidence_fields", d.Fields)
	}
}

// Terminal is supplied at the CLI boundary, after application cleanup and result
// presentation. Exit 3 must retain complete validation counters after commit.
type Terminal struct {
	ExitCode                                                   int
	Scope                                                      string
	ManifestCommitted                                          bool
	Outcome, OutputMode, TrustPolicy                           string
	CountersComplete                                           bool
	ValueRows, MetadataClaimedRows, AccountedRows, RowsScanned uint64
	RowGroupsScanned, RowGroupsStatisticsOnly, RowGroupsTotal  uint64
}

func (r *Recorder) Finish(ctx context.Context, t Terminal) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	r.finished = true
	phases, counters, reasons := r.phases, r.counters, r.reasons
	log, committed := r.log, r.committed
	decisions, skipped, scanned, other := r.decisions, r.skipped, r.scanned, r.reasonsOther
	complete := t.CountersComplete && !r.overflow
	peak := int64(0)
	if r.budget != nil {
		peak = r.budget.Peak()
	}
	elapsed := time.Since(r.started).Nanoseconds()
	r.mu.Unlock()
	if log == nil {
		return
	}
	for i, phase := range phases {
		if !phase.performed {
			continue
		}
		status := "ok"
		if phase.failed {
			status = "error"
		}
		log.InfoContext(ctx, "Phase completed", "event", "run.phase", "phase", phaseNames[i], "status", status, "work_ns", phase.work, "wait_ns", phase.wait, "rows", phase.rows, "bytes", phase.bytes)
	}
	if decisions != 0 {
		log.InfoContext(ctx, "Parquet statistics summary", "event", "parquet.stats_summary", "groups", decisions, "groups_skipped", skipped, "groups_scanned", scanned)
		for _, reason := range reasons {
			if reason.count == 0 {
				continue
			}
			log.InfoContext(ctx, "Parquet statistics reason", "event", "parquet.stats_summary", "reason", string(reason.reason[:reason.n]), "groups", reason.count)
		}
		if other != 0 {
			log.InfoContext(ctx, "Parquet statistics reason", "event", "parquet.stats_summary", "reason", "other", "groups", other)
		}
	}
	if !t.ManifestCommitted {
		committed = 0
	}
	level := slog.LevelInfo
	if t.ExitCode >= 2 {
		level = slog.LevelError
	}
	log.Log(ctx, level, "Run finished", "event", "run.finished", "exit_code", t.ExitCode, "scope", t.Scope, "manifest_committed", t.ManifestCommitted, "outcome", t.Outcome, "output_mode", t.OutputMode, "trust_policy", t.TrustPolicy, "elapsed_ns", elapsed, "managed_peak_bytes", peak, "source_metadata_bytes_read", counters[SourceMetadataBytesRead], "source_payload_bytes_read", counters[SourcePayloadBytesRead], "output_bytes_written", counters[OutputBytesWritten], "result_bytes_committed", committed, "value_rows", t.ValueRows, "metadata_claimed_rows", t.MetadataClaimedRows, "accounted_rows", t.AccountedRows, "rows_scanned", t.RowsScanned, "row_groups_scanned", t.RowGroupsScanned, "row_groups_statistics_only", t.RowGroupsStatisticsOnly, "row_groups_total", t.RowGroupsTotal, "counters_complete", complete)
}
