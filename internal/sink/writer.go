// Package sink writes bounded row parts and publishes a terminal manifest last.
package sink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// Budget charges held by the sink. Each reserve and its release use the same
// constant so the pair cannot drift.
const (
	writerBaseCharge                     = 4096  // Writer state
	writerPerFieldCharge                 = 512   // output schema bookkeeping per field
	ruleMetadataCharge                   = 512   // rules.json construction per rule
	fieldMetadataCharge                  = 256   // rules.json construction per field
	streamCharge                         = 8192  // digest state plus a retained line buffer of lineBufferRetain
	lineBufferRetain                     = 4096  // NDJSON line buffer capacity kept between records
	parquetEncoderBase                   = 65536 // non-Arrow Parquet encoder bookkeeping
	parquetEncoderPerField               = 4096
	rowGroupMetadataPerCol               = 1024 // footer metadata retained per column per row group
	csvRowBase                           = 4096
	csvRowPerField                       = 32
	selectionCharge                      = maxSelectionRows * 8 // Parquet row selection indices
	rejectIndexEntryCharge               = 160                  // reject diagnostic index entry per ingest issue
	allocationSlotCharge                 = 128                  // encoder allocation tracking slot
	jsonRecordGrowthReserve              = 4096                 // encoder scratch beyond the record estimate
	localCompletionStorageCharge   int64 = 64 << 10
	s3CompletionStorageCharge      int64 = 10 << 20
	completionWriterRegistryCharge int64 = 512 // storage writer registration, separate from its I/O buffer
)

type objectStore interface {
	CreateRunRoot(context.Context, string) error
	Create(context.Context, string) (storage.Writer, error)
}

type Reference struct {
	URI    string `json:"uri"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	Count  uint64 `json:"count"`
}

type artifact struct {
	ReportVersion int `json:"report_version"`
	Reference
	Kind   string `json:"kind"`
	Format string `json:"format"`
}

// stream hashes only successfully written bytes. After an error/abort, discard
// writes so a Parquet encoder can release its buffers without publishing data.
type stream struct {
	raw       storage.Writer
	owner     *Writer
	digest    hash.Hash
	uri       string
	size      int64
	count     uint64
	discard   bool
	closed    bool
	charge    int64
	budget    *resource.Budget
	line      lineEncoder
	completed storage.CompletedDigester
	sum       string
}

func (s *stream) Write(p []byte) (int, error) {
	if s.discard {
		return len(p), nil
	}
	n, err := s.raw.Write(p)
	if n > 0 {
		if s.digest != nil {
			_, _ = s.digest.Write(p[:n])
		}
		s.size += int64(n)
		if s.owner != nil {
			diagnostics.AddCounter(s.owner.ctx, diagnostics.OutputBytesWritten, uint64(n))
		}
	}
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.discard = true
	}
	return n, err
}
func (s *stream) ref() Reference {
	sum := s.sum
	if s.digest != nil {
		sum = hex.EncodeToString(s.digest.Sum(nil))
	}
	return Reference{s.uri, s.size, sum, s.count}
}

// finish marks the stream closed and returns its charge; shared by close and abort.
func (s *stream) finish() {
	s.closed = true
	s.line = lineEncoder{}
	s.budget.Release(s.charge)
	s.charge = 0
}
func (s *stream) close() (err error) {
	if s.closed {
		return nil
	}
	if s.discard {
		return errors.New("cannot close failed output")
	}
	ctx := context.Background()
	if s.owner != nil {
		ctx = s.owner.ctx
	}
	span := diagnostics.Start(ctx, diagnostics.Publish)
	defer func() { span.End(err) }()
	if err := s.raw.Close(); err != nil {
		s.discard = true
		return err
	}
	if s.completed != nil {
		size, digest, ok := s.completed.CompletedDigest()
		if !ok || size != s.size {
			s.discard = true
			return errors.New("completed output digest unavailable or size mismatch")
		}
		s.sum = hex.EncodeToString(digest[:])
	}
	if s.owner != nil {
		if err := addCount(&s.owner.publishedBytes, uint64(s.size), "published bytes"); err != nil {
			s.discard = true
			return err
		}
	}
	s.finish()
	return nil
}
func (s *stream) abort() error {
	if s == nil || s.closed {
		return nil
	}
	s.discard = true
	err := s.raw.Abort()
	s.finish()
	return err
}

type Writer struct {
	ctx                 context.Context
	cfg                 *config.Config
	store               objectStore
	budget              *resource.Budget
	logger              *slog.Logger
	runID, root         string
	schema              *arrow.Schema
	outputSchema        *arrow.Schema
	parquetOptions      []parquet.WriterProperty
	partBytes           int64
	groupTarget         int64
	artifacts           *stream
	inputsRef, rulesRef Reference
	parts               [numPartKinds]*part
	sequences           [numPartKinds]uint64
	counts              [numRowKinds]uint64
	evaluated           [numRowKinds]uint64
	omitted             [numRowKinds]uint64
	artifactCounts      [numPartKinds]uint64
	ledgerCounts        [numPartKinds]uint64
	publishedBytes      uint64
	rejects             uint64
	rejectsEmitted      uint64
	evidenceWritten     bool
	evidenceSource      uint64
	evidenceGroup       uint64
	metadataRows        uint64
	groupsScanned       uint64
	groupsSkipped       uint64
	inputsWritten       bool
	streamedSchema      []schemaIssueKey
	done                bool
	aborted             bool
	charge              int64
	completionCharge    int64
}

const schemaIssueIdentityCharge int64 = 256

type schemaIssueKey struct {
	scope, code, column, reason string
}

// New starts an unpublished run output. A nil logger discards part
// publication and cleanup events.
func New(ctx context.Context, cfg *config.Config, store objectStore, budget *resource.Budget, runID string, schema *arrow.Schema, logger *slog.Logger) (w *Writer, err error) {
	if runID == "" || strings.ContainsAny(runID, "/\\") || runID == "." || runID == ".." {
		return nil, errors.New("invalid output run ID")
	}
	if cfg.Output.URI == "" {
		return nil, errors.New("output URI is required")
	}
	if schema == nil {
		return nil, errors.New("output schema is required")
	}
	if cfg.Output.Format != "csv" && cfg.Output.Format != "parquet" {
		return nil, fmt.Errorf("unsupported output format %q", cfg.Output.Format)
	}
	mode := cfg.Output.Mode
	if mode == "" {
		mode = "all"
	}
	if mode != "all" && mode != "failures" && mode != "report" {
		return nil, fmt.Errorf("unsupported output mode %q", mode)
	}
	if cfg.Output.Parquet.Statistics != "" && cfg.Output.Parquet.Statistics != "none" && cfg.Output.Parquet.Statistics != "bounded" {
		return nil, fmt.Errorf("unsupported Parquet statistics policy %q", cfg.Output.Parquet.Statistics)
	}
	if cfg.Output.Parquet.Statistics == "bounded" && cfg.Output.Format != "parquet" {
		return nil, errors.New("bounded statistics require Parquet output")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	w = &Writer{ctx: ctx, cfg: cfg, store: store, budget: budget, logger: logger, runID: runID, root: storage.Join(cfg.Output.URI, runID), schema: schema}
	w.partBytes = cfg.PartBytes()
	w.groupTarget = max(int64(1), min(min(cfg.BatchBytes(), 2<<20)*4, w.partBytes))
	success := false
	defer func() {
		if !success {
			_ = w.Abort()
		}
	}()
	w.charge = int64(writerBaseCharge + schema.NumFields()*writerPerFieldCharge)
	if err = budget.Reserve(w.charge, "sink metadata"); err != nil {
		w.charge = 0
		return w, err
	}
	w.outputSchema = schema
	if mode != "report" {
		w.outputSchema, err = provenanceSchema(schema)
		if err != nil {
			return w, err
		}
	}
	if cfg.Output.Format == "parquet" && mode != "report" {
		resource.ConfigureParquetCodecs()
		w.parquetOptions = parquetWriterOptions()
		if cfg.Output.Parquet.Statistics == "bounded" {
			for _, field := range w.outputSchema.Fields() {
				if boundedStatisticsType(field.Type) {
					w.parquetOptions = append(w.parquetOptions, parquet.WithStatsFor(field.Name, true))
				}
			}
		}
	}
	if err = store.CreateRunRoot(ctx, w.root); err != nil {
		return w, err
	}
	if w.artifacts, err = w.open("artifacts.ndjson"); err != nil {
		return w, err
	}
	metadataCharge := int64((len(cfg.Rules)+len(cfg.DatasetRules))*ruleMetadataCharge + schema.NumFields()*fieldMetadataCharge)
	if err = budget.Reserve(metadataCharge, "rule report metadata"); err != nil {
		return w, err
	}
	defer budget.Release(metadataCharge)
	rules := rulesMetadata{ReportVersion: data.ReportVersion, ConfigVersion: cfg.ConfigVersion, Name: cfg.Name, Version: cfg.Version, Rules: make([]ruleMetadata, 0, len(cfg.Rules)), DatasetRules: make([]ruleMetadata, 0, len(cfg.DatasetRules)), Fields: make([]fieldMetadata, 0, w.outputSchema.NumFields()), CSVNull: "\\N", CSVStringEscape: "double_backslash", CSVBinary: "base64", JSONIntegerPrecision: "64-bit integers are JSON numbers; use a lossless JSON decoder"}
	for _, r := range cfg.Rules {
		rules.Rules = append(rules.Rules, ruleMetadata{r.Code, r.Message, r.Level, r})
	}
	for _, r := range cfg.DatasetRules {
		rules.DatasetRules = append(rules.DatasetRules, ruleMetadata{Code: r.Code, Severity: r.Level, Definition: r})
	}
	for _, f := range w.outputSchema.Fields() {
		rules.Fields = append(rules.Fields, fieldMetadata{f.Name, f.Type.String(), f.Nullable})
	}
	if w.rulesRef, err = w.writeMetadata("rules.json", rules); err != nil {
		return w, err
	}
	success = true
	return w, nil
}
func (w *Writer) open(path string) (*stream, error) {
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.budget.Reserve(streamCharge, "output stream"); err != nil {
		return nil, err
	}
	uri := storage.Join(w.root, path)
	raw, err := w.store.Create(w.ctx, uri)
	if err != nil {
		w.budget.Release(streamCharge)
		return nil, fmt.Errorf("create %s: %w", uri, err)
	}
	s := &stream{raw: raw, uri: uri, charge: streamCharge, budget: w.budget, owner: w}
	if completed, ok := raw.(storage.CompletedDigester); ok {
		s.completed = completed
	} else {
		s.digest = sha256.New()
	}
	return s, nil
}
func (w *Writer) ready() error {
	if w.done {
		return errors.New("output already completed")
	}
	if w.aborted {
		return errors.New("output aborted")
	}
	return w.ctx.Err()
}
func (w *Writer) fail(err *error) {
	if *err != nil {
		abortErr := w.Abort()
		*err = errors.Join(*err, abortErr)
	}
}

// WriteIssue streams preflight schema diagnostics without retaining the scan.
func (w *Writer) WriteIssue(issue data.Issue) (err error) {
	defer w.fail(&err)
	if err = w.ready(); err != nil {
		return err
	}
	if err = w.writeIssue(issue); err != nil {
		return err
	}
	key := schemaIssueKey{scope: issue.Scope, code: issue.Code, column: issue.Column, reason: issue.Reason}
	if issue.Scope == data.ScopeSchema && !slices.Contains(w.streamedSchema, key) {
		if err = w.budget.Reserve(schemaIssueIdentityCharge, "streamed schema issue identity"); err != nil {
			return err
		}
		w.charge += schemaIssueIdentityCharge
		w.streamedSchema = append(w.streamedSchema, key)
	}
	return nil
}

type inputRecord struct {
	ReportVersion         int    `json:"report_version"`
	SourceOrdinal         uint64 `json:"source_ordinal"`
	URI                   string `json:"uri"`
	Size                  int64  `json:"size"`
	ETag                  string `json:"etag,omitempty"`
	VersionID             string `json:"version_id,omitempty"`
	Modified              any    `json:"modified"`
	IdentityMethod        string `json:"identity_method"`
	PayloadVerification   string `json:"payload_verification"`
	IngestionVerification string `json:"ingestion_verification"`
}

func (w *Writer) WriteInputs(objects []storage.Object) (err error) {
	defer w.fail(&err)
	if err = w.ready(); err != nil {
		return err
	}
	if w.inputsWritten {
		return errors.New("input manifest already written")
	}
	s, err := w.open("inputs.ndjson")
	if err != nil {
		return err
	}
	defer s.abort()
	for i, obj := range objects {
		if err = w.ctx.Err(); err != nil {
			return err
		}
		record := inputRecord{ReportVersion: data.ReportVersion, SourceOrdinal: uint64(i + 1), URI: obj.URI, Size: obj.Size, ETag: obj.ETag, VersionID: obj.VersionID, Modified: obj.ModTime, IdentityMethod: obj.IdentityMethod, PayloadVerification: obj.PayloadVerification, IngestionVerification: obj.IngestionVerification}
		if err = s.writeRecord(record, textBytes(len(obj.URI)+len(obj.ETag)+len(obj.VersionID)+len(obj.IdentityMethod)+len(obj.PayloadVerification)+len(obj.IngestionVerification))+recordOverhead(10)); err != nil {
			return err
		}
		s.count++
	}
	if err = s.close(); err != nil {
		return err
	}
	w.inputsRef = s.ref()
	w.inputsWritten = true
	return nil
}

func (w *Writer) WriteBatch(batch *data.Batch, result *data.Result) (err error) {
	defer w.fail(&err)
	if err = w.ready(); err != nil {
		return err
	}
	if batch == nil || batch.Record == nil || result == nil {
		return errors.New("nil output batch/result")
	}
	n := int(batch.Record.NumRows())
	if len(result.Buckets) != n || len(batch.Locations) != n {
		return errors.New("output row/provenance count mismatch")
	}
	if !batch.Record.Schema().Equal(w.schema) {
		return errors.New("output batch schema changed")
	}
	span := diagnostics.Start(w.ctx, diagnostics.Encode)
	defer func() { span.End(err) }()
	emitRows := false
	for _, bucket := range result.Buckets {
		if !bucket.Valid() {
			return fmt.Errorf("unknown row bucket %v", bucket)
		}
		kind := partKind(bucket)
		if err = addCount(&w.evaluated[kind], 1, "evaluated rows"); err != nil {
			return err
		}
		if !w.emit(kind) {
			if err = addCount(&w.omitted[kind], 1, "omitted rows"); err != nil {
				return err
			}
		} else {
			emitRows = true
		}
	}
	if emitRows {
		if err = w.writeRows(batch, result); err != nil {
			return err
		}
	}
	for _, issue := range result.Issues {
		if err = w.writeIssue(issue); err != nil {
			return err
		}
	}
	return w.writeRejects(batch)
}

type manifestDocument struct {
	ReportVersion           int                                `json:"report_version"`
	RunID                   string                             `json:"run_id"`
	Status                  string                             `json:"status"`
	Validation              string                             `json:"validation"`
	Inputs                  Reference                          `json:"inputs"`
	Rules                   Reference                          `json:"rules"`
	Artifacts               Reference                          `json:"artifacts"`
	Summary                 Reference                          `json:"summary"`
	Counts                  map[string]uint64                  `json:"counts"`
	Scope                   string                             `json:"scope"`
	OutputMode              string                             `json:"output_mode"`
	Verification            string                             `json:"verification"`
	TrustPolicy             string                             `json:"trust_policy"`
	IngestionVerification   string                             `json:"ingestion_verification"`
	ProvenancePositionBasis string                             `json:"provenance_position_basis"`
	Coverage                data.Coverage                      `json:"coverage"`
	RuleCoverage            map[string]data.RuleCoverage       `json:"rule_coverage"`
	RowOutput               map[string]data.RowMaterialization `json:"row_output"`
}

func (w *Writer) Complete(summary *data.Summary, beforeCommit ...func(*data.Summary) error) (uri string, err error) {
	defer w.fail(&err)
	if err = w.ready(); err != nil {
		return "", err
	}
	if summary == nil {
		return "", errors.New("nil output summary")
	}
	if !w.inputsWritten {
		return "", errors.New("input manifest not written")
	}
	if summary.Status != data.StatusCompleted && summary.Status != data.StatusSchemaRejected {
		return "", fmt.Errorf("invalid terminal status %q", summary.Status)
	}
	if err = w.reconcileSummary(summary); err != nil {
		return "", err
	}
	for _, issue := range summary.SchemaIssues {
		if slices.Contains(w.streamedSchema, schemaIssueKey{scope: issue.Scope, code: issue.Code, column: issue.Column, reason: issue.Reason}) {
			continue
		}
		if err = w.writeIssue(issue); err != nil {
			return "", err
		}
	}
	for _, issue := range summary.DatasetIssues {
		if err = w.writeIssue(issue); err != nil {
			return "", err
		}
	}
	for kind := range partKind(numPartKinds) {
		if err = w.closePart(kind); err != nil {
			return "", err
		}
	}
	for kind := range partKind(numPartKinds) {
		// Ledger and issue counts are records, not logical rows. Each accepted line
		// updates its family counter, independently of artifact finalization.
		expected := w.ledgerCounts[kind]
		if kind.isRowKind() {
			expected = w.counts[kind]
		} else if kind == kindRejects {
			expected = w.rejectsEmitted
		}
		if w.artifactCounts[kind] != expected {
			return "", fmt.Errorf("%s artifact count %d does not match emitted count %d", kind, w.artifactCounts[kind], expected)
		}
	}
	if err = w.artifacts.close(); err != nil {
		return "", err
	}
	artifactsRef := w.artifacts.ref()
	uri = storage.Join(w.root, "manifest.json")
	summary.ReportVersion = data.ReportVersion
	summary.RunID = w.runID
	summary.ManifestURI = uri
	summaryDoc := &summaryDocument{summary, w.inputsRef, w.rulesRef, artifactsRef}
	predictedSummaryRef := Reference{URI: storage.Join(w.root, "summary.json"), SHA256: strings.Repeat("f", 64), Count: 1}
	predictedManifest := w.manifest(summary, artifactsRef, predictedSummaryRef)
	storageCharge := localCompletionStorageCharge
	if strings.HasPrefix(w.root, "s3://") {
		storageCharge = s3CompletionStorageCharge
	}
	// The storage writer registry node is live alongside its output buffer
	// or S3 workspace, including while the terminal manifest is encoded.
	storageCharge += completionWriterRegistryCharge
	// Hold a concrete charge for retained completion state and the external
	// pre-commit callback. A separate proof reservation includes that hold plus
	// the largest future metadata/storage operation, so the peak is final before
	// summary.json is encoded without double-charging those later operations.
	holdCharge := metadataJSONCharge(summaryDoc) + localCompletionStorageCharge
	futureCharge := max(
		streamCharge+metadataJSONCharge(summaryDoc)+storageCharge,
		streamCharge+metadataJSONCharge(predictedManifest)+storageCharge,
	)
	proofCharge := holdCharge + futureCharge
	if err = w.budget.Reserve(proofCharge, "completion working set proof"); err != nil {
		return "", err
	}
	summary.ManagedPeak = max(summary.ManagedPeak, w.budget.Peak())
	w.budget.Release(proofCharge)
	if err = w.budget.Reserve(holdCharge, "retained completion working set"); err != nil {
		return "", err
	}
	w.completionCharge = holdCharge
	summaryRef, err := w.writeMetadata("summary.json", summaryDoc)
	if err != nil {
		return "", err
	}
	if len(beforeCommit) > 0 && beforeCommit[0] != nil {
		if err = beforeCommit[0](summary); err != nil {
			return "", fmt.Errorf("publish requested summary: %w", err)
		}
	}
	manifest := w.manifest(summary, artifactsRef, summaryRef)
	if err = w.publishManifest(manifest); err != nil {
		summary.ManifestURI = ""
		return "", fmt.Errorf("publish completion manifest: %w", err)
	}
	w.logger.Info("completion manifest published", slog.String("phase", "output"), slog.Any("uri", storage.LogURI(uri)), slog.Uint64("rows", summary.Total))
	w.done = true
	w.budget.Release(w.completionCharge)
	w.completionCharge = 0
	w.budget.Release(w.charge)
	w.charge = 0
	return uri, nil
}

func (w *Writer) Abort() error {
	if w == nil || w.done || w.aborted {
		return nil
	}
	w.aborted = true
	var errs []error
	for kind, p := range w.parts {
		if p == nil {
			continue
		}
		if err := p.abort(w.budget); err != nil {
			errs = append(errs, err)
		}
		w.parts[kind] = nil
	}
	if err := w.artifacts.abort(); err != nil {
		errs = append(errs, err)
	}
	w.budget.Release(w.completionCharge)
	w.completionCharge = 0
	w.budget.Release(w.charge)
	w.charge = 0
	err := errors.Join(errs...)
	if err != nil {
		w.logger.Warn("output abort cleanup failed", slog.String("phase", "output"), slog.String("error", err.Error()))
	}
	return err
}
