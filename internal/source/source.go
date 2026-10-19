// Package source decodes replayable objects into bounded, owned Arrow batches.
package source

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// Defaults applied when the configuration leaves a limit unset.
const (
	defaultRecordBytes = 8 << 20
	defaultBatchBytes  = 8 << 20
	defaultMaxDepth    = 64
	defaultInferRows   = 1_000_000
)

// Retained schema metadata, charged to the dataset for its lifetime.
const (
	// columnMetadataBytes covers a field (name, config.Column, Arrow type),
	// its byName entry and the arrow.Field in the frozen schema.
	columnMetadataBytes = 1024
	// columnTokenBytes is the per-token overhead of formats/true/false lists.
	columnTokenBytes = 32
	// schemaIssueBytes covers one data.Issue plus slice growth.
	schemaIssueBytes = 256
	// renameEntryBytes covers one rename map entry besides its key/value bytes.
	renameEntryBytes = 256
)

// inputKind is the configured physical input format.
type inputKind uint8

const (
	inputCSV inputKind = iota + 1
	inputJSON
	inputNDJSON
	inputParquet
)

func parseInputKind(s string) (inputKind, error) {
	switch s {
	case "csv":
		return inputCSV, nil
	case "json":
		return inputJSON, nil
	case "ndjson":
		return inputNDJSON, nil
	case "parquet":
		return inputParquet, nil
	}
	return 0, fmt.Errorf("unsupported input type %q", s)
}

type Reader interface {
	Next(context.Context) (*data.Batch, error)
	Close() error
}

// Option configures New.
type Option func(*options)

type options struct {
	logger *slog.Logger
	plan   bool
}

// WithLogger sets the logger for discovery, inference and Parquet safety
// scan events. Row values are never logged.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithPlan limits discovery to metadata/sample work that cannot publish a
// result. In particular, Parquet range handles skip close-time content digests.
func WithPlan() Option {
	return func(o *options) { o.plan = true }
}

type field struct {
	name    string
	options config.Column
	typ     arrow.DataType
}
type Dataset struct {
	cfg                    *config.Config
	ctx                    context.Context
	kind                   inputKind
	log                    *slog.Logger
	plan                   bool
	store                  *storage.Store
	budget                 *resource.Budget
	objects                []storage.Object
	schema                 *arrow.Schema
	columns                []field
	byName                 map[string]int
	transforms             []map[string]string
	issues                 []data.Issue
	retained               int64
	parquet                map[string]*parquetPreparation
	groups                 []data.ParquetGroup
	statistics             *data.StatisticsPlan
	statisticsPending      bool
	openedBeforeStatistics bool
	// projection and selectionNames describe declared columns as JSON
	// Pointers; selectionNames[i] is column i's pre-rename source name.
	projection               *projection
	selectionNames           []string
	recordLimit, batchTarget int64
	maxDepth, passes         int
	inferenceRowsScanned     uint64
	generated                func(context.Context) (Reader, error)
	closed                   bool
}

func NewGenerated(schema *arrow.Schema, open func(context.Context) (Reader, error)) *Dataset {
	return &Dataset{schema: schema, generated: open, log: slog.New(slog.DiscardHandler)}
}
func (d *Dataset) Schema() *arrow.Schema      { return d.schema }
func (d *Dataset) Objects() []storage.Object  { return d.objects }
func (d *Dataset) SchemaIssues() []data.Issue { return d.issues }

// Passes reports source passes spent before the main read (schema inference).
func (d *Dataset) Passes() int { return d.passes }

// InferenceRowsScanned counts successful decoded row visits during schema
// inference/discovery, including the lexical drain after the sample limit.
// Raw identity/hash passes and later readers are not included.
func (d *Dataset) InferenceRowsScanned() uint64 { return d.inferenceRowsScanned }

// DecoderPassesPerOpen counts complete scans surrounding Parquet decoding.
// Page verification is fused with decoding; only local replay digests require
// an additional complete scan.
func (d *Dataset) DecoderPassesPerOpen() int {
	if d.generated == nil && d.kind == inputParquet {
		for _, obj := range d.objects {
			prep := d.parquet[obj.URI]
			if !strings.HasPrefix(obj.URI, "s3://") && (prep == nil || !prep.selective) {
				return 1
			}
		}
	}
	return 0
}

func (d *Dataset) hasLocalObjects() bool {
	for _, object := range d.objects {
		if !strings.HasPrefix(object.URI, "s3://") {
			return true
		}
	}
	return false
}
func (d *Dataset) Close() error {
	if !d.closed {
		d.closed = true
		if d.budget != nil {
			d.budget.Release(d.retained)
		}
		if d.statistics != nil {
			d.statistics.Release()
			d.statistics = nil
		}
		d.groups = nil
		d.retained = 0
		d.parquet = nil
	}
	return nil
}
func (d *Dataset) reserve(n int64, phase string) error {
	if err := d.budget.Reserve(n, phase); err != nil {
		return err
	}
	d.retained += n
	return nil
}
func (d *Dataset) rename(s string) string {
	for _, m := range d.transforms {
		if target, ok := m[s]; ok {
			s = target
		}
	}
	return s
}
func (d *Dataset) original(s string) (string, error) {
	for i := len(d.transforms) - 1; i >= 0; i-- {
		found := ""
		for from, to := range d.transforms[i] {
			if to == s {
				if found != "" && found != from {
					return "", errors.New("ambiguous rename mapping")
				}
				found = from
			}
		}
		if found != "" {
			s = found
		}
	}
	return s, nil
}
func (d *Dataset) ignoreExtras() bool {
	return d.cfg.Input.Schema != nil && d.cfg.Input.Schema.ExtraFields == "ignore"
}
func (d *Dataset) addIssue(column, reason string) error {
	if err := d.reserve(int64(len(column)+len(reason)+schemaIssueBytes), "source schema issues"); err != nil {
		return err
	}
	d.issues = append(d.issues, data.Issue{
		Code:     "INGEST_SCHEMA",
		Severity: "error",
		Origin:   data.OriginIngest,
		Scope:    data.ScopeSchema,
		Column:   column,
		Reason:   reason,
		Message:  reason,
	})
	return nil
}

func positiveOr[T int | int64](v, fallback T) T {
	if v <= 0 {
		return fallback
	}
	return v
}

// New discovers the configured objects and freezes the dataset schema, either
// from input.schema or by inspecting headers and sampling records.
func New(ctx context.Context, cfg *config.Config, store *storage.Store, budget *resource.Budget, opts ...Option) (*Dataset, error) {
	o := options{logger: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(&o)
	}
	kind, err := parseInputKind(cfg.Input.Type)
	if err != nil {
		return nil, err
	}
	d := &Dataset{
		ctx:               ctx,
		cfg:               cfg,
		kind:              kind,
		log:               o.logger,
		plan:              o.plan,
		store:             store,
		budget:            budget,
		recordLimit:       positiveOr(cfg.RecordBytes(), defaultRecordBytes),
		batchTarget:       positiveOr(cfg.BatchBytes(), defaultBatchBytes),
		maxDepth:          positiveOr(cfg.Execution.MaxDepth, defaultMaxDepth),
		statisticsPending: kind == inputParquet && cfg.Input.Parquet.Statistics == "trusted" && !o.plan,
	}
	complete := false
	defer func() {
		if !complete {
			_ = d.Close()
		}
	}()
	if err = d.discover(ctx); err != nil {
		return nil, err
	}
	if err = d.loadTransforms(ctx); err != nil {
		return nil, err
	}
	if cfg.Input.Schema != nil {
		if err = d.declareSchema(); err != nil {
			return nil, err
		}
	}
	switch kind {
	case inputParquet:
		err = d.inferParquet(ctx)
	case inputCSV:
		err = d.inferCSV(ctx)
	default:
		err = d.inferJSON(ctx)
	}
	if err != nil {
		return nil, err
	}
	if err = validateNames(d.columnNames()); err != nil {
		return nil, err
	}
	fields := make([]arrow.Field, len(d.columns))
	for i, c := range d.columns {
		fields[i] = arrow.Field{Name: c.name, Type: c.typ, Nullable: true}
	}
	d.schema = arrow.NewSchema(fields, nil)
	complete = true
	return d, nil
}

func (d *Dataset) discover(ctx context.Context) error {
	start := time.Now()
	objects, err := d.store.Discover(ctx, d.cfg.Source)
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return errors.New("source contains no input objects")
	}
	var total int64
	for _, obj := range objects {
		total += obj.Size
	}
	d.objects = objects
	d.log.Info("source discovered",
		"phase", "discovery",
		"objects", len(objects),
		"bytes", total,
		"duration_ms", time.Since(start).Milliseconds())
	return nil
}

// declareSchema adds input.schema columns and their JSON Pointer projection.
func (d *Dataset) declareSchema() error {
	s := d.cfg.Input.Schema
	order := s.Order
	if len(order) == 0 {
		for name := range s.Columns {
			order = append(order, name)
		}
		sort.Strings(order)
	}
	d.projection = &projection{}
	for _, name := range order {
		c := s.Columns[name]
		dt, err := columnType(c)
		if err != nil {
			return fmt.Errorf("column %s: %w", name, err)
		}
		if err = d.addColumn(name, c, dt); err != nil {
			return err
		}
		original, err := d.original(name)
		if err != nil {
			return err
		}
		ptr := jsontext.Pointer(c.Path)
		if c.Path == "" {
			ptr = ptr.AppendToken(original)
		}
		if !ptr.IsValid() {
			return fmt.Errorf("invalid JSON pointer for column %s", name)
		}
		added := d.projection.insert(ptr, len(d.columns)-1) + int64(len(original))
		if err = d.reserve(added, "source projection"); err != nil {
			return err
		}
		d.selectionNames = append(d.selectionNames, original)
	}
	return nil
}

// closeInto closes c, reporting its error only when no earlier error exists.
func closeInto(err *error, c interface{ close() error }) {
	if e := c.close(); *err == nil {
		*err = e
	}
}

func (d *Dataset) inferParquet(ctx context.Context) error {
	for _, obj := range d.objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.inspectParquet(ctx, obj); err != nil {
			return err
		}
	}
	// Validation setup verifies one complete digest pass for local inputs.
	// Plan mode performs metadata-only reads but reports the required pass.
	if d.hasLocalObjects() && !d.statisticsPending {
		d.passes = 1
	}
	return nil
}

func (d *Dataset) inspectParquet(ctx context.Context, obj storage.Object) (err error) {
	p, err := openParquet(ctx, d, obj, false)
	if err != nil {
		return err
	}
	defer closeInto(&err, p)
	if len(d.columns) == 0 && d.cfg.Input.Schema == nil {
		for i, name := range p.header {
			af := p.schema.Field(i)
			c, err := optionsForArrow(af.Type)
			if err != nil {
				return err
			}
			if err = d.addColumn(d.rename(name), c, af.Type); err != nil {
				return err
			}
		}
	}
	return d.checkHeader(p.header)
}

func (d *Dataset) inferCSV(ctx context.Context) error {
	s := d.newSampler()
	for _, obj := range d.objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.inspectCSV(ctx, obj, s); err != nil {
			return err
		}
	}
	s.finish()
	return nil
}

func (d *Dataset) inspectCSV(ctx context.Context, obj storage.Object, s *sampler) (err error) {
	r, err := openCSV(ctx, d, obj, d.csvOptions())
	if err != nil {
		return err
	}
	defer closeInto(&err, r)
	if len(d.columns) == 0 && d.cfg.Input.Schema == nil {
		for _, name := range r.header {
			dt, c := arrow.DataType(arrow.BinaryTypes.String), config.Column{Type: "string"}
			if s.inferred {
				dt, c = arrow.Null, config.Column{Type: "null"}
			}
			if err = d.addColumn(d.rename(name), c, dt); err != nil {
				return err
			}
		}
	}
	if err = d.checkHeader(r.header); err != nil {
		return err
	}
	// Validation pins the full local object whose header established the
	// schema. Plan mode publishes nothing and remains metadata/header-only.
	if !s.inferred {
		if d.plan {
			return nil
		}
		return r.src.drainIdentity(ctx)
	}
	return s.sample(ctx, r)
}

// inferJSON samples schema-less JSON to discover flat fields (and their
// types unless raw_strings). An explicit schema needs no sampling.
func (d *Dataset) inferJSON(ctx context.Context) error {
	if d.cfg.Input.Schema != nil {
		return nil
	}
	s := d.newSampler()
	for _, obj := range d.objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.sampleJSON(ctx, obj, s); err != nil {
			return err
		}
	}
	s.finish()
	return nil
}

func (d *Dataset) sampleJSON(ctx context.Context, obj storage.Object, s *sampler) (err error) {
	r, err := openJSON(ctx, d, obj)
	if err != nil {
		return err
	}
	defer closeInto(&err, r)
	return s.sample(ctx, r)
}

// sampler runs the bounded inference pass shared by all objects.
type sampler struct {
	d         *Dataset
	inferred  bool
	remaining int
	rows      int
	start     time.Time
}

func (d *Dataset) newSampler() *sampler {
	return &sampler{
		d:         d,
		inferred:  d.cfg.Input.Schema == nil && d.cfg.Input.Mode != "raw_strings",
		remaining: positiveOr(d.cfg.Input.InferSchemaLength, defaultInferRows),
	}
}

func (s *sampler) sample(ctx context.Context, r lexicalInput) error {
	if s.remaining <= 0 {
		return drainLexical(ctx, r, s.d)
	}
	d := s.d
	if d.passes == 0 {
		d.passes = 1
		s.start = time.Now()
		d.log.Info("source inference pass started", "phase", "inference")
	}
	var mapping []int
	if names := r.names(); names != nil {
		mapping = make([]int, len(names))
		for i, name := range names {
			mapping[i] = d.index(d.rename(name))
		}
	}
	for s.remaining > 0 {
		rec, err := r.next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = d.addInferenceRow(); err != nil {
			return err
		}
		s.remaining--
		s.rows++
		for i, name := range rec.names {
			var index int
			if mapping != nil {
				index = mapping[i]
			} else {
				index = d.index(d.rename(name))
			}
			if index < 0 {
				if mapping != nil {
					continue
				}
				if index, err = s.addSampledColumn(d.rename(name)); err != nil {
					return err
				}
			}
			if s.inferred {
				s.merge(index, rec.values[i])
			}
		}
	}
	return drainLexical(ctx, r, d)
}

func drainLexical(ctx context.Context, input lexicalInput, d *Dataset) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := input.next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = d.addInferenceRow(); err != nil {
			return err
		}
	}
}

func (d *Dataset) addInferenceRow() error {
	rows, err := data.CheckedAdd(d.inferenceRowsScanned, 1)
	if err != nil {
		return err
	}
	d.inferenceRowsScanned = rows
	return nil
}

// addSampledColumn adds a field first seen in a schema-less JSON record.
func (s *sampler) addSampledColumn(name string) (int, error) {
	dt, c := arrow.DataType(arrow.BinaryTypes.String), config.Column{Type: "string"}
	if s.inferred {
		dt, c = arrow.Null, config.Column{Type: "null"}
	}
	if err := s.d.addColumn(name, c, dt); err != nil {
		return -1, err
	}
	return len(s.d.columns) - 1, nil
}

func (s *sampler) merge(index int, v cell) {
	column := &s.d.columns[index]
	if s.d.isNull(v, column.options) {
		return
	}
	column.typ = mergeType(column.typ, inferType(v))
	column.options, _ = optionsForArrow(column.typ)
}

func (s *sampler) finish() {
	if s.start.IsZero() {
		return
	}
	s.d.log.Info("source inference pass finished",
		"phase", "inference",
		"rows", s.rows,
		"duration_ms", time.Since(s.start).Milliseconds())
}

func (d *Dataset) addColumn(name string, c config.Column, dt arrow.DataType) error {
	if d.index(name) >= 0 {
		return fmt.Errorf("ambiguous column rename %q", name)
	}
	n := int64(4*len(name) + 2*len(c.Path) + len(c.Timezone) + columnMetadataBytes)
	for _, tokens := range [][]string{c.Formats, c.TrueValues, c.FalseValues} {
		for _, token := range tokens {
			n += int64(len(token) + columnTokenBytes)
		}
	}
	if err := d.reserve(n, "source schema"); err != nil {
		return err
	}
	if d.byName == nil {
		d.byName = make(map[string]int)
	}
	d.byName[name] = len(d.columns)
	d.columns = append(d.columns, field{name: name, options: c, typ: dt})
	return nil
}
func (d *Dataset) columnNames() []string {
	out := make([]string, len(d.columns))
	for i, c := range d.columns {
		out[i] = c.name
	}
	return out
}
func (d *Dataset) index(name string) int {
	if i, ok := d.byName[name]; ok {
		return i
	}
	return -1
}
func (d *Dataset) checkHeader(names []string) error {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		target := d.rename(name)
		if seen[target] {
			return fmt.Errorf("ambiguous renamed column %q", target)
		}
		seen[target] = true
		if d.index(target) < 0 && !d.ignoreExtras() {
			if err := d.addIssue(target, "undeclared input column"); err != nil {
				return err
			}
		}
	}
	for _, c := range d.columns {
		required := d.cfg.Input.Schema == nil || c.options.Required
		if required && !seen[c.name] {
			if err := d.addIssue(c.name, "required input column is absent"); err != nil {
				return err
			}
		}
	}
	return nil
}
func inferType(v cell) arrow.DataType {
	if !v.present || v.kind == kindNull {
		return arrow.Null
	}
	if v.kind == kindBool || v.text == "true" || v.text == "false" {
		return arrow.FixedWidthTypes.Boolean
	}
	if _, e := parseSigned(v.text, 64); e == nil {
		return arrow.PrimitiveTypes.Int64
	}
	if _, e := parseUnsigned(v.text, 64); e == nil {
		return arrow.PrimitiveTypes.Uint64
	}
	if _, e := parseFloat(v.text, 64); e == nil {
		return arrow.PrimitiveTypes.Float64
	}
	return arrow.BinaryTypes.String
}
func mergeType(a, b arrow.DataType) arrow.DataType {
	if a.ID() == arrow.NULL {
		return b
	}
	if b.ID() == arrow.NULL || arrow.TypeEqual(a, b) {
		return a
	}
	if (a.ID() == arrow.INT64 && b.ID() == arrow.FLOAT64) || (b.ID() == arrow.INT64 && a.ID() == arrow.FLOAT64) {
		return arrow.PrimitiveTypes.Float64
	}
	return arrow.BinaryTypes.String
}

// isNull applies JSON/Parquet null tokens and, for CSV only, configured null
// markers after any requested trimming.
func (d *Dataset) isNull(v cell, c config.Column) bool {
	if v.kind == kindNull {
		return true
	}
	if d.kind != inputCSV {
		return false
	}
	s := v.text
	if c.Trim {
		s = strings.TrimSpace(s)
	}
	for _, marker := range d.cfg.Input.NullValues {
		if s == marker {
			return true
		}
	}
	return false
}
