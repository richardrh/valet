// Package checks compiles validation rules into run-scoped, budgeted evaluators.
package checks

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/source"
	"github.com/richardrh/valet/internal/storage"
)

// Rule levels and issue reasons produced by this package.
const (
	levelError = "error"
	levelWarn  = "warn"

	reasonMissingColumn    = "MISSING_COLUMN"
	reasonTypeMismatch     = "TYPE_MISMATCH"
	reasonUnexpectedColumn = "UNEXPECTED_COLUMN"
	reasonUncastableValue  = "UNCASTABLE_VALUE"
	reasonSchemaMismatch   = "SCHEMA_MISMATCH"
)

type operand struct {
	name  string
	index int
	typ   string
	bound *boundColumn
}

// compiledRule is a row rule with defaults applied once.
type compiledRule struct {
	code, level, message, column string
	checks                       []*compiledCheck
	when                         *condition
	evaluated                    uint64
	// failed, unevaluable and notApplicable count the current batch; Evaluate
	// folds them into the summary maps once per batch.
	failed, unevaluable, notApplicable uint64
	coverage                           bool
}

func newCompiledRule(spec config.Rule) *compiledRule {
	r := &compiledRule{code: spec.Code, level: spec.Level, message: spec.Message, column: spec.Column}
	if r.level == "" {
		r.level = levelError
	}
	if r.message == "" {
		r.message = r.code
	}
	return r
}

// schemaIssue is the schema-scope issue this rule reports for column.
func (r *compiledRule) schemaIssue(column, reason string) data.Issue {
	return data.Issue{Code: r.code, Severity: r.level, Origin: data.OriginRule, Scope: data.ScopeSchema, Column: column, Message: r.message, Reason: reason}
}

// Program is the compiled, run-scoped rule set. Compile resolves schema gates,
// reference indexes and the uniqueness prepass; Evaluate then processes
// batches in order; Finalize settles dataset assertions and validation; Close
// releases every reservation.
type Program struct {
	budget          *resource.Budget
	ctx             context.Context
	logger          *slog.Logger
	schemaIssueSink func(data.Issue) error
	rules           []*compiledRule
	datasetRules    []datasetRule
	metrics         map[string]*columnMetrics
	stats           []statState
	indexes         map[string]*keyIndex
	keys            keyBuffer
	columns         []boundColumn
	columnNames     []string
	results         resultCache
	epoch           uint64
	conditions      []*condition
	summary         *data.Summary
	retained        int64
	finalized       bool
	closed          bool
	cfg             *config.Config
	schema          *arrow.Schema
}

type Description struct {
	RequiredPasses int               `json:"required_passes"`
	GlobalState    []string          `json:"global_state"`
	Rules          []RuleDescription `json:"rules"`
	SchemaIssues   []data.Issue      `json:"schema_issues,omitempty"`
}
type RuleDescription struct {
	Code   string   `json:"code"`
	Scopes []string `json:"scopes"`
	NoOp   bool     `json:"no_op"`
}

// Option configures a Program. Options apply on every Compile path, including
// programs rejected before binding.
type Option func(*Program)

// WithSchemaIssueSink streams each castability failure found by the prepass,
// with its row location, instead of retaining row diagnostics.
func WithSchemaIssueSink(sink func(data.Issue) error) Option {
	return func(p *Program) { p.schemaIssueSink = sink }
}

// WithLogger sets the logger for compile, prepass and reference-load events.
// The default discards.
func WithLogger(logger *slog.Logger) Option {
	return func(p *Program) {
		if logger != nil {
			p.logger = logger
		}
	}
}

func (p *Program) reserve(n int64, phase string) error {
	if err := p.budget.Reserve(n, phase); err != nil {
		return err
	}
	p.retained += n
	return nil
}

// reject marks the dataset schema rejected with the given schema issues.
func (p *Program) reject(issues ...data.Issue) {
	p.summary.Status = data.StatusSchemaRejected
	p.summary.Verification = "not_evaluated"
	p.summary.IngestionVerification = "not_evaluated"
	p.summary.SchemaIssues = append(p.summary.SchemaIssues, issues...)
}

func (p *Program) rejected() bool { return p.summary.Status == data.StatusSchemaRejected }

// compiler holds compile-only state. It lives for one Compile or Describe
// call and is dropped afterwards; nothing in Program refers to it.
type compiler struct {
	p           *Program
	cfg         *config.Config
	schema      *arrow.Schema
	exact       bool
	types       map[string]columnContext
	dateFormats map[string][]string
	// gates caches first-pass gate compilation by rule/check position so the
	// full bind reuses those checks instead of compiling them twice.
	gates map[[2]int][]*compiledCheck
}

// newProgram reserves the compiled-state estimate and, with bind, compiles
// rules, conditions and dataset metrics against schema.
func newProgram(ctx context.Context, cfg *config.Config, schema *arrow.Schema, budget *resource.Budget, bind bool, options []Option) (*Program, *compiler, error) {
	p := &Program{
		ctx: ctx, budget: budget, logger: slog.New(slog.DiscardHandler),
		summary: data.NewSummary(), indexes: make(map[string]*keyIndex), metrics: make(map[string]*columnMetrics),
		keys: keyBuffer{budget: budget},
	}
	p.cfg, p.schema = cfg, schema
	p.summary.OutputMode = cfg.Output.Mode
	if p.summary.OutputMode == "" {
		p.summary.OutputMode = "all"
	}
	if cfg.Input.Parquet.Statistics == "trusted" {
		p.summary.TrustPolicy = "producer_statistics"
	}
	if cfg.Input.Type == "parquet" {
		p.summary.Coverage.RowGroupsScanned = new(uint64)
		p.summary.Coverage.RowGroupsStatisticsOnly = new(uint64)
		p.summary.Coverage.RowGroupsTotal = new(uint64)
	}
	p.results.budget = budget
	for _, option := range options {
		option(p)
	}
	for _, spec := range cfg.DatasetRules {
		p.datasetRules = append(p.datasetRules, datasetRule{spec: spec})
	}
	if err := p.reserve(compiledEstimate(cfg, schema, bind), "compiled checks"); err != nil {
		return nil, nil, err
	}
	for _, rule := range cfg.Rules {
		for _, check := range rule.Checks {
			if !config.Checks[check.Type].Gate || rule.Level == levelWarn {
				p.summary.RuleCoverage[rule.Code] = data.RuleCoverage{}
				break
			}
		}
	}
	p.columns = make([]boundColumn, len(schema.Fields()))
	p.columnNames = make([]string, len(p.columns))
	for i, field := range schema.Fields() {
		p.columnNames[i] = field.Name
	}
	c := &compiler{p: p, cfg: cfg, schema: schema, exact: cfg.Input.Schema != nil, gates: make(map[[2]int][]*compiledCheck)}
	if !bind {
		return p, c, nil
	}
	err := c.deriveContexts()
	if err == nil {
		err = c.bind()
	}
	if err != nil {
		p.Close()
		return nil, nil, err
	}
	return p, c, nil
}

func compiledEstimate(cfg *config.Config, schema *arrow.Schema, bind bool) int64 {
	items := len(schema.Fields())
	if !bind {
		return int64(items+len(cfg.Rules)+1) * bytesPerCompiledItem
	}
	items += len(cfg.Rules) + len(cfg.DatasetRules) + len(cfg.Stats)
	estimate := int64(items+1) * bytesPerCompiledItem
	for _, rule := range cfg.Rules {
		for _, check := range rule.Checks {
			estimate += bytesPerCheckDefinition + int64(len(str(check.Params, "pattern")))*bytesPerPatternByte
			if vals, ok := check.Params["values"].([]any); ok {
				estimate += int64(len(vals)) * bytesPerLiteral
			}
			if check.Type == "date_format" {
				chargeFormat := func(format string) {
					const retainedCopies = 2 // derived context and compiled check
					estimate += retainedCopies * (bytesPerDateFormat + int64(len(format))*bytesPerDateFormatByte)
				}
				if format := str(check.Params, "format"); format != "" {
					chargeFormat(format)
				}
				switch values := check.Params["formats"].(type) {
				case []any:
					for _, value := range values {
						if format, ok := value.(string); ok {
							chargeFormat(format)
						}
					}
				case []string:
					for _, format := range values {
						chargeFormat(format)
					}
				}
			}
		}
	}
	return estimate
}

// Compile binds cfg to the dataset schema, rejects the schema through error
// level gates, loads reference indexes and runs the uniqueness/castability
// prepass when needed.
func Compile(ctx context.Context, cfg *config.Config, ds *source.Dataset, store *storage.Store, budget *resource.Budget, options ...Option) (*Program, error) {
	start := time.Now()
	ingestIssues := ds.SchemaIssues()
	p, c, err := newProgram(ctx, cfg, ds.Schema(), budget, len(ingestIssues) == 0, options)
	if err != nil {
		return nil, err
	}
	complete := false
	defer func() {
		if !complete {
			p.Close()
		}
	}()
	switch {
	case len(ingestIssues) > 0:
		p.reject(ingestIssues...)
	case !p.rejected():
		if err = c.loadReferences(ctx, store); err != nil {
			return nil, err
		}
		p.resolveGates()
		if !p.rejected() && p.needsPrepass() {
			if err = p.prepass(ctx, ds); err != nil {
				return nil, err
			}
			p.resolveGates()
		}
	}
	complete = true
	p.logger.Info("checks compiled", "phase", "compile", "rules", len(p.rules), "dataset_rules", len(p.datasetRules), "status", p.summary.Status, "duration_ms", since(start))
	return p, nil
}

func since(t time.Time) int64 { return time.Since(t).Milliseconds() }

// Describe reports the passes and global state a run would need, without
// reading rows or reference inputs.
func Describe(ctx context.Context, cfg *config.Config, ds *source.Dataset, store *storage.Store, budget *resource.Budget) (*Description, error) {
	ingestIssues := ds.SchemaIssues()
	p, _, err := newProgram(ctx, cfg, ds.Schema(), budget, len(ingestIssues) == 0, nil)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	if len(ingestIssues) != 0 {
		p.reject(ingestIssues...)
	}
	p.resolveGates()
	d := &Description{RequiredPasses: 1 + ds.DecoderPassesPerOpen(), GlobalState: []string{}, Rules: []RuleDescription{}}
	d.SchemaIssues = append(d.SchemaIssues, p.summary.SchemaIssues...)
	if p.needsPrepass() {
		d.RequiredPasses += 1 + ds.DecoderPassesPerOpen()
	}
	if p.rejected() {
		d.RequiredPasses = 0
	}
	for _, r := range p.rules {
		rd := RuleDescription{Code: r.code, NoOp: len(r.checks) == 0, Scopes: []string{}}
		for _, c := range r.checks {
			scope := c.kind.spec().Scope
			if _, ok := c.check.(*schemaNamesCheck); ok {
				scope = config.CheckScopeReference
			}
			switch scope {
			case config.CheckScopeGlobal:
				d.GlobalState = append(d.GlobalState, r.code+": exact duplicate keys")
			case config.CheckScopeReference:
				d.GlobalState = append(d.GlobalState, r.code+": projected reference keys")
			}
			rd.Scopes = append(rd.Scopes, scope)
		}
		d.Rules = append(d.Rules, rd)
	}
	return d, nil
}

// Summary returns the live run summary. Validation is settled by Finalize.
func (p *Program) Summary() *data.Summary { return p.summary }

func (p *Program) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	for _, idx := range p.indexes {
		idx.close()
	}
	p.keys.release()
	p.results.close()
	p.budget.Release(p.retained)
	p.retained = 0
	p.rules = nil
	p.indexes = nil
	p.metrics = nil
	p.stats = nil
	p.columns = nil
	p.columnNames = nil
	p.conditions = nil
	p.schemaIssueSink = nil
	return nil
}

func (c *compiler) bindOperand(name string) (operand, error) {
	idx := c.schema.FieldIndices(name)
	if len(idx) != 1 {
		return operand{}, fmt.Errorf("operand column %q is absent or ambiguous", name)
	}
	return operand{name: name, index: idx[0], typ: physicalType(c.schema.Field(idx[0]).Type), bound: &c.p.columns[idx[0]]}, nil
}

func (c *compiler) bindAll(names []string) ([]operand, error) {
	out := make([]operand, len(names))
	for i, name := range names {
		var err error
		if out[i], err = c.bindOperand(name); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func str(m map[string]any, k string) string { v, _ := m[k].(string); return v }
func stringsArg(v any) ([]string, error) {
	switch v := v.(type) {
	case []string:
		return v, nil
	case []any:
		out := make([]string, len(v))
		for i, x := range v {
			s, ok := x.(string)
			if !ok {
				return nil, fmt.Errorf("expected a list of strings")
			}
			out[i] = s
		}
		return out, nil
	}
	return nil, fmt.Errorf("expected a list of strings")
}
func boolArg(m map[string]any, k string, def bool) bool {
	if v, ok := m[k].(bool); ok {
		return v
	}
	return def
}

// operandNames resolves `columns`, else `column`, else the rule's column.
func operandNames(m map[string]any, ruleColumn string) ([]string, error) {
	if v, ok := m["columns"]; ok {
		return stringsArg(v)
	}
	name := str(m, "column")
	if name == "" {
		name = ruleColumn
	}
	if name == "" {
		return nil, nil
	}
	return []string{name}, nil
}

func (c *compiler) bind() error {
	// Resolve error-level metadata gates before binding row operands. A
	// deliberately required but absent header rejects the schema rather than
	// failing unrelated rules.
	rules := make([]*compiledRule, len(c.cfg.Rules))
	for ri, spec := range c.cfg.Rules {
		rules[ri] = newCompiledRule(spec)
		if rules[ri].level != levelError {
			continue
		}
		for ci, def := range spec.Checks {
			if !config.Checks[def.Type].Gate {
				continue
			}
			checks, err := c.compileCheck(rules[ri], def)
			if err != nil {
				return fmt.Errorf("rule %s: %w", spec.Code, err)
			}
			c.gates[[2]int{ri, ci}] = checks
			for _, cc := range checks {
				if s, ok := cc.check.(*schemaCheck); ok && s.outcome {
					c.p.reject(s.diagnostics...)
					break
				}
			}
		}
	}
	if c.p.rejected() {
		return nil
	}
	for ri, spec := range c.cfg.Rules {
		r := rules[ri]
		c.p.summary.RuleCounts[r.code] = 0
		c.p.summary.Unevaluable[r.code] = 0
		c.p.summary.NotApplicable[r.code] = 0
		for ci, def := range spec.Checks {
			if !config.Checks[def.Type].Gate || r.level != levelError {
				r.coverage = true
			}
			checks, ok := c.gates[[2]int{ri, ci}]
			if !ok {
				var err error
				if checks, err = c.compileCheck(r, def); err != nil {
					return fmt.Errorf("rule %s: %w", r.code, err)
				}
			}
			r.checks = append(r.checks, checks...)
			for _, cc := range checks {
				cc.index = ci
			}
		}
		if spec.When != nil {
			for _, cc := range r.checks {
				if !cc.kind.spec().AllowsWhen() {
					return fmt.Errorf("rule %s: conditional global/schema checks are unsupported", r.code)
				}
			}
			nodes := 0
			cond, err := c.compileCondition(spec.When, 0, &nodes)
			if err != nil {
				return fmt.Errorf("rule %s condition: %w", r.code, err)
			}
			r.when = cond
		}
		c.p.rules = append(c.p.rules, r)
		if r.coverage {
			c.p.summary.RuleCoverage[r.code] = data.RuleCoverage{}
		}
	}
	return c.bindMetrics()
}

func (c *compiler) compileCheck(r *compiledRule, def config.Check) ([]*compiledCheck, error) {
	kind, ok := kindByName[def.Type]
	if !ok {
		return nil, fmt.Errorf("unknown check %q", def.Type)
	}
	d := &checkDef{kind: kind, null: kindDefs[kind].null, rule: r, params: def.Params}
	if kind.spec().Operands != config.OperandsNone {
		names, err := operandNames(def.Params, r.column)
		if err != nil {
			return nil, err
		}
		if len(names) == 0 {
			return nil, fmt.Errorf("%s requires column(s)", kind)
		}
		if err := c.p.reserve(int64(len(names))*bytesPerOperand, "bound check operands"); err != nil {
			return nil, err
		}
		d.names = names
	}
	checks, err := kindDefs[kind].compile(c, d)
	if err != nil {
		return nil, err
	}
	out := make([]*compiledCheck, len(checks))
	column := ""
	if len(d.names) == 1 {
		column = d.names[0]
	}
	for i, ch := range checks {
		out[i] = &compiledCheck{kind: kind, column: column, check: ch}
	}
	return out, nil
}

func (p *Program) needsPrepass() bool {
	for _, r := range p.rules {
		for _, c := range r.checks {
			switch c := c.check.(type) {
			case *uniqueCheck:
				return true
			case *schemaCheck:
				if c.castable {
					return true
				}
			}
		}
	}
	return false
}

// resolveGates publishes the resolved outcome of every schema gate: its
// diagnostics, per-column castability failures, or a generic mismatch.
// Error-level failures reject the dataset. Repeated calls add nothing twice.
func (p *Program) resolveGates() {
	type issueKey struct{ code, column, reason string }
	seen := make(map[issueKey]bool, len(p.summary.SchemaIssues))
	for _, issue := range p.summary.SchemaIssues {
		seen[issueKey{issue.Code, issue.Column, issue.Reason}] = true
	}
	add := func(issue data.Issue) {
		k := issueKey{issue.Code, issue.Column, issue.Reason}
		if !seen[k] {
			seen[k] = true
			p.summary.SchemaIssues = append(p.summary.SchemaIssues, issue)
		}
	}
	for _, r := range p.rules {
		for _, c := range r.checks {
			s, ok := c.check.(*schemaCheck)
			if !ok || !s.outcome {
				continue
			}
			for _, diagnostic := range s.diagnostics {
				add(diagnostic)
			}
			if r.level == levelError {
				p.reject()
			}
			reported := len(s.diagnostics) > 0
			for i, count := range s.failures {
				if count == 0 {
					continue
				}
				reported = true
				issue := r.schemaIssue(s.columns[i].name, reasonUncastableValue)
				issue.Count = count
				add(issue)
			}
			if !reported {
				add(r.schemaIssue("", reasonSchemaMismatch))
			}
		}
	}
}

// prepass reads the dataset once to build exact uniqueness indexes and to
// resolve castability gates. An error-level castability gate reads through
// the preflight reader so conversion failures do not abort the run.
func (p *Program) prepass(ctx context.Context, ds *source.Dataset) (err error) {
	start := time.Now()
	span := diagnostics.Start(ctx, diagnostics.Prepass)
	defer func() { span.End(err) }()
	var castable []*schemaCheck
	var feeders []*uniqueCheck
	preflight := false
	for _, r := range p.rules {
		for _, c := range r.checks {
			switch c := c.check.(type) {
			case *schemaCheck:
				if c.castable {
					castable = append(castable, c)
					c.failures = make([]uint64, len(c.columns))
					preflight = preflight || r.level == levelError
				}
			case *uniqueCheck:
				name := c.indexName()
				idx := p.indexes[name]
				if idx == nil {
					// Compatible checks share one index fed by the first.
					idx = newKeyIndex(p.budget)
					p.indexes[name] = idx
					feeders = append(feeders, c)
				}
				c.index = idx
			}
		}
	}
	open := ds.Open
	if preflight {
		open = ds.OpenPreflight
	}
	reader, err := open(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	rows := p.summary.RowsScanned
	for {
		b, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = func() error { defer b.Release(); return p.accumulatePrepass(b, castable, feeders) }()
		if err != nil {
			return err
		}
	}
	p.summary.Passes += 1 + ds.DecoderPassesPerOpen()
	if err := reader.Close(); err != nil {
		return err
	}
	p.logger.Info("prepass complete", "phase", "prepass", "rows", p.summary.RowsScanned-rows, "duration_ms", since(start))
	return nil
}

func (p *Program) accumulatePrepass(b *data.Batch, castable []*schemaCheck, feeders []*uniqueCheck) error {
	p.bindBatch(b)
	defer p.unbindBatch()
	rows, err := data.CheckedAdd(p.summary.RowsScanned, uint64(b.Record.NumRows()))
	if err != nil {
		return err
	}
	p.summary.RowsScanned = rows
	diagnostics.AddRows(p.ctx, diagnostics.Prepass, uint64(b.Record.NumRows()))
	for row := range int(b.Record.NumRows()) {
		for _, s := range castable {
			for i, col := range s.columns {
				state := col.state(row)
				if state != data.Invalid && (state != data.Valid || typeValid(col.value(row), s.types[i])) {
					continue
				}
				s.outcome = true
				s.failures[i]++
				if p.schemaIssueSink != nil {
					issue := s.rule.schemaIssue(col.name, reasonUncastableValue)
					if row < len(b.Locations) {
						issue.Location = b.Locations[row]
					}
					if err := p.schemaIssueSink(issue); err != nil {
						return err
					}
				}
			}
		}
		for _, u := range feeders {
			key, status, err := p.valueKey(b, u.columns, row)
			if err != nil {
				return err
			}
			if status == keyBuilt {
				if err = u.index.add(key, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// loadReferences shares one validated source scan across all projections of
// that URI, while each ordered projection retains its own exact key index.
func (c *compiler) loadReferences(ctx context.Context, store *storage.Store) error {
	p := c.p
	var groups []*referenceGroup
	byURI := make(map[string]*referenceGroup)
	for _, r := range p.rules {
		for _, cc := range r.checks {
			var ref *referenceSource
			switch ch := cc.check.(type) {
			case *referenceCheck:
				ref = &ch.ref
			case *schemaNamesCheck:
				ref = &ch.ref
			default:
				continue
			}
			name := ref.indexName()
			idx := p.indexes[name]
			if idx == nil {
				idx = newKeyIndex(p.budget)
				p.indexes[name] = idx
				group := byURI[ref.uri]
				if group == nil {
					group = &referenceGroup{uri: ref.uri}
					byURI[ref.uri] = group
					groups = append(groups, group)
				}
				group.refs = append(group.refs, ref)
				for _, column := range ref.columns {
					found := false
					for _, existing := range group.columns {
						found = found || existing == column
					}
					if !found {
						group.columns = append(group.columns, column)
					}
				}
			}
			ref.index = idx
		}
	}
	for _, group := range groups {
		if err := c.loadReference(ctx, store, group); err != nil {
			return err
		}
	}
	for _, r := range p.rules {
		for _, cc := range r.checks {
			names, ok := cc.check.(*schemaNamesCheck)
			if !ok {
				continue
			}
			for _, field := range c.schema.Fields() {
				if err := p.referenceText(data.Valid, field.Name, 0); err != nil {
					return err
				}
				if names.ref.index.count(p.keys.buf) == 0 {
					names.outcome = true
				}
			}
		}
	}
	return nil
}

type referenceGroup struct {
	uri     string
	columns []string
	refs    []*referenceSource
}

func (c *compiler) loadReference(ctx context.Context, store *storage.Store, group *referenceGroup) (err error) {
	start := time.Now()
	span := diagnostics.Start(ctx, diagnostics.Reference)
	defer func() { span.End(err) }()
	ds, err := source.New(ctx, c.cfg.ForReference(group.uri, group.columns), store, c.p.budget, source.WithLogger(c.p.logger))
	if err != nil {
		return fmt.Errorf("reference input: %w", err)
	}
	defer func() {
		if closeErr := ds.Close(); err == nil {
			err = closeErr
		}
	}()
	rows, err := c.p.readReference(ctx, ds, group)
	if err != nil {
		return err
	}
	diagnostics.AddRows(ctx, diagnostics.Reference, rows)
	var reserved int64
	for _, ref := range group.refs {
		reserved += ref.index.reserved
	}
	c.p.logger.Info("reference index loaded", "phase", "reference", "source", storage.LogURI(group.uri), "projections", len(group.refs), "rows", rows, "bytes", reserved, "duration_ms", since(start))
	return nil
}

func (p *Program) readReference(ctx context.Context, ds *source.Dataset, group *referenceGroup) (rows uint64, err error) {
	if len(ds.SchemaIssues()) != 0 {
		return 0, fmt.Errorf("reference schema does not contain required projection")
	}
	bound := make([]boundColumn, len(ds.Schema().Fields()))
	columns := make([][]operand, len(group.refs))
	for i, ref := range group.refs {
		columns[i] = make([]operand, len(ref.columns))
		for j, name := range ref.columns {
			indices := ds.Schema().FieldIndices(name)
			if len(indices) != 1 {
				return 0, fmt.Errorf("reference column %q missing", name)
			}
			columns[i][j] = operand{name: name, index: indices[0], bound: &bound[indices[0]]}
		}
	}
	rd, err := ds.Open(ctx)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := rd.Close(); err == nil {
			err = closeErr
		}
	}()
	for {
		b, readErr := rd.Next(ctx)
		if readErr == io.EOF {
			return rows, nil
		}
		if readErr != nil {
			return rows, readErr
		}
		err = func() error {
			defer b.Release()
			defer clear(bound)
			for i, field := range ds.Schema().Fields() {
				bound[i] = bindColumn(b, field.Name, i)
			}
			for row := range int(b.Record.NumRows()) {
				if row%rowsPerCancellationCheck == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				for i, ref := range group.refs {
					key, status, err := p.referenceKey(b, columns[i], row)
					if err != nil {
						return err
					}
					if status != keyBuilt {
						return fmt.Errorf("invalid reference conversion")
					}
					if err = ref.index.add(key, false); err != nil {
						return err
					}
				}
			}
			var err error
			rows, err = data.CheckedAdd(rows, uint64(b.Record.NumRows()))
			if err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return rows, err
		}
	}
}
