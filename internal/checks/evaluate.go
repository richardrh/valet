package checks

import (
	"fmt"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
)

// truth is a three-valued condition result.
type truth uint8

const (
	no truth = iota
	yes
	unknown
)

type conditionKind uint8

const (
	condAll conditionKind = iota
	condAny
	condNot
	condCompare
	condIn
	condIsNull
	condIsPresent
)

// condition is a compiled `when` acceptance predicate (not a violation
// predicate). Comparison leaves on numeric columns compare numerically
// (string literals parse as numbers); on other columns they follow the literal
// coercion policy of compareToLiteral, so a numeric literal against a
// non-numeric string cell is unknown rather than false.
type condition struct {
	kind        conditionKind
	op          operator
	column      operand
	numeric     bool
	value       scalar
	values      []scalar
	children    []*condition
	set         *literalSet
	epoch       *uint64
	cachedEpoch uint64
	cachedRow   int
	cached      truth
}

func (c *compiler) compileCondition(cc *config.Condition, depth int, nodes *int) (*condition, error) {
	*nodes++
	if depth > maxConditionDepth || *nodes > maxConditionNodes {
		return nil, fmt.Errorf("condition exceeds depth/node bound")
	}
	if err := c.p.reserve(bytesPerConditionNode, "condition nodes"); err != nil {
		return nil, err
	}
	out := &condition{epoch: &c.p.epoch}
	forms := 0
	compose := func(kind conditionKind, children []config.Condition) error {
		forms++
		out.kind = kind
		for i := range children {
			child, err := c.compileCondition(&children[i], depth+1, nodes)
			if err != nil {
				return err
			}
			out.children = append(out.children, child)
		}
		return nil
	}
	if cc.All != nil {
		if err := compose(condAll, cc.All); err != nil {
			return nil, err
		}
	}
	if cc.Any != nil {
		if err := compose(condAny, cc.Any); err != nil {
			return nil, err
		}
	}
	if cc.Not != nil {
		if err := compose(condNot, []config.Condition{*cc.Not}); err != nil {
			return nil, err
		}
	}
	if cc.Column != "" {
		forms++
		if err := c.compileLeaf(out, cc); err != nil {
			return nil, err
		}
	}
	if forms != 1 {
		return nil, fmt.Errorf("condition must have exactly one leaf/composition")
	}
	for _, existing := range c.p.conditions {
		if sameCondition(existing, out) {
			return existing, nil
		}
	}
	c.p.conditions = append(c.p.conditions, out)
	return out, nil
}

func (c *compiler) compileLeaf(out *condition, cc *config.Condition) error {
	col, err := c.bindOperand(cc.Column)
	if err != nil {
		return err
	}
	out.column = col
	out.numeric = isNumericType(col.typ)
	switch cc.Operator {
	case "is_null", "is_present":
		if cc.Value != nil {
			return fmt.Errorf("unary condition cannot have value")
		}
		out.kind = condIsNull
		if cc.Operator == "is_present" {
			out.kind = condIsPresent
		}
		return nil
	case "in":
		vals, ok := cc.Value.([]any)
		if !ok {
			return fmt.Errorf("in requires a value list")
		}
		if err := c.p.reserve(int64(len(vals))*bytesPerLiteral, "condition membership values"); err != nil {
			return err
		}
		out.kind = condIn
		for _, v := range vals {
			lit, err := literal(v)
			if err != nil {
				return err
			}
			bound, err := bindConditionLiteral(lit, col.typ)
			if err != nil {
				return err
			}
			out.values = append(out.values, bound)
		}
		out.set, err = c.p.membershipSet(out.values, out.numeric)
		return err
	}
	op, ok := parseOperator(cc.Operator)
	if !ok {
		return fmt.Errorf("unsupported condition operator")
	}
	out.kind, out.op = condCompare, op
	lit, err := literal(cc.Value)
	if err != nil {
		return err
	}
	out.value, err = bindConditionLiteral(lit, col.typ)
	return err
}

// bindConditionLiteral rounds numeric literals (including numeric strings) to
// a floating column's width and reads temporal literals as instants.
func bindConditionLiteral(v scalar, typ string) (scalar, error) {
	bound, err := bindFloatingLiteral(v, typ, true)
	if err != nil || !isTemporalType(typ) {
		return bound, err
	}
	if typ == "date" && bound.kind == valString {
		if d, ok := isoDateScalar(bound.s); ok {
			return d, nil
		}
	} else if t, ok := timestamp(bound); ok {
		return t, nil
	}
	return scalar{}, fmt.Errorf("invalid temporal condition value")
}

func (c *condition) eval(b *data.Batch, row int) truth {
	if c.cachedEpoch == *c.epoch && c.cachedRow == row {
		return c.cached
	}
	value := c.evaluate(b, row)
	c.cachedEpoch, c.cachedRow, c.cached = *c.epoch, row, value
	return value
}

func (c *condition) evaluate(b *data.Batch, row int) truth {
	switch c.kind {
	case condAll, condAny:
		// Three-valued logic: a determined child short-circuits even when a
		// sibling is unknown.
		decisive, otherwise := no, yes
		if c.kind == condAny {
			decisive, otherwise = yes, no
		}
		seenUnknown := false
		for _, child := range c.children {
			switch child.eval(b, row) {
			case decisive:
				return decisive
			case unknown:
				seenUnknown = true
			}
		}
		if seenUnknown {
			return unknown
		}
		return otherwise
	case condNot:
		switch c.children[0].eval(b, row) {
		case yes:
			return no
		case no:
			return yes
		}
		return unknown
	}
	state := c.column.state(row)
	switch c.kind {
	case condIsPresent:
		return truthOf(state != data.Missing)
	case condIsNull:
		return truthOf(state == data.Null)
	}
	if state == data.Invalid {
		return unknown
	}
	if state != data.Valid {
		return no
	}
	v := c.column.value(row)
	compare := compareToLiteral
	if c.numeric {
		compare = compareNumeric
	}
	if c.kind == condIn {
		if found, handled := c.set.lookup(v); handled {
			return truthOf(found)
		}
		found, unparsable := contains(v, c.values, compare)
		switch {
		case found:
			return yes
		case unparsable:
			return unknown
		}
		return no
	}
	o := compare(v, c.value)
	if o == orderUnparsable {
		return unknown
	}
	return truthOf(c.op.accepts(o))
}

func truthOf(ok bool) truth {
	if ok {
		return yes
	}
	return no
}

// Evaluate returns independently owned, budgeted result storage. The caller
// must Release it after consumption; later evaluations and Close do not
// invalidate it. Evaluate itself remains ordered and single-owner.
func (p *Program) Evaluate(b *data.Batch) (*data.Result, error) {
	if p.closed || p.finalized {
		return nil, fmt.Errorf("program is closed or finalized")
	}
	if p.rejected() {
		return nil, fmt.Errorf("cannot evaluate rejected schema")
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	if b == nil || b.Record == nil {
		return nil, fmt.Errorf("cannot evaluate control-only batch")
	}
	rows := uint64(b.Record.NumRows())
	total, err := data.CheckedAdd(p.summary.Total, rows)
	if err != nil {
		return nil, err
	}
	scanned, err := data.CheckedAdd(p.summary.RowsScanned, rows)
	if err != nil {
		return nil, err
	}
	accounted, err := data.CheckedAdd(total, p.summary.Coverage.MetadataClaimedRows)
	if err != nil {
		return nil, err
	}
	for code, coverage := range p.summary.RuleCoverage {
		if _, err := data.CheckedAdd(coverage.ValueRows, rows); err != nil {
			return nil, fmt.Errorf("rule %s: %w", code, err)
		}
	}
	p.bindBatch(b)
	defer p.unbindBatch()
	n := int(b.Record.NumRows())
	scratch, err := p.results.take(n)
	if err != nil {
		return nil, err
	}
	result := &data.Result{Buckets: scratch.buckets, Cleanup: func() { p.results.put(scratch) }}
	complete := false
	defer func() {
		if !complete {
			result.Release()
		}
	}()
	// Stdout-only validation needs aggregate counts, not per-row diagnostics.
	// Artifact report mode still retains issues for the NDJSON writer.
	retainIssues := p.summary.OutputMode != "report" || p.cfg.Output.URI != ""
	ingestCursor := 0
	for row := range n {
		if row%rowsPerCancellationCheck == 0 {
			if err := p.ctx.Err(); err != nil {
				return nil, err
			}
		}
		bucket := data.BucketPass
		location := data.Location{Row: uint64(row + 1), GlobalRow: p.summary.Coverage.AccountedRows + uint64(row+1)}
		if row < len(b.Locations) {
			location = b.Locations[row]
		}
		if location.PositionBasis == "metadata_prefix" {
			p.summary.ProvenancePositionBasis = "metadata_prefix"
		}
		// Source ingestion diagnostics are already in source row/field order.
		for ingestCursor < len(b.Ingest) {
			issue := b.Ingest[ingestCursor]
			if issue.GlobalRow != location.GlobalRow || issue.SourceOrdinal != location.SourceOrdinal {
				break
			}
			if retainIssues {
				if err := p.results.appendIssue(scratch, issue); err != nil {
					return nil, err
				}
			}
			if err := incrementCount(p.summary.IngestCounts, issue.Code); err != nil {
				return nil, err
			}
			bucket = data.BucketError
			ingestCursor++
		}
		for _, r := range p.rules {
			v, column, err := p.evaluateRule(r, b, row)
			if err != nil {
				return nil, fmt.Errorf("rule %s: %w", r.code, err)
			}
			if v != verdictFail {
				continue
			}
			if retainIssues {
				if err := p.results.appendIssue(scratch, data.Issue{Location: location, Code: r.code, Severity: r.level, Origin: data.OriginRule, Scope: data.ScopeRow, Column: column, Message: r.message}); err != nil {
					return nil, err
				}
			}
			if r.level == levelError {
				bucket = data.BucketError
			} else {
				bucket = max(bucket, data.BucketWarn)
			}
		}
		result.Buckets[row] = bucket
		switch bucket {
		case data.BucketError:
			p.summary.Errored++
		case data.BucketWarn:
			p.summary.Warned++
		default:
			p.summary.Passed++
		}
		if err := p.accumulateMetrics(b, row); err != nil {
			return nil, err
		}
	}
	if ingestCursor != len(b.Ingest) {
		return nil, fmt.Errorf("ingestion issues are not ordered with batch provenance")
	}
	p.summary.Total = total
	p.summary.RowsScanned = scanned
	p.summary.Coverage.ValueRows = total
	p.summary.Coverage.AccountedRows = accounted
	for code, coverage := range p.summary.RuleCoverage {
		coverage.ValueRows += rows
		p.summary.RuleCoverage[code] = coverage
	}
	result.Issues = scratch.issues
	complete = true
	return result, nil
}

// evaluateRule applies the rule's condition, then ORs its checks: any
// violation fails the rule; otherwise any unevaluable check makes the row
// unevaluable for the rule. Counters are updated here.
func (p *Program) evaluateRule(r *compiledRule, b *data.Batch, row int) (verdict, string, error) {
	if r.when != nil {
		switch r.when.eval(b, row) {
		case no:
			p.summary.NotApplicable[r.code]++
			return verdictPass, "", nil
		case unknown:
			p.summary.Unevaluable[r.code]++
			return verdictUnevaluable, "", nil
		}
	}
	out := verdictPass
	failingColumn := ""
	haveFailingColumn, ambiguousColumn := false, false
	for _, c := range r.checks {
		v, err := c.eval(p, b, row)
		if err != nil {
			return verdictPass, "", err
		}
		if v == verdictFail {
			out = verdictFail
			switch {
			case c.column == "":
				ambiguousColumn = true
			case !haveFailingColumn:
				failingColumn, haveFailingColumn = c.column, true
			case failingColumn != c.column:
				ambiguousColumn = true
			}
			continue
		}
		if v == verdictUnevaluable && out != verdictFail {
			out = verdictUnevaluable
		}
	}
	if ambiguousColumn {
		failingColumn = ""
	}
	if out == verdictUnevaluable {
		p.summary.Unevaluable[r.code]++
		return out, "", nil
	}
	r.evaluated++
	if out == verdictFail {
		p.summary.RuleCounts[r.code]++
	}
	return out, failingColumn, nil
}

func incrementCount(counts map[string]uint64, code string) error {
	next, err := data.CheckedAdd(counts[code], 1)
	if err != nil {
		return fmt.Errorf("counter %s: %w", code, err)
	}
	counts[code] = next
	return nil
}
