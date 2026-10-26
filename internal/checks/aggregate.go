package checks

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
)

// Dataset assertion statuses and reasons.
const (
	datasetPass         = "pass"
	datasetFail         = "fail"
	datasetNotEvaluated = "not_evaluated"

	reasonNoObservations  = "NO_OBSERVATIONS"
	reasonAssertionFailed = "ASSERTION_FAILED"
	reasonSchemaRejected  = "SCHEMA_REJECTED"
)

// metric is a dataset assertion metric, resolved once from its name.
type metric uint8

const (
	metricRowCount metric = iota + 1
	metricRuleFailureCount
	metricRuleFailureFraction
	metricMissingCount
	metricMissingFraction
	metricNullCount
	metricNullFraction
	metricBlankCount
	metricBlankFraction
	metricConversionErrorCount
	metricConversionErrorFraction
	metricSum
	metricMin
	metricMax
	metricAvg
)

var metricByName = map[string]metric{
	"row_count": metricRowCount, "rule_failure_count": metricRuleFailureCount, "rule_failure_fraction": metricRuleFailureFraction,
	"missing_count": metricMissingCount, "missing_fraction": metricMissingFraction,
	"null_count": metricNullCount, "null_fraction": metricNullFraction,
	"blank_count": metricBlankCount, "blank_fraction": metricBlankFraction,
	"conversion_error_count": metricConversionErrorCount, "conversion_error_fraction": metricConversionErrorFraction,
	"sum": metricSum, "min": metricMin, "max": metricMax, "avg": metricAvg,
}

func (m metric) fraction() bool {
	switch m {
	case metricRuleFailureFraction, metricMissingFraction, metricNullFraction, metricBlankFraction, metricConversionErrorFraction:
		return true
	}
	return false
}
func (m metric) ruleFailure() bool {
	return m == metricRuleFailureCount || m == metricRuleFailureFraction
}
func (m metric) aggregate() bool { return m >= metricSum }

// fieldCount reports whether m counts a field state.
func (m metric) fieldCount() bool {
	return m >= metricMissingCount && m <= metricConversionErrorFraction
}

// datasetRule is an assertion with its metric and thresholds bound once.
type datasetRule struct {
	spec             config.DatasetRule
	metric           metric
	min, max, equals *big.Rat
	level            string
}

// kahan is compensated float64 summation.
type kahan struct{ sum, compensation float64 }

func (k *kahan) add(f float64) {
	adjusted := f - k.compensation
	sum := k.sum + adjusted
	k.compensation = (sum - k.sum) - adjusted
	k.sum = sum
}

// accumulator folds valid cells of one column for the requested aggregates.
type accumulator interface {
	// add folds v; false means v has no value in the accumulator's domain and
	// is excluded.
	add(v scalar, first bool) (bool, error)
	// observe reports the value of m over n contributing cells (n > 0).
	observe(m metric, n uint64) (observation, error)
}

// observation is a defined dataset metric value.
type observation struct {
	value    *big.Rat
	reported any
	// floating values compare to equals with an absolute tolerance.
	floating bool
}

type requested struct{ sum, min, max bool }

// floatAccumulator keeps the declared-float domain: compensated sum and
// float extrema. An unrequested sum is not accumulated, so it cannot overflow.
type floatAccumulator struct {
	want     requested
	sum      kahan
	min, max float64
}

func (a *floatAccumulator) add(v scalar, first bool) (bool, error) {
	f, ok := v.number()
	if !ok {
		return false, nil
	}
	if a.want.sum {
		a.sum.add(f)
		if !finite(a.sum.sum) {
			return false, fmt.Errorf("dataset floating accumulator overflow")
		}
	}
	if a.want.min && (first || f < a.min) {
		a.min = f
	}
	if a.want.max && (first || f > a.max) {
		a.max = f
	}
	return true, nil
}

func (a *floatAccumulator) observe(m metric, n uint64) (observation, error) {
	f := a.sum.sum
	switch m {
	case metricMin:
		f = a.min
	case metricMax:
		f = a.max
	case metricAvg:
		f /= float64(n)
	}
	return observation{value: new(big.Rat).SetFloat64(f), reported: f, floating: true}, nil
}

// exactAccumulator keeps integer/decimal values exactly. While every cell is
// an integer or a decimal at the column scale and the sum fits, it folds
// 128-bit coefficients; otherwise it spills to checked rationals for good.
type exactAccumulator struct {
	want             requested
	spilled          bool
	nsum, nmin, nmax decimal128.Num
	sum, min, max    big.Rat
	decimal          bool
	scale            int32
}

type exactAverage struct {
	Sum   string `json:"sum"`
	Count uint64 `json:"count"`
}

func (a *exactAccumulator) add(v scalar, first bool) (bool, error) {
	if !a.spilled {
		if n, ok := a.coefficient(v); ok {
			sum := a.nsum.Add(n)
			if !a.want.sum || !addOverflows(a.nsum, n, sum) {
				if a.want.sum {
					a.nsum = sum
				}
				if a.want.min && (first || n.Less(a.nmin)) {
					a.nmin = n
				}
				if a.want.max && (first || n.Greater(a.nmax)) {
					a.nmax = n
				}
				return true, nil
			}
		}
		a.spill(first)
	}
	var r big.Rat
	if !v.rat(&r) {
		return false, nil
	}
	if a.want.sum {
		a.sum.Add(&a.sum, &r)
		if err := checkedRat(&a.sum); err != nil {
			return false, err
		}
	}
	if a.want.min && (first || r.Cmp(&a.min) < 0) {
		a.min.Set(&r)
	}
	if a.want.max && (first || r.Cmp(&a.max) > 0) {
		a.max.Set(&r)
	}
	return true, nil
}

// coefficient is v as a 128-bit coefficient at the accumulator's scale.
func (a *exactAccumulator) coefficient(v scalar) (decimal128.Num, bool) {
	switch {
	case v.kind == valInt && a.scale == 0:
		return decimal128.FromI64(v.i), true
	case v.kind == valUint && a.scale == 0:
		return decimal128.FromU64(v.u), true
	case v.kind == valDecimal && v.scale == a.scale:
		return decimal128.New(int64(v.decimal[1]), v.decimal[0]), true
	}
	return decimal128.Num{}, false
}

func addOverflows(a, b, sum decimal128.Num) bool {
	return (a.Sign() < 0) == (b.Sign() < 0) && (sum.Sign() < 0) != (a.Sign() < 0)
}

// spill moves the native state into the rational accumulators. With first,
// nothing has been folded yet.
func (a *exactAccumulator) spill(first bool) {
	a.spilled = true
	if first {
		return
	}
	if a.want.sum {
		a.ratOf(a.nsum, &a.sum)
	}
	if a.want.min {
		a.ratOf(a.nmin, &a.min)
	}
	if a.want.max {
		a.ratOf(a.nmax, &a.max)
	}
}

// ratOf sets dst to the coefficient n at the accumulator's scale. A 128-bit
// coefficient always converts.
func (a *exactAccumulator) ratOf(n decimal128.Num, dst *big.Rat) {
	scalar{kind: valDecimal, scale: a.scale, decimal: [2]uint64{n.LowBits(), uint64(n.HighBits())}}.rat(dst)
}

func (a *exactAccumulator) text(v *big.Rat) string {
	if a.decimal {
		return v.FloatString(int(a.scale))
	}
	return exactText(v)
}

func (a *exactAccumulator) observe(m metric, n uint64) (observation, error) {
	if !a.spilled {
		a.spill(false)
	}
	value := new(big.Rat)
	switch m {
	case metricMin:
		value.Set(&a.min)
	case metricMax:
		value.Set(&a.max)
	case metricAvg:
		// Report the exact sum/count pair rather than inventing rounding.
		value.Quo(&a.sum, ratUint(n))
		if err := checkedRat(value); err != nil {
			return observation{}, err
		}
		return observation{value: value, reported: exactAverage{Sum: a.text(&a.sum), Count: n}}, nil
	default:
		value.Set(&a.sum)
	}
	return observation{value: value, reported: a.text(value)}, nil
}

// columnMetrics counts field states of one column and folds requested
// aggregates.
type columnMetrics struct {
	column                                                 operand
	missing, nulls, blank, invalid, contributing, excluded uint64
	acc                                                    accumulator
	want                                                   requested
}

type statState struct {
	spec   config.Stat
	column operand
	sum    kahan
	count  uint64
}

func checkedRat(r *big.Rat) error {
	if r.Num().BitLen() > maxRatBits || r.Denom().BitLen() > maxRatBits {
		return fmt.Errorf("dataset numeric accumulator overflow (%d-bit precision)", maxRatBits)
	}
	return nil
}

func (c *compiler) bindMetrics() error {
	p := c.p
	for i := range p.datasetRules {
		dr := &p.datasetRules[i]
		rule := dr.spec
		dr.level = rule.Level
		if dr.level == "" {
			dr.level = levelError
		}
		if rule.Min == nil && rule.Max == nil && rule.Equals == nil {
			return fmt.Errorf("dataset rule %s requires a bound", rule.Code)
		}
		if rule.Tolerance != nil && (*rule.Tolerance < 0 || !finite(*rule.Tolerance)) {
			return fmt.Errorf("dataset rule %s: invalid absolute tolerance", rule.Code)
		}
		for _, bound := range []struct {
			v   any
			dst **big.Rat
		}{{rule.Min, &dr.min}, {rule.Max, &dr.max}, {rule.Equals, &dr.equals}} {
			if bound.v == nil {
				continue
			}
			v, err := literal(bound.v)
			if err != nil {
				return fmt.Errorf("dataset rule %s: %w", rule.Code, err)
			}
			r := new(big.Rat)
			if !v.rat(r) || checkedRat(r) != nil {
				return fmt.Errorf("dataset rule %s: numeric bound outside supported precision", rule.Code)
			}
			*bound.dst = r
		}
		m, ok := metricByName[rule.Metric]
		if !ok {
			return fmt.Errorf("dataset rule %s: unknown metric %q", rule.Code, rule.Metric)
		}
		dr.metric = m
		switch {
		case m == metricRowCount:
			if rule.Column != "" || rule.RuleCode != "" {
				return fmt.Errorf("row_count has no column/rule operand")
			}
		case m.ruleFailure():
			found := false
			for _, r := range p.rules {
				found = found || r.code == rule.RuleCode
			}
			if !found {
				return fmt.Errorf("dataset rule %s: unknown row rule", rule.Code)
			}
		default:
			if err := c.bindColumnMetric(dr); err != nil {
				return err
			}
		}
	}
	for _, stat := range c.cfg.Stats {
		col, err := c.bindOperand(stat.Column)
		if err != nil {
			return err
		}
		switch stat.Aggregate {
		case "sum", "count", "avg":
		default:
			return fmt.Errorf("stat %s: unknown aggregate %q", stat.Code, stat.Aggregate)
		}
		p.stats = append(p.stats, statState{spec: stat, column: col})
	}
	for _, m := range p.metrics {
		if m.want == (requested{}) {
			continue
		}
		if isFloatType(m.column.typ) {
			m.acc = &floatAccumulator{want: m.want}
			continue
		}
		exact := &exactAccumulator{want: m.want}
		if typ, ok := c.schema.Field(m.column.index).Type.(*arrow.Decimal128Type); ok {
			exact.decimal, exact.scale = true, typ.Scale
		}
		m.acc = exact
	}
	return nil
}

func (c *compiler) bindColumnMetric(dr *datasetRule) error {
	rule := dr.spec
	col, err := c.bindOperand(rule.Column)
	if err != nil {
		return fmt.Errorf("dataset rule %s: %w", rule.Code, err)
	}
	if dr.metric.aggregate() {
		if !c.exact || !isNumericType(col.typ) {
			return fmt.Errorf("dataset rule %s: numeric aggregate requires declared numeric column", rule.Code)
		}
		if isFloatType(col.typ) && rule.Equals != nil && rule.Tolerance == nil {
			return fmt.Errorf("dataset rule %s: floating equals requires tolerance", rule.Code)
		}
	}
	m := c.p.metrics[col.name]
	if m == nil {
		m = &columnMetrics{column: col}
		c.p.metrics[col.name] = m
	}
	switch dr.metric {
	case metricSum, metricAvg:
		m.want.sum = true
	case metricMin:
		m.want.min = true
	case metricMax:
		m.want.max = true
	}
	return nil
}

func (p *Program) accumulateMetrics(b *data.Batch, row int) error {
	for _, m := range p.metrics {
		state := m.column.state(row)
		switch state {
		case data.Missing:
			m.missing++
		case data.Null:
			m.nulls++
		case data.Invalid:
			m.invalid++
		}
		if state != data.Valid {
			m.excluded++
			continue
		}
		v := m.column.value(row)
		if v.kind == valString && strings.TrimSpace(v.s) == "" {
			m.blank++
		}
		if m.acc == nil {
			continue
		}
		ok, err := m.acc.add(v, m.contributing == 0)
		if err != nil {
			return err
		}
		if !ok {
			m.excluded++
			continue
		}
		m.contributing++
	}
	for i := range p.stats {
		s := &p.stats[i]
		if s.column.state(row) != data.Valid {
			continue
		}
		if s.spec.Aggregate == "count" {
			s.count++
			continue
		}
		f, ok := s.column.value(row).number()
		if !ok {
			continue
		}
		s.sum.add(f)
		if !finite(s.sum.sum) {
			return fmt.Errorf("display statistic overflow")
		}
		s.count++
	}
	return nil
}

// Finalize settles display statistics, dataset assertions and the run's
// validation outcome. It is idempotent.
func (p *Program) Finalize() error {
	if p.closed {
		return fmt.Errorf("program is closed")
	}
	if p.finalized {
		return nil
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	p.finalized = true
	failed := p.rejected() || p.summary.Errored > 0
	defer func() {
		if failed {
			p.summary.Validation = data.ValidationFail
		}
	}()
	if p.rejected() {
		for _, r := range p.datasetRules {
			p.summary.DatasetResults = append(p.summary.DatasetResults, data.DatasetResult{Code: r.spec.Code, Metric: r.spec.Metric, Status: datasetNotEvaluated, Reason: reasonSchemaRejected})
		}
		return nil
	}
	for _, s := range p.stats {
		key := s.spec.Code
		if key == "" {
			key = s.spec.Name
		}
		if key == "" {
			key = s.spec.Column + "_" + s.spec.Aggregate
		}
		switch s.spec.Aggregate {
		case "count":
			p.summary.Stats[key] = s.count
		case "sum":
			p.summary.Stats[key] = s.sum.sum
		case "avg":
			value := 0.0
			if s.count > 0 {
				value = s.sum.sum / float64(s.count)
			}
			p.summary.Stats[key] = value
		}
	}
	for i := range p.datasetRules {
		rule := &p.datasetRules[i]
		result, err := p.assert(rule)
		if err != nil {
			return err
		}
		p.summary.DatasetResults = append(p.summary.DatasetResults, result)
		if result.Status == datasetFail {
			p.summary.DatasetIssues = append(p.summary.DatasetIssues, data.Issue{Code: rule.spec.Code, Severity: rule.level, Origin: data.OriginDataset, Scope: data.ScopeDataset, Column: rule.spec.Column, Reason: result.Reason})
			failed = failed || rule.level == levelError
		}
	}
	return nil
}

// assert observes the rule's metric and checks its inclusive bounds.
func (p *Program) assert(rule *datasetRule) (data.DatasetResult, error) {
	result, obs, defined, err := p.observe(rule)
	if err != nil {
		return result, err
	}
	result.Status = datasetPass
	if !defined {
		result.Status, result.Reason = datasetFail, reasonNoObservations
		return result, nil
	}
	violated := false
	if rule.min != nil && obs.value.Cmp(rule.min) < 0 {
		violated = true
	}
	if rule.max != nil && obs.value.Cmp(rule.max) > 0 {
		violated = true
	}
	if rule.equals != nil {
		if obs.floating {
			var difference big.Rat
			difference.Sub(obs.value, rule.equals)
			difference.Abs(&difference)
			violated = violated || difference.Cmp(new(big.Rat).SetFloat64(*rule.spec.Tolerance)) > 0
		} else {
			violated = violated || obs.value.Cmp(rule.equals) != 0
		}
	}
	if violated {
		result.Status, result.Reason = datasetFail, reasonAssertionFailed
	}
	return result, nil
}

func ratUint(v uint64) *big.Rat { var n big.Int; n.SetUint64(v); return new(big.Rat).SetInt(&n) }

// observe computes the metric. defined is false when the metric has no value
// (no contributing cells, or a fraction over zero rows).
func (p *Program) observe(rule *datasetRule) (result data.DatasetResult, obs observation, defined bool, err error) {
	spec := rule.spec
	result = data.DatasetResult{Code: spec.Code, Metric: spec.Metric}
	if rule.metric.aggregate() {
		m := p.metrics[spec.Column]
		result.Contributing = m.contributing
		result.Excluded = m.excluded
		if m.contributing == 0 {
			return result, obs, false, nil
		}
		obs, err = m.acc.observe(rule.metric, m.contributing)
		if err != nil {
			return result, obs, false, err
		}
		result.Value = obs.reported
		return result, obs, true, nil
	}
	count, denominator := uint64(0), p.summary.Total
	switch {
	case rule.metric == metricRowCount:
		count = p.summary.Total
	case rule.metric.ruleFailure():
		count = p.summary.RuleCounts[spec.RuleCode]
		for _, r := range p.rules {
			if r.code == spec.RuleCode {
				denominator = r.evaluated
				break
			}
		}
		result.NotApplicable = p.summary.NotApplicable[spec.RuleCode]
		result.Unevaluable = p.summary.Unevaluable[spec.RuleCode]
	case rule.metric.fieldCount():
		m := p.metrics[spec.Column]
		switch rule.metric {
		case metricMissingCount, metricMissingFraction:
			count = m.missing
		case metricNullCount, metricNullFraction:
			count = m.nulls
		case metricBlankCount, metricBlankFraction:
			count = m.blank
		default:
			count = m.invalid
		}
	}
	obs.value = ratUint(count)
	result.Value = count
	if rule.metric.ruleFailure() || rule.metric.fraction() {
		result.Numerator = count
		result.Denominator = denominator
	}
	if rule.metric.fraction() {
		if denominator == 0 {
			return result, obs, false, nil
		}
		obs.value.Quo(obs.value, ratUint(denominator))
		f, _ := obs.value.Float64()
		result.Value = f
	}
	return result, obs, true, nil
}

func exactText(v *big.Rat) string {
	if v.IsInt() {
		return v.Num().String()
	}
	den := new(big.Int).Set(v.Denom())
	two, five := big.NewInt(2), big.NewInt(5)
	twos, fives := 0, 0
	var remainder big.Int
	for {
		remainder.Mod(den, two)
		if remainder.Sign() != 0 {
			break
		}
		den.Quo(den, two)
		twos++
	}
	for {
		remainder.Mod(den, five)
		if remainder.Sign() != 0 {
			break
		}
		den.Quo(den, five)
		fives++
	}
	if den.Cmp(big.NewInt(1)) == 0 {
		return v.FloatString(max(twos, fives))
	}
	return v.RatString()
}
