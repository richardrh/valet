package checks

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
)

// checkKind is a built-in check type, resolved from its configuration name
// once at compile time.
type checkKind uint8

const (
	checkNotNull checkKind = iota
	checkNotBlank
	checkIsPresent
	checkValidLength
	checkMinLength
	checkMaxLength
	checkValidRange
	checkEquals
	checkNotEquals
	checkAllEqual
	checkHasNullKeys
	checkValidValues
	checkMatchesPattern
	checkIsUnique
	checkIsUniqueComposite
	checkCompareColumns
	checkReferentialIntegrity
	checkReferentialIntegrityComposite
	checkRequiredColumns
	checkSchemaMatches
	checkValidSchemaColumns
	checkTypeCheck
	checkDateFormat
	checkTimestampWindow
	checkKindCount
)

// nullPolicy declares how a kind treats operand field states other than Valid.
type nullPolicy uint8

const (
	// nullPasses: null/missing operands satisfy the check; invalid operands
	// make it unevaluable.
	nullPasses nullPolicy = iota
	// nullFails: null/missing operands violate; invalid operands make it
	// unevaluable.
	nullFails
	// nullFailsTextTarget: as nullFails for string/bool type_check targets,
	// otherwise as nullPasses.
	nullFailsTextTarget
	// missingFails: state-only presence check; only an absent field violates
	// and invalid conversions are not inspected.
	missingFails
	// nullOrMissingFails: state-only null check; null or absent violates and
	// invalid conversions are not inspected.
	nullOrMissingFails
	// ownStateHandling: the kind's evaluator defines state handling (keys,
	// column comparison, dataset-wide schema outcomes).
	ownStateHandling
)

type kindDef struct {
	name    string
	null    nullPolicy
	compile compileFunc
}

// checkDef is the input to one per-kind compile function.
type checkDef struct {
	kind   checkKind
	null   nullPolicy
	rule   *compiledRule
	params map[string]any
	// names are the resolved operand names; nil for kinds without operands.
	names []string
}

type compileFunc func(c *compiler, d *checkDef) ([]check, error)

var kindDefs = [checkKindCount]kindDef{
	checkNotNull:                       {"not_null", nullOrMissingFails, compileStateCheck},
	checkNotBlank:                      {"not_blank", nullFails, compileNotBlank},
	checkIsPresent:                     {"is_present", missingFails, compileStateCheck},
	checkValidLength:                   {"valid_length", nullFails, compileLength},
	checkMinLength:                     {"min_length", nullFails, compileLength},
	checkMaxLength:                     {"max_length", nullFails, compileLength},
	checkValidRange:                    {"valid_range", nullPasses, compileRange},
	checkEquals:                        {"equals", nullFails, compileEquality},
	checkNotEquals:                     {"not_equals", nullPasses, compileEquality},
	checkAllEqual:                      {"all_equal", nullFails, compileEquality},
	checkHasNullKeys:                   {"has_null_keys", nullOrMissingFails, compileStateCheck},
	checkValidValues:                   {"valid_values", nullPasses, compileMembership},
	checkMatchesPattern:                {"matches_pattern", nullFails, compilePattern},
	checkIsUnique:                      {"is_unique", ownStateHandling, compileUnique},
	checkIsUniqueComposite:             {"is_unique_composite", ownStateHandling, compileUnique},
	checkCompareColumns:                {"compare_columns", ownStateHandling, compileCompareColumns},
	checkReferentialIntegrity:          {"referential_integrity", ownStateHandling, compileReference},
	checkReferentialIntegrityComposite: {"referential_integrity_composite", ownStateHandling, compileReference},
	checkRequiredColumns:               {"required_columns", ownStateHandling, compileRequiredColumns},
	checkSchemaMatches:                 {"schema_matches", ownStateHandling, compileSchemaMatches},
	checkValidSchemaColumns:            {"valid_schema_columns", ownStateHandling, compileSchemaNames},
	checkTypeCheck:                     {"type_check", nullFailsTextTarget, compileTypeCheck},
	checkDateFormat:                    {"date_format", nullPasses, compileDateFormat},
	checkTimestampWindow:               {"timestamp_window", nullPasses, compileTimestampWindow},
}

var kindByName = func() map[string]checkKind {
	out := make(map[string]checkKind, len(kindDefs))
	for k, def := range kindDefs {
		if _, ok := config.Checks[def.name]; !ok {
			panic("checks: kind " + def.name + " missing from config.Checks")
		}
		out[def.name] = checkKind(k)
	}
	for name := range config.Checks {
		if _, ok := out[name]; !ok {
			panic("checks: config.Checks entry " + name + " has no compiler")
		}
	}
	return out
}()

func (k checkKind) String() string         { return kindDefs[k].name }
func (k checkKind) spec() config.CheckSpec { return config.Checks[kindDefs[k].name] }

// verdict is one check's outcome for one row.
type verdict uint8

const (
	verdictPass verdict = iota
	verdictFail
	verdictUnevaluable
)

func failIf(bad bool) verdict {
	if bad {
		return verdictFail
	}
	return verdictPass
}

// check evaluates one compiled check against one row.
type check interface {
	eval(p *Program, b *data.Batch, row int) (verdict, error)
}

type compiledCheck struct {
	index  int
	kind   checkKind
	column string
	check
}

// --- state-only checks: not_null, has_null_keys, is_present ---

type stateCheck struct {
	columns []operand
	failOn  [data.Invalid + 1]bool
}

func compileStateCheck(c *compiler, d *checkDef) ([]check, error) {
	cols, err := c.bindAll(d.names)
	if err != nil {
		return nil, err
	}
	s := &stateCheck{columns: cols}
	s.failOn[data.Missing] = true
	s.failOn[data.Null] = d.null == nullOrMissingFails
	return []check{s}, nil
}

func (s *stateCheck) eval(_ *Program, b *data.Batch, row int) (verdict, error) {
	for _, col := range s.columns {
		if s.failOn[col.state(row)] {
			return verdictFail, nil
		}
	}
	return verdictPass, nil
}

// --- per-cell value checks ---

// cellTest judges one Valid cell.
type cellTest interface{ test(v scalar) verdict }

// valueCheck applies a per-column cell test under the kind's null policy.
// Multiple columns combine with OR: any failed assertion violates the rule.
type valueCheck struct {
	columns []operand
	tests   []cellTest
	onNull  verdict
}

func (c *compiler) valueCheck(d *checkDef, onNull verdict, bind func(col operand) (cellTest, error)) ([]check, error) {
	cols, err := c.bindAll(d.names)
	if err != nil {
		return nil, err
	}
	v := &valueCheck{columns: cols, tests: make([]cellTest, len(cols)), onNull: onNull}
	for i, col := range cols {
		if v.tests[i], err = bind(col); err != nil {
			return nil, err
		}
	}
	return []check{v}, nil
}

func (d *checkDef) onNull() verdict {
	if d.null == nullFails {
		return verdictFail
	}
	return verdictPass
}

func (v *valueCheck) column(b *data.Batch, row, i int) verdict {
	col := v.columns[i]
	switch col.state(row) {
	case data.Valid:
		return v.tests[i].test(col.value(row))
	case data.Invalid:
		return verdictUnevaluable
	}
	return v.onNull
}

func (v *valueCheck) eval(_ *Program, b *data.Batch, row int) (verdict, error) {
	out := verdictPass
	for i := range v.columns {
		switch v.column(b, row, i) {
		case verdictFail:
			return verdictFail, nil
		case verdictUnevaluable:
			out = verdictUnevaluable
		}
	}
	return out, nil
}

type blankTest struct{}

func (blankTest) test(v scalar) verdict {
	return failIf(v.kind != valString || strings.TrimSpace(v.s) == "")
}

func compileNotBlank(c *compiler, d *checkDef) ([]check, error) {
	return c.valueCheck(d, d.onNull(), func(operand) (cellTest, error) { return blankTest{}, nil })
}

type lengthBound uint8

const (
	lengthExact lengthBound = iota
	lengthAtLeast
	lengthAtMost
)

// lengthTest counts Unicode code points. valid_length requires string cells;
// min/max_length measure the documented text conversion of any cell.
type lengthTest struct {
	n     int
	bound lengthBound
}

func (t lengthTest) test(v scalar) verdict {
	switch t.bound {
	case lengthAtLeast:
		return failIf(utf8.RuneCountInString(v.text()) < t.n)
	case lengthAtMost:
		return failIf(utf8.RuneCountInString(v.text()) > t.n)
	}
	return failIf(v.kind != valString || utf8.RuneCountInString(v.s) != t.n)
}

func compileLength(c *compiler, d *checkDef) ([]check, error) {
	key, bound := "length", lengthExact
	switch d.kind {
	case checkMinLength:
		key, bound = "min_length", lengthAtLeast
	case checkMaxLength:
		key, bound = "max_length", lengthAtMost
	}
	lit, err := literal(d.params[key])
	if err != nil {
		return nil, err
	}
	f, ok := lit.number()
	if !ok || f < 0 || f != float64(int(f)) {
		return nil, fmt.Errorf("invalid length")
	}
	t := lengthTest{n: int(f), bound: bound}
	return c.valueCheck(d, d.onNull(), func(operand) (cellTest, error) { return t, nil })
}

type patternTest struct{ rx *regexp.Regexp }

func (t patternTest) test(v scalar) verdict {
	return failIf(v.kind != valString || !t.rx.MatchString(v.s))
}

func compilePattern(c *compiler, d *checkDef) ([]check, error) {
	rx, err := c.compileRegex(str(d.params, "pattern"))
	if err != nil {
		return nil, err
	}
	return c.valueCheck(d, d.onNull(), func(operand) (cellTest, error) { return patternTest{rx}, nil })
}

// exactRangeTest compares declared-schema cells exactly against bounds bound
// to the operand width. Non-numeric and non-finite values violate.
type exactRangeTest struct {
	min, max                   *scalar
	exclusiveMin, exclusiveMax bool
}

func (t exactRangeTest) test(v scalar) verdict {
	if !v.isNumber() || (v.kind == valFloat && !finite(v.f)) {
		return verdictFail
	}
	if t.min != nil {
		if o := compareNumeric(v, *t.min); !o.ordered() || o == orderLess || t.exclusiveMin && o == orderEqual {
			return verdictFail
		}
	}
	if t.max != nil {
		if o := compareNumeric(v, *t.max); !o.ordered() || o == orderGreater || t.exclusiveMax && o == orderEqual {
			return verdictFail
		}
	}
	return verdictPass
}

// floatRangeTest is the schema-less legacy range: cells (including text)
// convert through float64, independently of physical width. Values that are
// not finite numbers violate.
type floatRangeTest struct {
	min, max                   float64
	hasMin, hasMax             bool
	exclusiveMin, exclusiveMax bool
}

func (t floatRangeTest) test(v scalar) verdict {
	f, ok := v.number()
	return failIf(!ok ||
		t.hasMin && (f < t.min || t.exclusiveMin && f == t.min) ||
		t.hasMax && (f > t.max || t.exclusiveMax && f == t.max))
}

func compileRange(c *compiler, d *checkDef) ([]check, error) {
	min, err := optionalLiteral(d.params, "min")
	if err != nil {
		return nil, err
	}
	max, err := optionalLiteral(d.params, "max")
	if err != nil {
		return nil, err
	}
	exclusiveMin, _ := d.params["exclusive_min"].(bool)
	exclusiveMax, _ := d.params["exclusive_max"].(bool)
	legacy := floatRangeTest{exclusiveMin: exclusiveMin, exclusiveMax: exclusiveMax}
	for _, bound := range []struct {
		v   *scalar
		f   *float64
		has *bool
	}{{min, &legacy.min, &legacy.hasMin}, {max, &legacy.max, &legacy.hasMax}} {
		if bound.v == nil {
			continue
		}
		f, ok := bound.v.number()
		if !ok {
			return nil, fmt.Errorf("range bound must be numeric")
		}
		*bound.f, *bound.has = f, true
	}
	if err := c.p.reserve(int64(len(d.names))*bytesPerOperand, "bound comparison literals"); err != nil {
		return nil, err
	}
	return c.valueCheck(d, d.onNull(), func(col operand) (cellTest, error) {
		if !c.exact {
			return legacy, nil
		}
		t := exactRangeTest{exclusiveMin: exclusiveMin, exclusiveMax: exclusiveMax}
		for _, bound := range []struct{ src, dst **scalar }{{&min, &t.min}, {&max, &t.max}} {
			if *bound.src == nil {
				continue
			}
			v, err := bindFloatingLiteral(**bound.src, col.typ, true)
			if err != nil {
				return nil, err
			}
			*bound.dst = &v
		}
		return t, nil
	})
}

// equalityTest compares a cell to a literal under the literal coercion policy
// (compareToLiteral). equals/all_equal are assertions that fail when values
// differ; not_equals is an assertion that fails when values match. A string
// cell that is not a number, compared to a numeric literal, is unevaluable.
type equalityTest struct {
	value       scalar
	failIfEqual bool
}

func (t equalityTest) test(v scalar) verdict {
	o := compareToLiteral(v, t.value)
	if o == orderUnparsable {
		return verdictUnevaluable
	}
	return failIf((o == orderEqual) == t.failIfEqual)
}

func compileEquality(c *compiler, d *checkDef) ([]check, error) {
	value, err := literal(d.params["value"])
	if err != nil {
		return nil, err
	}
	if err := c.p.reserve(int64(len(d.names))*bytesPerOperand, "bound comparison literals"); err != nil {
		return nil, err
	}
	failIfEqual := d.kind == checkNotEquals
	return c.valueCheck(d, d.onNull(), func(col operand) (cellTest, error) {
		bound, err := bindFloatingLiteral(value, col.typ, false)
		if err != nil {
			return nil, err
		}
		return equalityTest{value: bound, failIfEqual: failIfEqual}, nil
	})
}

// membershipTest violates when the cell equals no allowed value. When no value
// matches and some numeric literal could not be compared because the string
// cell is not a number, the outcome is unevaluable.
type membershipTest struct {
	values []scalar
	set    *literalSet
}

func (t membershipTest) test(v scalar) verdict {
	if found, handled := t.set.lookup(v); handled {
		return failIf(!found)
	}
	found, unparsable := contains(v, t.values, compareToLiteral)
	switch {
	case found:
		return verdictPass
	case unparsable:
		return verdictUnevaluable
	}
	return verdictFail
}

// contains reports whether v equals a candidate under compare, and whether
// any comparison was unparsable.
func contains(v scalar, candidates []scalar, compare func(a, b scalar) ordering) (found, unparsable bool) {
	for _, candidate := range candidates {
		switch compare(v, candidate) {
		case orderEqual:
			return true, false
		case orderUnparsable:
			unparsable = true
		}
	}
	return false, unparsable
}

func compileMembership(c *compiler, d *checkDef) ([]check, error) {
	list, ok := d.params["values"].([]any)
	if !ok {
		return nil, fmt.Errorf("values must be a list")
	}
	var values []scalar
	for _, v := range list {
		if v == nil {
			continue
		}
		lit, err := literal(v)
		if err != nil {
			return nil, err
		}
		values = append(values, lit)
	}
	if err := c.p.reserve(int64(len(d.names))*bytesPerOperand, "bound comparison literals"); err != nil {
		return nil, err
	}
	return c.valueCheck(d, d.onNull(), func(col operand) (cellTest, error) {
		if !isFloatType(col.typ) {
			set, err := c.p.membershipSet(values, false)
			return membershipTest{values: values, set: set}, err
		}
		if err := c.p.reserve(int64(len(values))*bytesPerLiteral, "bound floating membership values"); err != nil {
			return nil, err
		}
		bound := make([]scalar, len(values))
		for i, v := range values {
			var err error
			if bound[i], err = bindFloatingLiteral(v, col.typ, false); err != nil {
				return nil, err
			}
		}
		set, err := c.p.membershipSet(bound, false)
		return membershipTest{values: bound, set: set}, err
	})
}

type typeTest struct{ target string }

func (t typeTest) test(v scalar) verdict { return failIf(!typeValid(v, t.target)) }

func compileTypeCheck(c *compiler, d *checkDef) ([]check, error) {
	target := typeName(str(d.params, "expected_type"))
	if !knownType(target) {
		return nil, fmt.Errorf("unknown expected_type %q", target)
	}
	onNull := failIf(target == "string" || target == "bool")
	return c.valueCheck(d, onNull, func(operand) (cellTest, error) { return typeTest{target}, nil })
}

type dateFormatTest struct{ layouts []string }

func (t dateFormatTest) test(v scalar) verdict {
	text := v.text()
	for _, layout := range t.layouts {
		if _, err := time.Parse(layout, text); err == nil {
			return verdictPass
		}
	}
	return verdictFail
}

func compileDateFormat(c *compiler, d *checkDef) ([]check, error) {
	layouts, err := dateLayoutsParam(d.params)
	if err != nil {
		return nil, err
	}
	if len(layouts) == 0 {
		return nil, fmt.Errorf("date formats required")
	}
	return c.valueCheck(d, d.onNull(), func(operand) (cellTest, error) { return dateFormatTest{layouts}, nil })
}

// windowTest checks inclusive instant bounds; text cells without an explicit
// offset use the configured timezone. Unparseable values violate.
type windowTest struct {
	min, max *scalar
	zone     string
}

func (t windowTest) test(v scalar) verdict {
	instant, ok := timestampInZone(v, t.zone)
	if !ok {
		return verdictFail
	}
	if t.min != nil && compareExact(instant, *t.min) == orderLess {
		return verdictFail
	}
	if t.max != nil && compareExact(instant, *t.max) == orderGreater {
		return verdictFail
	}
	return verdictPass
}

func compileTimestampWindow(c *compiler, d *checkDef) ([]check, error) {
	t := windowTest{zone: str(d.params, "timezone")}
	for _, bound := range []struct {
		key string
		dst **scalar
	}{{"min", &t.min}, {"max", &t.max}} {
		lit, err := optionalLiteral(d.params, bound.key)
		if err != nil {
			return nil, err
		}
		if lit == nil {
			continue
		}
		instant, ok := timestampInZone(*lit, t.zone)
		if !ok {
			return nil, fmt.Errorf("timestamp bounds require explicit offset/timezone and unambiguous instant")
		}
		*bound.dst = &instant
	}
	if t.min == nil && t.max == nil {
		return nil, fmt.Errorf("timestamp bound required")
	}
	return c.valueCheck(d, d.onNull(), func(operand) (cellTest, error) { return t, nil })
}

// --- compare_columns ---

type compareColumnsCheck struct {
	left, right               operand
	op                        operator
	numeric, temporal, exact  bool
	leftLayouts, rightLayouts []string
}

func compileCompareColumns(c *compiler, d *checkDef) ([]check, error) {
	if len(d.names) != 1 {
		return nil, fmt.Errorf("compare_columns requires one left operand")
	}
	left, err := c.bindOperand(d.names[0])
	if err != nil {
		return nil, err
	}
	right, err := c.bindOperand(str(d.params, "other_column"))
	if err != nil {
		return nil, err
	}
	op, ok := parseOperator(str(d.params, "operator"))
	if !ok {
		return nil, fmt.Errorf("invalid comparison operator")
	}
	cc := &compareColumnsCheck{left: left, right: right, op: op, exact: c.exact}
	cc.numeric = isNumericType(left.typ) || isNumericType(right.typ)
	cc.temporal = isTemporalType(left.typ) || isTemporalType(right.typ)
	if c.exact && !compatible(left.typ, right.typ) {
		return nil, fmt.Errorf("incompatible or lossy mixed comparison types %s and %s", left.typ, right.typ)
	}
	if !c.exact {
		// Schema-less inputs take their comparison domain from type_check,
		// date_format and schema_matches declarations on the operands.
		l, lk := c.contextType(left.name)
		r, rk := c.contextType(right.name)
		if !lk {
			l, lk = r, rk
		}
		if !rk {
			r, rk = l, lk
		}
		if lk && rk {
			cc.numeric = isNumericType(l) && isNumericType(r)
			cc.temporal = isTemporalType(l) && l == r
		}
	}
	cc.leftLayouts = c.dateFormats[left.name]
	cc.rightLayouts = c.dateFormats[right.name]
	return []check{cc}, nil
}

// eval fails when either operand is null/missing, cannot be read in the
// comparison domain, or the comparison is false.
func (cc *compareColumnsCheck) eval(_ *Program, b *data.Batch, row int) (verdict, error) {
	ls, rs := cc.left.state(row), cc.right.state(row)
	if ls == data.Invalid || rs == data.Invalid {
		return verdictUnevaluable, nil
	}
	if ls != data.Valid || rs != data.Valid {
		return verdictFail, nil
	}
	a, z := cc.left.value(row), cc.right.value(row)
	if cc.temporal {
		var ok, zok bool
		a, ok = parseTemporal(a, cc.leftLayouts)
		z, zok = parseTemporal(z, cc.rightLayouts)
		if !ok || !zok {
			return verdictFail, nil
		}
	}
	if cc.numeric && !cc.exact {
		af, ok := a.number()
		zf, zok := z.number()
		if !ok || !zok {
			return verdictFail, nil
		}
		a, z = floatScalar(af), floatScalar(zf)
	}
	var o ordering
	if cc.numeric {
		o = compareNumeric(a, z)
	} else {
		o = compareExact(a, z)
	}
	return failIf(!cc.op.accepts(o)), nil
}

// parseTemporal reads an instant using the column's date layouts, or, when it
// has none, ISO date then RFC 3339.
func parseTemporal(v scalar, layouts []string) (scalar, bool) {
	if v.kind == valTime {
		return v, true
	}
	if v.kind != valString {
		return scalar{}, false
	}
	for _, layout := range layouts {
		if t, e := time.Parse(layout, v.s); e == nil {
			return timeScalar(t), true
		}
	}
	if len(layouts) > 0 {
		return scalar{}, false
	}
	if d, ok := isoDateScalar(v.s); ok {
		return d, true
	}
	return timestamp(v)
}

// --- uniqueness (global scope) ---

// uniqueCheck fails every occurrence of a duplicated key, including the
// first, and any row with a null/missing component. A NaN component has no
// identity and makes the row unevaluable.
type uniqueCheck struct {
	columns []operand
	index   *keyIndex
}

func compileUnique(c *compiler, d *checkDef) ([]check, error) {
	cols, err := c.bindAll(d.names)
	if err != nil {
		return nil, err
	}
	// The multiple-column is_unique wrapper is OR of per-column constraints,
	// not a composite key.
	if d.kind == checkIsUnique && len(cols) > 1 {
		out := make([]check, len(cols))
		for i, col := range cols {
			out[i] = &uniqueCheck{columns: []operand{col}}
		}
		return out, nil
	}
	return []check{&uniqueCheck{columns: cols}}, nil
}

func (u *uniqueCheck) indexName() string {
	names := make([]string, len(u.columns))
	for i, col := range u.columns {
		names[i] = col.name
	}
	return indexName("unique", names...)
}

func (u *uniqueCheck) eval(p *Program, b *data.Batch, row int) (verdict, error) {
	if anyInvalid(b, u.columns, row) {
		return verdictUnevaluable, nil
	}
	key, status, err := p.valueKey(b, u.columns, row)
	if err != nil {
		return verdictPass, err
	}
	switch status {
	case keyAbsent:
		return verdictFail, nil
	case keyNaN:
		return verdictUnevaluable, nil
	}
	if u.index == nil {
		return verdictPass, fmt.Errorf("global index not prepared")
	}
	return failIf(u.index.count(key) > 1), nil
}

func anyInvalid(b *data.Batch, columns []operand, row int) bool {
	for _, col := range columns {
		if col.state(row) == data.Invalid {
			return true
		}
	}
	return false
}

// --- reference membership ---

// referenceSource is a projected reference input loaded into a shared index.
type referenceSource struct {
	uri     string
	columns []string
	index   *keyIndex
}

func (r *referenceSource) indexName() string {
	return indexName("reference", append([]string{r.uri}, r.columns...)...)
}

// referenceCheck fails rows whose ordered canonical-text tuple is absent from
// the reference. Null and empty text stay distinct; missing fields fail.
type referenceCheck struct {
	columns []operand
	ref     referenceSource
}

func compileReference(c *compiler, d *checkDef) ([]check, error) {
	ref := referenceSource{uri: str(d.params, "reference_file")}
	if ref.uri == "" {
		return nil, fmt.Errorf("reference_file required")
	}
	if d.kind == checkReferentialIntegrity {
		ref.columns = []string{str(d.params, "reference_column")}
	} else if v, ok := d.params["reference_columns"]; ok {
		var err error
		if ref.columns, err = stringsArg(v); err != nil {
			return nil, err
		}
	} else {
		ref.columns = d.names
	}
	if len(ref.columns) != len(d.names) {
		return nil, fmt.Errorf("reference operand counts differ")
	}
	cols, err := c.bindAll(d.names)
	if err != nil {
		return nil, err
	}
	return []check{&referenceCheck{columns: cols, ref: ref}}, nil
}

func (r *referenceCheck) eval(p *Program, b *data.Batch, row int) (verdict, error) {
	if anyInvalid(b, r.columns, row) {
		return verdictUnevaluable, nil
	}
	key, status, err := p.referenceKey(b, r.columns, row)
	if err != nil {
		return verdictPass, err
	}
	if status != keyBuilt {
		return verdictFail, nil
	}
	if r.ref.index == nil {
		return verdictPass, fmt.Errorf("global index not prepared")
	}
	return failIf(r.ref.index.count(key) == 0), nil
}

// --- schema scope ---

// schemaCheck is a schema gate (required_columns, schema_matches) with one
// dataset-wide outcome. Strict metadata mismatches are known at compile time;
// castability is resolved by the prepass.
type schemaCheck struct {
	rule        *compiledRule
	outcome     bool
	diagnostics []data.Issue
	castable    bool
	columns     []operand
	types       []string
	failures    []uint64
}

func (s *schemaCheck) reject(column, reason string) {
	s.outcome = true
	s.diagnostics = append(s.diagnostics, s.rule.schemaIssue(column, reason))
}

func (s *schemaCheck) eval(*Program, *data.Batch, int) (verdict, error) {
	return failIf(s.outcome), nil
}

func compileRequiredColumns(c *compiler, d *checkDef) ([]check, error) {
	s := &schemaCheck{rule: d.rule}
	for _, name := range d.names {
		op, err := c.bindOperand(name)
		if err != nil {
			s.reject(name, reasonMissingColumn)
			continue
		}
		s.columns = append(s.columns, op)
	}
	return []check{s}, nil
}

func compileSchemaMatches(c *compiler, d *checkDef) ([]check, error) {
	m := d.params
	types := make(map[string]string)
	switch v := m["columns"].(type) {
	case map[string]any:
		for n, t := range v {
			s, ok := t.(string)
			if !ok {
				return nil, fmt.Errorf("schema types must be strings")
			}
			types[n] = typeName(s)
		}
	case map[string]string:
		for n, t := range v {
			types[n] = typeName(t)
		}
	default:
		return nil, fmt.Errorf("schema columns must be a type mapping")
	}
	names := make([]string, 0, len(types))
	for n := range types {
		names = append(names, n)
	}
	sort.Strings(names)
	if err := c.p.reserve(int64(len(names)+len(c.schema.Fields()))*bytesPerSchemaOperand, "schema check operands"); err != nil {
		return nil, err
	}
	mode := str(m, "mode")
	if mode == "" {
		mode = "castable"
	}
	if mode != "castable" && mode != "strict" {
		return nil, fmt.Errorf("invalid schema mode")
	}
	s := &schemaCheck{rule: d.rule, castable: mode == "castable"}
	for _, name := range names {
		typ := types[name]
		if !knownType(typ) {
			return nil, fmt.Errorf("unknown schema type %q", typ)
		}
		op, err := c.bindOperand(name)
		if err != nil {
			if boolArg(m, "require_columns", true) {
				s.reject(name, reasonMissingColumn)
			}
			continue
		}
		s.columns = append(s.columns, op)
		s.types = append(s.types, typ)
		if !s.castable && op.typ != typ {
			s.reject(name, reasonTypeMismatch)
		}
	}
	if !boolArg(m, "allow_extra_columns", true) {
		for _, field := range c.schema.Fields() {
			if _, ok := types[field.Name]; !ok {
				s.reject(field.Name, reasonUnexpectedColumn)
			}
		}
	}
	return []check{s}, nil
}

// schemaNamesCheck (valid_schema_columns) compares user column names with a
// reference list. Its dataset-wide outcome applies to every row; it is not a
// gate and produces no schema-scope issue.
type schemaNamesCheck struct {
	ref     referenceSource
	outcome bool
}

func compileSchemaNames(_ *compiler, d *checkDef) ([]check, error) {
	ref := referenceSource{uri: str(d.params, "reference_file"), columns: []string{str(d.params, "reference_column")}}
	if ref.uri == "" || ref.columns[0] == "" {
		return nil, fmt.Errorf("reference_file and reference_column are required")
	}
	return []check{&schemaNamesCheck{ref: ref}}, nil
}

func (s *schemaNamesCheck) eval(*Program, *data.Batch, int) (verdict, error) {
	return failIf(s.outcome), nil
}
