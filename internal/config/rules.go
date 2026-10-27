package config

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
	"time"
)

// Check scopes, as reported by the plan and used to decide which checks may be
// conditional.
const (
	// CheckScopeRow checks are evaluated independently per row.
	CheckScopeRow = "row"
	// CheckScopeSchema checks have one dataset-wide metadata outcome.
	CheckScopeSchema = "schema"
	// CheckScopeGlobal checks need a whole-dataset prepass (exact key sets).
	CheckScopeGlobal = "global"
	// CheckScopeReference checks probe an index loaded from a reference input.
	CheckScopeReference = "reference"
)

// CheckOperands describes which column operands a check accepts.
type CheckOperands uint8

const (
	// OperandsColumn accepts `column`, `columns`, or the rule's column.
	OperandsColumn CheckOperands = iota
	// OperandsColumns requires an explicit `columns` list.
	OperandsColumns
	// OperandsNone binds no row operands (schema metadata checks).
	OperandsNone
)

// CheckSpec is the single vocabulary entry for one built-in check: its accepted
// parameters (an allowlist, so misspelled arguments never silently change a
// rule's meaning), whether an error-level use is a schema gate, its scope and
// its operand shape. Configuration validation and check compilation both read
// this table.
type CheckSpec struct {
	Name     string
	Params   []string
	Gate     bool
	Scope    string
	Operands CheckOperands
	// Shorthand is the argument a non-mapping value sets in the map form of
	// checks, as in `valid_values: [A, B]`; empty when the check has none.
	Shorthand string
}

// AllowsWhen reports whether a rule using this check may carry a `when`
// condition. Conditional global/schema checks would change which rows enter
// dataset-wide state and are rejected.
func (s CheckSpec) AllowsWhen() bool {
	return s.Scope == CheckScopeRow || s.Scope == CheckScopeReference
}

func (s CheckSpec) allows(param string) bool {
	for _, p := range s.Params {
		if p == param {
			return true
		}
	}
	return false
}

// Checks is the built-in check vocabulary keyed by check type name.
var Checks = func() map[string]CheckSpec {
	columnOrColumns := []string{"column", "columns"}
	with := func(extra ...string) []string { return append(append([]string(nil), columnOrColumns...), extra...) }
	specs := []CheckSpec{
		{Name: "not_null", Params: with(), Scope: CheckScopeRow},
		{Name: "not_blank", Params: with(), Scope: CheckScopeRow},
		{Name: "is_present", Params: with(), Scope: CheckScopeRow},
		{Name: "valid_length", Params: with("length"), Scope: CheckScopeRow, Shorthand: "length"},
		{Name: "min_length", Params: with("min_length"), Scope: CheckScopeRow, Shorthand: "min_length"},
		{Name: "max_length", Params: with("max_length"), Scope: CheckScopeRow, Shorthand: "max_length"},
		{Name: "valid_range", Params: with("min", "max", "exclusive_min", "exclusive_max"), Scope: CheckScopeRow},
		{Name: "equals", Params: with("value"), Scope: CheckScopeRow, Shorthand: "value"},
		{Name: "not_equals", Params: with("value"), Scope: CheckScopeRow, Shorthand: "value"},
		{Name: "all_equal", Params: []string{"columns", "value"}, Scope: CheckScopeRow, Operands: OperandsColumns},
		{Name: "has_null_keys", Params: []string{"columns"}, Scope: CheckScopeRow, Operands: OperandsColumns, Shorthand: "columns"},
		{Name: "valid_values", Params: with("values"), Scope: CheckScopeRow, Shorthand: "values"},
		{Name: "matches_pattern", Params: with("pattern"), Scope: CheckScopeRow, Shorthand: "pattern"},
		{Name: "is_unique", Params: with(), Scope: CheckScopeGlobal},
		{Name: "is_unique_composite", Params: []string{"columns"}, Scope: CheckScopeGlobal, Operands: OperandsColumns, Shorthand: "columns"},
		{Name: "compare_columns", Params: []string{"column", "other_column", "operator"}, Scope: CheckScopeRow},
		{Name: "referential_integrity", Params: []string{"column", "reference_file", "reference_column"}, Scope: CheckScopeReference},
		{Name: "referential_integrity_composite", Params: []string{"columns", "reference_file", "reference_columns"}, Scope: CheckScopeReference, Operands: OperandsColumns},
		{Name: "required_columns", Params: []string{"columns"}, Gate: true, Scope: CheckScopeSchema, Operands: OperandsColumns, Shorthand: "columns"},
		{Name: "schema_matches", Params: []string{"columns", "mode", "allow_extra_columns", "require_columns"}, Gate: true, Scope: CheckScopeSchema, Operands: OperandsNone},
		{Name: "valid_schema_columns", Params: []string{"reference_file", "reference_column"}, Scope: CheckScopeSchema, Operands: OperandsNone},
		{Name: "type_check", Params: with("expected_type"), Scope: CheckScopeRow, Shorthand: "expected_type"},
		{Name: "date_format", Params: with("format", "formats"), Scope: CheckScopeRow, Shorthand: "format"},
		{Name: "timestamp_window", Params: with("min", "max", "timezone"), Scope: CheckScopeRow},
	}
	out := make(map[string]CheckSpec, len(specs))
	for _, s := range specs {
		out[s.Name] = s
	}
	return out
}()

func validateCheck(r Rule, c Check) error {
	spec, ok := Checks[c.Type]
	if !ok {
		return fmt.Errorf("unknown check %q", c.Type)
	}
	p := c.Params
	for key := range p {
		if !spec.allows(key) {
			return fmt.Errorf("%s: unknown argument %q", c.Type, key)
		}
	}
	if _, ok := p["column"]; ok {
		if _, err := stringArg(p, "column", true); err != nil {
			return err
		}
	}
	if _, a := p["column"]; a {
		if _, b := p["columns"]; b {
			return fmt.Errorf("%s: column and columns are mutually exclusive", c.Type)
		}
	}
	if c.Type == "schema_matches" {
		cols, ok := p["columns"].(map[string]any)
		if !ok || len(cols) == 0 {
			return fmt.Errorf("schema_matches.columns must be a nonempty mapping of column names to types")
		}
		for name, v := range cols {
			s, ok := v.(string)
			if name == "" || !ok {
				return fmt.Errorf("schema_matches column %q must map to a type name", name)
			}
			if !supportedType(s) {
				return unsupportedType(fmt.Sprintf("schema_matches column %q type", name), s)
			}
		}
	} else if _, ok := p["columns"]; ok {
		if _, err := stringList(p["columns"], "columns", true); err != nil {
			return err
		}
	}
	switch spec.Operands {
	case OperandsColumns:
		if _, ok := p["columns"]; !ok {
			return fmt.Errorf("%s requires columns", c.Type)
		}
	case OperandsColumn:
		_, col := p["column"]
		_, cols := p["columns"]
		if !col && !cols && r.Column == "" {
			return fmt.Errorf("%s requires column or columns", c.Type)
		}
	}
	switch c.Type {
	case "valid_length", "min_length", "max_length":
		key := c.Type
		if key == "valid_length" {
			key = "length"
		}
		n, ok := nonnegativeInteger(p[key])
		if !ok || n > math.MaxInt64 {
			return fmt.Errorf("%s must be a nonnegative integer", key)
		}
	case "valid_range":
		if err := numericBounds(p["min"], p["max"], nil, false); err != nil {
			return fmt.Errorf("valid_range: %w", err)
		}
		for _, key := range []string{"exclusive_min", "exclusive_max"} {
			if value, ok := p[key]; ok {
				if _, ok = value.(bool); !ok {
					return fmt.Errorf("valid_range.%s must be boolean", key)
				}
			}
		}
		if exclusive, _ := p["exclusive_min"].(bool); exclusive && p["min"] == nil {
			return fmt.Errorf("valid_range.exclusive_min requires min")
		}
		if exclusive, _ := p["exclusive_max"].(bool); exclusive && p["max"] == nil {
			return fmt.Errorf("valid_range.exclusive_max requires max")
		}
	case "equals", "not_equals", "all_equal":
		v, ok := p["value"]
		if !ok || v == nil || !scalar(v) {
			return fmt.Errorf("%s requires a non-null scalar value", c.Type)
		}
	case "valid_values":
		values, ok := p["values"].([]any)
		if !ok {
			return fmt.Errorf("valid_values.values must be a list")
		}
		for _, v := range values {
			if !scalar(v) {
				return fmt.Errorf("valid_values.values must contain scalars")
			}
		}
	case "matches_pattern":
		s, err := stringArg(p, "pattern", false)
		if err != nil {
			return err
		}
		if _, err = regexp.Compile(s); err != nil {
			return fmt.Errorf("matches_pattern: unsupported regular expression: %w", err)
		}
	case "compare_columns":
		if _, err := stringArg(p, "other_column", true); err != nil {
			return err
		}
		op, err := stringArg(p, "operator", true)
		if err != nil {
			return err
		}
		if !oneOf(op, "==", "!=", "<", "<=", ">", ">=") {
			return fmt.Errorf("compare_columns.operator must be ==, !=, <, <=, >, or >=")
		}
	case "referential_integrity", "referential_integrity_composite", "valid_schema_columns":
		file, err := stringArg(p, "reference_file", true)
		if err != nil {
			return err
		}
		if err := validateURI(file, "reference_file"); err != nil {
			return err
		}
		if c.Type != "referential_integrity_composite" {
			if _, err := stringArg(p, "reference_column", true); err != nil {
				return err
			}
		} else if v, ok := p["reference_columns"]; ok {
			ref, err := stringList(v, "reference_columns", true)
			if err != nil {
				return err
			}
			cols, _ := stringList(p["columns"], "columns", true)
			if len(ref) != len(cols) {
				return fmt.Errorf("reference_columns and columns must have the same length")
			}
		}
	case "schema_matches":
		if mode, exists := p["mode"]; exists {
			s, ok := mode.(string)
			if !ok || !oneOf(s, "strict", "castable") {
				return fmt.Errorf("schema_matches.mode must be strict or castable")
			}
		}
		for _, k := range []string{"allow_extra_columns", "require_columns"} {
			if v, ok := p[k]; ok {
				if _, ok := v.(bool); !ok {
					return fmt.Errorf("schema_matches.%s must be boolean", k)
				}
			}
		}
	case "type_check":
		s, err := stringArg(p, "expected_type", true)
		if err != nil {
			return err
		}
		if !supportedType(s) {
			return unsupportedType("expected_type", s)
		}
	case "date_format":
		var formats []string
		if _, ok := p["format"]; ok {
			s, err := stringArg(p, "format", true)
			if err != nil {
				return err
			}
			formats = append(formats, s)
		}
		if v, ok := p["formats"]; ok {
			list, err := stringList(v, "formats", true)
			if err != nil {
				return err
			}
			formats = append(formats, list...)
		}
		if len(formats) == 0 {
			return fmt.Errorf("date_format requires format or formats")
		}
		for _, f := range formats {
			if err := validateDateFormat(f); err != nil {
				return err
			}
		}
	case "timestamp_window":
		tz := ""
		if _, ok := p["timezone"]; ok {
			var err error
			tz, err = stringArg(p, "timezone", true)
			if err != nil {
				return err
			}
			if _, err := time.LoadLocation(tz); err != nil {
				return fmt.Errorf("timestamp_window timezone: %w", err)
			}
		}
		if tz == "Local" {
			return fmt.Errorf("timestamp_window timezone Local is machine-dependent; use UTC or an IANA timezone")
		}
		var bounds [2]*time.Time
		for i, key := range []string{"min", "max"} {
			if _, ok := p[key]; ok {
				s, err := stringArg(p, key, true)
				if err != nil {
					return err
				}
				stamp, err := parseTimestampBound(s, tz)
				if err != nil {
					return err
				}
				bounds[i] = &stamp
			}
		}
		if bounds[0] == nil && bounds[1] == nil {
			return fmt.Errorf("timestamp_window requires min or max")
		}
		if bounds[0] != nil && bounds[1] != nil && bounds[0].After(*bounds[1]) {
			return fmt.Errorf("timestamp_window min must not exceed max")
		}
	}
	return nil
}

func stringArg(p map[string]any, key string, nonempty bool) (string, error) {
	s, ok := p[key].(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", key)
	}
	if nonempty && s == "" {
		return "", fmt.Errorf("%s must be a nonempty string", key)
	}
	return s, nil
}
func stringList(v any, label string, nonempty bool) ([]string, error) {
	var result []string
	switch list := v.(type) {
	case []string:
		result = list
	case []any:
		result = make([]string, len(list))
		for i, v := range list {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a list of strings", label)
			}
			result[i] = s
		}
	default:
		return nil, fmt.Errorf("%s must be a list of strings", label)
	}
	if nonempty && len(result) == 0 {
		return nil, fmt.Errorf("%s must not be empty", label)
	}
	if err := uniqueNames(result, label); err != nil {
		return nil, err
	}
	return result, nil
}
func nonnegativeInteger(v any) (uint64, bool) {
	switch n := v.(type) {
	case int:
		return uint64(n), n >= 0
	case int64:
		return uint64(n), n >= 0
	case uint64:
		return n, true
	case uint:
		return uint64(n), true
	}
	return 0, false
}
func scalar(v any) bool {
	switch n := v.(type) {
	case nil, string, bool, int, int64, uint64:
		return true
	case float64:
		return !math.IsNaN(n) && !math.IsInf(n, 0)
	case json.Number:
		_, ok := decimalNumber(n)
		return ok
	}
	return false
}
func validateDateFormat(s string) error {
	_, err := DateLayout(s)
	return err
}
func parseTimestampBound(s, timezone string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if timezone != "" {
		loc, err := time.LoadLocation(timezone)
		if err != nil {
			return time.Time{}, err
		}
		for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999"} {
			if t, err := time.ParseInLocation(layout, s, loc); err == nil {
				// Parsing normalizes nonexistent local times; reject those and ambiguous
				// folds instead of choosing a daylight-saving offset silently.
				lexical, err := time.Parse(layout, s)
				if err != nil {
					continue
				}
				_, off := t.Zone()
				if t.Unix()+int64(off) != lexical.Unix() {
					return time.Time{}, fmt.Errorf("timestamp bound falls in a nonexistent local time")
				}
				for _, delta := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
					_, other := t.Add(delta).Zone()
					candidate := t.Add(time.Duration(off-other) * time.Second)
					if other != off {
						_, actual := candidate.Zone()
						if actual == other {
							return time.Time{}, fmt.Errorf("timestamp bound is ambiguous; specify an offset")
						}
					}
				}
				return t, nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("timestamp bound must use RFC3339 with an offset or a configured timezone")
}

func validateCondition(c *Condition, depth int, nodes *int) error {
	*nodes++
	if depth > 16 || *nodes > 1024 {
		return fmt.Errorf("condition exceeds depth 16/node 1024 limit")
	}
	forms := 0
	if c.Column != "" || c.Operator != "" || c.Value != nil {
		forms++
	}
	if c.All != nil {
		forms++
	}
	if c.Any != nil {
		forms++
	}
	if c.Not != nil {
		forms++
	}
	if forms != 1 {
		return fmt.Errorf("condition requires exactly one leaf, all, any, or not")
	}
	if c.All != nil || c.Any != nil {
		children := c.All
		if c.Any != nil {
			children = c.Any
		}
		if len(children) == 0 {
			return fmt.Errorf("all/any requires at least one condition")
		}
		for i := range children {
			if err := validateCondition(&children[i], depth+1, nodes); err != nil {
				return err
			}
		}
		return nil
	}
	if c.Not != nil {
		return validateCondition(c.Not, depth+1, nodes)
	}
	if c.Column == "" {
		return fmt.Errorf("condition leaf requires column")
	}
	if !oneOf(c.Operator, "eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "is_present") {
		return fmt.Errorf("unsupported condition operator %q", c.Operator)
	}
	if oneOf(c.Operator, "is_null", "is_present") {
		if c.Value != nil {
			return fmt.Errorf("unary condition must omit value")
		}
		return nil
	}
	if c.Operator == "in" {
		values, ok := c.Value.([]any)
		if !ok || len(values) == 0 {
			return fmt.Errorf("in condition requires a nonempty value list")
		}
		for _, v := range values {
			if v == nil || !scalar(v) {
				return fmt.Errorf("in condition values must be non-null scalars")
			}
		}
	} else if c.Value == nil || !scalar(c.Value) {
		return fmt.Errorf("condition requires a non-null scalar value")
	}
	return nil
}

func numericBounds(min, max, equals any, required bool) error {
	if equals != nil && (min != nil || max != nil) {
		return fmt.Errorf("equals is mutually exclusive with min/max")
	}
	if required && min == nil && max == nil && equals == nil {
		return fmt.Errorf("at least one min/max bound or equals is required")
	}
	var lo, hi *big.Rat
	for i, v := range []any{min, max, equals} {
		if v == nil {
			continue
		}
		n, ok := decimalNumber(v)
		if !ok {
			return fmt.Errorf("bounds must be finite numeric values or decimal strings")
		}
		if i == 0 {
			lo = n
		}
		if i == 1 {
			hi = n
		}
	}
	if lo != nil && hi != nil && lo.Cmp(hi) > 0 {
		return fmt.Errorf("min must not exceed max")
	}
	return nil
}

func validateDatasetRule(r DatasetRule, rowCodes map[string]bool, schema *Schema) error {
	fieldMetric := oneOf(r.Metric, "missing_count", "null_count", "blank_count", "conversion_error_count", "missing_fraction", "null_fraction", "blank_fraction", "conversion_error_fraction")
	ruleMetric := oneOf(r.Metric, "rule_failure_count", "rule_failure_fraction")
	aggregate := oneOf(r.Metric, "sum", "min", "max", "avg")
	if r.Metric != "row_count" && !fieldMetric && !ruleMetric && !aggregate {
		return fmt.Errorf("unknown metric %q", r.Metric)
	}
	if fieldMetric || aggregate {
		if r.Column == "" || r.RuleCode != "" {
			return fmt.Errorf("metric %s requires column and forbids rule_code", r.Metric)
		}
	} else if ruleMetric {
		if !rowCodes[r.RuleCode] || r.Column != "" {
			return fmt.Errorf("rule failure metric requires a known row rule_code and forbids column")
		}
	} else if r.Column != "" || r.RuleCode != "" {
		return fmt.Errorf("row_count forbids column and rule_code")
	}
	if err := numericBounds(r.Min, r.Max, r.Equals, true); err != nil {
		return err
	}
	if !aggregate {
		fraction := strings.HasSuffix(r.Metric, "_fraction")
		for _, v := range []any{r.Min, r.Max, r.Equals} {
			if v == nil {
				continue
			}
			n, _ := decimalNumber(v)
			if n.Sign() < 0 || (fraction && n.Cmp(big.NewRat(1, 1)) > 0) || (!fraction && !n.IsInt()) {
				return fmt.Errorf("count bounds must be nonnegative integers; fraction bounds must be between 0 and 1")
			}
		}
	}
	if r.Tolerance != nil {
		if r.Equals == nil || *r.Tolerance < 0 || math.IsNaN(*r.Tolerance) || math.IsInf(*r.Tolerance, 0) {
			return fmt.Errorf("tolerance requires equals and must be finite/nonnegative")
		}
		if !aggregate {
			return fmt.Errorf("tolerance is supported only for floating aggregate equality")
		}
	}
	if aggregate {
		if schema == nil {
			return fmt.Errorf("numeric dataset aggregates require an explicit input schema")
		}
		col, ok := schema.Columns[r.Column]
		if !ok || !numericType(col.Type) {
			return fmt.Errorf("aggregate column must be a declared numeric column")
		}
		canonical, _ := CanonicalType(col.Type)
		floating := canonical == "float32" || canonical == "float64"
		if r.Tolerance != nil && !floating {
			return fmt.Errorf("tolerance is supported only for floating aggregate equality")
		}
		if floating && r.Equals != nil && r.Tolerance == nil {
			return fmt.Errorf("float equals requires an explicit absolute tolerance")
		}
	}
	return nil
}
