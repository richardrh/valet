package checks

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/richardrh/valet/internal/data"
)

// PlanStatistics makes whole-group decisions from the already compiled checks.
// The plan never changes the predicates used when any group must be decoded.
func (p *Program) PlanStatistics(groups []data.ParquetGroup) (*data.StatisticsPlan, error) {
	if p.closed || p.finalized {
		return nil, fmt.Errorf("program is closed or finalized")
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	force := p.statisticsObligation()
	var proofCharge int64
	var total uint64
	var charge int64 = 256
	addCharge := func(n int64) error {
		if n < 0 || charge > math.MaxInt64-n {
			return fmt.Errorf("statistics plan size overflow")
		}
		charge += n
		return nil
	}
	if force == "" {
		before := charge
		for _, r := range p.rules {
			for _, c := range r.checks {
				if err := addCharge(768 + int64(len(r.code)) + int64(len(c.kind.String()))); err != nil {
					return nil, err
				}
				for _, col := range proofOperands(c) {
					if err := addCharge(256 + int64(len(col.name))); err != nil {
						return nil, err
					}
				}
			}
		}
		if p.cfg.Input.Schema != nil {
			for name, col := range p.cfg.Input.Schema.Columns {
				if col.Nullable != nil && !*col.Nullable {
					if err := addCharge(768 + int64(len(name))); err != nil {
						return nil, err
					}
				}
			}
		}
		proofCharge, charge = charge-before, before
	}
	for _, g := range groups {
		var err error
		total, err = data.CheckedAdd(total, g.DeclaredRows)
		if err != nil {
			return nil, err
		}
		if err = addCharge(512 + int64(len(g.Source)+len(g.FooterSHA256)+len(g.IdentityMethod))); err != nil {
			return nil, err
		}
		for k, v := range g.Identity {
			if err = addCharge(64 + int64(len(k)+len(v))); err != nil {
				return nil, err
			}
		}
		if g.DeclaredRows > 0 {
			if err = addCharge(proofCharge); err != nil {
				return nil, err
			}
		}
	}
	if err := p.budget.Reserve(charge, "statistics proof plan"); err != nil {
		return nil, err
	}
	budget := p.budget
	plan := data.NewStatisticsPlan(make([]data.GroupEvidence, len(groups)), func() { budget.Release(charge) })
	complete := false
	defer func() {
		if !complete {
			plan.Release()
		}
	}()
	for i, g := range groups {
		if err := p.ctx.Err(); err != nil {
			return nil, err
		}
		e := data.GroupEvidence{SourceOrdinal: g.SourceOrdinal, RowGroup: g.RowGroup, DeclaredRows: g.DeclaredRows, Source: g.Source, FooterSHA256: g.FooterSHA256, FooterOffset: g.FooterOffset, FooterLength: g.FooterLength, IdentityMethod: g.IdentityMethod, Identity: make(map[string]string, len(g.Identity)), Action: "skip", Reason: "complete_pass_proof", Checks: []data.CheckEvidence{}, IngestionVerification: "not_verified", PayloadVerification: "not_read"}
		for k, v := range g.Identity {
			e.Identity[k] = v
		}
		scan := func(reason string) {
			if e.Action != "scan" {
				e.Reason = reason
			}
			e.Action = "scan"
			e.IngestionVerification = "values"
			e.PayloadVerification = "decoded"
		}
		if force != "" {
			scan(force)
		}
		if g.DeclaredRows == 0 {
			scan("empty_group")
		}
		if e.Action == "scan" {
			plan.Groups[i] = e
			continue
		}
		// A conversion anywhere in the group is an ingestion obligation, including
		// columns that are not rule operands or omitted from row output.
		if p.cfg.Input.Schema != nil {
			for _, field := range p.schema.Fields() {
				s := groupColumn(g, field.Name)
				if s == nil || s.Type == nil || !arrow.TypeEqual(s.Type, field.Type) {
					scan("input_conversion")
				}
			}
		}
		if e.Action == "scan" {
			plan.Groups[i] = e
			continue
		}
		for _, r := range p.rules {
			for _, c := range r.checks {
				ev := data.CheckEvidence{Code: r.code, CheckIndex: c.index, Columns: proofColumns(c), Claim: "unknown", Fields: []string{}, Reason: "unsupported_check"}
				if r.when != nil {
					ev.Reason = "condition_requires_values"
				} else {
					ev.Claim, ev.Fields, ev.Reason = proveCheck(c, g)
				}
				e.Checks = append(e.Checks, ev)
				if ev.Claim != "pass" {
					scan(ev.Reason)
				}
			}
		}
		if p.cfg.Input.Schema != nil {
			for _, field := range p.schema.Fields() {
				name := field.Name
				col := p.cfg.Input.Schema.Columns[name]
				if col.Nullable == nil || *col.Nullable {
					continue
				}
				claim, fields, reason := proveNotNull(groupColumn(g, name), g.DeclaredRows)
				e.Checks = append(e.Checks, data.CheckEvidence{Code: "INGEST_NULL", CheckIndex: -1, Columns: []string{name}, Claim: claim, Fields: fields, Reason: reason, ImplicitConstraintKind: "nullable_false"})
				if claim != "pass" {
					scan(reason)
				}
			}
		}
		plan.Groups[i] = e
	}
	complete = true
	return plan, nil
}

func (p *Program) statisticsObligation() string {
	switch {
	case p.rejected():
		return "schema_rejected"
	case p.cfg.Input.Parquet.Statistics != "trusted":
		return "statistics_off"
	case p.summary.OutputMode == "all":
		return "pass_materialization_required"
	case p.cfg.Input.Schema == nil:
		return "explicit_schema_required"
	case len(p.cfg.Stats) > 0 || len(p.cfg.DatasetRules) > 0 || p.needsPrepass():
		return "global_values_required"
	}
	for _, col := range p.cfg.Input.Schema.Columns {
		if col.Trim || len(col.Formats) > 0 || len(col.TrueValues) > 0 || len(col.FalseValues) > 0 || col.Path != "" {
			return "input_conversion"
		}
	}
	for _, r := range p.rules {
		if r.when != nil {
			return "condition_requires_values"
		}
		for _, c := range r.checks {
			switch v := c.check.(type) {
			case *schemaCheck:
				if v.castable {
					return "castability_requires_values"
				}
				if v.outcome {
					return "schema_mismatch"
				}
			case *stateCheck:
				if c.kind != checkNotNull {
					return "unsupported_check"
				}
			case *valueCheck:
				if c.kind != checkValidRange {
					return "unsupported_check"
				}
				for _, test := range v.tests {
					if _, ok := test.(exactRangeTest); !ok {
						return "legacy_numeric_coercion"
					}
				}
			default:
				return "unsupported_check"
			}
		}
	}
	return ""
}
func groupColumn(g data.ParquetGroup, name string) *data.ColumnStatistics {
	var out *data.ColumnStatistics
	for i := range g.Columns {
		if g.Columns[i].Name == name {
			if out != nil {
				return nil
			}
			out = &g.Columns[i]
		}
	}
	return out
}
func proofOperands(c *compiledCheck) []operand {
	switch v := c.check.(type) {
	case *stateCheck:
		return v.columns
	case *valueCheck:
		return v.columns
	case *schemaCheck:
		return v.columns
	}
	return nil
}
func proofColumns(c *compiledCheck) []string {
	cols := proofOperands(c)
	out := make([]string, len(cols))
	for i, col := range cols {
		out[i] = col.name
	}
	return out
}
func validCounts(s *data.ColumnStatistics, rows uint64) bool {
	if s == nil {
		return false
	}
	for _, n := range []*int64{s.NullCount, s.NaNCount} {
		if n != nil && (*n < 0 || uint64(*n) > rows) {
			return false
		}
	}
	return s.NullCount == nil || s.NaNCount == nil || uint64(*s.NaNCount) <= rows-uint64(*s.NullCount)
}
func proveNotNull(s *data.ColumnStatistics, rows uint64) (string, []string, string) {
	fields := []string{}
	if s == nil || !validCounts(s, rows) {
		return "unknown", fields, "invalid_optional_statistics"
	}
	if s.NullCount == nil {
		return "unknown", fields, "missing_null_count"
	}
	fields = append(fields, "null_count")
	if *s.NullCount != 0 {
		return "fail", fields, "nulls_present"
	}
	return "pass", fields, "zero_null_count"
}
func proveCheck(c *compiledCheck, g data.ParquetGroup) (string, []string, string) {
	switch v := c.check.(type) {
	case *schemaCheck:
		if v.castable {
			return "unknown", []string{}, "castability_requires_values"
		}
		if v.outcome {
			return "fail", []string{"schema"}, "schema_mismatch"
		}
		return "pass", []string{"schema"}, "resolved_schema_gate"
	case *stateCheck:
		if c.kind != checkNotNull {
			break
		}
		fields := []string{}
		claim, reason := "pass", "zero_null_count"
		for _, col := range v.columns {
			cc, ff, rr := proveNotNull(groupColumn(g, col.name), g.DeclaredRows)
			fields = append(fields, ff...)
			if cc != "pass" {
				claim, reason = cc, rr
			}
		}
		return claim, fields, reason
	case *valueCheck:
		if c.kind != checkValidRange {
			break
		}
		fields := []string{}
		claim, reason := "pass", "safe_numeric_bounds"
		for i, col := range v.columns {
			test, ok := v.tests[i].(exactRangeTest)
			if !ok {
				claim, reason = "unknown", "legacy_numeric_coercion"
				continue
			}
			cc, ff, rr := proveRange(groupColumn(g, col.name), g.DeclaredRows, test, v.onNull)
			fields = append(fields, ff...)
			if cc != "pass" {
				claim, reason = cc, rr
			}
		}
		return claim, fields, reason
	}
	return "unknown", []string{}, "unsupported_check"
}
func proveRange(s *data.ColumnStatistics, rows uint64, t exactRangeTest, onNull verdict) (string, []string, string) {
	fields := []string{}
	if s == nil || !validCounts(s, rows) {
		return "unknown", fields, "invalid_optional_statistics"
	}
	if s.NullCount == nil {
		return "unknown", fields, "missing_null_count"
	}
	fields = append(fields, "null_count")
	if s.Type == nil || !isNumericType(physicalType(s.Type)) {
		return "unknown", fields, "unsupported_numeric_type"
	}
	if uint64(*s.NullCount) == rows && rows > 0 && onNull == verdictPass {
		return "pass", fields, "all_null_range"
	}
	if s.LegacyOnly || !s.TypeDefinedOrder {
		return "unknown", fields, "unsupported_statistics_order"
	}
	fields = append(fields, "type_defined_order")
	if s.Type == nil {
		return "unknown", fields, "missing_column_type"
	}
	if s.Type.ID() == arrow.FLOAT32 || s.Type.ID() == arrow.FLOAT64 {
		if s.NaNCount == nil {
			return "unknown", fields, "missing_nan_count"
		}
		fields = append(fields, "nan_count")
		if *s.NaNCount != 0 {
			return "unknown", fields, "nan_values"
		}
	}
	lo, ok := statisticScalar(s, s.MinValue)
	if !ok {
		return "unknown", fields, "invalid_min_value"
	}
	hi, ok := statisticScalar(s, s.MaxValue)
	if !ok {
		return "unknown", fields, "invalid_max_value"
	}
	fields = append(fields, "min_value", "max_value")
	if compareNumeric(lo, hi) == orderGreater {
		return "unknown", fields, "reversed_bounds"
	}
	if t.test(lo) != verdictPass || t.test(hi) != verdictPass {
		return "unknown", fields, "inconclusive_bounds"
	}
	return "pass", fields, "safe_numeric_bounds"
}

// statisticScalar decodes Parquet physical representations into the same native
// scalar domain as decoded Arrow values. Decimal byte arrays are big-endian
// two's complement; primitive integers/floats are little-endian.
func statisticScalar(s *data.ColumnStatistics, b []byte) (scalar, bool) {
	if s.Type == nil {
		return scalar{}, false
	}
	physical := strings.ToUpper(s.PhysicalType)
	if physical == "FIXED_LEN_BYTE_ARRAY" && (s.PhysicalLength <= 0 || len(b) != s.PhysicalLength) {
		return scalar{}, false
	}
	switch s.Type.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		var n int64
		if physical == "INT32" && len(b) == 4 {
			n = int64(int32(binary.LittleEndian.Uint32(b)))
		} else if physical == "INT64" && len(b) == 8 {
			n = int64(binary.LittleEndian.Uint64(b))
		} else {
			return scalar{}, false
		}
		bits := s.Type.(arrow.FixedWidthDataType).BitWidth()
		if bits < 64 && (n < -(int64(1)<<(bits-1)) || n > (int64(1)<<(bits-1))-1) {
			return scalar{}, false
		}
		return intScalar(n), true
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		var n uint64
		if physical == "INT32" && len(b) == 4 {
			n = uint64(binary.LittleEndian.Uint32(b))
		} else if physical == "INT64" && len(b) == 8 {
			n = binary.LittleEndian.Uint64(b)
		} else {
			return scalar{}, false
		}
		bits := s.Type.(arrow.FixedWidthDataType).BitWidth()
		if bits < 64 && n > ((uint64(1)<<bits)-1) {
			return scalar{}, false
		}
		return uintScalar(n), true
	case arrow.FLOAT32:
		if physical != "FLOAT" || len(b) != 4 {
			return scalar{}, false
		}
		f := float64(math.Float32frombits(binary.LittleEndian.Uint32(b)))
		return floatScalar(f), finite(f)
	case arrow.FLOAT64:
		if physical != "DOUBLE" || len(b) != 8 {
			return scalar{}, false
		}
		f := math.Float64frombits(binary.LittleEndian.Uint64(b))
		return floatScalar(f), finite(f)
	case arrow.DECIMAL128:
		typ := s.Type.(*arrow.Decimal128Type)
		var low, high uint64
		switch physical {
		case "INT32":
			if len(b) != 4 {
				return scalar{}, false
			}
			n := int64(int32(binary.LittleEndian.Uint32(b)))
			low = uint64(n)
			if n < 0 {
				high = math.MaxUint64
			}
		case "INT64":
			if len(b) != 8 {
				return scalar{}, false
			}
			n := int64(binary.LittleEndian.Uint64(b))
			low = uint64(n)
			if n < 0 {
				high = math.MaxUint64
			}
		case "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY":
			if len(b) == 0 || len(b) > 16 {
				return scalar{}, false
			}
			if b[0]&128 != 0 {
				low, high = math.MaxUint64, math.MaxUint64
			}
			for _, v := range b {
				high = high<<8 | low>>56
				low = low<<8 | uint64(v)
			}
		default:
			return scalar{}, false
		}
		v := scalar{kind: valDecimal, scale: typ.Scale, decimal: [2]uint64{low, high}}
		// The declared precision is part of the ingestion contract, not merely a
		// hint about physical width.
		coefficient := v
		coefficient.scale = 0
		text := coefficient.text()
		if text[0] == '-' {
			text = text[1:]
		}
		if len(text) > int(typ.Precision) {
			return scalar{}, false
		}
		return v, true
	}
	return scalar{}, false
}

// ApplyStatistics accounts one ordered group event. It must not race Evaluate.
// Only fully skipped groups contribute producer-declared row/rule coverage.
func (p *Program) ApplyStatistics(e *data.GroupEvidence) error {
	if p.closed || p.finalized {
		return fmt.Errorf("program is closed or finalized")
	}
	if e == nil || (e.Action != "scan" && e.Action != "skip") {
		return fmt.Errorf("invalid statistics group decision")
	}
	if p.summary.Coverage.RowGroupsTotal == nil {
		return fmt.Errorf("statistics evidence requires parquet input")
	}
	total, err := data.CheckedAdd(*p.summary.Coverage.RowGroupsTotal, 1)
	if err != nil {
		return err
	}
	target := p.summary.Coverage.RowGroupsScanned
	if e.Action == "skip" {
		target = p.summary.Coverage.RowGroupsStatisticsOnly
	}
	groups, err := data.CheckedAdd(*target, 1)
	if err != nil {
		return err
	}
	if e.Action == "skip" {
		if p.summary.TrustPolicy != "producer_statistics" || p.summary.OutputMode == "all" || e.DeclaredRows == 0 {
			return fmt.Errorf("ineligible statistics skip")
		}
		if p.statisticsObligation() != "" {
			return fmt.Errorf("statistics skip bypasses value obligation")
		}
		for _, check := range e.Checks {
			if check.Claim != "pass" {
				return fmt.Errorf("statistics skip lacks complete pass proof")
			}
		}
		metadata, err := data.CheckedAdd(p.summary.Coverage.MetadataClaimedRows, e.DeclaredRows)
		if err != nil {
			return err
		}
		accounted, err := data.CheckedAdd(p.summary.Total, metadata)
		if err != nil {
			return err
		}
		for code, c := range p.summary.RuleCoverage {
			if _, err = data.CheckedAdd(c.MetadataClaimedRows, e.DeclaredRows); err != nil {
				return fmt.Errorf("rule %s: %w", code, err)
			}
		}
		for code, c := range p.summary.RuleCoverage {
			c.MetadataClaimedRows += e.DeclaredRows
			p.summary.RuleCoverage[code] = c
		}
		p.summary.Coverage.MetadataClaimedRows = metadata
		p.summary.Coverage.AccountedRows = accounted
		p.summary.Verification = "metadata_assisted"
		p.summary.IngestionVerification = "partial"
	}
	*p.summary.Coverage.RowGroupsTotal = total
	*target = groups
	return nil
}
