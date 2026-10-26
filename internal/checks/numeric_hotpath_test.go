package checks

import (
	"encoding/json"
	"math"
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

// raceEnabled is set by race_enabled_test.go: race instrumentation allocates,
// so allocation counts are meaningless under -race.
var raceEnabled bool

// numericColumn is the Arrow type of column x. scale applies to decimal128
// coefficients and to the decimal text written into string cells.
type numericColumn struct {
	typ   arrow.DataType
	scale int32
}

var (
	int64Column  = numericColumn{typ: arrow.PrimitiveTypes.Int64}
	uint32Column = numericColumn{typ: arrow.PrimitiveTypes.Uint32}
	uint64Column = numericColumn{typ: arrow.PrimitiveTypes.Uint64}
)

func decimalColumn(precision, scale int32) numericColumn {
	return numericColumn{typ: &arrow.Decimal128Type{Precision: precision, Scale: scale}, scale: scale}
}

func textColumn(scale int32) numericColumn {
	return numericColumn{typ: arrow.BinaryTypes.String, scale: scale}
}

func pow10(n int32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// maxDecimal128 is the largest coefficient magnitude of a precision-38 decimal.
var maxDecimal128 = new(big.Int).Sub(pow10(38), big.NewInt(1))

// coefficient maps raw fuzz bits onto a coefficient column c can hold.
func (c numericColumn) coefficient(hi int64, lo uint64) (*big.Int, bool) {
	switch c.typ.ID() {
	case arrow.INT64:
		return big.NewInt(int64(lo)), true
	case arrow.UINT32:
		return new(big.Int).SetUint64(uint64(uint32(lo))), true
	case arrow.UINT64:
		return new(big.Int).SetUint64(lo), true
	}
	v := new(big.Int).Lsh(big.NewInt(hi), 64)
	v.Add(v, new(big.Int).SetUint64(lo))
	return v, new(big.Int).Abs(v).Cmp(maxDecimal128) <= 0
}

// exactValue is the rational a coefficient denotes in column c.
func (c numericColumn) exactValue(coef *big.Int) *big.Rat {
	return new(big.Rat).SetFrac(coef, pow10(c.scale))
}

// batch builds a one-column batch of valid cells named x.
func (c numericColumn) batch(budget *resource.Budget, coefs []*big.Int) *data.Batch {
	var column arrow.Array
	switch c.typ.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(budget.Allocator())
		for _, v := range coefs {
			b.Append(v.Int64())
		}
		column = b.NewArray()
		b.Release()
	case arrow.UINT32:
		b := array.NewUint32Builder(budget.Allocator())
		for _, v := range coefs {
			b.Append(uint32(v.Uint64()))
		}
		column = b.NewArray()
		b.Release()
	case arrow.UINT64:
		b := array.NewUint64Builder(budget.Allocator())
		for _, v := range coefs {
			b.Append(v.Uint64())
		}
		column = b.NewArray()
		b.Release()
	case arrow.DECIMAL128:
		b := array.NewDecimal128Builder(budget.Allocator(), c.typ.(*arrow.Decimal128Type))
		for _, v := range coefs {
			b.Append(decimal128.FromBigInt(v))
		}
		column = b.NewArray()
		b.Release()
	case arrow.STRING:
		b := array.NewStringBuilder(budget.Allocator())
		for _, v := range coefs {
			b.Append(c.exactValue(v).FloatString(int(c.scale)))
		}
		column = b.NewArray()
		b.Release()
	default:
		panic("unsupported numeric column " + c.typ.String())
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: c.typ, Nullable: true}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(coefs)))
	column.Release()
	locations := make([]data.Location, len(coefs))
	for row := range locations {
		locations[row] = data.Location{Source: "fixture", Row: uint64(row + 1), GlobalRow: uint64(row + 1)}
	}
	return &data.Batch{Record: record, Locations: locations}
}

// exactConfig declares a schema so rules compile in exact mode.
func exactConfig(rules []config.Rule, datasetRules ...config.DatasetRule) *config.Config {
	return &config.Config{Input: config.Input{Schema: &config.Schema{}}, Rules: rules, DatasetRules: datasetRules}
}

func rangeConfig(min, max any) *config.Config {
	return exactConfig([]config.Rule{rule("R", "valid_range", map[string]any{"min": min, "max": max})})
}

// numberLiteral is the configuration literal mant×10^exp and its exact value.
func numberLiteral(mant int64, exp int32) (json.Number, *big.Rat) {
	if exp >= 0 {
		v := new(big.Int).Mul(big.NewInt(mant), pow10(exp))
		return json.Number(v.String()), new(big.Rat).SetInt(v)
	}
	v := new(big.Rat).SetFrac(big.NewInt(mant), pow10(-exp))
	return json.Number(v.FloatString(int(-exp))), v
}

// evaluateAndFinalize runs cfg over coefs in one batch.
func evaluateAndFinalize(tb testing.TB, cfg *config.Config, c numericColumn, coefs []*big.Int) *data.Summary {
	tb.Helper()
	budget := resource.New(64 << 20)
	batch := c.batch(budget, coefs)
	defer batch.Release()
	p, err := bindProgramErr(cfg, batch.Record.Schema(), budget)
	if err != nil {
		tb.Fatalf("compile: %v", err)
	}
	defer p.Close()
	result, err := p.Evaluate(batch)
	if err != nil {
		tb.Fatalf("evaluate: %v", err)
	}
	result.Release()
	if err := p.Finalize(); err != nil {
		tb.Fatalf("finalize: %v", err)
	}
	return p.Summary()
}

// hotPathCase is a numeric check over a column where every row passes, so
// row issues never contribute allocations of their own.
type hotPathCase struct {
	name   string
	column numericColumn
	cell   func(row int) int64
	cfg    *config.Config
}

func hotPathCases() []hotPathCase {
	inRange := func(row int) int64 { return int64(row % 150) }
	seven := func(int) int64 { return 7 }
	nonNegative := config.DatasetRule{Code: "SUM", Metric: "sum", Column: "x", Min: 0}
	return []hotPathCase{
		{"valid_range int64 integer bounds", int64Column, inRange, rangeConfig(0, 150)},
		{"valid_range uint32 integer bounds", uint32Column, inRange, rangeConfig(0, 150)},
		{"valid_range uint64 integer bounds", uint64Column, inRange, rangeConfig(0, 150)},
		{"valid_range int64 fractional bounds", int64Column, inRange, rangeConfig(json.Number("-0.5"), json.Number("150.5"))},
		{"valid_range decimal128 bounds", decimalColumn(18, 2), inRange, rangeConfig(json.Number("0"), json.Number("150.005"))},
		{"equals uint32 integer literal", uint32Column, seven, equalsConfig(7)},
		{"equals decimal text literal", textColumn(2), seven, equalsConfig(json.Number("0.07"))},
		{"sum int64", int64Column, inRange, exactConfig(nil, nonNegative)},
		{"sum min max decimal128", decimalColumn(38, 4), inRange, exactConfig(nil,
			nonNegative,
			config.DatasetRule{Code: "MIN", Metric: "min", Column: "x", Min: 0},
			config.DatasetRule{Code: "MAX", Metric: "max", Column: "x", Max: 1},
		)},
	}
}

func (tc hotPathCase) batch(budget *resource.Budget, rows int) *data.Batch {
	coefs := make([]*big.Int, rows)
	for row := range coefs {
		coefs[row] = big.NewInt(tc.cell(row))
	}
	return tc.column.batch(budget, coefs)
}

func equalsConfig(value any) *config.Config {
	return exactConfig([]config.Rule{rule("R", "equals", map[string]any{"value": value})})
}

func allocsPerEvaluate(t *testing.T, tc hotPathCase, rows int) float64 {
	t.Helper()
	budget := resource.New(64 << 20)
	batch := tc.batch(budget, rows)
	defer batch.Release()
	p, err := bindProgramErr(tc.cfg, batch.Record.Schema(), budget)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	defer p.Close()
	var evalErr error
	allocs := testing.AllocsPerRun(20, func() {
		result, err := p.Evaluate(batch)
		if err != nil {
			evalErr = err
			return
		}
		result.Release()
	})
	if evalErr != nil {
		t.Fatalf("evaluate: %v", evalErr)
	}
	if got := p.Summary().RuleCounts["R"]; got != 0 {
		t.Fatalf("fixture rows must pass, got %d failures", got)
	}
	return allocs
}

// Per-batch allocations (result buckets, issue slices) are fine; a count that
// grows with the row count means the hot path allocates per cell.
func TestNumericChecksDoNotAllocatePerRow(t *testing.T) {
	if raceEnabled {
		t.Skip("race instrumentation allocates")
	}
	for _, tc := range hotPathCases() {
		t.Run(tc.name, func(t *testing.T) {
			small, large := allocsPerEvaluate(t, tc, 1024), allocsPerEvaluate(t, tc, 4096)
			if large > small {
				t.Errorf("Evaluate allocates per row: %.0f allocs at 1024 rows, %.0f at 4096 (%.2f per row)",
					small, large, (large-small)/3072)
			}
		})
	}
}

func BenchmarkEvaluateNumeric(b *testing.B) {
	const rows = 8192
	for _, tc := range hotPathCases() {
		b.Run(tc.name, func(b *testing.B) {
			budget := resource.New(64 << 20)
			batch := tc.batch(budget, rows)
			defer batch.Release()
			p, err := bindProgramErr(tc.cfg, batch.Record.Schema(), budget)
			if err != nil {
				b.Fatal(err)
			}
			defer p.Close()
			b.ReportAllocs()
			for b.Loop() {
				result, err := p.Evaluate(batch)
				if err != nil {
					b.Fatal(err)
				}
				result.Release()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
		})
	}
}

// fuzzColumn maps fuzz bytes onto the column kinds the exact comparison fast
// paths cover.
func fuzzColumn(kind, scale uint8) numericColumn {
	s := int32(scale % 39)
	switch kind % 5 {
	case 0:
		return int64Column
	case 1:
		return uint32Column
	case 2:
		return uint64Column
	case 3:
		return decimalColumn(38, s)
	}
	return textColumn(s)
}

// Numeric literal comparisons (valid_range bounds, equals) must agree with
// exact rational comparison for every column representation, including
// literals finer than the column scale, negative fractions and literals
// beyond the column's range. Declared string columns are compared only by
// equals: valid_range treats them as non-numeric by contract.
func FuzzNumericLiteralComparisonsMatchExactArithmetic(f *testing.F) {
	const (
		opMin = iota
		opMax
		opExclusiveMin
		opExclusiveMax
		opEquals
		opCount
	)
	type seed struct {
		kind, scale uint8
		hi          int64
		lo          uint64
		mant        int64
		exp         int8
		op          uint8
	}
	for _, s := range []seed{
		{kind: 0, lo: uint64(1<<64 - 1), mant: -5, exp: -1, op: opMin},          // int64 -1 vs -0.5
		{kind: 0, lo: 0, mant: -5, exp: -1, op: opMin},                          // int64 0 vs -0.5
		{kind: 0, lo: 1 << 63, mant: math.MinInt64, exp: 0, op: opExclusiveMin}, // int64 min
		{kind: 0, lo: 1<<63 - 1, mant: 1, exp: 30, op: opMax},                   // literal beyond int64
		{kind: 1, lo: 1<<32 - 1, mant: -1, exp: 0, op: opMin},                   // uint32 vs negative
		{kind: 2, lo: 1<<64 - 1, mant: 18446744073, exp: 9, op: opMax},          // uint64 max
		{kind: 2, lo: 7, mant: 7, exp: 0, op: opExclusiveMax},                   // equal, exclusive
		{kind: 3, scale: 2, lo: 15000, mant: 150005, exp: -3, op: opMax},        // finer literal
		{kind: 3, scale: 2, hi: -1, lo: uint64(1<<64 - 15001), mant: -150005, exp: -3, op: opMin},
		{kind: 3, scale: 38, hi: 5421010862427522170, lo: 687399551400673279, mant: 1, exp: 0, op: opMax},
		{kind: 3, scale: 0, hi: 5421010862427522170, lo: 687399551400673279, mant: 1, exp: 40, op: opMax},
		{kind: 3, scale: 2, lo: 7, mant: 70, exp: -3, op: opEquals}, // 0.07 == 0.070
		{kind: 4, scale: 2, lo: 10, mant: 1, exp: -1, op: opEquals}, // "0.10" == 0.1
		{kind: 4, scale: 0, lo: 3, mant: 25, exp: -1, op: opEquals}, // "3" != 2.5
		{kind: 4, scale: 38, hi: -1, lo: 1<<64 - 1, mant: -1, exp: -38, op: opEquals},
	} {
		f.Add(s.kind, s.scale, s.hi, s.lo, s.mant, s.exp, s.op)
	}
	f.Fuzz(func(t *testing.T, kind, scale uint8, hi int64, lo uint64, mant int64, exp int8, op uint8) {
		column := fuzzColumn(kind, scale)
		coef, ok := column.coefficient(hi, lo)
		if !ok {
			return
		}
		lit, litValue := numberLiteral(mant, int32(max(min(exp, 40), -40)))
		params := map[string]any{}
		var pass bool
		cmp := column.exactValue(coef).Cmp(litValue)
		kindOp := op % opCount
		if column.typ.ID() == arrow.STRING {
			kindOp = opEquals
		}
		check := "valid_range"
		switch kindOp {
		case opMin:
			params["min"], pass = lit, cmp >= 0
		case opMax:
			params["max"], pass = lit, cmp <= 0
		case opExclusiveMin:
			params["min"], params["exclusive_min"], pass = lit, true, cmp > 0
		case opExclusiveMax:
			params["max"], params["exclusive_max"], pass = lit, true, cmp < 0
		case opEquals:
			check, params["value"], pass = "equals", lit, cmp == 0
		}
		s := evaluateAndFinalize(t, exactConfig([]config.Rule{rule("R", check, params)}), column, []*big.Int{coef})
		wantFailures := uint64(1)
		if pass {
			wantFailures = 0
		}
		if s.RuleCounts["R"] != wantFailures || s.Unevaluable["R"] != 0 {
			t.Errorf("%s %s cell %s, params %v: failures=%d unevaluable=%d, want failures=%d",
				check, column.typ, column.exactValue(coef).RatString(), params,
				s.RuleCounts["R"], s.Unevaluable["R"], wantFailures)
		}
	})
}

// Exact sum/min/max must equal rational arithmetic, including decimal sums
// that overflow 128 bits.
func FuzzExactAggregatesMatchExactArithmetic(f *testing.F) {
	const maxHi = 5421010862427522170 // high word of 10^38-1
	const maxLo = 687399551400673279  // low word of 10^38-1
	f.Add(uint8(0), uint8(0), int64(0), uint64(1<<63-1), int64(0), uint64(1<<63-1), int64(0), uint64(1))
	f.Add(uint8(0), uint8(0), int64(0), uint64(1<<63), int64(0), uint64(1<<63), int64(0), uint64(1<<64-1))
	f.Add(uint8(2), uint8(0), int64(0), uint64(1<<64-1), int64(0), uint64(1<<64-1), int64(0), uint64(1<<64-1))
	f.Add(uint8(3), uint8(4), int64(maxHi), uint64(maxLo), int64(maxHi), uint64(maxLo), int64(maxHi), uint64(maxLo))
	f.Add(uint8(3), uint8(4), int64(-maxHi-1), uint64(1<<64-maxLo), int64(maxHi), uint64(maxLo), int64(-1), uint64(1<<64-5))
	f.Add(uint8(3), uint8(38), int64(0), uint64(1), int64(-1), uint64(1<<64-1), int64(0), uint64(0))
	f.Fuzz(func(t *testing.T, kind, scale uint8, aHi int64, aLo uint64, bHi int64, bLo uint64, cHi int64, cLo uint64) {
		column := fuzzColumn(kind%4, scale) // aggregates take numeric columns only
		coefs := make([]*big.Int, 0, 3)
		for _, raw := range [][2]uint64{{uint64(aHi), aLo}, {uint64(bHi), bLo}, {uint64(cHi), cLo}} {
			coef, ok := column.coefficient(int64(raw[0]), raw[1])
			if !ok {
				return
			}
			coefs = append(coefs, coef)
		}
		sum, lo, hi := new(big.Rat), column.exactValue(coefs[0]), column.exactValue(coefs[0])
		for _, coef := range coefs {
			v := column.exactValue(coef)
			sum.Add(sum, v)
			if v.Cmp(lo) < 0 {
				lo = v
			}
			if v.Cmp(hi) > 0 {
				hi = v
			}
		}
		text := func(v *big.Rat) json.Number { return json.Number(v.FloatString(int(column.scale))) }
		ulp := new(big.Rat).SetFrac(big.NewInt(1), pow10(column.scale))
		cfg := exactConfig(nil,
			config.DatasetRule{Code: "SUM", Metric: "sum", Column: "x", Equals: text(sum)},
			config.DatasetRule{Code: "SUM_BELOW", Metric: "sum", Column: "x", Min: text(new(big.Rat).Add(sum, ulp))},
			config.DatasetRule{Code: "MIN", Metric: "min", Column: "x", Equals: text(lo)},
			config.DatasetRule{Code: "MAX", Metric: "max", Column: "x", Equals: text(hi)},
		)
		want := map[string]string{"SUM": datasetPass, "SUM_BELOW": datasetFail, "MIN": datasetPass, "MAX": datasetPass}
		s := evaluateAndFinalize(t, cfg, column, coefs)
		for _, r := range s.DatasetResults {
			if r.Status != want[r.Code] {
				t.Errorf("%s values %v: %s status=%s reason=%s value=%v, want %s",
					column.typ, []string{text(column.exactValue(coefs[0])).String(), text(column.exactValue(coefs[1])).String(),
						text(column.exactValue(coefs[2])).String()}, r.Code, r.Status, r.Reason, r.Value, want[r.Code])
			}
		}
		if len(s.DatasetResults) != len(want) {
			t.Fatalf("dataset results = %d, want %d", len(s.DatasetResults), len(want))
		}
	})
}

// Arbitrary text cells compared with a numeric literal must parse exactly as
// decimalRat, the decimal grammar of record: unparsable text is unevaluable,
// everything else compares by exact value.
func FuzzDecimalTextCellsMatchDecimalRat(f *testing.F) {
	for _, seed := range []string{
		"0.07", ".07", "7e-2", "+0.0700", "0.07.", "1.", "-.5", "1e", "e5", "--1", "+-1",
		"1e+-5", "1_0", " 7", "0x1p4", "1e1001", "1e-1001", "123456789012345678901234567890123456789",
		"0.000000000000000000000000000000000000000070", "", "-0", "7E-0002", "٧",
	} {
		f.Add(seed, int64(7), int8(-2))
	}
	f.Fuzz(func(t *testing.T, text string, mant int64, exp int8) {
		lit, litValue := numberLiteral(mant, int32(max(min(exp, 40), -40)))
		var want big.Rat
		parsed := decimalRat(text, &want)
		budget := resource.New(64 << 20)
		batch := stringBatch(t, budget, []string{"x"}, [][]string{{text}}, nil)
		defer batch.Release()
		p, err := bindProgramErr(equalsConfig(lit), batch.Record.Schema(), budget)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		defer p.Close()
		result, err := p.Evaluate(batch)
		if err != nil {
			t.Fatalf("evaluate: %v", err)
		}
		result.Release()
		s := p.Summary()
		var wantFailures, wantUnevaluable uint64
		switch {
		case !parsed:
			wantUnevaluable = 1
		case want.Cmp(litValue) != 0:
			wantFailures = 1
		}
		if s.RuleCounts["R"] != wantFailures || s.Unevaluable["R"] != wantUnevaluable {
			t.Errorf("cell %q equals %s: failures=%d unevaluable=%d, want %d/%d",
				text, lit, s.RuleCounts["R"], s.Unevaluable["R"], wantFailures, wantUnevaluable)
		}
	})
}
