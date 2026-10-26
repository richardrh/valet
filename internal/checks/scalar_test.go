package checks

import (
	"bytes"
	"math"
	"testing"
)

// Deliberate unit layer over the scalar helper API (constructors, typeValid,
// compare*, appendExactKey); row behaviour is tested through Program.

func TestTypeValidNativeBoundaries(t *testing.T) {
	tests := []struct {
		value scalar
		typ   string
		valid bool
	}{
		{intScalar(math.MaxInt64), "int64", true},
		{uintScalar(uint64(math.MaxInt64) + 1), "int64", false},
		{floatScalar(math.Exp2(63)), "int64", false},
		{floatScalar(1.5), "int32", false},
		{boolScalar(true), "int64", false},
		{stringScalar("9223372036854775807"), "int64", true},
		{stringScalar("1.0"), "int64", false},
		{floatScalar(math.MaxFloat64), "float32", false},
		{dateScalar(0), "date", true},
		{stringScalar("2024-02-30"), "date", false},
		{stringScalar("2026-01-01T00:00:00Z"), "timestamp", true},
		{scalar{}, "string", false},
	}
	for _, test := range tests {
		if got := typeValid(test.value, test.typ); got != test.valid {
			t.Errorf("typeValid(%+v, %s) = %v, want %v", test.value, test.typ, got, test.valid)
		}
	}
}

func TestComparisons(t *testing.T) {
	tests := []struct {
		name    string
		compare func(a, b scalar) ordering
		a, b    scalar
		want    ordering
	}{
		{
			name:    "integers beyond float precision",
			compare: compareNumeric,
			a:       intScalar(9007199254740993),
			b:       intScalar(9007199254740992),
			want:    orderGreater,
		},
		{
			name:    "lexeme against decimal cell",
			compare: compareNumeric,
			a:       scalar{kind: valDecimal, scale: 2, decimal: [2]uint64{101}},
			b:       numberTextScalar("1.01"),
			want:    orderEqual,
		},
		{
			name:    "NaN is incomparable",
			compare: compareNumeric,
			a:       floatScalar(math.NaN()),
			b:       floatScalar(1),
			want:    orderIncomparable,
		},
		{
			name:    "text is never coerced",
			compare: compareExact,
			a:       stringScalar("10"),
			b:       numberTextScalar("10"),
			want:    orderIncomparable,
		},
		{
			name:    "bool against text",
			compare: compareExact,
			a:       boolScalar(true),
			b:       stringScalar("true"),
			want:    orderIncomparable,
		},
		{
			name:    "dates compare as instants",
			compare: compareExact,
			a:       dateScalar(86400),
			b:       instantScalar(86400, 1),
			want:    orderLess,
		},
		{
			name:    "numeric literal parses string cell",
			compare: compareToLiteral,
			a:       stringScalar("1e1"),
			b:       intScalar(10),
			want:    orderEqual,
		},
		{
			name:    "unparsable string cell",
			compare: compareToLiteral,
			a:       stringScalar("ten"),
			b:       intScalar(10),
			want:    orderUnparsable,
		},
		{
			name:    "string literal stays text",
			compare: compareToLiteral,
			a:       stringScalar("10.0"),
			b:       stringScalar("10"),
			want:    orderGreater,
		},
	}
	for _, test := range tests {
		if got := test.compare(test.a, test.b); got != test.want {
			t.Errorf("%s: ordering = %d, want %d", test.name, got, test.want)
		}
	}
}

func TestOperatorsRejectUnorderedResults(t *testing.T) {
	for _, o := range []ordering{orderIncomparable, orderUnparsable} {
		for _, op := range []operator{opEq, opNe, opLt, opLe, opGt, opGe} {
			if op.accepts(o) {
				t.Errorf("operator %d accepts unordered result %d = true, want false", op, o)
			}
		}
	}
}

func TestExactKeys(t *testing.T) {
	tests := []struct {
		name     string
		a, b     scalar
		wantSame bool
	}{
		{name: "distinct exact integers", a: intScalar(9007199254740993), b: intScalar(9007199254740992), wantSame: false},
		{name: "-0 and +0", a: floatScalar(0), b: floatScalar(math.Copysign(0, -1)), wantSame: true},
		{name: "text and integer", a: stringScalar("1"), b: intScalar(1), wantSame: false},
	}
	for _, test := range tests {
		if got := bytes.Equal(appendExactKey(nil, test.a), appendExactKey(nil, test.b)); got != test.wantSame {
			t.Errorf("%s: keys equal = %v, want %v", test.name, got, test.wantSame)
		}
	}
}
