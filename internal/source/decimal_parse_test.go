package source

import (
	"errors"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// referenceParseDecimal is the previous math/big implementation, kept to
// check that the allocation-free parser accepts and rejects exactly the same
// text with the same values.
func referenceParseDecimal(s string, precision, scale int32) (decimal128.Num, error) {
	bad := func() (decimal128.Num, error) {
		return decimal128.Num{}, errors.New("decimal is not exactly representable at target precision and scale")
	}
	if s == "" {
		return bad()
	}
	negative := false
	if s[0] == '-' || s[0] == '+' {
		negative = s[0] == '-'
		s = s[1:]
	}
	if s == "" {
		return bad()
	}
	exponent := int64(0)
	if p := strings.IndexAny(s, "eE"); p >= 0 {
		var e error
		exponent, e = strconv.ParseInt(s[p+1:], 10, 32)
		if e != nil {
			return bad()
		}
		s = s[:p]
	}
	dot := strings.IndexByte(s, '.')
	fraction := 0
	if dot >= 0 {
		fraction = len(s) - dot - 1
		s = s[:dot] + s[dot+1:]
	}
	if !integerSyntax(s, false) || strings.HasPrefix(s, "+") {
		return bad()
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return decimal128.Num{}, nil
	}
	shift := int64(scale) + exponent - int64(fraction)
	if shift < 0 {
		drop := -shift
		if drop > int64(len(s)) {
			return bad()
		}
		for _, ch := range s[len(s)-int(drop):] {
			if ch != '0' {
				return bad()
			}
		}
		s = s[:len(s)-int(drop)]
	} else {
		if int64(len(s))+shift > int64(precision) {
			return bad()
		}
		s += strings.Repeat("0", int(shift))
	}
	if len(s) > int(precision) {
		return bad()
	}
	n := new(big.Int)
	if _, ok := n.SetString(s, 10); !ok {
		return bad()
	}
	if negative {
		n.Neg(n)
	}
	return decimal128.FromBigInt(n), nil
}

func FuzzParseDecimalMatchesReference(f *testing.F) {
	for _, seed := range []string{
		"0", "-0", "+0", "1", "-1", "1.23", "1.2300", "-0.00100", ".5", "5.", ".", "-", "+", "",
		"1e2", "1.23e2", "1E-2", "0e999999", "1e999999", "1e", "e1", "1e+", "1.2.3", "++1", "+-1",
		"9007199254740993.10", "999999999999999999", "1000000000000000000", "123456789012345678.9",
		"99999999999999999999999999999999999999", "100000000000000000000000000000000000000",
		"00000000000000000000000000000000000000001", "1_0", " 1", "1 ", "NaN", "0x10", "1.0e-1",
	} {
		f.Add(seed, int32(18), int32(2))
		f.Add(seed, int32(38), int32(0))
		f.Add(seed, int32(5), int32(5))
	}
	f.Fuzz(func(t *testing.T, s string, precision, scale int32) {
		precision = 1 + abs32(precision)%38
		scale = abs32(scale) % (precision + 1)
		want, wantErr := referenceParseDecimal(s, precision, scale)
		got, gotErr := parseDecimal(s, precision, scale)
		if (wantErr == nil) != (gotErr == nil) || got != want {
			t.Errorf("parseDecimal(%q, %d, %d) = %v, %v; reference %v, %v", s, precision, scale, got, gotErr, want, wantErr)
		}
	})
}

func abs32(v int32) int32 {
	if v < 0 {
		if v == -v {
			return 0
		}
		return -v
	}
	return v
}

func BenchmarkParseDecimal(b *testing.B) {
	for _, s := range []string{"1234.56", "-0.01", "9007199254740993.10"} {
		b.Run(s, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := parseDecimal(s, 20, 2); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
