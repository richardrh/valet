package checks

import "math/bits"

// fixed is the exact decimal (-1)^neg × (hi·2^64 + lo) × 10^-scale with an
// unsigned 128-bit magnitude and scale ≥ 0. Every integer and decimal128 cell
// and every decimal literal or text of up to 38 significant digits has one, so
// exact comparisons among them need no math/big.
type fixed struct {
	neg    bool
	hi, lo uint64
	scale  int32
}

// fixedStatus distinguishes text that is certainly not a decimal number from
// text this fast path merely does not represent (math/big decides those).
type fixedStatus uint8

const (
	fixedUnknown fixedStatus = iota
	fixedOK
	fixedRejected
)

var pow10u64 = [20]uint64{
	1, 10, 100, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
}

func (f fixed) signum() int {
	switch {
	case f.hi == 0 && f.lo == 0:
		return 0
	case f.neg:
		return -1
	}
	return 1
}

// mulPow10 multiplies a 128-bit magnitude by 10^k; ok is false on overflow.
func mulPow10(hi, lo uint64, k int32) (uint64, uint64, bool) {
	if hi == 0 && lo == 0 {
		return 0, 0, true
	}
	for k > 0 {
		step := min(k, 19)
		m := pow10u64[step]
		carry, low := bits.Mul64(lo, m)
		over, high := bits.Mul64(hi, m)
		if over != 0 {
			return 0, 0, false
		}
		high, c := bits.Add64(high, carry, 0)
		if c != 0 {
			return 0, 0, false
		}
		hi, lo = high, low
		k -= step
	}
	return hi, lo, true
}

// compareFixed orders two exact decimals. Aligning scales can overflow 128
// bits only for the side whose magnitude then exceeds every 128-bit value, so
// overflow itself decides the order.
func compareFixed(x, y fixed) ordering {
	sx, sy := x.signum(), y.signum()
	if sx != sy {
		return orderOf(sx - sy)
	}
	if sx == 0 {
		return orderEqual
	}
	xh, xl, yh, yl := x.hi, x.lo, y.hi, y.lo
	c := 0
	switch {
	case x.scale < y.scale:
		var ok bool
		if xh, xl, ok = mulPow10(xh, xl, y.scale-x.scale); !ok {
			c = 1
		}
	case y.scale < x.scale:
		var ok bool
		if yh, yl, ok = mulPow10(yh, yl, x.scale-y.scale); !ok {
			c = -1
		}
	}
	if c == 0 {
		switch {
		case xh != yh:
			c = cmpUint(xh, yh)
		case xl != yl:
			c = cmpUint(xl, yl)
		}
	}
	return orderOf(sx * c)
}

// neg128 returns the two's-complement negation of a 128-bit value.
func neg128(hi, lo uint64) (uint64, uint64) {
	lo = ^lo + 1
	hi = ^hi
	if lo == 0 {
		hi++
	}
	return hi, lo
}

// twosFixed reads a 128-bit two's-complement coefficient (low, high) with a
// base-10 scale. Negative scales are folded into the magnitude.
func twosFixed(d [2]uint64, scale int32) (fixed, bool) {
	f := fixed{lo: d[0], hi: d[1], scale: scale}
	if int64(d[1]) < 0 {
		f.neg = true
		f.hi, f.lo = neg128(d[1], d[0])
	}
	if scale < 0 {
		var ok bool
		if f.hi, f.lo, ok = mulPow10(f.hi, f.lo, -scale); !ok {
			return fixed{}, false
		}
		f.scale = 0
	}
	return f, true
}

// twos encodes f as a 128-bit two's-complement coefficient; ok is false when
// the magnitude needs the sign bit.
func (f fixed) twos() ([2]uint64, bool) {
	if f.hi>>63 != 0 {
		return [2]uint64{}, false
	}
	if !f.neg {
		return [2]uint64{f.lo, f.hi}, true
	}
	hi, lo := neg128(f.hi, f.lo)
	return [2]uint64{lo, hi}, true
}

// parseFixed reads decimal text without allocating. It accepts the subset of
// decimalRat's grammar that fits 128 bits: [+-] digits [. digits] [eE [+-]
// digits], with at least one mantissa digit. fixedRejected is returned only
// where decimalRat rejects too (empty, oversized, or a byte outside its
// alphabet); anything else it does not represent is fixedUnknown.
func parseFixed(s string) (fixed, fixedStatus) {
	if len(s) == 0 || len(s) > maxDecimalTextBytes {
		return fixed{}, fixedRejected
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && c != '.' && c != '+' && c != '-' && c != 'e' && c != 'E' {
			return fixed{}, fixedRejected
		}
	}
	var f fixed
	i := 0
	if s[0] == '+' || s[0] == '-' {
		f.neg = s[0] == '-'
		i++
	}
	digits, frac := 0, int64(0)
	point := false
	for ; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			if point {
				return fixed{}, fixedUnknown
			}
			point = true
			continue
		}
		if c < '0' || c > '9' {
			break
		}
		digits++
		if point {
			frac++
		}
		high, low, ok := mulPow10(f.hi, f.lo, 1)
		if !ok {
			return fixed{}, fixedUnknown
		}
		low, carry := bits.Add64(low, uint64(c-'0'), 0)
		high, carry = bits.Add64(high, 0, carry)
		if carry != 0 {
			return fixed{}, fixedUnknown
		}
		f.hi, f.lo = high, low
	}
	if digits == 0 {
		return fixed{}, fixedUnknown
	}
	exp := int64(0)
	if i < len(s) {
		if s[i] != 'e' && s[i] != 'E' {
			return fixed{}, fixedUnknown
		}
		i++
		negExp := false
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			negExp = s[i] == '-'
			i++
		}
		if i == len(s) {
			return fixed{}, fixedUnknown
		}
		for ; i < len(s); i++ {
			c := s[i]
			if c < '0' || c > '9' {
				return fixed{}, fixedUnknown
			}
			exp = exp*10 + int64(c-'0')
			if exp > maxDecimalExponent {
				return fixed{}, fixedUnknown
			}
		}
		if negExp {
			exp = -exp
		}
	}
	scale := frac - exp
	if scale < 0 {
		var ok bool
		if f.hi, f.lo, ok = mulPow10(f.hi, f.lo, int32(-scale)); !ok {
			return fixed{}, fixedUnknown
		}
		scale = 0
	}
	f.scale = int32(scale)
	return f, fixedOK
}
