package checks

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/richardrh/valet/internal/config"
)

// isoDate is the canonical calendar-date layout used for date literals, date
// text and implicit date parsing.
const isoDate = "2006-01-02"

// valueKind tags which scalar fields carry the value.
type valueKind uint8

const (
	// valUnknown is an unsupported Arrow type; no payload.
	valUnknown valueKind = iota
	// valString is UTF-8 text in s (string cells and YAML string literals).
	valString
	// valBinary is raw bytes in s.
	valBinary
	// valNumberText is an exact configuration number lexeme (json.Number) in s.
	// It is numeric, never text: it is compared as an exact rational. When
	// exact is set, decimal and scale also hold its value, parsed once.
	valNumberText
	// valInt is a signed integer in i.
	valInt
	// valUint is an unsigned integer in u.
	valUint
	// valFloat is a binary floating value in f.
	valFloat
	// valBool is a boolean in b.
	valBool
	// valTime is an instant: Unix seconds in i, nanoseconds within the second
	// in u. date marks a calendar date (time of day is midnight UTC).
	valTime
	// valDecimal is a 128-bit two's-complement coefficient in decimal
	// (low, high) with base-10 scale.
	valDecimal
)

// scalar retains native integer widths and decimal coefficients; no exact key or
// explicitly typed numeric comparison passes through a float64 intermediary.
// Only the fields documented for kind are meaningful.
type scalar struct {
	kind    valueKind
	b       bool
	date    bool
	exact   bool
	scale   int32
	s       string
	i       int64
	u       uint64
	f       float64
	decimal [2]uint64
}

func stringScalar(s string) scalar { return scalar{kind: valString, s: s} }

// numberTextScalar keeps the lexeme and, when it fits 128 bits, its exact
// coefficient so comparisons need not re-parse it per row.
func numberTextScalar(s string) scalar {
	v := scalar{kind: valNumberText, s: s}
	if f, status := parseFixed(s); status == fixedOK {
		if d, ok := f.twos(); ok {
			v.decimal, v.scale, v.exact = d, f.scale, true
		}
	}
	return v
}

func intScalar(v int64) scalar                    { return scalar{kind: valInt, i: v} }
func uintScalar(v uint64) scalar                  { return scalar{kind: valUint, u: v} }
func floatScalar(v float64) scalar                { return scalar{kind: valFloat, f: v} }
func boolScalar(v bool) scalar                    { return scalar{kind: valBool, b: v} }
func instantScalar(sec int64, nsec uint64) scalar { return scalar{kind: valTime, i: sec, u: nsec} }
func timeScalar(t time.Time) scalar               { return instantScalar(t.Unix(), uint64(t.Nanosecond())) }

// dateScalar is a calendar date at midnight UTC, sec seconds after the epoch.
func dateScalar(sec int64) scalar { return scalar{kind: valTime, i: sec, date: true} }

func (v scalar) isNumber() bool {
	switch v.kind {
	case valInt, valUint, valFloat, valDecimal, valNumberText:
		return true
	}
	return false
}

// fixed is the exact 128-bit form of an integer, decimal, parsed number
// literal or decimal text. fixedRejected means v is text decimalRat rejects;
// fixedUnknown means only math/big can represent v.
func (v scalar) fixed() (fixed, fixedStatus) {
	switch v.kind {
	case valInt:
		if v.i < 0 {
			return fixed{neg: true, lo: uint64(-v.i)}, fixedOK
		}
		return fixed{lo: uint64(v.i)}, fixedOK
	case valUint:
		return fixed{lo: v.u}, fixedOK
	case valDecimal:
		if f, ok := twosFixed(v.decimal, v.scale); ok {
			return f, fixedOK
		}
	case valNumberText:
		if v.exact {
			if f, ok := twosFixed(v.decimal, v.scale); ok {
				return f, fixedOK
			}
		}
	case valString:
		return parseFixed(v.s)
	}
	return fixed{}, fixedUnknown
}

func cell(a arrow.Array, row int) scalar {
	switch a := a.(type) {
	case *array.String:
		return stringScalar(a.Value(row))
	case *array.LargeString:
		return stringScalar(a.Value(row))
	case *array.Binary:
		return scalar{kind: valBinary, s: string(a.Value(row))}
	case *array.Boolean:
		return boolScalar(a.Value(row))
	case *array.Int8:
		return intScalar(int64(a.Value(row)))
	case *array.Int16:
		return intScalar(int64(a.Value(row)))
	case *array.Int32:
		return intScalar(int64(a.Value(row)))
	case *array.Int64:
		return intScalar(a.Value(row))
	case *array.Uint8:
		return uintScalar(uint64(a.Value(row)))
	case *array.Uint16:
		return uintScalar(uint64(a.Value(row)))
	case *array.Uint32:
		return uintScalar(uint64(a.Value(row)))
	case *array.Uint64:
		return uintScalar(a.Value(row))
	case *array.Float32:
		return floatScalar(float64(a.Value(row)))
	case *array.Float64:
		return floatScalar(a.Value(row))
	case *array.Date32:
		return dateScalar(int64(a.Value(row)) * 86400)
	case *array.Date64:
		return dateScalar(int64(a.Value(row)) / 1000)
	case *array.Timestamp:
		typ := a.DataType().(*arrow.TimestampType)
		v := int64(a.Value(row))
		unit := int64(1)
		switch typ.Unit {
		case arrow.Millisecond:
			unit = 1000
		case arrow.Microsecond:
			unit = 1000000
		case arrow.Nanosecond:
			unit = 1000000000
		}
		seconds := v / unit
		remainder := v % unit
		if remainder < 0 {
			seconds--
			remainder += unit
		}
		return instantScalar(seconds, uint64(remainder)*(1000000000/uint64(unit)))
	case *array.Decimal128:
		v := a.Value(row)
		return scalar{kind: valDecimal, scale: a.DataType().(*arrow.Decimal128Type).Scale, decimal: [2]uint64{v.LowBits(), uint64(v.HighBits())}}
	}
	return scalar{kind: valUnknown}
}

// literal converts a configuration argument. Numbers keep their YAML kind:
// strings stay text even when they look numeric.
func literal(v any) (scalar, error) {
	switch v := v.(type) {
	case string:
		return stringScalar(v), nil
	case json.Number:
		var r big.Rat
		if !decimalRat(string(v), &r) {
			return scalar{}, fmt.Errorf("numeric literal outside supported decimal precision")
		}
		return numberTextScalar(string(v)), nil
	case int:
		return intScalar(int64(v)), nil
	case int64:
		return intScalar(v), nil
	case uint64:
		return uintScalar(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			break
		}
		return floatScalar(v), nil
	case float32:
		return floatScalar(float64(v)), nil
	case bool:
		return boolScalar(v), nil
	}
	return scalar{}, fmt.Errorf("expected a non-null finite scalar argument")
}

func (v scalar) text() string {
	switch v.kind {
	case valString, valNumberText:
		return v.s
	case valBinary:
		return base64.StdEncoding.EncodeToString([]byte(v.s))
	case valInt:
		return strconv.FormatInt(v.i, 10)
	case valUint:
		return strconv.FormatUint(v.u, 10)
	case valFloat:
		return strconv.FormatFloat(v.f, 'g', -1, 64)
	case valBool:
		return strconv.FormatBool(v.b)
	case valTime:
		if v.date {
			return time.Unix(v.i, 0).UTC().Format(isoDate)
		}
		return time.Unix(v.i, int64(v.u)).UTC().Format(time.RFC3339Nano)
	case valDecimal:
		var n big.Rat
		v.rat(&n)
		if v.scale < 0 {
			return n.FloatString(0)
		}
		return n.FloatString(int(v.scale))
	}
	return ""
}

// number converts to a finite float64; text is parsed as a float.
func (v scalar) number() (float64, bool) {
	switch v.kind {
	case valInt:
		return float64(v.i), true
	case valUint:
		return float64(v.u), true
	case valFloat:
		return v.f, finite(v.f)
	case valString, valNumberText:
		f, e := strconv.ParseFloat(v.s, 64)
		return f, e == nil && finite(f)
	case valDecimal:
		var r big.Rat
		v.rat(&r)
		f, _ := r.Float64()
		return f, !math.IsInf(f, 0)
	}
	return 0, false
}

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// rat converts to an exact rational; text is parsed as a bounded decimal.
func (v scalar) rat(dst *big.Rat) bool {
	switch v.kind {
	case valInt:
		dst.SetInt64(v.i)
	case valUint:
		var n big.Int
		n.SetUint64(v.u)
		dst.SetInt(&n)
	case valFloat:
		return dst.SetFloat64(v.f) != nil
	case valString, valNumberText:
		return decimalRat(v.s, dst)
	case valDecimal:
		var hi, lo, n big.Int
		hi.SetInt64(int64(v.decimal[1]))
		hi.Lsh(&hi, 64)
		lo.SetUint64(v.decimal[0])
		n.Add(&hi, &lo)
		var scratch big.Int
		den := pow10Int(abs32(v.scale), &scratch)
		if v.scale < 0 {
			n.Mul(&n, den)
			dst.SetInt(&n)
		} else {
			dst.SetFrac(&n, den)
		}
	default:
		return false
	}
	return true
}
func abs32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}

// pow10Table holds 10^0..10^38, the decimal128 scale range. Entries are only
// ever read, so concurrent programs may share them.
var pow10Table = func() (t [39]big.Int) {
	for i := range t {
		t[i].Exp(big.NewInt(10), big.NewInt(int64(i)), nil)
	}
	return t
}()

// pow10Int returns 10^n, from the table when it covers n, else computed into
// scratch. The result must not be modified.
func pow10Int(n int32, scratch *big.Int) *big.Int {
	if n >= 0 && int(n) < len(pow10Table) {
		return &pow10Table[n]
	}
	return scratch.Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// ordering is a three-way comparison result plus the two ways a comparison
// can have no answer.
type ordering int8

const (
	orderLess    ordering = -1
	orderEqual   ordering = 0
	orderGreater ordering = 1
	// orderIncomparable: different value domains, or a non-finite float.
	orderIncomparable ordering = 2
	// orderUnparsable: a string cell compared to a numeric literal is not a
	// decimal number. Callers report the operand as unevaluable.
	orderUnparsable ordering = 3
)

func (o ordering) ordered() bool { return o >= orderLess && o <= orderGreater }

func orderOf(c int) ordering {
	switch {
	case c < 0:
		return orderLess
	case c > 0:
		return orderGreater
	}
	return orderEqual
}

// compareNumeric compares two values as exact numbers; text is parsed as a
// bounded decimal. Values without an exact rational are incomparable.
func compareNumeric(a, b scalar) ordering {
	// Fast exact cases cover the common paths without heap allocation.
	switch {
	case a.kind == valInt && b.kind == valInt:
		return orderOf(cmpInt(a.i, b.i))
	case a.kind == valUint && b.kind == valUint:
		return orderOf(cmpUint(a.u, b.u))
	case a.kind == valFloat && b.kind == valFloat:
		if !finite(a.f) || !finite(b.f) {
			return orderIncomparable
		}
		switch {
		case a.f < b.f:
			return orderLess
		case a.f > b.f:
			return orderGreater
		}
		return orderEqual
	case a.kind == valInt && b.kind == valUint:
		if a.i < 0 {
			return orderLess
		}
		return orderOf(cmpUint(uint64(a.i), b.u))
	case a.kind == valUint && b.kind == valInt:
		if b.i < 0 {
			return orderGreater
		}
		return orderOf(cmpUint(a.u, uint64(b.i)))
	}
	x, xs := a.fixed()
	y, ys := b.fixed()
	switch {
	case xs == fixedRejected || ys == fixedRejected:
		return orderIncomparable
	case xs == fixedOK && ys == fixedOK:
		return compareFixed(x, y)
	}
	var xr, yr big.Rat
	if !a.rat(&xr) || !b.rat(&yr) {
		return orderIncomparable
	}
	return orderOf(xr.Cmp(&yr))
}

// compareExact compares values within one domain: instants, booleans, text
// (string or binary), or numbers. Text is never coerced to a number.
func compareExact(a, b scalar) ordering {
	switch {
	case a.kind == valTime && b.kind == valTime:
		if c := cmpInt(a.i, b.i); c != 0 {
			return orderOf(c)
		}
		return orderOf(cmpUint(a.u, b.u))
	case a.kind == valBool && b.kind == valBool:
		return orderOf(cmpBool(a.b, b.b))
	case isText(a.kind) && isText(b.kind):
		return orderOf(strings.Compare(a.s, b.s))
	case a.isNumber() && b.isNumber():
		return compareNumeric(a, b)
	}
	return orderIncomparable
}

// compareToLiteral applies the literal coercion policy: a numeric literal
// against a string cell parses the cell as a decimal number (orderUnparsable
// when it is not one); a string literal is always compared as exact text;
// everything else compares within its domain.
func compareToLiteral(v, lit scalar) ordering {
	if v.kind == valString && lit.isNumber() {
		if o := compareNumeric(v, lit); o.ordered() {
			return o
		}
		return orderUnparsable
	}
	return compareExact(v, lit)
}

func isText(k valueKind) bool { return k == valString || k == valBinary }

func cmpInt(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func cmpUint(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func cmpBool(a, b bool) int {
	if a == b {
		return 0
	}
	if b {
		return -1
	}
	return 1
}

// operator is a comparison normalised once at compile time.
type operator uint8

const (
	opEq operator = iota + 1
	opNe
	opLt
	opLe
	opGt
	opGe
)

// parseOperator accepts the symbolic (compare_columns) and mnemonic (when)
// spellings.
func parseOperator(s string) (operator, bool) {
	switch s {
	case "==", "eq":
		return opEq, true
	case "!=", "ne":
		return opNe, true
	case "<", "lt":
		return opLt, true
	case "<=", "le":
		return opLe, true
	case ">", "gt":
		return opGt, true
	case ">=", "ge":
		return opGe, true
	}
	return 0, false
}

// accepts reports whether the ordering satisfies the operator. A comparison
// without an ordering satisfies no operator, including !=.
func (o operator) accepts(ord ordering) bool {
	if !ord.ordered() {
		return false
	}
	switch o {
	case opEq:
		return ord == orderEqual
	case opNe:
		return ord != orderEqual
	case opLt:
		return ord < orderEqual
	case opLe:
		return ord <= orderEqual
	case opGt:
		return ord > orderEqual
	case opGe:
		return ord >= orderEqual
	}
	return false
}

func typeName(s string) string {
	if canonical, ok := config.CanonicalType(s); ok {
		return canonical
	}
	return strings.ToLower(s)
}
func physicalType(t arrow.DataType) string {
	switch t.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		return "string"
	case arrow.BOOL:
		return "bool"
	case arrow.DATE32, arrow.DATE64:
		return "date"
	case arrow.TIMESTAMP:
		return "timestamp"
	case arrow.DECIMAL128:
		return "decimal"
	}
	return t.String()
}
func knownType(t string) bool {
	return isNumericType(t) || t == "string" || t == "bool" || isTemporalType(t) || t == "decimal"
}
func isNumericType(t string) bool {
	switch t {
	case "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "decimal":
		return true
	}
	return false
}
func isFloatType(t string) bool    { return t == "float32" || t == "float64" }
func isTemporalType(t string) bool { return t == "date" || t == "timestamp" }

// compatible reports whether two declared types compare without loss.
func compatible(a, b string) bool {
	if a == b {
		return true
	}
	if isNumericType(a) && isNumericType(b) {
		return isFloatType(a) == isFloatType(b)
	}
	return false
}

func typeValid(v scalar, typ string) bool {
	typ = typeName(typ)
	switch typ {
	case "string":
		return v.kind != valUnknown
	case "bool":
		return v.kind == valBool
	case "date":
		if v.kind == valTime {
			return true
		}
		if v.kind != valString || len(v.s) != len(isoDate) {
			return false
		}
		_, e := time.Parse(isoDate, v.s)
		return e == nil
	case "timestamp":
		_, ok := timestamp(v)
		return ok
	case "float32", "float64":
		f, ok := v.number()
		return ok && (typ != "float32" || math.Abs(f) <= math.MaxFloat32)
	case "decimal":
		var r big.Rat
		return v.rat(&r)
	}
	if strings.HasPrefix(typ, "int") || strings.HasPrefix(typ, "uint") {
		unsigned := strings.HasPrefix(typ, "uint")
		bits, e := strconv.Atoi(strings.TrimPrefix(strings.TrimPrefix(typ, "uint"), "int"))
		if e != nil {
			return false
		}
		if v.kind == valString {
			if unsigned {
				_, e := strconv.ParseUint(v.s, 10, bits)
				return e == nil
			}
			_, e := strconv.ParseInt(v.s, 10, bits)
			return e == nil
		}
		var r big.Rat
		if !v.rat(&r) || !r.IsInt() {
			return false
		}
		n := r.Num()
		if unsigned {
			return n.Sign() >= 0 && n.BitLen() <= bits
		}
		if n.Sign() >= 0 {
			return n.BitLen() < bits
		}
		var min big.Int
		min.Lsh(big.NewInt(1), uint(bits-1))
		min.Neg(&min)
		return n.Cmp(&min) >= 0
	}
	return false
}

// timestamp accepts an instant or RFC 3339 text with an explicit offset.
func timestamp(v scalar) (scalar, bool) {
	if v.kind == valTime {
		return v, true
	}
	if v.kind != valString {
		return scalar{}, false
	}
	t, e := time.Parse(time.RFC3339Nano, v.s)
	if e != nil {
		return scalar{}, false
	}
	return timeScalar(t), true
}

// isoDateScalar parses YYYY-MM-DD text as a calendar date.
func isoDateScalar(s string) (scalar, bool) {
	t, e := time.Parse(isoDate, s)
	if e != nil {
		return scalar{}, false
	}
	return dateScalar(t.Unix()), true
}

func dateLayout(format string) (string, error) {
	return config.DateLayout(format)
}

// dateLayoutsParam collects `format` then `formats` from check parameters and
// translates each strftime pattern to a Go layout. The result may be empty.
func dateLayoutsParam(m map[string]any) ([]string, error) {
	var formats []string
	if s := str(m, "format"); s != "" {
		formats = append(formats, s)
	}
	if v, ok := m["formats"]; ok {
		ss, err := stringsArg(v)
		if err != nil {
			return nil, err
		}
		formats = append(formats, ss...)
	}
	layouts := make([]string, 0, len(formats))
	for _, f := range formats {
		l, err := dateLayout(f)
		if err != nil {
			return nil, err
		}
		layouts = append(layouts, l)
	}
	return layouts, nil
}

// decimalRat parses s within the fixed arithmetic envelope for configuration
// thresholds and exact comparisons; gigantic exponents are rejected before
// math/big can allocate their powers.
func decimalRat(s string, dst *big.Rat) bool {
	if len(s) == 0 || len(s) > maxDecimalTextBytes {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && r != '.' && r != '+' && r != '-' && r != 'e' && r != 'E' {
			return false
		}
	}
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.ParseInt(s[i+1:], 10, 32)
		if err != nil || e < -maxDecimalExponent || e > maxDecimalExponent {
			return false
		}
	}
	_, ok := dst.SetString(s)
	return ok
}
