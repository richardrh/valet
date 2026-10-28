package source

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/richardrh/valet/internal/config"
)

// cellKind classifies the lexical origin of a cell so conversion can apply
// format-specific rules (for example, JSON booleans are never numeric).
type cellKind uint8

const (
	kindString          cellKind = iota // CSV field, JSON string or textual Parquet value
	kindNull                            // JSON null or Parquet null
	kindNumber                          // JSON number or numeric Parquet value (exact lexeme)
	kindBool                            // JSON or Parquet boolean ("true"/"false")
	kindObject                          // selected JSON object (never scalar)
	kindArray                           // selected JSON array (never scalar)
	kindBytes                           // Parquet binary; text holds raw bytes, not base64
	kindDate32                          // Parquet Date32 integer in text
	kindDate64                          // Parquet Date64 integer in text
	kindTimestampSecond                 // Parquet timestamp integer in text
	kindTimestampMillisecond
	kindTimestampMicrosecond
	kindTimestampNanosecond
)

// cell holds lexical content, never a float64 intermediary for JSON numbers.
type cell struct {
	text    string
	kind    cellKind
	present bool
}

func columnType(c config.Column) (arrow.DataType, error) {
	kind, ok := config.CanonicalType(c.Type)
	if !ok {
		return nil, fmt.Errorf("unsupported column type %q", c.Type)
	}
	switch kind {
	case "null":
		return arrow.Null, nil
	case "string", "utf8", "str":
		return arrow.BinaryTypes.String, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "uint8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "uint16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "uint32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "uint64", "uint":
		return arrow.PrimitiveTypes.Uint64, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	case "timestamp":
		zone := c.Timezone
		if zone == "" {
			zone = "UTC"
		}
		if _, err := time.LoadLocation(zone); err != nil {
			return nil, fmt.Errorf("invalid timestamp timezone: %w", err)
		}
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: zone}, nil
	case "decimal":
		if c.Precision < 1 || c.Precision > 38 || c.Scale < 0 || c.Scale > c.Precision {
			return nil, errors.New("decimal requires precision 1..38 and scale 0..precision")
		}
		return &arrow.Decimal128Type{Precision: c.Precision, Scale: c.Scale}, nil
	}
	return nil, fmt.Errorf("unsupported column type %q", c.Type)
}

func appendCell(b array.Builder, c config.Column, v cell) error {
	s := v.text
	if c.Trim && v.kind != kindBytes {
		s = strings.TrimSpace(s)
	}
	if v.kind == kindObject || v.kind == kindArray {
		return errors.New("selected value is not scalar")
	}
	// Booleans are not numeric, even though some formats expose them as 0/1.
	if v.kind == kindBool && b.Type().ID() != arrow.BOOL && b.Type().ID() != arrow.STRING {
		return errors.New("boolean is not a numeric value")
	}
	switch out := b.(type) {
	case *array.NullBuilder:
		return errors.New("non-null value for null type")
	case *array.StringBuilder:
		if !utf8.ValidString(s) {
			return errors.New("invalid UTF-8 string")
		}
		out.Append(s)
	case *array.BinaryBuilder:
		if v.kind == kindBytes {
			out.Append([]byte(s))
			break
		}
		x, e := base64.StdEncoding.DecodeString(s)
		if e != nil {
			return errors.New("invalid base64 binary")
		}
		out.Append(x)
	case *array.FixedSizeBinaryBuilder:
		var x []byte
		var e error
		if v.kind == kindBytes {
			x = []byte(s)
		} else {
			x, e = base64.StdEncoding.DecodeString(s)
		}
		if e != nil || len(x) != out.Type().(*arrow.FixedSizeBinaryType).ByteWidth {
			return errors.New("invalid fixed-size binary")
		}
		out.Append(x)
	case *array.BooleanBuilder:
		tr, fa := c.TrueValues, c.FalseValues
		if tr == nil {
			tr = []string{"true"}
		}
		if fa == nil {
			fa = []string{"false"}
		}
		if v.kind == kindBool {
			out.Append(s == "true")
			break
		}
		for _, x := range tr {
			if s == x {
				out.Append(true)
				return nil
			}
		}
		for _, x := range fa {
			if s == x {
				out.Append(false)
				return nil
			}
		}
		return errors.New("invalid boolean token")
	case *array.Int8Builder:
		x, e := parseSigned(s, 8)
		if e != nil {
			return e
		}
		out.Append(int8(x))
	case *array.Int16Builder:
		x, e := parseSigned(s, 16)
		if e != nil {
			return e
		}
		out.Append(int16(x))
	case *array.Int32Builder:
		x, e := parseSigned(s, 32)
		if e != nil {
			return e
		}
		out.Append(int32(x))
	case *array.Int64Builder:
		x, e := parseSigned(s, 64)
		if e != nil {
			return e
		}
		out.Append(x)
	case *array.Uint8Builder:
		x, e := parseUnsigned(s, 8)
		if e != nil {
			return e
		}
		out.Append(uint8(x))
	case *array.Uint16Builder:
		x, e := parseUnsigned(s, 16)
		if e != nil {
			return e
		}
		out.Append(uint16(x))
	case *array.Uint32Builder:
		x, e := parseUnsigned(s, 32)
		if e != nil {
			return e
		}
		out.Append(uint32(x))
	case *array.Uint64Builder:
		x, e := parseUnsigned(s, 64)
		if e != nil {
			return e
		}
		out.Append(x)
	case *array.Float32Builder:
		x, e := parseFloat(s, 32)
		if e != nil {
			return e
		}
		out.Append(float32(x))
	case *array.Float64Builder:
		x, e := parseFloat(s, 64)
		if e != nil {
			return e
		}
		out.Append(x)
	case *array.Decimal128Builder:
		dt := out.Type().(*arrow.Decimal128Type)
		x, e := parseDecimal(s, dt.Precision, dt.Scale)
		if e != nil {
			return e
		}
		out.Append(x)
	case *array.Date32Builder:
		if v.kind == kindDate32 {
			raw, e := strconv.ParseInt(v.text, 10, 32)
			if e != nil {
				return errors.New("invalid native Date32 value")
			}
			out.Append(arrow.Date32(raw))
			break
		}
		if v.kind == kindDate64 {
			raw, e := strconv.ParseInt(v.text, 10, 64)
			if e != nil || raw%86400000 != 0 || raw/86400000 < math.MinInt32 || raw/86400000 > math.MaxInt32 {
				return errors.New("native Date64 value outside Date32 range")
			}
			out.Append(arrow.Date32(raw / 86400000))
			break
		}
		t, e := parseDate(s, c.Formats)
		if e != nil {
			return e
		}
		day, e := date32FromCivil(t)
		if e != nil {
			return e
		}
		out.Append(day)
	case *array.Date64Builder:
		if v.kind == kindDate32 || v.kind == kindDate64 {
			raw, e := strconv.ParseInt(v.text, 10, 64)
			if e != nil {
				return errors.New("invalid native date value")
			}
			if v.kind == kindDate32 {
				raw *= 86400000
			}
			out.Append(arrow.Date64(raw))
			break
		}
		t, e := parseDate(s, c.Formats)
		if e != nil {
			return e
		}
		day, e := date32FromCivil(t)
		if e != nil {
			return e
		}
		out.Append(arrow.Date64(day) * 86400000)
	case *array.TimestampBuilder:
		dt := out.Type().(*arrow.TimestampType)
		// Native Parquet timestamps carry their unit in the cell kind; rescale
		// exactly between ticks per second.
		var fromTicks int64
		switch v.kind {
		case kindTimestampSecond:
			fromTicks = 1
		case kindTimestampMillisecond:
			fromTicks = 1000
		case kindTimestampMicrosecond:
			fromTicks = 1000000
		case kindTimestampNanosecond:
			fromTicks = 1000000000
		}
		if fromTicks != 0 {
			raw, e := strconv.ParseInt(v.text, 10, 64)
			if e != nil {
				return errors.New("invalid native timestamp value")
			}
			var toTicks int64
			switch dt.Unit {
			case arrow.Second:
				toTicks = 1
			case arrow.Millisecond:
				toTicks = 1000
			case arrow.Microsecond:
				toTicks = 1000000
			case arrow.Nanosecond:
				toTicks = 1000000000
			default:
				return errors.New("unsupported timestamp unit")
			}
			converted := raw
			if toTicks > fromTicks {
				factor := toTicks / fromTicks
				if raw > 0 && raw > math.MaxInt64/factor || raw < 0 && raw < math.MinInt64/factor {
					return errors.New("timestamp outside target range")
				}
				converted = raw * factor
			} else if fromTicks > toTicks {
				factor := fromTicks / toTicks
				if raw%factor != 0 {
					return errors.New("timestamp conversion would lose precision")
				}
				converted = raw / factor
			}
			out.Append(arrow.Timestamp(converted))
			break
		}
		t, e := ParseTimestamp(s, c)
		if e != nil {
			return e
		}
		x, e := arrow.TimestampFromTime(t, dt.Unit)
		if e != nil {
			return errors.New("timestamp outside target range")
		}
		out.Append(x)
	default:
		return fmt.Errorf("unsupported Arrow type %s", b.Type())
	}
	return nil
}

func integerSyntax(s string, signed bool) bool {
	if s == "" {
		return false
	}
	if s[0] == '-' || s[0] == '+' {
		if !signed && s[0] == '-' {
			return false
		}
		s = s[1:]
	}
	if s == "" {
		return false
	}
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
func parseSigned(s string, bits int) (int64, error) {
	if !integerSyntax(s, true) {
		return 0, errors.New("invalid integer syntax")
	}
	v, e := strconv.ParseInt(s, 10, bits)
	if e != nil {
		return 0, errors.New("integer outside target range")
	}
	return v, nil
}
func parseUnsigned(s string, bits int) (uint64, error) {
	if !integerSyntax(s, false) {
		return 0, errors.New("invalid unsigned integer syntax")
	}
	v, e := strconv.ParseUint(strings.TrimPrefix(s, "+"), 10, bits)
	if e != nil {
		return 0, errors.New("integer outside target range")
	}
	return v, nil
}
func parseFloat(s string, bits int) (float64, error) {
	v, e := strconv.ParseFloat(s, bits)
	if e != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, errors.New("invalid or non-finite float")
	}
	return v, nil
}

var errInexactDecimal = errors.New("decimal is not exactly representable at target precision and scale")

// parseDecimal reads [+-]digits[.digits][(e|E)[+-]digits] exactly at the
// target precision and scale. Values of up to 18 digits are accumulated in a
// uint64 without allocating; longer ones use math/big.
func parseDecimal(s string, precision, scale int32) (decimal128.Num, error) {
	if s == "" {
		return decimal128.Num{}, errInexactDecimal
	}
	negative := false
	if s[0] == '-' || s[0] == '+' {
		negative = s[0] == '-'
		s = s[1:]
	}
	exponent := int64(0)
	if p := exponentIndex(s); p >= 0 {
		var e error
		if exponent, e = strconv.ParseInt(s[p+1:], 10, 32); e != nil {
			return decimal128.Num{}, errInexactDecimal
		}
		s = s[:p]
	}
	whole, frac := s, ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		whole, frac = s[:dot], s[dot+1:]
	}
	if len(whole)+len(frac) == 0 || !allDigits(whole) || !allDigits(frac) {
		return decimal128.Num{}, errInexactDecimal
	}
	// digit indexes whole and frac as one digit string without joining them.
	digit := func(i int) byte {
		if i < len(whole) {
			return whole[i]
		}
		return frac[i-len(whole)]
	}
	end := len(whole) + len(frac)
	start := 0
	for start < end && digit(start) == '0' {
		start++
	}
	if start == end {
		return decimal128.Num{}, nil
	}
	shift := int64(scale) + exponent - int64(len(frac))
	if shift < 0 {
		// Digits below the target scale must all be zero.
		if -shift > int64(end-start) {
			return decimal128.Num{}, errInexactDecimal
		}
		for i := end + int(shift); i < end; i++ {
			if digit(i) != '0' {
				return decimal128.Num{}, errInexactDecimal
			}
		}
		end += int(shift)
		shift = 0
	}
	significant := int64(end - start)
	if significant+shift > int64(precision) {
		return decimal128.Num{}, errInexactDecimal
	}
	if significant+shift <= 18 {
		var v uint64
		for i := start; i < end; i++ {
			v = v*10 + uint64(digit(i)-'0')
		}
		for range shift {
			v *= 10
		}
		n := int64(v)
		if negative {
			n = -n
		}
		return decimal128.FromI64(n), nil
	}
	var b strings.Builder
	b.Grow(int(significant + shift))
	for i := start; i < end; i++ {
		b.WriteByte(digit(i))
	}
	for range shift {
		b.WriteByte('0')
	}
	n, ok := new(big.Int).SetString(b.String(), 10)
	if !ok {
		return decimal128.Num{}, errInexactDecimal
	}
	if negative {
		n.Neg(n)
	}
	return decimal128.FromBigInt(n), nil
}

func allDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// exponentIndex is strings.IndexAny(s, "eE") without its general set search.
func exponentIndex(s string) int {
	for i := range len(s) {
		if s[i]|0x20 == 'e' {
			return i
		}
	}
	return -1
}

func parseDate(s string, formats []string) (time.Time, error) {
	if len(formats) == 0 {
		formats = []string{"2006-01-02"}
	}
	for _, f := range formats {
		layout, e := config.DateLayout(f)
		if e != nil {
			return time.Time{}, e
		}
		if t, e := time.Parse(layout, s); e == nil {
			return t, nil
		}
	}
	return time.Time{}, errors.New("invalid calendar date")
}

func date32FromCivil(t time.Time) (arrow.Date32, error) {
	year, month, day := t.Date()
	midnight := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	seconds := midnight.Unix()
	days := seconds / 86400
	if seconds < 0 && seconds%86400 != 0 {
		days--
	}
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, errors.New("calendar date outside Date32 range")
	}
	return arrow.Date32(days), nil
}

// ParseTimestamp parses an instant without consulting the machine's local timezone.
func ParseTimestamp(s string, c config.Column) (time.Time, error) {
	formats := c.Formats
	if len(formats) == 0 {
		formats = []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999"}
	}
	for _, f := range formats {
		layout, layoutErr := config.DateLayout(f)
		if layoutErr != nil {
			return time.Time{}, layoutErr
		}
		if strings.Contains(layout, "Z07") || strings.Contains(layout, "-07") {
			if t, e := time.Parse(layout, s); e == nil {
				return t.UTC(), nil
			}
			continue
		}
		parsed, parseErr := time.Parse(layout, s)
		if parseErr != nil {
			continue
		}
		abbreviation := ""
		if strings.Contains(layout, "MST") {
			abbreviation, _ = parsed.Zone()
			if abbreviation == "UTC" || abbreviation == "GMT" {
				return wallClock(parsed).UTC(), nil
			}
		}
		if c.Timezone == "" {
			continue
		}
		loc, e := time.LoadLocation(c.Timezone)
		if e != nil {
			return time.Time{}, errors.New("invalid source timezone")
		}
		result, e := resolveWallTimestamp(wallClock(parsed), loc, abbreviation)
		if e != nil {
			return time.Time{}, e
		}
		return result, nil
	}
	return time.Time{}, errors.New("timestamp requires valid offset or explicit source timezone")
}

// wallClock drops the zone, keeping only the calendar date and clock reading.
func wallClock(t time.Time) time.Time {
	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	return time.Date(year, month, day, hour, minute, second, t.Nanosecond(), time.UTC)
}

func resolveWallTimestamp(wall time.Time, loc *time.Location, abbreviation string) (time.Time, error) {
	// Enumerate offsets surrounding the wall clock. Exactly one instant must
	// map back to it and, when supplied, carry the parsed abbreviation.
	offsets := map[int]bool{}
	for h := -36; h <= 36; h++ {
		_, offset := wall.Add(time.Duration(h) * time.Hour).In(loc).Zone()
		offsets[offset] = true
	}
	count := 0
	var result time.Time
	for offset := range offsets {
		candidate := wall.Add(-time.Duration(offset) * time.Second)
		local := candidate.In(loc)
		name, _ := local.Zone()
		if wallClock(local).Equal(wall) && (abbreviation == "" || name == abbreviation) {
			count++
			result = candidate.UTC()
		}
	}
	if count != 1 {
		return time.Time{}, errors.New("ambiguous, nonexistent, or mismatched local timestamp")
	}
	return result, nil
}
