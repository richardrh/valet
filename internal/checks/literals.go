package checks

import (
	"fmt"
	"strconv"
)

// Literal conversion belongs to compilation, not generic numeric comparison:
// integer/decimal operands retain exact rationals, while floating operands use
// literals rounded once to the same declared width as their Arrow cells.
//
// bindFloatingLiteral rounds a numeric literal for a float32/float64 operand.
// With numericString, numeric-looking string literals are rounded too (ranges
// and conditions); otherwise strings stay text so equality never coerces them.
func bindFloatingLiteral(value scalar, typ string, numericString bool) (scalar, error) {
	if !isFloatType(typ) {
		return value, nil
	}
	if !value.isNumber() && !(numericString && value.kind == valString) {
		return value, nil
	}
	bits := 64
	if typ == "float32" {
		bits = 32
	}
	var f float64
	var err error
	switch {
	case value.kind == valNumberText || value.kind == valString:
		// Parse directly at target width to avoid decimal -> Float64 -> Float32
		// double rounding at halfway boundaries.
		f, err = strconv.ParseFloat(value.s, bits)
	case bits == 32 && value.kind == valInt:
		f = float64(float32(value.i))
	case bits == 32 && value.kind == valUint:
		f = float64(float32(value.u))
	default:
		var ok bool
		f, ok = value.number()
		if !ok {
			return scalar{}, fmt.Errorf("invalid floating comparison literal")
		}
		if bits == 32 {
			f = float64(float32(f))
		}
	}
	if err != nil || !finite(f) {
		return scalar{}, fmt.Errorf("comparison literal is outside %s range", typ)
	}
	return floatScalar(f), nil
}

// optionalLiteral returns nil when key is absent.
func optionalLiteral(m map[string]any, key string) (*scalar, error) {
	v, ok := m[key]
	if !ok {
		return nil, nil
	}
	lit, err := literal(v)
	if err != nil {
		return nil, err
	}
	return &lit, nil
}
