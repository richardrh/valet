package config

import (
	"fmt"
	"strings"
)

// Values of options that the command line can also set. Configuration
// validation, the JSON Schema and the CLI flags all read these lists.
var (
	OutputModes               = []string{"all", "failures", "report"}
	ParquetStatisticsPolicies = []string{"off", "trusted"}
)

// OrList renders allowed values as "a, b or c".
func OrList(values []string) string {
	if len(values) < 2 {
		return strings.Join(values, "")
	}
	return strings.Join(values[:len(values)-1], ", ") + " or " + values[len(values)-1]
}

// typeSpellings maps every accepted (lower-case) schema/check type spelling to
// its canonical name. Sized names follow C's fixed-width integers without
// _t and Arrow/Parquet; bare names follow Python, so int and float are
// 64-bit (float is 32-bit in C, Arrow and Parquet, hence float32/double).
var typeSpellings = map[string]string{
	"null": "null",
	"bool": "bool", "boolean": "bool",
	"int": "int64", "integer": "int64", "int64": "int64",
	"int8": "int8", "int16": "int16", "int32": "int32",
	"uint8": "uint8", "uint16": "uint16", "uint32": "uint32", "uint64": "uint64",
	"float": "float64", "double": "float64", "float64": "float64",
	"float32": "float32",
	"str":     "string", "string": "string", "utf8": "string",
	"large_string": "string", "large_utf8": "string",
	"binary": "binary", "bytes": "binary", "large_binary": "binary",
	"date": "date", "date32": "date", "date64": "date",
	"timestamp": "timestamp", "datetime": "timestamp",
	"decimal": "decimal", "decimal128": "decimal",
}

// CanonicalType returns the single internal spelling for every supported
// schema/check type name. Names are case-insensitive.
func CanonicalType(name string) (string, bool) {
	canonical, ok := typeSpellings[strings.ToLower(name)]
	return canonical, ok
}

// typeHints answers names from C that Valet does not guess, because their
// size or sign differs between platforms or they hide the size.
var typeHints = map[string]string{
	"long":               "long is 32-bit on Windows and 64-bit on Linux and macOS; write int32 or int64",
	"unsigned long":      "unsigned long is 32-bit on Windows and 64-bit on Linux and macOS; write uint32 or uint64",
	"long long":          "write int64",
	"unsigned long long": "write uint64",
	"short":              "write int16",
	"unsigned short":     "write uint16",
	"unsigned":           "write uint32",
	"unsigned int":       "write uint32",
	"char":               "char may be signed or unsigned; write int8 or uint8, or str for text",
	"signed char":        "write int8",
	"unsigned char":      "write uint8",
	"long double":        "long double is not supported; write double (64-bit)",
}

// unsupportedType explains a type name Valet does not accept, suggesting the
// spelling to write where one is known.
func unsupportedType(what, name string) error {
	lower := strings.Join(strings.Fields(strings.ToLower(name)), " ")
	hint := typeHints[lower]
	if trimmed, ok := strings.CutSuffix(lower, "_t"); ok && hint == "" {
		if _, known := typeSpellings[trimmed]; known {
			hint = "write " + trimmed
		}
	}
	if hint == "" {
		return fmt.Errorf("%s %q is not a supported type", what, name)
	}
	return fmt.Errorf("%s %q is not a supported type: %s", what, name, hint)
}

var dateDirectives = map[string]string{
	"Y": "2006", "y": "06", "m": "01", "d": "02", "e": "_2",
	"H": "15", "I": "03", "M": "04", "S": "05", "f": "000000",
	"z": "-0700", ":z": "-07:00", "Z": "MST", "b": "Jan", "h": "Jan",
	"B": "January", "p": "PM", "j": "002", "a": "Mon", "A": "Monday",
	"F": "2006-01-02", "T": "15:04:05", "R": "15:04", "D": "01/02/06",
	"%": "%",
}

// DateLayout validates and translates the supported strftime subset to a Go
// time layout. Multi-byte directives are consumed atomically.
func DateLayout(format string) (string, error) {
	if format == "" {
		return "", fmt.Errorf("date format must not be empty")
	}
	var out strings.Builder
	out.Grow(len(format))
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			out.WriteByte(format[i])
			continue
		}
		i++
		if i >= len(format) {
			return "", fmt.Errorf("date format ends with incomplete directive")
		}
		directive := string(format[i])
		if format[i] == ':' {
			if i+1 >= len(format) || format[i+1] != 'z' {
				return "", fmt.Errorf("unsupported date directive %%:%c", format[min(i+1, len(format)-1)])
			}
			directive = ":z"
			i++
		}
		layout, ok := dateDirectives[directive]
		if !ok {
			return "", fmt.Errorf("unsupported date directive %%%s", directive)
		}
		out.WriteString(layout)
	}
	return out.String(), nil
}
