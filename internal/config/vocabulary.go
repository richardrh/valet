package config

import (
	"fmt"
	"strings"
)

// CanonicalType returns the single internal spelling for every supported
// schema/check type name.
func CanonicalType(name string) (string, bool) {
	switch strings.ToLower(name) {
	case "null":
		return "null", true
	case "bool", "boolean":
		return "bool", true
	case "int", "integer", "int64":
		return "int64", true
	case "int8", "int16", "int32", "uint8", "uint16", "uint32", "uint64":
		return strings.ToLower(name), true
	case "float", "double", "float64":
		return "float64", true
	case "float32":
		return "float32", true
	case "str", "string", "utf8":
		return "string", true
	case "binary":
		return "binary", true
	case "date", "date32":
		return "date", true
	case "timestamp", "datetime":
		return "timestamp", true
	case "decimal", "decimal128":
		return "decimal", true
	default:
		return "", false
	}
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
