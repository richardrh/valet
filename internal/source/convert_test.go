package source

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/richardrh/valet/internal/config"
)

func TestExactIntegerAndDecimal(t *testing.T) {
	t.Run("integer rejects inexact text", func(t *testing.T) {
		for _, s := range []string{"1.0", "1.5", "9223372036854775808", "true", " 1"} {
			if _, err := parseSigned(s, 64); err == nil {
				t.Errorf("parseSigned(%q) error = nil, want error", s)
			}
		}
	})
	t.Run("integer above 2^53 stays exact", func(t *testing.T) {
		n, err := parseSigned("9007199254740993", 64)
		if err != nil {
			t.Fatalf("parseSigned error = %v, want nil", err)
		}
		if n != 9007199254740993 {
			t.Errorf("parseSigned = %d, want 9007199254740993", n)
		}
	})
	t.Run("decimal accepts exact values", func(t *testing.T) {
		for _, tc := range []struct {
			input, want      string
			precision, scale int32
		}{
			{input: "9007199254740993.10", want: "9007199254740993.10", precision: 20, scale: 2},
			{input: "1.2300", want: "1.23", precision: 6, scale: 2},
			{input: "1.23e2", want: "123.00", precision: 6, scale: 2},
			{input: "-0.00100", want: "-0.001", precision: 6, scale: 3},
			{input: "0e999999", want: "0.00", precision: 4, scale: 2},
		} {
			n, err := parseDecimal(tc.input, tc.precision, tc.scale)
			if err != nil {
				t.Errorf("parseDecimal(%q, %d, %d) error = %v, want nil", tc.input, tc.precision, tc.scale, err)
				continue
			}
			if got := n.ToString(tc.scale); got != tc.want {
				t.Errorf("parseDecimal(%q, %d, %d) = %s, want %s", tc.input, tc.precision, tc.scale, got, tc.want)
			}
		}
	})
	t.Run("decimal(5,2) rejects inexact or out-of-range values", func(t *testing.T) {
		for _, s := range []string{"1.231", "1000", "1e999999", "NaN", "1.2.3"} {
			if _, err := parseDecimal(s, 5, 2); err == nil {
				t.Errorf("parseDecimal(%q, 5, 2) error = nil, want error", s)
			}
		}
	})
}

func TestTimestampOffsetsAndDST(t *testing.T) {
	for _, tc := range []struct {
		name, value, zone string
		valid             bool
	}{
		{name: "UTC designator", value: "2026-01-01T10:00:00Z", valid: true},
		{name: "explicit offset", value: "2026-01-01T11:00:00+01:00", valid: true},
		{name: "no offset and no zone", value: "2026-01-01T10:00:00", valid: false},
		{name: "zone supplies the offset", value: "2026-01-01T10:00:00", zone: "UTC", valid: true},
		{name: "DST gap", value: "2026-03-08T02:30:00", zone: "America/New_York", valid: false},
		{name: "DST overlap", value: "2026-11-01T01:30:00", zone: "America/New_York", valid: false},
		{name: "after DST overlap", value: "2026-11-01T03:30:00", zone: "America/New_York", valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseTimestamp(tc.value, config.Column{Timezone: tc.zone})
			if (err == nil) != tc.valid {
				t.Errorf("ParseTimestamp(%s, zone %q) error = %v, want valid=%v", tc.value, tc.zone, err, tc.valid)
			}
		})
	}
	t.Run("offset is converted to UTC", func(t *testing.T) {
		got, err := ParseTimestamp("2026-01-01T11:00:00+01:00", config.Column{})
		if err != nil {
			t.Fatalf("ParseTimestamp error = %v, want nil", err)
		}
		if s := got.Format(time.RFC3339); s != "2026-01-01T10:00:00Z" {
			t.Errorf("ParseTimestamp = %s, want 2026-01-01T10:00:00Z", s)
		}
	})
}

func TestDate32UsesCivilDay(t *testing.T) {
	builder := array.NewDate32Builder(memory.NewGoAllocator())
	defer builder.Release()
	column := config.Column{Type: "date", Formats: []string{"%Y-%m-%d %H:%M%:z"}}
	// The civil date in the value's own +14:00 offset is 1969-12-31, epoch day -1.
	value := cell{present: true, kind: kindString, text: "1969-12-31 23:30+14:00"}
	if err := appendCell(builder, column, value); err != nil {
		t.Fatal(err)
	}
	values := builder.NewDate32Array()
	defer values.Release()
	if got := values.Value(0); got != -1 {
		t.Fatalf("Date32 = %d, want -1", got)
	}
}

func TestTimestampZoneAbbreviationPolicy(t *testing.T) {
	column := config.Column{Timezone: "America/New_York", Formats: []string{"%Y-%m-%d %H:%M:%S %Z"}}
	t.Run("accepted abbreviations", func(t *testing.T) {
		for _, tc := range []struct{ value, want string }{
			{value: "2026-01-01 10:00:00 UTC", want: "2026-01-01T10:00:00Z"},
			{value: "2026-01-01 10:00:00 EST", want: "2026-01-01T15:00:00Z"},
		} {
			got, err := ParseTimestamp(tc.value, column)
			if err != nil {
				t.Errorf("ParseTimestamp(%s) error = %v, want nil", tc.value, err)
				continue
			}
			if s := got.Format(time.RFC3339); s != tc.want {
				t.Errorf("ParseTimestamp(%s) = %s, want %s", tc.value, s, tc.want)
			}
		}
	})
	t.Run("rejected abbreviations", func(t *testing.T) {
		for _, value := range []string{
			"2026-01-01 10:00:00 EDT", // daylight abbreviation in a standard-time month
			"2026-01-01 10:00:00 XYZ", // unknown abbreviation
		} {
			if _, err := ParseTimestamp(value, column); err == nil {
				t.Errorf("ParseTimestamp(%s) error = nil, want error", value)
			}
		}
	})
}

func TestDatetimeAliasBuildsTimestamp(t *testing.T) {
	typ, err := columnType(config.Column{Type: "datetime", Timezone: "UTC"})
	if err != nil {
		t.Fatalf("columnType(datetime) error = %v, want nil", err)
	}
	if typ.ID() != arrow.TIMESTAMP {
		t.Fatalf("columnType(datetime) = %v, want a timestamp type", typ)
	}
}

func TestConversionDoesNotAppendOnFailure(t *testing.T) {
	t.Run("int64 failure appends nothing", func(t *testing.T) {
		b := array.NewInt64Builder(memory.NewGoAllocator())
		defer b.Release()
		int64Column := config.Column{Type: "int64"}
		if err := appendCell(b, int64Column, cell{present: true, kind: kindNumber, text: "1.5"}); err == nil {
			t.Fatal("appendCell(1.5) error = nil, want conversion failure")
		}
		if b.Len() != 0 {
			t.Fatalf("builder length after failure = %d, want 0", b.Len())
		}
		b.AppendNull()
		if err := appendCell(b, int64Column, cell{present: true, kind: kindNumber, text: "9007199254740993"}); err != nil {
			t.Fatal(err)
		}
		a := b.NewInt64Array()
		defer a.Release()
		if !a.IsNull(0) {
			t.Error("row 0 null = false, want true")
		}
		if got := a.Value(1); got != 9007199254740993 {
			t.Errorf("row 1 = %d, want 9007199254740993", got)
		}
	})
	t.Run("float32 rejects nonfinite and overflow", func(t *testing.T) {
		f := array.NewFloat32Builder(memory.NewGoAllocator())
		defer f.Release()
		for _, s := range []string{"NaN", "Infinity", "3.5e38"} {
			if err := appendCell(f, config.Column{}, cell{kind: kindString, text: s}); err == nil {
				t.Errorf("appendCell(float32, %s) error = nil, want error", s)
			}
		}
	})
	t.Run("nanosecond timestamp overflow", func(t *testing.T) {
		nanos := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
		tb := array.NewTimestampBuilder(memory.NewGoAllocator(), nanos)
		defer tb.Release()
		// int64 nanoseconds only reach year 2262.
		if err := appendCell(tb, config.Column{}, cell{kind: kindString, text: "9999-01-01T00:00:00Z"}); err == nil {
			t.Fatal("appendCell(9999-01-01 as ns timestamp) error = nil, want overflow error")
		}
	})
}
