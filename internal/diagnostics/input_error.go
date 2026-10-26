package diagnostics

import (
	"strconv"
	"strings"
)

// Reasons for malformed input. They are a fixed vocabulary so the message and
// log fields never carry record contents.
const (
	ReasonFieldCount      = "field_count"
	ReasonQuote           = "quote"
	ReasonBareQuote       = "bare_quote"
	ReasonCSVSyntax       = "csv_syntax"
	ReasonBlankLine       = "blank_line"
	ReasonMissingHeader   = "missing_header"
	ReasonInvalidUTF8     = "invalid_utf8"
	ReasonRecordTooLarge  = "record_too_large"
	ReasonJSONSyntax      = "json_syntax"
	ReasonTruncated       = "truncated"
	ReasonNotArray        = "not_array"
	ReasonNotObject       = "not_object"
	ReasonTrailingData    = "trailing_data"
	ReasonMultipleRecords = "multiple_records"
	ReasonMultilineRecord = "multiline_record"
	ReasonDuplicateKey    = "duplicate_key"
	ReasonTooDeep         = "too_deep"
)

var reasonText = map[string]string{
	ReasonFieldCount:      "field count does not match header",
	ReasonQuote:           `extraneous or missing " in quoted field`,
	ReasonBareQuote:       `bare " in unquoted field`,
	ReasonCSVSyntax:       "invalid CSV syntax",
	ReasonBlankLine:       "blank line",
	ReasonMissingHeader:   "no header",
	ReasonInvalidUTF8:     "invalid UTF-8",
	ReasonJSONSyntax:      "invalid JSON syntax",
	ReasonTruncated:       "unexpected end of input",
	ReasonNotArray:        "top-level value must be an array",
	ReasonNotObject:       "records must be objects",
	ReasonTrailingData:    "trailing data after the top-level array",
	ReasonMultipleRecords: "more than one record on a line",
	ReasonMultilineRecord: "record spans multiple lines",
	ReasonDuplicateKey:    "duplicate object key",
	ReasonTooDeep:         "nesting exceeds max_depth",
}

// InputError is malformed input described only by where it is and a fixed
// reason, so it can be shown to the user without exposing record contents.
type InputError struct {
	Format    string // csv, json or ndjson; empty when not format-specific
	Reason    string // one of the Reason constants
	Source    string // URI of the object being read; redacted when shown
	Line      int64  // 1-based line where the record starts; 0 when unknown
	Column    int64  // 1-based column; 0 when unknown
	Offset    int64  // 0-based byte offset, reported when Line is unknown
	Limit     int64  // max_record_bytes, for ReasonRecordTooLarge
	Expected  int    // header field count, for ReasonFieldCount; 0 when unknown
	Found     int    // record field count, for ReasonFieldCount
	Delimiter rune   // CSV delimiter, for hints; 0 means ','
}

func (e *InputError) Error() string {
	var b strings.Builder
	b.WriteString("malformed ")
	if e.Format != "" {
		b.WriteString(strings.ToUpper(e.Format))
		b.WriteByte(' ')
	}
	if e.Source != "" {
		b.WriteString("in ")
		b.WriteString(redactURI(e.Source))
	} else {
		b.WriteString("input")
	}
	if e.Line > 0 {
		b.WriteString(" at line ")
		b.WriteString(strconv.FormatInt(e.Line, 10))
		if e.Column > 0 {
			b.WriteString(", column ")
			b.WriteString(strconv.FormatInt(e.Column, 10))
		}
	} else {
		b.WriteString(" at byte offset ")
		b.WriteString(strconv.FormatInt(e.Offset, 10))
	}
	b.WriteString(": ")
	switch {
	case e.Reason == ReasonRecordTooLarge:
		b.WriteString("record exceeds max_record_bytes (")
		b.WriteString(strconv.FormatInt(e.Limit, 10))
		b.WriteByte(')')
	case e.Reason == ReasonFieldCount && e.Expected > 0:
		b.WriteString(FieldCountMismatch(e.Found, e.Expected))
	default:
		if text, ok := reasonText[e.Reason]; ok {
			b.WriteString(text)
		} else {
			b.WriteString("unrecognised input")
		}
	}
	return b.String()
}

// FieldCountMismatch describes a record with found fields where the header
// has expected, as used in errors and reject reasons.
func FieldCountMismatch(found, expected int) string {
	noun := " fields"
	if found == 1 {
		noun = " field"
	}
	return "found " + strconv.Itoa(found) + noun + "; the header has " + strconv.Itoa(expected)
}

// Hint is fixed advice for fixing the input; empty when there is none.
func (e *InputError) Hint() string {
	switch e.Reason {
	case ReasonFieldCount:
		d := strconv.QuoteRune(e.delimiter())
		d = `"` + d[1:len(d)-1] + `"`
		return "quote any field that contains " + d + ", and check this line for a missing or extra " + d +
			"; set input.on_malformed: reject to reject such rows and check the rest of the file"
	case ReasonRecordTooLarge:
		if e.Format == "csv" {
			return "raise execution.max_record_bytes, or look for an unclosed quote that joins many lines"
		}
		return "raise execution.max_record_bytes"
	}
	return reasonHints[e.Reason]
}

func (e *InputError) delimiter() rune {
	if e.Delimiter == 0 {
		return ','
	}
	return e.Delimiter
}

var reasonHints = map[string]string{
	ReasonQuote:           `end the quoted field with ", and write a " inside quotes as ""`,
	ReasonBareQuote:       `quote the whole field and write each " inside it as ""`,
	ReasonCSVSyntax:       "check the quoting on this line",
	ReasonBlankLine:       "remove the empty line",
	ReasonMissingHeader:   "the file is empty; add a header line, or set input.header: false and list input.columns",
	ReasonInvalidUTF8:     "save the file as UTF-8",
	ReasonJSONSyntax:      "check the JSON at this position, for example with jq",
	ReasonTruncated:       "the input ends inside a value; check that the file was written completely",
	ReasonNotArray:        "type: json expects one top-level array of objects; use type: ndjson for one object per line",
	ReasonNotObject:       "each record must be a JSON object",
	ReasonTrailingData:    "remove the data after the closing ], or use type: ndjson",
	ReasonMultipleRecords: "put each NDJSON object on its own line",
	ReasonMultilineRecord: "put each NDJSON object on one line, or use type: json for pretty-printed JSON",
	ReasonDuplicateKey:    "use each key once per object",
	ReasonTooDeep:         "raise execution.max_depth, or flatten the nesting",
}
