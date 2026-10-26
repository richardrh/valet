package diagnostics

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"testing"

	"github.com/richardrh/valet/internal/resource"
)

// logError emits one JSON record carrying err and returns its fields.
func logError(t *testing.T, err error) (map[string]any, string) {
	t.Helper()
	var out bytes.Buffer
	slog.New(NewHandler(&out, "json", slog.LevelInfo, nil)).Error("command failed", "error", err)
	var record map[string]any
	if e := json.Unmarshal(out.Bytes(), &record); e != nil {
		t.Fatalf("decode %q: %v", out.String(), e)
	}
	return record, out.String()
}

// Errors are shown only when typed or explicitly public; anything else keeps
// its class but never its text.
func TestErrorPresentation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		want   map[string]any
		absent []string
	}{
		{
			name:   "untyped error hides its text",
			err:    fmt.Errorf("convert cell %q", "SECRET-CELL"),
			want:   map[string]any{"error": "operation failed", "error_class": "operation"},
			absent: []string{"SECRET-CELL"},
		},
		{
			name: "public error is shown without URI credentials",
			err:  PublicError(errors.New("source.uris[0]: cannot read s3://user:hunter2@bucket/key?token=abc")),
			want: map[string]any{"error_class": "operation"},
			absent: []string{
				"hunter2", "token=abc", "operation failed",
			},
		},
		{
			name: "wrapped input error is located",
			err:  fmt.Errorf("decode: %w", &InputError{Format: "csv", Reason: ReasonFieldCount, Line: 3, Column: 7}),
			want: map[string]any{
				"error":          "malformed CSV input at line 3, column 7: field count does not match header",
				"error_class":    "input_format",
				"input_format":   "csv",
				"input_line":     3.0,
				"input_column":   7.0,
				"framing_reason": "field_count",
				"input_offset":   nil,
			},
		},
		{
			name: "oversized record reports its offset and limit",
			err:  &InputError{Reason: ReasonRecordTooLarge, Offset: 10, Limit: 64},
			want: map[string]any{
				"error":          "malformed input at byte offset 10: record exceeds max_record_bytes (64)",
				"error_class":    "input_format",
				"input_offset":   10.0,
				"framing_reason": "record_too_large",
				"input_line":     nil,
				"input_format":   nil,
			},
		},
		{
			name: "resource limit keeps typed fields only",
			err:  fmt.Errorf("reserve: %w", &resource.ResourceLimitError{Phase: "CSV header", Limit: 64, Used: 60, Requested: 8}),
			want: map[string]any{"error": "operation failed", "error_class": "resource", "limit_bytes": 64.0, "requested_bytes": 8.0},
		},
		{
			name: "missing file is classified",
			err:  fmt.Errorf("open: %w", fs.ErrNotExist),
			want: map[string]any{"error": "operation failed", "error_class": "not_found"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record, raw := logError(t, tc.err)
			for key, want := range tc.want {
				if record[key] != want {
					t.Errorf("%s = %#v, want %#v", key, record[key], want)
				}
			}
			for _, text := range tc.absent {
				if strings.Contains(raw, text) {
					t.Errorf("record contains %q:\n%s", text, raw)
				}
			}
		})
	}
}

// The terminal explanation follows the log's rules: public and typed errors
// are shown, other errors only by class, and nothing can carry credentials or
// terminal control sequences.
func TestExplain(t *testing.T) {
	located := &InputError{
		Format: "csv", Reason: ReasonFieldCount, Line: 7, Expected: 3, Found: 4, Delimiter: ';',
		Source: "s3://user:hunter2@bucket/in.csv?token=abc",
	}
	for _, tc := range []struct {
		name          string
		err           error
		message, hint string
		absent        []string
	}{
		{
			name:    "located input",
			err:     fmt.Errorf("read: %w", located),
			message: `malformed CSV in s3://bucket/in.csv at line 7: found 4 fields; the header has 3`,
			hint:    `quote any field that contains ";", and check this line for a missing or extra ";"; set input.on_malformed: reject to reject such rows and check the rest of the file`,
			absent:  []string{"hunter2", "token=abc"},
		},
		{
			name:    "control characters in a local path",
			err:     &InputError{Format: "ndjson", Reason: ReasonNotObject, Line: 2, Source: "/data/in\x1b[2J.ndjson"},
			message: `malformed NDJSON in /data/in\x1b[2J.ndjson at line 2: records must be objects`,
			hint:    "each record must be a JSON object",
			absent:  []string{"\x1b"},
		},
		{
			name:    "malformed URI",
			err:     &InputError{Format: "csv", Reason: ReasonBlankLine, Line: 4, Source: "s3://bucket/in\x1b.csv"},
			message: `malformed CSV in [redacted URI] at line 4: blank line`,
			hint:    "remove the empty line",
		},
		{
			name:    "public",
			err:     PublicError(errors.New("execution.max_depth must be between 1 and 1024")),
			message: "execution.max_depth must be between 1 and 1024",
		},
		{
			name:    "resource limit",
			err:     fmt.Errorf("reserve: %w", &resource.ResourceLimitError{Phase: "CSV header", Limit: 64, Used: 60, Requested: 8}),
			message: "memory budget of 64 bytes exceeded while reserving 8 bytes for CSV header (60 in use)",
			hint:    "raise --memory-budget or execution.memory_budget, or lower --batch-target",
		},
		{
			name:    "not found",
			err:     fmt.Errorf("open %q: %w", "SECRET-PATH", fs.ErrNotExist),
			message: "a file or object was not found",
			hint:    "check the -i/-o locations and the source, reference and mapping files in the pipeline",
			absent:  []string{"SECRET-PATH"},
		},
		{
			name:    "untyped",
			err:     fmt.Errorf("convert cell %q", "SECRET-CELL"),
			message: "operation failed (operation)",
			absent:  []string{"SECRET-CELL"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, hint := Explain(tc.err)
			if message != tc.message || hint != tc.hint {
				t.Errorf("Explain() = %q, %q; want %q, %q", message, hint, tc.message, tc.hint)
			}
			for _, text := range tc.absent {
				if strings.Contains(message+hint, text) {
					t.Errorf("explanation contains %q: %q %q", text, message, hint)
				}
			}
		})
	}
	record, raw := logError(t, located)
	if record["input_source"] != "s3://bucket/in.csv" {
		t.Errorf("input_source = %q, want the URI without credentials or query", record["input_source"])
	}
	for _, text := range []string{"hunter2", "token=abc"} {
		if strings.Contains(raw, text) {
			t.Errorf("log record contains %q:\n%s", text, raw)
		}
	}
}
