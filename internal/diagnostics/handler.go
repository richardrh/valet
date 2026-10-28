package diagnostics

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/richardrh/valet/internal/resource"
)

// The workspace and derived-handler slots are fixed, charged by BindBudget, and
// shared across every worker. Bound attributes are encoded immediately so no
// arbitrary LogValuer, error, record or input backing array is retained.
const boundAttributesBytes = 4096

type handlerState struct {
	mu       sync.Mutex
	writer   io.Writer
	json     bool
	level    slog.Leveler
	recorder *Recorder
	buffer   [32768]byte
	derived  [32]Handler
	next     int
	failed   bool
}

// Handler serializes complete records and treats destination errors as
// best-effort diagnostics failures, never application/presentation failures.
type Handler struct {
	state     *handlerState
	bound     [boundAttributesBytes]byte
	n         int
	group     [128]byte
	groupN    int
	truncated bool
}

// NewHandler accepts json or text format. A nil recorder is useful for early
// command-line diagnostics before a run has been allocated.
func NewHandler(writer io.Writer, format string, level slog.Leveler, recorder *Recorder) *Handler {
	if writer == nil {
		writer = io.Discard
	}
	if level == nil {
		level = slog.LevelInfo
	}
	state := &handlerState{writer: writer, json: format == "json", level: level, recorder: recorder}
	h := &Handler{state: state}
	if recorder != nil {
		recorder.mu.Lock()
		recorder.handler = state
		recorder.log = slog.New(h)
		recorder.mu.Unlock()
	}
	return h
}

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.state.level.Level()
}

// derivedHandler returns a fixed slot; excessive logger decoration is bounded
// and explicitly marked rather than retaining an unbounded chain of attributes.
func (h *Handler) derivedHandler() *Handler {
	if h.state.next == len(h.state.derived) {
		return nil
	}
	copy := &h.state.derived[h.state.next]
	h.state.next++
	*copy = *h
	return copy
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	derived := h.derivedHandler()
	if derived == nil {
		return h
	}
	b := recordBuilder{buf: derived.bound[:derived.n], json: h.state.json, capacity: boundAttributesBytes}
	for _, attr := range attrs {
		derived.appendAttr(&b, attr, 0)
	}
	derived.n = len(b.buf)
	derived.truncated = derived.truncated || b.truncated
	return derived
}

func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h.state.mu.Lock()
	defer h.state.mu.Unlock()
	derived := h.derivedHandler()
	if derived == nil {
		return h
	}
	if derived.groupN != 0 && derived.groupN < len(derived.group) {
		derived.group[derived.groupN] = '.'
		derived.groupN++
	}
	name = clipped(name, 64)
	derived.groupN += copy(derived.group[derived.groupN:], name)
	return derived
}

func (h *Handler) Handle(_ context.Context, record slog.Record) error {
	s := h.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return nil
	}
	b := recordBuilder{buf: s.buffer[:0], json: s.json, capacity: len(s.buffer) - 2}
	if s.json {
		b.buf = append(b.buf, '{')
	}
	b.attr("log_version", slog.IntValue(1))
	event := "diagnostic"
	record.Attrs(func(a slog.Attr) bool {
		if a.Key == "event" && a.Value.Kind() == slog.KindString {
			event = clipped(a.Value.String(), 96)
		}
		return true
	})
	b.attr("event", slog.StringValue(event))
	b.attr("level", slog.StringValue(record.Level.String()))
	timestamp := record.Time
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	var timeBuffer [64]byte
	formattedTime := timestamp.UTC().AppendFormat(timeBuffer[:0], time.RFC3339Nano)
	b.attr("time", slog.StringValue(string(formattedTime)))
	b.attr("msg", slog.StringValue(record.Message))
	if r := s.recorder; r != nil {
		r.mu.Lock()
		if r.runID != "" {
			r.seq++
			b.attr("run_id", slog.StringValue(r.runID))
			b.attr("command", slog.StringValue(r.command))
			b.attr("seq", slog.Uint64Value(r.seq))
		}
		r.mu.Unlock()
	}
	if h.n != 0 && len(b.buf)+h.n < b.capacity {
		if !s.json {
			b.buf = append(b.buf, ' ')
		}
		b.buf = append(b.buf, h.bound[:h.n]...)
	}
	count := 0
	record.Attrs(func(a slog.Attr) bool {
		count++
		if count > 64 {
			b.truncated = true
			return false
		}
		h.appendAttr(&b, a, 0)
		return true
	})
	if b.truncated || h.truncated {
		b.attr("attributes_truncated", slog.BoolValue(true))
	}
	if s.json {
		b.buf = append(b.buf, '}')
	}
	b.buf = append(b.buf, '\n')
	n, err := s.writer.Write(b.buf)
	if err != nil || n != len(b.buf) {
		s.failed = true
	}
	return nil
}

func reserved(key string) bool {
	switch key {
	case "log_version", "event", "level", "time", "msg", "run_id", "command", "seq":
		return true
	}
	return false
}

func sensitive(key string) bool {
	key = strings.ToLower(key)
	for _, token := range [...]string{"password", "secret", "credential", "token", "access_key", "min_value", "max_value", "literal", "record", "cell"} {
		if strings.Contains(key, token) {
			return true
		}
	}
	switch key {
	case "message", "min", "max", "value", "values", "minimum", "maximum":
		return true
	}
	return false
}

func (h *Handler) appendAttr(b *recordBuilder, attr slog.Attr, depth int) {
	if attr.Key == "" && attr.Value.Kind() != slog.KindGroup {
		return
	}
	if reserved(attr.Key) || sensitive(attr.Key) {
		return
	}
	value := attr.Value.Resolve()
	if value.Kind() == slog.KindGroup {
		if depth == 4 {
			b.truncated = true
			return
		}
		for i, child := range value.Group() {
			if i == 32 {
				b.truncated = true
				break
			}
			// Flatten structured diagnostics without retaining child arrays.
			if attr.Key != "" {
				child.Key = clipped(attr.Key, 64) + "." + clipped(child.Key, 64)
			}
			h.appendAttr(b, child, depth+1)
		}
		return
	}
	key := clipped(attr.Key, 96)
	if h.groupN != 0 {
		key = string(h.group[:h.groupN]) + "." + key
	}
	if attr.Key == "evidence_fields" && value.Kind() == slog.KindAny {
		if fields, ok := value.Any().([]string); ok {
			b.evidenceFields(key, fields)
			return
		}
	}
	// Operational errors may contain input cells, rule messages or response
	// bodies. Preserve their classification at the callsite, not their text.
	// Only audited public messages and typed InputError fields are shown.
	if strings.EqualFold(attr.Key, "error") {
		original := value
		b.attr("error_class", slog.StringValue(errorClass(value)))
		value = slog.StringValue("operation failed")
		if original.Kind() == slog.KindAny {
			if err, ok := original.Any().(error); ok {
				appendErrorDetails(b, err)
				var public *publicError
				var input *InputError
				switch {
				case errors.As(err, &public):
					value = slog.StringValue(public.message)
				case errors.As(err, &input):
					value = slog.StringValue(input.Error())
				}
			}
		}
	}
	if value.Kind() == slog.KindString {
		text := value.String()
		if attr.Key == "uri" || attr.Key == "source" || attr.Key == "manifest" {
			text = redactURI(clipped(text, 512))
		}
		value = slog.StringValue(clipped(text, 512))
	}
	// Never format arbitrary Any values: String methods can expose whole rows
	// or SDK response bodies. Explicit LogValuers above are resolved first.
	if value.Kind() == slog.KindAny {
		value = slog.StringValue("[redacted]")
	}
	b.attr(key, value)
}

func clipped(text string, size int) string {
	if len(text) > size {
		return text[:size]
	}
	return text
}

func redactURI(text string) string {
	if !strings.Contains(text, "://") {
		if i := strings.IndexAny(text, "?#"); i >= 0 {
			return text[:i]
		}
		return text
	}
	u, err := url.Parse(text)
	if err != nil {
		return "[redacted URI]"
	}
	if u.User == nil && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && !strings.Contains(text, "#") {
		// Nothing to remove; re-serialising would rewrite text such as "s3://".
		return text
	}
	u.User, u.RawQuery, u.ForceQuery, u.Fragment = nil, "", false, ""
	return u.String()
}

type recordBuilder struct {
	buf       []byte
	json      bool
	capacity  int
	truncated bool
}

// key reserves a complete bounded value before writing any part of its member.
func (b *recordBuilder) key(key string, valueBytes int) bool {
	key = clipped(key, 240)
	if len(b.buf)+6*len(key)+valueBytes+8 > b.capacity {
		b.truncated = true
		return false
	}
	if b.json {
		if len(b.buf) != 0 && b.buf[len(b.buf)-1] != '{' {
			b.buf = append(b.buf, ',')
		}
		if len(b.buf) == 0 {
			b.buf = append(b.buf, ',')
		}
		b.buf = appendJSONQuote(b.buf, key)
		b.buf = append(b.buf, ':')
	} else {
		if len(b.buf) != 0 {
			b.buf = append(b.buf, ' ')
		}
		b.buf = append(b.buf, key...)
		b.buf = append(b.buf, '=')
	}
	return true
}

// evidenceFields is the sole array-valued exception to Any redaction. Only
// canonical internal proof-field names are emitted, never arbitrary strings.
func (b *recordBuilder) evidenceFields(key string, fields []string) {
	names := [...]string{"null_count", "nan_count", "min_value", "max_value", "type_defined_order", "schema"}
	var seen uint8
	for i, field := range fields {
		if i == 64 {
			b.truncated = true
			break
		}
		for j, name := range names {
			if field == name {
				seen |= 1 << j
				break
			}
		}
	}
	if !b.key(key, 128) {
		return
	}
	b.buf = append(b.buf, '[')
	first := true
	for i, name := range names {
		if seen&(1<<i) == 0 {
			continue
		}
		if !first {
			b.buf = append(b.buf, ',')
		}
		first = false
		b.buf = appendJSONQuote(b.buf, name)
	}
	b.buf = append(b.buf, ']')
}

func (b *recordBuilder) attr(key string, value slog.Value) {
	// Worst-case JSON quoting expands each byte sixfold; never let the fixed
	// workspace grow or leave a partial member.
	need := 96
	if value.Kind() == slog.KindString {
		need += 6 * len(clipped(value.String(), 512))
	}
	if !b.key(key, need) {
		return
	}
	switch value.Kind() {
	case slog.KindString:
		b.buf = appendJSONQuote(b.buf, clipped(value.String(), 512))
	case slog.KindBool:
		b.buf = strconv.AppendBool(b.buf, value.Bool())
	case slog.KindInt64:
		b.buf = strconv.AppendInt(b.buf, value.Int64(), 10)
	case slog.KindUint64:
		b.buf = strconv.AppendUint(b.buf, value.Uint64(), 10)
	case slog.KindDuration:
		b.buf = strconv.AppendInt(b.buf, int64(value.Duration()), 10)
	case slog.KindFloat64:
		v := value.Float64()
		if math.IsNaN(v) || math.IsInf(v, 0) {
			b.buf = append(b.buf, "null"...)
		} else {
			b.buf = strconv.AppendFloat(b.buf, v, 'g', -1, 64)
		}
	default:
		b.buf = append(b.buf, "null"...)
	}
}

// strconv's Go string quoting can emit non-JSON escapes (such as \xNN).
// This encoder escapes controls and replaces invalid UTF-8 without allocations.
func appendJSONQuote(dst []byte, text string) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	for i := 0; i < len(text); {
		c := text[i]
		switch {
		case c == '"' || c == '\\':
			dst = append(dst, '\\', c)
			i++
		case c < 0x20:
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&15])
			i++
		case c < utf8.RuneSelf:
			dst = append(dst, c)
			i++
		default:
			r, n := utf8.DecodeRuneInString(text[i:])
			if r == utf8.RuneError && n == 1 {
				dst = append(dst, "\\ufffd"...)
				i++
			} else {
				dst = append(dst, text[i:i+n]...)
				i += n
			}
		}
	}
	return append(dst, '"')
}

func errorClass(value slog.Value) string {
	if value.Kind() == slog.KindAny {
		if err, ok := value.Any().(error); ok {
			var limit *resource.ResourceLimitError
			var input *InputError
			switch {
			case errors.As(err, &input):
				return "input_format"
			case errors.As(err, &limit):
				return "resource"
			case errors.Is(err, context.Canceled):
				return "canceled"
			case errors.Is(err, context.DeadlineExceeded):
				return "deadline"
			case errors.Is(err, fs.ErrNotExist):
				return "not_found"
			case errors.Is(err, fs.ErrPermission):
				return "access_denied"
			}
		}
		return "operation"
	}
	if value.Kind() == slog.KindString {
		text := clipped(value.String(), 512)
		switch {
		case strings.Contains(text, "resource limit"):
			return "resource"
		case strings.Contains(text, "context canceled"):
			return "canceled"
		case strings.Contains(text, "deadline exceeded"):
			return "deadline"
		case strings.Contains(text, "no such file"):
			return "not_found"
		case strings.Contains(text, "AccessDenied") || strings.Contains(text, "403"):
			return "access_denied"
		}
	}
	return "operation"
}
