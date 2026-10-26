package diagnostics

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/richardrh/valet/internal/resource"
)

// classExplanations describe errors whose text is never shown, by class only.
var classExplanations = map[string][2]string{
	"not_found":     {"a file or object was not found", "check the -i/-o locations and the source, reference and mapping files in the pipeline"},
	"access_denied": {"access was denied", "check file permissions, or the S3 credentials and bucket policy"},
	"canceled":      {"the run was canceled", ""},
	"deadline":      {"an operation timed out", ""},
}

// Explain returns a terminal-safe message and optional hint for a failed
// command. It follows the log handler's rules: only audited public messages
// and typed error fields are shown, never arbitrary error text.
func Explain(err error) (message, hint string) {
	var public *publicError
	var input *InputError
	var limit *resource.ResourceLimitError
	switch {
	case err == nil:
		return "", ""
	case errors.As(err, &public):
		message = public.message
	case errors.As(err, &input):
		message, hint = input.Error(), input.Hint()
	case errors.As(err, &limit) && limit != nil:
		message = fmt.Sprintf("memory budget of %d bytes exceeded while reserving %d bytes for %s (%d in use)",
			limit.Limit, limit.Requested, clipped(limit.Phase, 96), limit.Used)
		hint = "raise --memory-budget or execution.memory_budget, or lower --batch-target"
	default:
		class := errorClass(slog.AnyValue(err))
		if known, ok := classExplanations[class]; ok {
			message, hint = known[0], known[1]
		} else {
			message = "operation failed (" + class + ")"
		}
	}
	return printable(clipped(message, 512)), printable(hint)
}

// printable escapes control characters and invalid UTF-8 so a message cannot
// rewrite the terminal.
func printable(text string) string {
	if !strings.ContainsFunc(text, func(r rune) bool { return r == utf8.RuneError || unicode.IsControl(r) }) {
		return text
	}
	var b strings.Builder
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRuneInString(text[i:])
		switch {
		case r == utf8.RuneError && size <= 1:
			fmt.Fprintf(&b, `\x%02x`, text[i])
		case unicode.IsControl(r):
			quoted := strconv.QuoteRune(r)
			b.WriteString(quoted[1 : len(quoted)-1])
		default:
			b.WriteString(text[i : i+size])
		}
		i += size
	}
	return b.String()
}
