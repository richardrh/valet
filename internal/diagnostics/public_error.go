package diagnostics

import "strings"

// publicError is an explicit capability to display a caller-audited message.
// Its cause remains available for error classification and exit-code mapping.
type publicError struct {
	cause   error
	message string
}

func (e *publicError) Error() string { return e.message }
func (e *publicError) Unwrap() error { return e.cause }

// PublicError marks an error from CLI parsing or configuration validation as
// safe to present. Callers MUST NOT use this for data/conversion errors, rule
// messages, SDK responses, credentials or arbitrary backend errors. Embedded URI
// userinfo, queries and fragments are still removed, the message is capped, and
// the handler escapes terminal controls. A nil error remains nil.
func PublicError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*publicError); ok {
		return err
	}
	return &publicError{cause: err, message: publicMessage(err.Error())}
}

func publicMessage(text string) string {
	// Only this fixed-size prefix is scanned or retained. A URI truncated in
	// userinfo is either redacted by the parser or replaced as malformed.
	text = clipped(text, 512)
	var result [512]byte
	n := 0
	for len(text) != 0 && n < len(result) {
		scheme := strings.Index(text, "://")
		if scheme < 0 {
			n += copy(result[n:], text)
			break
		}
		start := scheme
		for start > 0 {
			c := text[start-1]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '-' || c == '.') {
				break
			}
			start--
		}
		end := scheme + 3
		for end < len(text) {
			c := text[end]
			if c <= ' ' || c == '"' || c == '\'' || c == '<' || c == '>' || c == ')' || c == ']' || c == '}' {
				break
			}
			end++
		}
		n += copy(result[n:], text[:start])
		n += copy(result[n:], redactURI(text[start:end]))
		text = text[end:]
	}
	return string(result[:n])
}
