//go:build unix

package diagnostics

import (
	"os/signal"
	"syscall"
)

// IgnoreBrokenPipe makes stdout/stderr failures ordinary write errors. It does
// not change SIGINT or SIGTERM registration or cancellation behavior.
func IgnoreBrokenPipe() { signal.Ignore(syscall.SIGPIPE) }
