//go:build !unix

package diagnostics

// IgnoreBrokenPipe is unnecessary on platforms without Unix SIGPIPE behavior.
func IgnoreBrokenPipe() {}
