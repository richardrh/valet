//go:build integration && windows

package integration_test

import (
	"os"
	"testing"
)

// Go cannot deliver CTRL_C_EVENT to an arbitrary child console, so graceful
// interruption (exit 130) is not exercised on Windows. This covers only abrupt
// TerminateProcess: the exit code is chosen by os.Process.Kill, not the CLI, so
// the sole assertion is that nothing terminal was published.
func TestE2E18TerminateDuringObjectRead(t *testing.T) {
	interruptDuringRead(t, (*os.Process).Kill)
}
