//go:build integration && !windows

package integration_test

import (
	"os"
	"syscall"
	"testing"
)

// A signal during an in-flight read cancels gracefully with 128+signo and
// publishes no manifest.
func TestE2E18InterruptDuringObjectRead(t *testing.T) {
	cases := []struct {
		signal   os.Signal
		wantExit int
	}{
		{signal: os.Interrupt, wantExit: exitInterrupted},
		{signal: syscall.SIGTERM, wantExit: exitTerminated},
	}
	for _, c := range cases {
		t.Run(c.signal.String(), func(t *testing.T) {
			code := interruptDuringRead(t, func(p *os.Process) error { return p.Signal(c.signal) })
			if code != c.wantExit {
				t.Errorf("%v exit = %d, want %d", c.signal, code, c.wantExit)
			}
		})
	}
}
