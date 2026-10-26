//go:build windows

package config

import (
	"context"
	"testing"
)

func TestLoadRejectsNamedPipeAndDevice(t *testing.T) {
	for _, tc := range []struct{ name, path string }{
		{name: "NUL device", path: `\\.\NUL.yaml`},
		{name: "reserved device name", path: `NUL.yaml`},
		{name: "named pipe", path: `\\?\pipe\valet-test.yaml`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(context.Background(), tc.path)
			if err == nil || cfg != nil {
				t.Fatalf("special-file pipeline accepted: config=%v error=%v", cfg, err)
			}
		})
	}
}
