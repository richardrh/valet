//go:build unix

package config

import (
	"context"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLoadRejectsFIFOAndObservesCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipeline.yml")
	if err := unix.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(context.Background(), path)
	assertErrorContains(t, "Load(FIFO)", err, "not a regular file")
}
