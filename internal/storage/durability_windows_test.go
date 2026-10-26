//go:build windows

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsDirectoryHandleCanBeFlushed(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "published")
	if err := os.WriteFile(path, []byte("durable"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SyncParentDirectory(path); err != nil {
		t.Fatalf("flush published directory: %v", err)
	}
}
