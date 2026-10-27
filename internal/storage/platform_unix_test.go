//go:build unix

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/richardrh/valet/internal/config"
	"golang.org/x/sys/unix"
)

// fifoOpenTimeout bounds how long a regressed, blocking FIFO open may hang.
const fifoOpenTimeout = 2 * time.Second

func rejectFIFOWithoutBlocking(t *testing.T, name string, operation func() error) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- operation() }()
	select {
	case err := <-result:
		if err == nil {
			t.Fatalf("operation on FIFO %s err = nil, want file type rejection", name)
		}
	case <-time.After(fifoOpenTimeout):
		// Release a regressed blocking reader so the failing test cannot strand it.
		fd, err := unix.Open(name, unix.O_WRONLY|unix.O_NONBLOCK, 0)
		if err == nil {
			unix.Close(fd)
		}
		t.Fatalf("operation on FIFO %s blocked for %v, want immediate file type rejection", name, fifoOpenTimeout)
	}
}

func TestLocalReplacementWithFIFOIsRejected(t *testing.T) {
	store, _ := testStore(t, 1<<20, "")
	name := filepath.Join(t.TempDir(), "input.csv")
	fixture(t, name, "value\n")
	obj, err := store.Stat(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(name); err != nil {
		t.Fatal(err)
	}
	if err = unix.Mkfifo(name, 0600); err != nil {
		t.Fatal(err)
	}

	t.Run("Open", func(t *testing.T) {
		rejectFIFOWithoutBlocking(t, name, func() error {
			reader, err := store.Open(context.Background(), obj)
			if reader != nil {
				t.Errorf("Open reader = %v, want nil for rejected FIFO", reader)
			}
			return err
		})
	})
	t.Run("OpenAt", func(t *testing.T) {
		rejectFIFOWithoutBlocking(t, name, func() error {
			reader, err := store.OpenAt(context.Background(), obj, ReadVerified)
			if reader != nil {
				t.Errorf("OpenAt reader = %v, want nil for rejected FIFO", reader)
			}
			return err
		})
	})
}

func TestLocalFIFOPrefixIsRejected(t *testing.T) {
	store, _ := testStore(t, 1<<20, "")
	name := filepath.Join(t.TempDir(), "prefix")
	if err := unix.Mkfifo(name, 0600); err != nil {
		t.Fatal(err)
	}
	rejectFIFOWithoutBlocking(t, name, func() error {
		_, err := store.Discover(context.Background(), config.Source{Prefix: name, Filter: "csv"})
		return err
	})
}
