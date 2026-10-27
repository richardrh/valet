//go:build unix

package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	err = directory.Sync()
	if closeErr := directory.Close(); closeErr != nil && err == nil {
		return closeErr
	}
	return err
}

// openLocalInput opens without blocking so a FIFO or device swapped in for a
// regular file is rejected by checkedLocalInput instead of hanging the open.
func openLocalInput(name string, directory bool) (*os.File, error) {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NONBLOCK
	if directory {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Open(name, flags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	return checkedLocalInput(os.NewFile(uintptr(fd), name), directory)
}
