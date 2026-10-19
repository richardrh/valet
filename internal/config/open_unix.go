//go:build unix

package config

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// openPipelineFile opens without blocking so a FIFO or device is rejected
// instead of hanging configuration loading.
func openPipelineFile(name string) (*os.File, error) {
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = &os.PathError{Op: "open", Path: name, Err: errors.New("pipeline configuration is not a regular file")}
	}
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}
