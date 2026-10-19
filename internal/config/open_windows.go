//go:build windows

package config

import (
	"errors"
	"os"
	"strings"

	"golang.org/x/sys/windows"
)

func openPipelineFile(name string) (*os.File, error) {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, `\\.\`) || strings.HasPrefix(lower, `\\?\pipe\`) {
		return nil, &os.PathError{Op: "open", Path: name, Err: errors.New("pipeline configuration devices and named pipes are not supported")}
	}
	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(path, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	kind, err := windows.GetFileType(handle)
	if err != nil || kind != windows.FILE_TYPE_DISK {
		windows.CloseHandle(handle)
		if err == nil {
			err = errors.New("pipeline configuration is not a disk file")
		}
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	file := os.NewFile(uintptr(handle), name)
	info, err := file.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = errors.New("pipeline configuration is not a regular file")
	}
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}
