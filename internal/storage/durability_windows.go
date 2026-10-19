//go:build windows

package storage

import (
	"os"

	"golang.org/x/sys/windows"
)

func syncDirectory(name string) error {
	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return err
	}
	handle, err := windows.CreateFile(path, windows.GENERIC_READ|windows.GENERIC_WRITE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_WRITE_THROUGH, 0)
	if err != nil {
		return &os.PathError{Op: "open directory for sync", Path: name, Err: err}
	}
	flushErr := windows.FlushFileBuffers(handle)
	closeErr := windows.CloseHandle(handle)
	if flushErr != nil {
		return &os.PathError{Op: "sync directory", Path: name, Err: flushErr}
	}
	if closeErr != nil {
		return &os.PathError{Op: "close directory", Path: name, Err: closeErr}
	}
	return nil
}
