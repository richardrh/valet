//go:build windows

package storage

import (
	"errors"
	"os"
	"strings"

	"golang.org/x/sys/windows"
)

func openLocalInput(name string, directory bool) (*os.File, error) {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, `\\.\`) || strings.HasPrefix(lower, `\\?\pipe\`) {
		return nil, &os.PathError{Op: "open", Path: name, Err: errors.New("device and named-pipe inputs are not supported")}
	}
	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(path, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	kind, err := windows.GetFileType(handle)
	if err != nil || kind != windows.FILE_TYPE_DISK {
		windows.CloseHandle(handle)
		if err == nil {
			err = errors.New("input is not a disk file")
		}
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	return checkedLocalInput(os.NewFile(uintptr(handle), name), directory)
}

func publishLocalNoReplace(source, target string) error {
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	// Omit MOVEFILE_REPLACE_EXISTING: an existing destination must always win.
	if err = windows.MoveFileEx(from, to, windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return &os.LinkError{Op: "rename", Old: source, New: target, Err: err}
	}
	return nil
}
