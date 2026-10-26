//go:build unix

package storage

import "os"

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	err = directory.Sync()
	return errorsJoinClose(err, directory)
}

func errorsJoinClose(err error, file *os.File) error {
	if closeErr := file.Close(); closeErr != nil && err == nil {
		return closeErr
	}
	return err
}
