//go:build unix && !linux

package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

// publishLocalNoReplace uses link(2), which fails with EEXIST when target
// exists, so an existing destination always wins. The temporary name is then
// removed; once the link succeeded the target is published, so a failure to
// remove the temporary name only leaves a hidden .valet-* file behind and
// is not reported as a publication failure.
func publishLocalNoReplace(source, target string) error {
	if err := unix.Link(source, target); err != nil {
		return &os.LinkError{Op: "link", Old: source, New: target, Err: err}
	}
	_ = unix.Unlink(source)
	return nil
}
