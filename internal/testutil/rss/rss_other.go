//go:build !linux && !windows

package rss

import "fmt"

// Snapshot is the process's current and peak resident set size in bytes.
type Snapshot struct {
	Current uint64 `json:"current_bytes"`
	Peak    uint64 `json:"peak_bytes"`
}

// Read always fails: RSS is only measured on the Linux and Windows release targets.
func Read() (Snapshot, error) {
	return Snapshot{}, fmt.Errorf("RSS acceptance measurement supports release targets Linux and Windows only")
}
