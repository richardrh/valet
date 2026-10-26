//go:build linux

// Package rss measures process residency separately from managed reservations.
package rss

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Snapshot is the process's current and peak resident set size in bytes.
type Snapshot struct {
	Current uint64 `json:"current_bytes"`
	Peak    uint64 `json:"peak_bytes"`
}

const kib = 1024

// Read parses VmRSS (current) and VmHWM (peak) from /proc/self/status.
func Read() (Snapshot, error) {
	body, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return Snapshot{}, err
	}
	var result Snapshot
	for _, line := range strings.Split(string(body), "\n") {
		// Lines of interest look like "VmRSS:	   12345 kB".
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		if fields[0] != "VmRSS:" && fields[0] != "VmHWM:" {
			continue
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return Snapshot{}, err
		}
		if fields[2] != "kB" {
			return Snapshot{}, fmt.Errorf("unexpected RSS units %s", fields[2])
		}
		if fields[0] == "VmRSS:" {
			result.Current = n * kib
		} else {
			result.Peak = n * kib
		}
	}
	if result.Current == 0 {
		return Snapshot{}, fmt.Errorf("process RSS unavailable")
	}
	return result, nil
}
