//go:build windows

package rss

import (
	"fmt"
	"syscall"
	"unsafe"
)

// Snapshot is the process's current and peak resident set size in bytes.
type Snapshot struct {
	Current uint64 `json:"current_bytes"`
	Peak    uint64 `json:"peak_bytes"`
}

// processCounters mirrors the Win32 PROCESS_MEMORY_COUNTERS layout.
type processCounters struct {
	Size             uint32
	PageFaults       uint32
	PeakWorkingSet   uintptr
	WorkingSet       uintptr
	PeakPagedPool    uintptr
	PagedPool        uintptr
	PeakNonPagedPool uintptr
	NonPagedPool     uintptr
	Pagefile         uintptr
	PeakPagefile     uintptr
}

var processMemoryInfo = syscall.NewLazyDLL("psapi.dll").NewProc("GetProcessMemoryInfo")

// Read reports the working set (current) and peak working set of this process.
func Read() (Snapshot, error) {
	var counters processCounters
	counters.Size = uint32(unsafe.Sizeof(counters))
	process, err := syscall.GetCurrentProcess()
	if err != nil {
		return Snapshot{}, err
	}
	ok, _, callErr := processMemoryInfo.Call(
		uintptr(process),
		uintptr(unsafe.Pointer(&counters)),
		uintptr(counters.Size),
	)
	if ok == 0 {
		return Snapshot{}, fmt.Errorf("GetProcessMemoryInfo: %w", callErr)
	}
	return Snapshot{Current: uint64(counters.WorkingSet), Peak: uint64(counters.PeakWorkingSet)}, nil
}
