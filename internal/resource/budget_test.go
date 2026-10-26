package resource_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/richardrh/valet/internal/resource"
)

// arrowOverhead mirrors the per-allocation alignment overhead the Arrow
// allocator charges on top of each buffer's capacity.
const arrowOverhead = 64

// assertAccounting checks a budget's current and peak usage.
func assertAccounting(t *testing.T, b *resource.Budget, wantUsed, wantPeak int64) {
	t.Helper()
	if got := b.Used(); got != wantUsed {
		t.Errorf("used = %d, want %d", got, wantUsed)
	}
	if got := b.Peak(); got != wantPeak {
		t.Errorf("peak = %d, want %d", got, wantPeak)
	}
}

func TestRefusedReservationDoesNotConsumeBudget(t *testing.T) {
	const (
		limit    = 100
		reserved = 80
		overflow = limit - reserved + 1
	)
	b := resource.New(limit)
	if err := b.Reserve(reserved, "reference"); err != nil {
		t.Fatal(err)
	}

	err := b.Reserve(overflow, "reference")
	var limitErr *resource.ResourceLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("over-limit reserve error = %v, want *ResourceLimitError", err)
	}
	if limitErr.Used != reserved {
		t.Errorf("limit error used = %d, want %d", limitErr.Used, reserved)
	}
	if limitErr.Requested != overflow {
		t.Errorf("limit error requested = %d, want %d", limitErr.Requested, overflow)
	}
	assertAccounting(t, b, reserved, reserved)

	b.Release(reserved)
	if err := b.Reserve(limit, "batch"); err != nil {
		t.Fatal(err)
	}
	b.Release(limit)
	assertAccounting(t, b, 0, limit)
}

func TestHoldReleasesItsOwnChargeOnce(t *testing.T) {
	const (
		limit = 100
		held  = 60
	)
	b := resource.New(limit)
	r, err := b.Hold(held, "writer")
	if err != nil {
		t.Fatal(err)
	}
	if got := b.Used(); got != held {
		t.Fatalf("used after hold = %d, want %d", got, held)
	}
	if _, err := b.Hold(limit-held+1, "writer"); err == nil {
		t.Error("over-limit hold error = nil, want refusal")
	}

	r.Release()
	r.Release()
	if got := b.Used(); got != 0 {
		t.Errorf("used after double release = %d, want 0", got)
	}

	var empty resource.Reservation
	empty.Release() // must not panic
}

func TestArrowGrowthChargesOldAndNewBuffers(t *testing.T) {
	const (
		initialSize = 64
		grownSize   = 128
		marker      = 19
		initialCost = initialSize + arrowOverhead
		grownCost   = grownSize + arrowOverhead
	)
	b := resource.New(400)
	a := b.Allocator()
	buf := a.Allocate(initialSize)
	buf[0] = marker

	grown := a.Reallocate(grownSize, buf)
	if len(grown) != grownSize {
		t.Fatalf("grown len = %d, want %d", len(grown), grownSize)
	}
	if grown[0] != marker {
		t.Errorf("grown[0] = %d, want %d", grown[0], marker)
	}
	// Growth briefly holds both buffers, so the peak covers old and new.
	assertAccounting(t, b, grownCost, initialCost+grownCost)

	shrunk := a.Reallocate(1, grown)
	if got := b.Used(); got != grownCost {
		t.Errorf("used after shrink = %d, want %d (original backing capacity)", got, grownCost)
	}

	a.Free(shrunk)
	if got := b.Used(); got != 0 {
		t.Errorf("used after free = %d, want 0", got)
	}
}

func TestArrowRefusedGrowthLeavesOriginalOwned(t *testing.T) {
	const (
		initialSize = 64
		marker      = 7
		initialCost = initialSize + arrowOverhead
	)
	b := resource.New(200)
	a := b.Allocator()
	buf := a.Allocate(initialSize)
	buf[0] = marker

	func() {
		defer func() {
			p := recover()
			if _, ok := p.(*resource.ResourceLimitError); !ok {
				t.Fatalf("reallocate panic = %v, want *ResourceLimitError", p)
			}
		}()
		_ = a.Reallocate(128, buf)
		t.Fatal("over-budget reallocate returned, want panic")
	}()

	if got := b.Used(); got != initialCost {
		t.Errorf("used after refused growth = %d, want %d", got, initialCost)
	}
	if buf[0] != marker {
		t.Errorf("buf[0] after refused growth = %d, want %d", buf[0], marker)
	}

	a.Free(buf)
	if got := b.Used(); got != 0 {
		t.Errorf("used after free = %d, want 0", got)
	}
}

func TestConcurrentReservationsBalance(t *testing.T) {
	const (
		workers    = 16
		iterations = 100
		chunk      = 128
	)
	b := resource.New(workers * chunk)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				if err := b.Reserve(chunk, "worker"); err != nil {
					t.Error(err)
					return
				}
				b.Release(chunk)
			}
		}()
	}
	wg.Wait()

	if got := b.Used(); got != 0 {
		t.Errorf("used = %d, want 0", got)
	}
	if peak, limit := b.Peak(), b.Limit(); peak > limit {
		t.Errorf("peak = %d, want <= limit %d", peak, limit)
	}
}
