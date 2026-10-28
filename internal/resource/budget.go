// Package resource accounts for live managed allocations, not process RSS.
package resource

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ResourceLimitError reports a refused allocation before it takes place.
type ResourceLimitError struct {
	Phase     string
	Limit     int64
	Used      int64
	Requested int64
}

func (e *ResourceLimitError) Error() string {
	return fmt.Sprintf("resource limit: phase=%s limit=%d used=%d requested=%d", e.Phase, e.Limit, e.Used, e.Requested)
}

// Budget is safe for concurrent reservation and release. A reservation must
// include temporary copies during growth, not only the final retained size.
type Budget struct {
	mu        sync.Mutex
	limit     int64
	used      int64
	peak      int64
	allocator *allocator
}

func New(limit int64) *Budget {
	if limit < 0 {
		panic("resource: negative budget")
	}
	b := &Budget{limit: limit}
	b.allocator = &allocator{budget: b, base: memory.NewGoAllocator()}
	return b
}
func (b *Budget) Reserve(n int64, phase string) error {
	if n < 0 {
		return fmt.Errorf("resource: negative reservation for %s", phase)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.limit-b.used {
		return &ResourceLimitError{Phase: phase, Limit: b.limit, Used: b.used, Requested: n}
	}
	b.used += n
	if b.used > b.peak {
		b.peak = b.used
	}
	return nil
}
func (b *Budget) Release(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || n > b.used {
		panic("resource: unbalanced release")
	}
	b.used -= n
}

// Reservation is a held charge that records its own size, so the release can
// never drift from the reserve. Release is idempotent; the zero value is an
// empty reservation whose Release does nothing.
type Reservation struct {
	budget *Budget
	n      int64
}

// Hold reserves n bytes for phase and returns a handle owning that charge.
func (b *Budget) Hold(n int64, phase string) (Reservation, error) {
	if err := b.Reserve(n, phase); err != nil {
		return Reservation{}, err
	}
	return Reservation{budget: b, n: n}, nil
}

// Release returns the held charge once; later calls do nothing.
func (r *Reservation) Release() {
	if r.budget != nil {
		r.budget.Release(r.n)
		r.budget, r.n = nil, 0
	}
}
func (b *Budget) Used() int64                 { b.mu.Lock(); defer b.mu.Unlock(); return b.used }
func (b *Budget) Peak() int64                 { b.mu.Lock(); defer b.mu.Unlock(); return b.peak }
func (b *Budget) Limit() int64                { return b.limit }
func (b *Budget) Allocator() memory.Allocator { return b.allocator }

// Arrow's Allocator cannot return an error. Only a refused managed allocation
// panics with ResourceLimitError; callers recover this specific type at a run
// or worker boundary, never suppress arbitrary panics.
type allocator struct {
	budget *Budget
	base   memory.Allocator
}

const alignmentOverhead int64 = 64

func (a *allocator) Allocate(size int) []byte {
	if size < 0 {
		panic("resource: negative Arrow allocation")
	}
	if size == 0 {
		return nil
	}
	n := int64(size)
	if n > int64(^uint64(0)>>1)-alignmentOverhead {
		panic(&ResourceLimitError{Phase: "arrow", Limit: a.budget.Limit(), Used: a.budget.Used(), Requested: n})
	}
	n += alignmentOverhead
	if err := a.budget.Reserve(n, "arrow"); err != nil {
		panic(err)
	}
	return a.base.Allocate(size)
}
func (a *allocator) Reallocate(size int, b []byte) []byte {
	if size < 0 {
		panic("resource: negative Arrow reallocation")
	}
	if size == 0 {
		a.Free(b)
		return nil
	}
	if size <= cap(b) {
		return b[:size]
	}
	// Reserve the complete new allocation while the original is still live.
	// A failed growth leaves the original allocation and its reservation intact.
	next := a.Allocate(size)
	copy(next, b)
	a.Free(b)
	return next
}
func (a *allocator) Free(b []byte) {
	if cap(b) == 0 {
		return
	}
	a.base.Free(b)
	a.budget.Release(int64(cap(b)) + alignmentOverhead)
}
