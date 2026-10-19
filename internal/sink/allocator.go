package sink

import (
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/richardrh/valet/internal/resource"
)

// A Parquet writer may fail partway through construction or footer encoding,
// before exposing a usable Close method. The part owns all encoder allocations
// so abort can release them even on those paths, without invoking another
// allocating encoder operation while the budget is already exhausted.
type encoderAllocator struct {
	base   memory.Allocator
	budget *resource.Budget
	live   map[*byte][]byte
	slots  int
	charge int64
}

func newEncoderAllocator(budget *resource.Budget) *encoderAllocator {
	return &encoderAllocator{base: budget.Allocator(), budget: budget, live: make(map[*byte][]byte)}
}
func (a *encoderAllocator) slot() {
	if len(a.live) < a.slots {
		return
	}
	slots := max(8, a.slots*2)
	charge := int64(slots-a.slots) * allocationSlotCharge
	if err := a.budget.Reserve(charge, "Parquet allocation tracking"); err != nil {
		panic(err)
	}
	a.charge += charge
	a.slots = slots
}
func (a *encoderAllocator) Allocate(size int) []byte {
	if size == 0 {
		return nil
	}
	a.slot()
	b := a.base.Allocate(size)
	a.live[&b[0]] = b
	return b
}
func (a *encoderAllocator) Reallocate(size int, b []byte) []byte {
	if len(b) == 0 {
		return a.Allocate(size)
	}
	key := &b[0]
	out := a.base.Reallocate(size, b)
	delete(a.live, key)
	if len(out) > 0 {
		a.live[&out[0]] = out
	}
	return out
}
func (a *encoderAllocator) Free(b []byte) {
	if len(b) == 0 {
		return
	}
	key := &b[0]
	if _, ok := a.live[key]; !ok {
		return
	}
	delete(a.live, key)
	a.base.Free(b)
}
func (a *encoderAllocator) release() {
	if a == nil {
		return
	}
	for key, b := range a.live {
		a.base.Free(b)
		delete(a.live, key)
	}
	a.budget.Release(a.charge)
	a.charge = 0
	a.slots = 0
}
