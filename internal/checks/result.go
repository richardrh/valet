package checks

import (
	"fmt"
	"math"
	"sync"

	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
)

// A cache retains at most one released result. Outstanding results remain
// independently charged and never alias either the cache or another result.
const resultStorageBytes = 256

type resultCache struct {
	mu     sync.Mutex
	budget *resource.Budget
	idle   *resultStorage
	closed bool
}
type resultStorage struct {
	buckets []data.Bucket
	issues  []data.Issue
}

func (s *resultStorage) charge() int64 {
	return resultStorageBytes + int64(cap(s.buckets))*bytesPerResultRow + int64(cap(s.issues))*bytesPerResultIssue
}
func (c *resultCache) take(rows int) (*resultStorage, error) {
	c.mu.Lock()
	s := c.idle
	c.idle = nil
	c.mu.Unlock()
	if s == nil {
		if err := c.budget.Reserve(resultStorageBytes, "row result storage"); err != nil {
			return nil, err
		}
		s = &resultStorage{}
	}
	if rows > cap(s.buckets) {
		if int64(rows) > math.MaxInt64/bytesPerResultRow {
			c.budget.Release(s.charge())
			return nil, fmt.Errorf("result size overflow")
		}
		n := int64(rows) * bytesPerResultRow
		if err := c.budget.Reserve(n, "row result buckets"); err != nil {
			c.budget.Release(s.charge())
			return nil, err
		}
		next := make([]data.Bucket, rows)
		c.budget.Release(int64(cap(s.buckets)) * bytesPerResultRow)
		s.buckets = next
	} else {
		s.buckets = s.buckets[:rows]
		clear(s.buckets)
	}
	return s, nil
}
func (c *resultCache) appendIssue(s *resultStorage, issue data.Issue) error {
	if len(s.issues) == cap(s.issues) {
		capacity := max(1, cap(s.issues)*2)
		if capacity <= cap(s.issues) || int64(capacity) > int64(math.MaxInt)/bytesPerResultIssue {
			return fmt.Errorf("result size overflow")
		}
		if err := c.budget.Reserve(int64(capacity)*bytesPerResultIssue, "row result issues"); err != nil {
			return err
		}
		next := make([]data.Issue, len(s.issues), capacity)
		copy(next, s.issues)
		c.budget.Release(int64(cap(s.issues)) * bytesPerResultIssue)
		s.issues = next
	}
	s.issues = append(s.issues, issue)
	return nil
}
func (c *resultCache) put(s *resultStorage) {
	clear(s.issues)
	s.issues = s.issues[:0]
	c.mu.Lock()
	if !c.closed && c.idle == nil {
		c.idle = s
		s = nil
	}
	c.mu.Unlock()
	if s != nil {
		c.budget.Release(s.charge())
	}
}
func (c *resultCache) close() {
	c.mu.Lock()
	c.closed = true
	s := c.idle
	c.idle = nil
	c.mu.Unlock()
	if s != nil {
		c.budget.Release(s.charge())
	}
}
