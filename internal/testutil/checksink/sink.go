// Package checksink verifies streaming engine output without retaining rows.
package checksink

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"time"

	"github.com/richardrh/valet/internal/data"
)

// Sink tallies rows per bucket and issues per rule code, and folds every row's
// provenance and issues into an order-sensitive digest. A non-zero Delay is
// slept once per written batch to simulate a slow consumer.
type Sink struct {
	Rows    uint64
	Counts  map[string]uint64
	Rules   map[string]uint64
	digest  hash.Hash
	scratch [256]byte
	Delay   time.Duration
}

func New() *Sink {
	return &Sink{Counts: map[string]uint64{}, Rules: map[string]uint64{}, digest: sha256.New()}
}

// Write checks a batch arrives in global row order with one valid bucket per
// row and issues grouped under their rows, then records it.
func (s *Sink) Write(batch *data.Batch, result *data.Result) error {
	if len(result.Buckets) != int(batch.Record.NumRows()) {
		return fmt.Errorf("bucket count %d != rows %d", len(result.Buckets), batch.Record.NumRows())
	}
	if s.Delay > 0 {
		time.Sleep(s.Delay)
	}
	issueIndex := 0
	for i, bucket := range result.Buckets {
		location := batch.Locations[i]
		if location.GlobalRow != s.Rows+1 {
			return fmt.Errorf("out of order row %d after %d", location.GlobalRow, s.Rows)
		}
		if !bucket.Valid() {
			return fmt.Errorf("invalid bucket %d", bucket)
		}
		name := bucket.String()
		s.Rows++
		s.Counts[name]++
		s.number(location.GlobalRow)
		s.number(location.SourceOrdinal)
		s.number(location.Row)
		s.text(location.Source)
		s.text(name)
		for issueIndex < len(result.Issues) && result.Issues[issueIndex].GlobalRow == location.GlobalRow {
			issue := result.Issues[issueIndex]
			s.Rules[issue.Code]++
			s.number(issue.GlobalRow)
			s.text(issue.Code)
			s.text(issue.Column)
			s.text(issue.Severity)
			s.text(issue.Origin)
			issueIndex++
		}
		s.text("end-row")
	}
	if issueIndex != len(result.Issues) {
		return fmt.Errorf("issue provenance out of order or outside batch")
	}
	return nil
}

func (s *Sink) number(n uint64) {
	binary.LittleEndian.PutUint64(s.scratch[:8], n)
	_, _ = s.digest.Write(s.scratch[:8])
}

// text writes v length-prefixed so adjacent strings cannot collide in the digest.
func (s *Sink) text(v string) {
	s.number(uint64(len(v)))
	for len(v) > 0 {
		n := copy(s.scratch[:], v)
		_, _ = s.digest.Write(s.scratch[:n])
		v = v[n:]
	}
}

// Digest is the hex SHA-256 of everything written so far.
func (s *Sink) Digest() string { return fmt.Sprintf("%x", s.digest.Sum(nil)) }
