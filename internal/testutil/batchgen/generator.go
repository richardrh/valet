// Package batchgen supplies deterministic, replayable Arrow batches for acceptance tests.
package batchgen

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/source"
)

// Options fixes values by seed and ordinal, independently of batch boundaries.
// FailureEvery, NullEvery and DuplicateEvery use one-based row ordinals; zero disables them.
type Options struct {
	Rows           uint64
	BatchRows      int
	Seed           uint64
	StringWidth    int
	NumericOnly    bool
	Cardinality    uint64
	FailureEvery   uint64
	NullEvery      uint64
	DuplicateEvery uint64
	LateFailure    bool
	FileRows       uint64
}

// Schema is id uint64, value int64 and, unless NumericOnly, a nullable text string.
func (o Options) Schema() *arrow.Schema {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}
	if !o.NumericOnly {
		fields = append(fields, arrow.Field{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true})
	}
	return arrow.NewSchema(fields, nil)
}

// fails reports whether the row at the one-based ordinal carries a negative value.
func (o Options) fails(ordinal uint64) bool {
	periodic := o.FailureEvery > 0 && ordinal%o.FailureEvery == 0
	late := o.LateFailure && ordinal == o.Rows
	return periodic || late
}

// Dataset exposes the generator as a replayable source whose readers charge budget.
func (o Options) Dataset(budget *resource.Budget) *source.Dataset {
	return source.NewGenerated(o.Schema(), func(ctx context.Context) (source.Reader, error) {
		return New(o, budget), nil
	})
}

const (
	defaultBatchRows = 1024
	// Estimated Go-heap bytes per generated row for provenance (data.Location),
	// plus a fixed per-batch overhead.
	provenanceBytesPerRow = 96
	provenanceBatchBytes  = 128
)

// Reader streams generated batches; LogicalBytes accumulates what it produced.
type Reader struct {
	options      Options
	budget       *resource.Budget
	row          uint64
	sourceFile   uint64
	sourceURI    string
	closed       bool
	LogicalBytes uint64
}

func New(o Options, budget *resource.Budget) *Reader {
	if o.BatchRows <= 0 {
		o.BatchRows = defaultBatchRows
	}
	return &Reader{options: o, budget: budget}
}

func (r *Reader) Close() error { r.closed = true; return nil }

func (r *Reader) Next(ctx context.Context) (*data.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.closed || r.row >= r.options.Rows {
		return nil, io.EOF
	}
	n := r.options.BatchRows
	if uint64(n) > r.options.Rows-r.row {
		n = int(r.options.Rows - r.row)
	}
	// Provenance is ordinary Go memory and must be accounted separately from Arrow.
	retained := int64(n)*provenanceBytesPerRow + int64(r.options.StringWidth) + provenanceBatchBytes
	if err := r.budget.Reserve(retained, "generated provenance"); err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned {
			r.budget.Release(retained)
		}
	}()
	b := array.NewRecordBuilder(r.budget.Allocator(), r.options.Schema())
	defer b.Release()
	ids := b.Field(0).(*array.Uint64Builder)
	values := b.Field(1).(*array.Int64Builder)
	ids.Reserve(n)
	values.Reserve(n)
	var texts *array.StringBuilder
	if !r.options.NumericOnly {
		texts = b.Field(2).(*array.StringBuilder)
		texts.Reserve(n)
	}
	locations := make([]data.Location, n)
	for i := range n {
		ordinal := r.row + uint64(i) + 1
		id := ordinal
		if r.options.Cardinality > 0 {
			id = (ordinal - 1) % r.options.Cardinality
		}
		if r.options.DuplicateEvery > 0 && ordinal%r.options.DuplicateEvery == 0 {
			id = ordinal - 1
		}
		value := int64(ordinal % 1000)
		if r.options.fails(ordinal) {
			value = -1
		}
		ids.Append(id)
		values.Append(value)
		if texts != nil {
			if r.options.NullEvery > 0 && ordinal%r.options.NullEvery == 0 {
				texts.AppendNull()
			} else {
				texts.Append(Text(r.options, ordinal))
			}
		}
		file, row := uint64(1), ordinal
		if r.options.FileRows > 0 {
			file = (ordinal-1)/r.options.FileRows + 1
			row = (ordinal-1)%r.options.FileRows + 1
		}
		if file != r.sourceFile {
			r.sourceFile = file
			r.sourceURI = fmt.Sprintf("generated://part/%d", file)
		}
		locations[i] = data.Location{
			Source:        r.sourceURI,
			SourceOrdinal: file,
			Row:           row,
			GlobalRow:     ordinal,
		}
	}
	record := b.NewRecordBatch()
	r.row += uint64(n)
	r.LogicalBytes += LogicalBytes(record)
	owned = true
	cleanup := func() { r.budget.Release(retained) }
	return &data.Batch{Record: record, Locations: locations, Cleanup: cleanup}, nil
}

const (
	emptyTextEvery      = 29
	whitespaceTextEvery = 31
	// Payloads exceed StringWidth by up to widthJitter-1 bytes so rows vary in width.
	widthJitter = 17
)

// Text includes fresh ordinal-derived payload, Unicode and empty strings. It is
// intentionally not a repeated constant or dictionary-encoded scale shortcut.
func Text(o Options, ordinal uint64) string {
	if ordinal%emptyTextEvery == 0 {
		return ""
	}
	if ordinal%whitespaceTextEvery == 0 {
		return " \t"
	}
	s := strconv.FormatUint(mix(ordinal^o.Seed), 16) + "/" + strconv.FormatUint(ordinal, 10) + "/λ界"
	width := o.StringWidth
	if width <= len(s) {
		return s
	}
	width += int(ordinal % widthJitter)
	var out strings.Builder
	out.Grow(width + 16)
	out.WriteString(s)
	var scratch [16]byte
	for out.Len() < width {
		encoded := strconv.AppendUint(scratch[:0], mix(ordinal+uint64(out.Len())+o.Seed), 16)
		out.Write(encoded)
	}
	return out.String()[:width]
}

// mix is the SplitMix64 finaliser.
func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// LogicalBytes counts actual data, offset and validity buffer lengths, counting
// a shared buffer once per batch. Allocator capacity and evaluation scratch are excluded.
func LogicalBytes(record arrow.RecordBatch) uint64 {
	seen := map[*memory.Buffer]bool{}
	var bytes uint64
	var visit func(arrow.ArrayData)
	visit = func(d arrow.ArrayData) {
		for _, b := range d.Buffers() {
			if b != nil && !seen[b] {
				seen[b] = true
				bytes += uint64(b.Len())
			}
		}
		for _, child := range d.Children() {
			visit(child)
		}
		if d.DataType().ID() == arrow.DICTIONARY {
			visit(d.Dictionary())
		}
	}
	for _, column := range record.Columns() {
		visit(column.Data())
	}
	return bytes
}
