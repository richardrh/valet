package checks

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/richardrh/valet/internal/data"
)

// boundColumn is rebound once per batch; operands retain only its stable slot.
// Static readers move the common Arrow type dispatch out of the row loop.
type boundColumn struct {
	state       data.ColumnState
	values      arrow.Array
	read        func(arrow.Array, int) scalar
	cachedRow   int
	cachedValue scalar
	haveValue   bool
	stateRow    int
	cachedState data.State
	haveState   bool
}

func readString(a arrow.Array, row int) scalar { return stringScalar(a.(*array.String).Value(row)) }
func readLargeString(a arrow.Array, row int) scalar {
	return stringScalar(a.(*array.LargeString).Value(row))
}
func readInt64(a arrow.Array, row int) scalar   { return intScalar(a.(*array.Int64).Value(row)) }
func readFloat64(a arrow.Array, row int) scalar { return floatScalar(a.(*array.Float64).Value(row)) }
func readBool(a arrow.Array, row int) scalar    { return boolScalar(a.(*array.Boolean).Value(row)) }

func bindColumn(b *data.Batch, name string, index int) boundColumn {
	out := boundColumn{state: b.BindState(name, index), values: b.Record.Column(index), read: cell}
	switch out.values.(type) {
	case *array.String:
		out.read = readString
	case *array.LargeString:
		out.read = readLargeString
	case *array.Int64:
		out.read = readInt64
	case *array.Float64:
		out.read = readFloat64
	case *array.Boolean:
		out.read = readBool
	}
	return out
}

func (p *Program) bindBatch(b *data.Batch) {
	p.epoch++
	for i, name := range p.columnNames {
		p.columns[i] = bindColumn(b, name, i)
	}
}
func (o operand) state(row int) data.State {
	bound := o.bound
	if !bound.haveState || bound.stateRow != row {
		bound.cachedState = bound.state.State(row)
		bound.stateRow, bound.haveState = row, true
	}
	return bound.cachedState
}
func (o operand) value(row int) scalar {
	bound := o.bound
	if !bound.haveValue || bound.cachedRow != row {
		bound.cachedValue = bound.read(bound.values, row)
		bound.cachedRow, bound.haveValue = row, true
	}
	return bound.cachedValue
}
