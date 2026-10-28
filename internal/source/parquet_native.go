package source

//lint:file-ignore ST1005 Error strings here begin with the proper nouns Parquet or Arrow.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/richardrh/valet/internal/data"
)

// nextNativeParquet keeps already compatible column buffers in Arrow. Genuine
// conversions (including trimming and changed types) use the lexical converter.
// handled=false leaves the opened reader untouched for that fallback.
func (r *datasetReader) nextNativeParquet(ctx context.Context) (batch *data.Batch, handled bool, err error) {
	d := r.d
	var charge int64
	complete := false
	defer func() {
		if !complete {
			if batch != nil {
				batch.Release()
			}
			d.budget.Release(charge)
			if closeErr := r.Close(); closeErr != nil && (err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				err = closeErr
			}
		}
	}()
	for {
		if err = ctx.Err(); err != nil {
			return nil, true, err
		}
		if r.input == nil {
			if r.object >= len(d.objects) {
				complete = true
				return nil, true, io.EOF
			}
			input, e := openParquet(ctx, d, d.objects[r.object], true)
			if e != nil {
				return nil, true, e
			}
			r.input = input
			r.row = 0
		}
		p, ok := r.input.(*parquetInput)
		if !ok {
			complete = true
			return nil, false, nil
		}
		// Lexical conversion can cross an object boundary and stop partway
		// through this reader's current record. Drain it before advancing:
		// reader.Next would discard the unread rows.
		if p.record != nil && p.row < int(p.record.NumRows()) {
			complete = true
			return nil, false, nil
		}
		// Bind once per physical input, retaining renamed output metadata.
		if !p.nativeBound {
			if err = p.reserve(int64(len(d.columns))*16, "native Parquet column binding"); err != nil {
				return nil, true, err
			}
			p.nativeIndices = make([]int, len(d.columns))
			p.nativeCompatible = true
			for i, f := range d.columns {
				original, e := d.original(f.name)
				if e != nil {
					return nil, true, e
				}
				found := p.schema.FieldIndices(original)
				if len(found) != 1 || !arrow.TypeEqual(p.schema.Field(found[0]).Type, f.typ) || f.options.Trim || len(f.options.Formats) > 0 {
					p.nativeCompatible = false
					break
				}
				p.nativeIndices[i] = found[0]
			}
			p.nativeBound = true
		}
		if !p.nativeCompatible {
			complete = true
			return nil, false, nil
		}
		indices := p.nativeIndices
		if !p.advance(ctx) {
			if e := p.reader.Err(); e != nil {
				return nil, true, e
			}
			e := p.close()
			r.input = nil
			if e != nil {
				return nil, true, e
			}
			r.object++
			continue
		}
		if p.evidence != nil {
			batch, err = r.takeParquetEvidence(ctx, p)
			if err != nil {
				return nil, true, err
			}
			complete = true
			return batch, true, nil
		}
		rec := p.reader.RecordBatch()
		rows := int(rec.NumRows())
		if rows == 0 {
			continue
		}
		rowEnd, e := data.CheckedAdd(r.row, uint64(rows))
		if e != nil {
			return nil, true, e
		}
		globalEnd, e := data.CheckedAdd(r.global, uint64(rows))
		if e != nil {
			return nil, true, e
		}
		reserve := func(n int64, phase string) error {
			if e := d.budget.Reserve(n, phase); e != nil {
				return e
			}
			charge += n
			return nil
		}
		if err = reserve(int64(rows)*(locationBytes+16)+int64(len(indices))*512+1024, "native Parquet batch metadata"); err != nil {
			return nil, true, err
		}
		batch = &data.Batch{Locations: make([]data.Location, rows)}
		columns := make([]arrow.Array, len(indices))
		replacements := make([]arrow.Array, 0)
		defer func() {
			for _, col := range replacements {
				col.Release()
			}
		}()
		for i, index := range indices {
			columns[i] = rec.Column(index)
		}
		for row := range rows {
			if err = ctx.Err(); err != nil {
				return nil, true, err
			}
			loc := data.Location{Source: d.objects[r.object].URI, SourceOrdinal: uint64(r.object + 1), Row: r.row + uint64(row) + 1, GlobalRow: r.global + uint64(row) + 1,
				RowGroup: p.groupCoordinate, RowInGroup: p.groupRow + uint64(row) + 1, PositionBasis: r.positionBasis()}
			batch.Locations[row] = loc
			if err = validateParquetRowWidth(rec, row, d.recordLimit); err != nil {
				return nil, true, err
			}
			for i, col := range columns {
				f := d.columns[i]
				reason, code := "", ""
				if col.IsNull(row) {
					if f.options.Nullable != nil && !*f.options.Nullable {
						code = "INGEST_NULL"
						reason = "null is forbidden"
					}
				} else {
					invalid, why := false, ""
					switch a := col.(type) {
					case *array.Float32:
						v := float64(a.Value(row))
						invalid, why = math.IsNaN(v) || math.IsInf(v, 0), "non-finite number"
					case *array.Float64:
						v := a.Value(row)
						invalid, why = math.IsNaN(v) || math.IsInf(v, 0), "non-finite number"
					case *array.String:
						// Arrow does not validate Parquet string bytes; match the lexical converter.
						invalid, why = !utf8.ValidString(a.Value(row)), "invalid UTF-8 string"
					case *array.Decimal128:
						invalid, why = !a.Value(row).FitsInPrecision(a.DataType().(*arrow.Decimal128Type).Precision), "decimal exceeds declared precision"
					}
					if invalid {
						if d.cfg.Input.Schema == nil || (d.cfg.Input.OnTypeError == "abort" && !r.preflight) {
							return nil, true, fmt.Errorf("source %d row %d column %s: %s", r.object+1, loc.Row, f.name, why)
						}
						code = "INGEST_TYPE"
						reason = why
						if batch.States == nil {
							batch.States = make(map[string][]data.State)
						}
						states := batch.States[f.name]
						if states == nil {
							if err = reserve(int64(rows)+256, "native Parquet invalid mask"); err != nil {
								return nil, true, err
							}
							states = make([]data.State, rows)
							for j := range rows {
								if col.IsNull(j) {
									states[j] = data.Null
								}
							}
							batch.States[f.name] = states
						}
						states[row] = data.Invalid
					}
				}
				if code != "" {
					if err = reserve(512, "native Parquet ingestion issue"); err != nil {
						return nil, true, err
					}
					batch.Ingest = append(batch.Ingest, data.Issue{Location: loc, Code: code, Severity: "error", Origin: "ingest", Scope: "row", Column: f.name, Message: reason, Reason: reason})
				}
			}
		}
		// Only columns with invalid values need replacement buffers.
		for i, f := range d.columns {
			states := batch.States[f.name]
			if states == nil {
				continue
			}
			builder := array.NewBuilder(d.budget.Allocator(), f.typ)
			func() {
				defer builder.Release()
				for row, state := range states {
					if state != data.Valid {
						builder.AppendNull()
						continue
					}
					switch b := builder.(type) {
					case *array.Float32Builder:
						b.Append(columns[i].(*array.Float32).Value(row))
					case *array.Float64Builder:
						b.Append(columns[i].(*array.Float64).Value(row))
					case *array.StringBuilder:
						b.Append(columns[i].(*array.String).Value(row))
					case *array.Decimal128Builder:
						b.Append(columns[i].(*array.Decimal128).Value(row))
					default:
						panic(fmt.Sprintf("source: no native replacement for %s", f.typ))
					}
				}
				col := builder.NewArray()
				replacements = append(replacements, col)
				columns[i] = col
			}()
		}
		if len(batch.Ingest) > 0 {
			if err = reserve(int64(rows)*24, "native Parquet reject references"); err != nil {
				return nil, true, err
			}
			batch.Raw = make([]json.RawMessage, rows)
			for _, issue := range batch.Ingest {
				row := int(issue.Row - r.row - 1)
				if batch.Raw[row] != nil {
					continue
				}
				// Account JSON escaping and transient encoded-cell copies before encoding.
				size := int64(128)
				for i := range int(rec.NumCols()) {
					a := rec.Column(i)
					size += int64(len(rec.ColumnName(i))*6 + 256)
					switch v := a.(type) {
					case *array.String:
						size += int64(v.ValueLen(row)) * 12
					case *array.Binary:
						size += int64(v.ValueLen(row)) * 12
					case *array.FixedSizeBinary:
						size += int64(len(v.Value(row))) * 12
					}
				}
				if err = reserve(size, "native Parquet reject record"); err != nil {
					return nil, true, err
				}
				raw, e := nativeRawRecord(rec, row)
				if e != nil {
					return nil, true, e
				}
				batch.Raw[row] = raw
			}
		}
		batch.Record = array.NewRecordBatch(d.schema, columns, int64(rows))
		batch.Cleanup = func() { d.budget.Release(charge) }
		p.record = rec
		p.row = rows
		r.row, r.global = rowEnd, globalEnd
		p.groupRow += uint64(rows)
		complete = true
		return batch, true, nil
	}
}

func validateParquetRowWidth(record arrow.RecordBatch, row int, limit int64) error {
	var width int64
	for i, column := range record.Columns() {
		field := record.Schema().Field(i)
		width += int64(len(field.Name)+parquetCellOverhead) + parquetValueWidth(column, row)
		if width > limit {
			return errors.New("Parquet row exceeds max_record_bytes")
		}
	}
	return nil
}

func nativeRawRecord(rec arrow.RecordBatch, row int) (json.RawMessage, error) {
	out := []byte{'{'}
	for i := range int(rec.NumCols()) {
		if i > 0 {
			out = append(out, ',')
		}
		key, _ := json.Marshal(rec.ColumnName(i))
		out = append(out, key...)
		out = append(out, ':')
		a := rec.Column(i)
		if a.IsNull(row) {
			out = append(out, "null"...)
			continue
		}
		var value any = a.GetOneForMarshal(row)
		switch v := a.(type) {
		case *array.Decimal128:
			value = json.Number(v.ValueStr(row))
		case *array.Float32:
			f := float64(v.Value(row))
			if math.IsNaN(f) || math.IsInf(f, 0) {
				value = v.ValueStr(row)
			}
		case *array.Float64:
			f := v.Value(row)
			if math.IsNaN(f) || math.IsInf(f, 0) {
				value = v.ValueStr(row)
			}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded...)
	}
	return append(out, '}'), nil
}
