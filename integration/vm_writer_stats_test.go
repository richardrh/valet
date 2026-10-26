//go:build integration

package integration_test

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
)

func TestVMWriterStatistics(t *testing.T) {
	t.Run("VM-STAT-19", func(t *testing.T) {
		for _, option := range []string{"default", "none", "bounded"} {
			t.Run(option, func(t *testing.T) {
				root := t.TempDir()
				input := put(t, filepath.Join(root, "input.csv"), "i,u,b,d,f,s\n-9,9223372036854775810,false,-1.25,-0.5,PRIVATE_EXTREMA_A\n7,18446744073709551614,true,2.50,3.25,PRIVATE_EXTREMA_Z\n")
				p := defaultPipeline(input, "csv", filepath.Join(root, "out"), "parquet").with(t, `input:
  schema:
    columns:
      i: {type: int64}
      u: {type: uint64}
      b: {type: bool}
      d: {type: decimal, precision: 12, scale: 2}
      f: {type: float64}
      s: {type: string}
`)
				if option != "default" {
					p.with(t, "output: {parquet: {statistics: "+option+"}}")
				}
				r := runPipeline(t, root, p)
				if r.code != 0 || countsOf(r) != (counts{2, 2, 0, 0}) {
					t.Fatalf("writer exit=%d summary=%+v stderr=%s", r.code, r.summary, r.stderr)
				}
				vmAssertArtifacts(t, r)
				rows := vmRows(t, r, "pass")
				if len(rows) != 2 || rows[0]["i"] != "-9" || rows[1]["u"] != "18446744073709551614" || rows[0]["s"] != "PRIVATE_EXTREMA_A" {
					t.Fatalf("roundtrip=%v", rows)
				}
				parts := files(t, filepath.Join(r.dir(t), "pass", "*.parquet"))
				decimalRow := 0
				for _, part := range parts {
					readParquet(t, part, func(rec arrow.RecordBatch, row int) {
						col, ok := arrowColumn(t, rec, "d").(*array.Decimal128)
						if !ok || col.IsNull(row) {
							t.Fatal("decimal type or validity lost")
						}
						typ := col.DataType().(*arrow.Decimal128Type)
						if typ.Precision != 12 || typ.Scale != 2 {
							t.Fatalf("decimal type=%s", typ)
						}
						if got, want := col.Value(row).BigInt().String(), []string{"-125", "250"}[decimalRow]; got != want {
							t.Errorf("decimal coefficient=%s want=%s", got, want)
						}
						decimalRow++
					})
				}
				for _, part := range parts {
					desc := vmInspectParquet(t, part)
					for _, group := range desc.Metadata.RowGroups {
						for _, col := range group.Columns {
							name := strings.Join(col.MetaData.PathInSchema, ".")
							stats := col.MetaData.Statistics
							supported := name == "i" || name == "u" || name == "b" || name == "d" || name == "__valet_source_ordinal" || name == "__valet_row" || name == "__valet_global_row"
							if option != "bounded" || !supported {
								if stats != nil {
									t.Errorf("%s %s unexpectedly retained statistics: %v", option, name, stats)
								}
								continue
							}
							if stats == nil || stats.NullCount == nil || *stats.NullCount != 0 || len(stats.MinValue) == 0 || len(stats.MaxValue) == 0 {
								t.Fatalf("missing bounded %s statistics: %v", name, stats)
							}
							switch name {
							case "i":
								if int64(binary.LittleEndian.Uint64(stats.MinValue)) != -9 || int64(binary.LittleEndian.Uint64(stats.MaxValue)) != 7 {
									t.Errorf("int bounds=%v", stats)
								}
							case "u":
								if binary.LittleEndian.Uint64(stats.MinValue) != 9223372036854775810 || binary.LittleEndian.Uint64(stats.MaxValue) != 18446744073709551614 {
									t.Errorf("uint bounds=%v", stats)
								}
							case "b":
								if stats.MinValue[0] != 0 || stats.MaxValue[0] != 1 {
									t.Errorf("bool bounds=%v", stats)
								}
							case "d":
								min := new(big.Int).SetBytes(stats.MinValue)
								if stats.MinValue[0]&128 != 0 {
									min.Sub(min, new(big.Int).Lsh(big.NewInt(1), uint(8*len(stats.MinValue))))
								}
								max := new(big.Int).SetBytes(stats.MaxValue)
								if min.Int64() != -125 || max.Int64() != 250 {
									t.Errorf("decimal coefficients %s..%s", min, max)
								}
							}
						}
					}
				}
				// Self-produced bounded output never opts a later reader into trust.
				for _, trust := range []string{"off", "trusted"} {
					nextRoot := t.TempDir()
					q := defaultPipeline(parts[0], "parquet", filepath.Join(nextRoot, "out"), "parquet")
					q.Source.URIs = parts
					q.Input.Mode = ""
					q.with(t, `input:
  schema:
    extra_fields: ignore
    columns:
      i: {type: int64}
rules: [{code: RANGE, level: error, column: i, checks: [{type: valid_range, min: -10}]}]
`)
					flags := []string{"--output-mode", "report"}
					if trust == "trusted" {
						flags = append(flags, "--parquet-stats", "trusted")
					}
					rr := runPipeline(t, nextRoot, q, flags...)
					wantValues, wantMetadata := uint64(2), uint64(0)
					if trust == "trusted" && option == "bounded" {
						wantValues, wantMetadata = 0, 2
					}
					if rr.code != 0 || rr.summary.Total != wantValues || rr.summary.Coverage.MetadataClaimedRows != wantMetadata {
						t.Fatalf("reread %s total=%d coverage=%+v stderr=%s", trust, rr.summary.Total, rr.summary.Coverage, rr.stderr)
					}
					if trust == "off" && rr.summary.TrustPolicy != "none" {
						t.Errorf("implicit trust=%s", rr.summary.TrustPolicy)
					}
					vmAssertArtifacts(t, rr)
				}
			})
		}
	})
	t.Run("VM-STAT-20", func(t *testing.T) {
		root := t.TempDir()
		const n = 180
		schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "text", Type: arrow.BinaryTypes.String}, {Name: "blob", Type: arrow.BinaryTypes.Binary}}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		var records []arrow.RecordBatch
		for i := range n {
			builder.Field(0).(*array.Int64Builder).Append(int64(i))
			builder.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("SECRET_%04d_", i) + strings.Repeat(string(rune('a'+i%26)), 4096+i))
			builder.Field(2).(*array.BinaryBuilder).Append([]byte(strings.Repeat(fmt.Sprintf("%02x", i%256), 2048+i)))
			if i%2 == 1 {
				records = append(records, builder.NewRecordBatch())
			}
		}
		builder.Release()
		input := filepath.Join(root, "input.parquet")
		vmWriteParquet(t, input, schema, records, parquet.WithDictionaryDefault(false), parquet.WithDataPageSize(16<<10), parquet.WithMaxRowGroupLength(2))
		for _, record := range records {
			record.Release()
		}
		p := defaultPipeline(input, "parquet", filepath.Join(root, "out"), "parquet").with(t, `input:
  schema:
    columns:
      id: {type: int64}
      text: {type: string}
      blob: {type: binary}
output:
  parquet: {statistics: bounded}
`)
		p.Execution.BatchTarget = "32KiB"
		p.Execution.MaxRecordBytes = "32KiB"
		p.Execution.MemoryBudget = "32MiB"
		p.Output.PartSize = "64KiB"
		r := runPipeline(t, root, p)
		if r.code != 0 || r.summary.Total != n {
			t.Fatalf("exit=%d total=%d stderr=%s", r.code, r.summary.Total, r.stderr)
		}
		vmAssertArtifacts(t, r)
		rows := vmRows(t, r, "pass")
		if len(rows) != n {
			t.Fatalf("rows=%d", len(rows))
		}
		for i, row := range rows {
			want := fmt.Sprintf("SECRET_%04d_", i) + strings.Repeat(string(rune('a'+i%26)), 4096+i)
			if row["id"] != fmt.Sprint(i) || row["text"] != want {
				t.Fatalf("value/order mismatch row=%d", i)
			}
		}
		binaryIndex := 0
		for _, part := range files(t, filepath.Join(r.dir(t), "pass", "*.parquet")) {
			readParquet(t, part, func(rec arrow.RecordBatch, row int) {
				col, ok := arrowColumn(t, rec, "blob").(*array.Binary)
				if !ok || col.IsNull(row) {
					t.Fatalf("binary type/validity row=%d", binaryIndex)
				}
				want := strings.Repeat(fmt.Sprintf("%02x", binaryIndex%256), 2048+binaryIndex)
				if string(col.Value(row)) != want {
					t.Fatalf("binary value mismatch row=%d", binaryIndex)
				}
				binaryIndex++
			})
		}
		groups := 0
		for _, part := range files(t, filepath.Join(r.dir(t), "pass", "*.parquet")) {
			desc := vmInspectParquet(t, part)
			groups += len(desc.Metadata.RowGroups)
			for _, group := range desc.Metadata.RowGroups {
				for _, col := range group.Columns {
					name := strings.Join(col.MetaData.PathInSchema, ".")
					if (name == "text" || name == "blob") && col.MetaData.Statistics != nil {
						t.Fatalf("variable-width statistics retained: %s", name)
					}
				}
			}
		}
		if groups < 2 {
			t.Fatalf("fixture did not change output groups: %d", groups)
		}
		if r.summary.ManagedPeak > 32<<20 {
			t.Fatalf("managed peak=%d", r.summary.ManagedPeak)
		}
		next := t.TempDir()
		parts := files(t, filepath.Join(r.dir(t), "pass", "*.parquet"))
		q := defaultPipeline(parts[0], "parquet", filepath.Join(next, "out"), "parquet")
		q.Source.URIs = parts
		q.Input.Mode = ""
		q.with(t, "rules: [{code: TEXT, level: error, column: text, checks: [{type: min_length, min_length: 1}]}]")
		rr := runPipeline(t, next, q, "--output-mode", "report", "--parquet-stats", "trusted")
		if rr.code != 0 || rr.summary.Total != n || rr.summary.Coverage.MetadataClaimedRows != 0 {
			t.Fatalf("fallback exit=%d total=%d coverage=%+v stderr=%s", rr.code, rr.summary.Total, rr.summary.Coverage, rr.stderr)
		}
		vmAssertArtifacts(t, rr)
	})
}

func TestVMWriterStatisticsReportHasNoEncoder(t *testing.T) {
	root := t.TempDir()
	p := vmOutputPipeline(t, root, "csv", "parquet")
	p.with(t, "output: {mode: report, parquet: {statistics: bounded}}")
	r := runPipeline(t, root, p)
	vmCheckOUT(t, r, "report")
	for _, bucket := range []string{"pass", "warn", "error", "rejects"} {
		if parts := files(t, filepath.Join(r.dir(t), bucket, "*")); len(parts) != 0 {
			t.Fatalf("bounded report initialized %s encoder: %v", bucket, parts)
		}
	}
}
