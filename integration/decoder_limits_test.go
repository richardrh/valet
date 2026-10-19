//go:build integration

package integration_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// Published CSV writes null as \N and escapes a literal backslash as \\.
func TestCSVNullAndEscapeContract(t *testing.T) {
	input := `{"text":null}
{"text":""}
{"text":"\\N"}
{"text":"a\\b"}
`
	r := run(t, scenario{
		input:     input,
		inputType: "ndjson",
		config:    []string{"input: {null_values: [], schema: {columns: {text: {type: string}}}}"},
	})
	if r.code != exitOK {
		t.Fatalf("validate NDJSON null/escape fixture exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
	}
	if got := countsOf(r); got != (counts{total: 4, pass: 4}) {
		t.Errorf("validate NDJSON null/escape counts = %+v, want total:4 pass:4", got)
	}
	type cell struct {
		value string
		null  bool
	}
	var got []cell
	for _, physical := range column(r.rows(t, "pass"), "text") {
		if physical == `\N` {
			got = append(got, cell{null: true})
			continue
		}
		got = append(got, cell{value: strings.ReplaceAll(physical, `\\`, `\`)})
	}
	want := []cell{{null: true}, {value: ""}, {value: `\N`}, {value: `a\b`}}
	if !slices.Equal(got, want) {
		t.Errorf("decoded text cells = %+v, want %+v", got, want)
	}
}

func TestE2E20ParquetDecoderLimits(t *testing.T) {
	t.Run("oversized declared footer", func(t *testing.T) {
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], 0xfffffff0)
		path := put(t, filepath.Join(t.TempDir(), "malicious.parquet"), "PAR1"+string(length[:])+"PAR1")
		r := run(t, scenario{inputFile: path, inputType: "parquet"})
		if r.code != exitOperational {
			t.Errorf("validate oversized Parquet footer exit = %d, want %d", r.code, exitOperational)
		}
	})
	t.Run("oversized compressed row group", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "large-page.parquet")
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
		b := array.NewStringBuilder(memory.DefaultAllocator)
		// One 10MiB value against a 2MiB memory budget.
		b.Append(strings.Repeat("large-decoded-value-", 1<<19))
		values := b.NewArray()
		b.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
		values.Release()
		defer record.Release()
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		properties := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
		writer, err := pqarrow.NewFileWriter(schema, f, properties, pqarrow.DefaultWriterProps())
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := writer.Write(record); err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		p := defaultPipeline(path, "parquet", filepath.Join(root, "output"), "csv")
		p.Input = inputSpec{Type: "parquet"}
		p.Execution.MemoryBudget = "2MiB"
		r := runPipeline(t, root, p)
		if r.code != exitOperational {
			t.Fatalf("validate oversized Parquet row group exit = %d, want %d", r.code, exitOperational)
		}
		if !strings.Contains(strings.ToLower(string(r.stderr)), "resource") {
			t.Errorf("stderr lacks a resource diagnostic:\n%s", r.stderr)
		}
	})
}
