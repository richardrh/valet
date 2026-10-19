//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

func TestE2E15LocalOrderingAndOverlap(t *testing.T) {
	cases := []struct {
		name    string
		source  func(inputDir string) sourceSpec
		wantIDs []string
	}{
		{
			name: "explicit uris keep listed order",
			source: func(inputDir string) sourceSpec {
				return sourceSpec{URIs: []string{
					filepath.ToSlash(filepath.Join(inputDir, "z.csv")),
					filepath.ToSlash(filepath.Join(inputDir, "a.csv")),
				}}
			},
			wantIDs: []string{"z", "a"},
		},
		{
			name: "prefix discovery orders by key and filters",
			source: func(inputDir string) sourceSpec {
				return sourceSpec{Prefix: filepath.ToSlash(inputDir), Filter: "csv", Recursive: true}
			},
			wantIDs: []string{"a", "z"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			inputDir := filepath.Join(root, "input")
			put(t, filepath.Join(inputDir, "a.csv"), "id\na\n")
			put(t, filepath.Join(inputDir, "z.csv"), "id\nz\n")
			put(t, filepath.Join(inputDir, "ignore.txt"), "not,csv\n")
			p := &pipeline{
				Source: c.source(inputDir),
				Input:  inputSpec{Type: "csv", Mode: "raw_strings"},
				Output: outputSpec{URI: filepath.ToSlash(filepath.Join(root, "output")), Format: "csv"},
			}
			r := runPipeline(t, root, p)
			if r.code != exitOK {
				t.Fatalf("validate ordered local inputs exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
			}
			if got := column(r.rows(t, "pass"), "id"); !slices.Equal(got, c.wantIDs) {
				t.Errorf("pass ids = %q, want %q", got, c.wantIDs)
			}
		})
	}
	t.Run("output inside input prefix is rejected", func(t *testing.T) {
		root := t.TempDir()
		put(t, filepath.Join(root, "data.csv"), "id\na\n")
		r := runPipeline(t, root, &pipeline{
			Source: sourceSpec{Prefix: filepath.ToSlash(root), Filter: "csv"},
			Input:  inputSpec{Type: "csv"},
			Output: outputSpec{URI: filepath.ToSlash(filepath.Join(root, "output"))},
		})
		if r.code != exitOperational {
			t.Errorf("validate overlapping input/output exit = %d, want %d", r.code, exitOperational)
		}
	})
}

func TestE2E17TinyBatchesAndWorkers(t *testing.T) {
	const memoryBudget = 64 << 20
	tinyBatches := func(t *testing.T, workers int) (string, *pipeline) {
		t.Helper()
		root := t.TempDir()
		input := put(t, filepath.Join(root, "input.csv"), fixtureA)
		p := defaultPipeline(input, "csv", filepath.Join(root, "output"), "csv").with(t, rulesA)
		p.Input.NullValues = nulls("")
		p.Output.PartSize = "128B"
		p.Execution = executionSpec{BatchTarget: "64B", MaxRecordBytes: "1KiB", MemoryBudget: "64MiB", Workers: workers}
		return root, p
	}
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("%d workers", workers), func(t *testing.T) {
			root, p := tinyBatches(t, workers)
			r := runPipeline(t, root, p)
			if r.code != exitValidationFailed {
				t.Fatalf("validate tiny batches exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
			}
			if got := r.summary; got.Total != 5 || got.Passed != 1 || got.Warned != 1 || got.Errored != 3 {
				t.Errorf("summary counts = total:%d pass:%d warn:%d error:%d, want 5/1/1/3",
					got.Total, got.Passed, got.Warned, got.Errored)
			}
			if got, want := r.globalRows(t, "error"), []string{"2", "4", "5"}; !slices.Equal(got, want) {
				t.Errorf("error rows = %v, want %v", got, want)
			}
			var gotIssues []string
			for _, issue := range r.issues {
				gotIssues = append(gotIssues, fmt.Sprintf("%d:%s", issue.GlobalRow, issue.Code))
			}
			wantIssues := []string{"2:AMOUNT_RANGE", "3:COUNTRY_REQUIRED", "4:AMOUNT_RANGE", "4:COUNTRY_REQUIRED", "5:ID_REQUIRED"}
			if !slices.Equal(gotIssues, wantIssues) {
				t.Errorf("issues = %v, want %v", gotIssues, wantIssues)
			}
			if r.summary.ManagedPeak > memoryBudget {
				t.Errorf("managed_peak_bytes = %d, want at most %d", r.summary.ManagedPeak, memoryBudget)
			}
		})
	}
}

func TestE2E18LocalWriteFailure(t *testing.T) {
	const userContent = "do not destroy"
	root := t.TempDir()
	input := put(t, filepath.Join(root, "input.csv"), fixtureA)
	out := put(t, filepath.Join(root, "existing-user-file"), userContent)
	p := defaultPipeline(input, "csv", out, "csv").with(t, rulesA)
	p.Execution = executionSpec{}
	r := runPipeline(t, root, p)
	if r.code != exitOperational {
		t.Errorf("validate existing output file exit = %d, want %d", r.code, exitOperational)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("unrelated output file: %v", err)
	}
	if string(body) != userContent {
		t.Errorf("unrelated output file = %q, want %q", body, userContent)
	}
}

func TestE2E20ResourceLimits(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		rules  string
		budget string
	}{
		{name: "decoder", input: "id\n" + strings.Repeat("x", 1<<20) + "\n", budget: "1MiB"},
		{
			name:   "uniqueness",
			input:  distinctCSV(100000),
			rules:  "rules: [{code: UNIQUE, level: error, column: id, checks: [{type: is_unique}]}]\n",
			budget: "2MiB",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			input := put(t, filepath.Join(root, "input.csv"), c.input)
			p := defaultPipeline(input, "csv", filepath.Join(root, "output"), "parquet").with(t, c.rules)
			p.Execution = executionSpec{MemoryBudget: c.budget, BatchTarget: "16KiB", MaxRecordBytes: "4MiB"}
			r := runPipeline(t, root, p)
			if r.code != exitOperational {
				t.Fatalf("validate %s resource limit exit = %d, want %d", c.name, r.code, exitOperational)
			}
			if !strings.Contains(strings.ToLower(string(r.stderr)), "resource") {
				t.Errorf("stderr lacks a resource diagnostic:\n%s", r.stderr)
			}
		})
	}
	t.Run("writer", func(t *testing.T) {
		// Identical rows fit in the streaming CSV writer's 1MiB budget. The
		// Parquet writer retains row-group metadata and must fail explicitly.
		const columns, rows = 32, 128
		root := t.TempDir()
		var content strings.Builder
		for j := range columns {
			if j > 0 {
				content.WriteByte(',')
			}
			fmt.Fprintf(&content, "c%02d", j)
		}
		content.WriteByte('\n')
		for i := range rows {
			for j := range columns {
				if j > 0 {
					content.WriteByte(',')
				}
				fmt.Fprintf(&content, "%04d-%02d-%s", i, j, strings.Repeat("x", 120))
			}
			content.WriteByte('\n')
		}
		input := put(t, filepath.Join(root, "input.csv"), content.String())
		writerPipeline := func(out, format string) *pipeline {
			p := defaultPipeline(input, "csv", out, format)
			p.Output.PartSize = "128MiB"
			p.Execution = executionSpec{MemoryBudget: "1MiB", BatchTarget: "4KiB", MaxRecordBytes: "8KiB"}
			return p
		}
		control := runPipeline(t, root, writerPipeline(filepath.Join(root, "csv-control"), "csv"))
		if control.code != exitOK {
			t.Fatalf("validate CSV writer control exit = %d, want %d; stderr=%s", control.code, exitOK, control.stderr)
		}
		if got := control.summary; got.Total != rows || got.Passed != rows || got.Warned != 0 || got.Errored != 0 {
			t.Errorf("CSV control counts = total:%d pass:%d warn:%d error:%d, want %d/%d/0/0",
				got.Total, got.Passed, got.Warned, got.Errored, rows, rows)
		}
		out := filepath.Join(root, "parquet-output")
		failed := runPipeline(t, root, writerPipeline(out, "parquet"))
		if failed.code != exitOperational {
			t.Fatalf("validate constrained Parquet writer exit = %d, want %d", failed.code, exitOperational)
		}
		if !strings.Contains(strings.ToLower(string(failed.stderr)), "resource") {
			t.Errorf("stderr lacks a resource diagnostic:\n%s", failed.stderr)
		}
		// Aborting removes the temporary part, but the opened bucket directory remains.
		if len(files(t, filepath.Join(out, "*", "pass"))) == 0 {
			t.Errorf("no pass bucket directory, want the resource fault after a Parquet writer opened:\nstdout=%s\nstderr=%s",
				failed.stdout, failed.stderr)
		}
	})
	t.Run("reference", func(t *testing.T) {
		root := t.TempDir()
		put(t, filepath.Join(root, "ref.csv"), distinctCSV(100000))
		input := put(t, filepath.Join(root, "input.csv"), "id\na\n")
		p := defaultPipeline(input, "csv", filepath.Join(root, "output"), "csv").with(t, `rules:
  - code: REF
    level: error
    column: id
    checks: [{type: referential_integrity, reference_file: ref.csv, reference_column: id}]
`)
		p.Execution = executionSpec{MemoryBudget: "2MiB", BatchTarget: "16KiB", MaxRecordBytes: "64KiB"}
		r := runPipeline(t, root, p)
		if r.code != exitOperational {
			t.Fatalf("validate oversized reference exit = %d, want %d", r.code, exitOperational)
		}
		if !strings.Contains(strings.ToLower(string(r.stderr)), "resource") {
			t.Errorf("stderr lacks a resource diagnostic:\n%s", r.stderr)
		}
	})
}
func distinctCSV(n int) string {
	var s strings.Builder
	s.WriteString("id\n")
	for i := range n {
		fmt.Fprintf(&s, "unique-%012d\n", i)
	}
	return s.String()
}

func TestE2E21CompressionPaths(t *testing.T) {
	gzipCases := []struct {
		inputType                   string
		input, rules                string
		total, pass, warn, failures uint64
	}{
		{inputType: "csv", input: fixtureA, rules: rulesA, total: 5, pass: 1, warn: 1, failures: 3},
		{inputType: "ndjson", input: fixtureC, rules: rulesC, total: 3, warn: 2, failures: 1},
		{inputType: "json", input: fixtureCArray, rules: rulesC, total: 3, warn: 2, failures: 1},
	}
	for _, c := range gzipCases {
		t.Run("gzip "+c.inputType, func(t *testing.T) {
			compressed := gzipped(t, c.input)
			path := put(t, filepath.Join(t.TempDir(), "data."+c.inputType+".gz"), compressed)
			r := run(t, scenario{
				inputFile: path,
				inputType: c.inputType,
				config:    []string{c.rules},
			})
			if r.code != exitValidationFailed {
				t.Fatalf("validate gzip %s exit = %d, want %d; stderr=%s", c.inputType, r.code, exitValidationFailed, r.stderr)
			}
			if got := r.summary; got.Total != c.total || got.Passed != c.pass || got.Warned != c.warn || got.Errored != c.failures {
				t.Errorf("summary counts = total:%d pass:%d warn:%d error:%d, want %d/%d/%d/%d",
					got.Total, got.Passed, got.Warned, got.Errored, c.total, c.pass, c.warn, c.failures)
			}

			truncated := put(t, filepath.Join(t.TempDir(), "truncated."+c.inputType+".gz"), compressed[:len(compressed)-5])
			truncatedRun := run(t, scenario{
				inputFile: truncated,
				inputType: c.inputType,
				config:    []string{c.rules},
			})
			if truncatedRun.code != exitOperational {
				t.Errorf("validate truncated gzip %s exit = %d, want %d", c.inputType, truncatedRun.code, exitOperational)
			}
		})
	}
	codecs := []struct {
		name  string
		codec compress.Compression
	}{
		{name: "uncompressed", codec: compress.Codecs.Uncompressed},
		{name: "snappy", codec: compress.Codecs.Snappy},
		{name: "zstd", codec: compress.Codecs.Zstd},
	}
	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.parquet")
			writeParquetFixture(t, path, c.codec)
			r := run(t, scenario{
				inputFile: path,
				inputType: "parquet",
				config:    []string{rulesA},
			})
			if r.code != exitValidationFailed {
				t.Fatalf("validate %s Parquet exit = %d, want %d; stderr=%s", c.name, r.code, exitValidationFailed, r.stderr)
			}
			if got := r.summary; got.Total != 5 || got.Passed != 1 || got.Warned != 1 || got.Errored != 3 {
				t.Errorf("summary counts = total:%d pass:%d warn:%d error:%d, want 5/1/1/3",
					got.Total, got.Passed, got.Warned, got.Errored)
			}
			var gotIssues []string
			for _, issue := range r.issues {
				gotIssues = append(gotIssues, fmt.Sprintf("%d:%s", issue.GlobalRow, issue.Code))
			}
			wantIssues := []string{"2:AMOUNT_RANGE", "3:COUNTRY_REQUIRED", "4:AMOUNT_RANGE", "4:COUNTRY_REQUIRED", "5:ID_REQUIRED"}
			if !slices.Equal(gotIssues, wantIssues) {
				t.Errorf("issues = %v, want %v", gotIssues, wantIssues)
			}

			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			corrupt := put(t, filepath.Join(t.TempDir(), "corrupt.parquet"), string(body[:len(body)-9]))
			corruptRun := run(t, scenario{inputFile: corrupt, inputType: "parquet", config: []string{rulesA}})
			if corruptRun.code != exitOperational {
				t.Errorf("validate corrupt Parquet exit = %d, want %d", corruptRun.code, exitOperational)
			}
		})
	}
}

// writeParquetFixture writes fixture A as Parquet with nullable id and country.
func writeParquetFixture(t testing.TB, path string, codec compress.Compression) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: arrow.BinaryTypes.String},
		{Name: "country", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "b", "c", "d", ""}, []bool{true, true, true, true, false})
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"10", "-1", "20", "-1", "5"}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"GB", "GB", "", "", "GB"}, []bool{true, true, false, false, true})
	record := b.NewRecordBatch()
	defer record.Release()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	properties := parquet.NewWriterProperties(parquet.WithCompression(codec))
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
}

func TestE2E23NativeTarget(t *testing.T) {
	releaseTargets := map[string][]string{"linux": {"amd64", "arm64"}, "windows": {"amd64"}}
	if !slices.Contains(releaseTargets[runtime.GOOS], runtime.GOARCH) {
		t.Fatalf("runner %s/%s is not a release target, want one of %v", runtime.GOOS, runtime.GOARCH, releaseTargets)
	}
	// With the binary built, no compiler, Python or auxiliary programs are on PATH.
	t.Setenv("PATH", t.TempDir())
	for _, arg := range []string{"--help", "--version"} {
		out, stderr, code := invoke(t, arg)
		if code != exitOK {
			t.Errorf("%s exit = %d, want %d; stderr=%s", arg, code, exitOK, stderr)
		}
		if len(out) == 0 {
			t.Errorf("%s stdout is empty, want output", arg)
		}
	}
	r := run(t, scenario{input: fixtureA, config: []string{rulesA}})
	if r.code != exitValidationFailed {
		t.Fatalf("validate native target fixture exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
	}
	if got := r.summary; got.Total != 5 || got.Passed != 1 || got.Warned != 1 || got.Errored != 3 {
		t.Errorf("summary counts = total:%d pass:%d warn:%d error:%d, want 5/1/1/3",
			got.Total, got.Passed, got.Warned, got.Errored)
	}
	t.Logf("executed native target %s/%s", runtime.GOOS, runtime.GOARCH)
}
