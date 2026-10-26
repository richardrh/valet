//go:build integration

package integration_test

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

// CLI exit codes.
const (
	exitOK               = 0
	exitValidationFailed = 1
	exitOperational      = 2
	exitInterrupted      = 130 // 128 + SIGINT
	exitTerminated       = 143 // 128 + SIGTERM
)

var binaryPath string

// TestMain runs the suite against VALET_BINARY (the release artifact in
// CI) or, when unset, a freshly built CLI.
func TestMain(m *testing.M) {
	binaryPath = os.Getenv("VALET_BINARY")
	cleanup := func() {}
	if binaryPath == "" {
		dir, err := os.MkdirTemp("", "valet-cli-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cleanup = func() { _ = os.RemoveAll(dir) }
		binaryPath = filepath.Join(dir, "valet")
		if runtime.GOOS == "windows" {
			binaryPath += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/valet")
		cmd.Dir = ".."
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if output, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "building CLI: %v\n%s", err, output)
			cleanup()
			os.Exit(2)
		}
	} else if abs, err := filepath.Abs(binaryPath); err == nil {
		binaryPath = abs
	}
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// pipeline is the YAML document handed to the CLI. Harness-owned sections are
// typed; fixture-owned sections (rules, transforms, schema, ...) arrive as YAML
// fragments decoded into the inline maps.
type pipeline struct {
	Source    sourceSpec     `yaml:"source"`
	Input     inputSpec      `yaml:"input"`
	Output    outputSpec     `yaml:"output"`
	Execution executionSpec  `yaml:"execution,omitempty"`
	Extra     map[string]any `yaml:",inline"`
}
type sourceSpec struct {
	URIs      []string `yaml:"uris,omitempty"`
	Prefix    string   `yaml:"prefix,omitempty"`
	Filter    string   `yaml:"filter,omitempty"`
	Recursive bool     `yaml:"recursive,omitempty"`
	S3        *s3Spec  `yaml:"s3,omitempty"`
}
type s3Spec struct {
	Region    string `yaml:"region,omitempty"`
	Endpoint  string `yaml:"endpoint,omitempty"`
	PathStyle bool   `yaml:"path_style,omitempty"`
	SSE       string `yaml:"sse,omitempty"`
	KMSKeyID  string `yaml:"kms_key_id,omitempty"`
}
type inputSpec struct {
	Type       string         `yaml:"type,omitempty"`
	Mode       string         `yaml:"mode,omitempty"`
	NullValues *[]string      `yaml:"null_values,omitempty"`
	Extra      map[string]any `yaml:",inline"`
}
type outputSpec struct {
	URI      string         `yaml:"uri"`
	Format   string         `yaml:"format,omitempty"`
	PartSize string         `yaml:"part_size,omitempty"`
	Mode     string         `yaml:"mode,omitempty"`
	Parquet  map[string]any `yaml:"parquet,omitempty"`
	S3       *s3Spec        `yaml:"s3,omitempty"`
}
type executionSpec struct {
	MemoryBudget   string `yaml:"memory_budget,omitempty"`
	BatchTarget    string `yaml:"batch_target,omitempty"`
	MaxRecordBytes string `yaml:"max_record_bytes,omitempty"`
	Workers        int    `yaml:"workers,omitempty"`
}

func nulls(values ...string) *[]string { return &values }

// obj spells fixture sections that embed run-specific values such as paths.
type obj = map[string]any

// fragment marshals top-level sections into a fixture fragment for scenario.config.
func fragment(t testing.TB, sections obj) string {
	t.Helper()
	body, err := yaml.Marshal(sections)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// yamlValue decodes one YAML value, such as a flow-style check, for embedding.
func yamlValue(t testing.TB, text string) any {
	t.Helper()
	var v any
	if err := yaml.Unmarshal([]byte(text), &v); err != nil {
		t.Fatalf("fixture value %q: %v", text, err)
	}
	return v
}

// joinLines terminates every line with \n, so short fixtures inside tables
// can be spelled one record per argument.
func joinLines(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

// with decodes YAML fragments shaped like the full pipeline document into p.
// Later fragments add to, or override, earlier ones.
func (p *pipeline) with(t testing.TB, fragments ...string) *pipeline {
	t.Helper()
	for _, fragment := range fragments {
		if err := yaml.Unmarshal([]byte(fragment), p); err != nil {
			t.Fatalf("fixture fragment %q: %v", fragment, err)
		}
	}
	return p
}
func (p *pipeline) write(t testing.TB, path string) string {
	t.Helper()
	body, err := yaml.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return put(t, path, string(body))
}

// defaultPipeline is the small-batch local configuration most scenarios share.
func defaultPipeline(input, inputType, out, format string) *pipeline {
	return &pipeline{
		Source:    sourceSpec{URIs: []string{filepath.ToSlash(input)}},
		Input:     inputSpec{Type: inputType, Mode: "raw_strings"},
		Output:    outputSpec{URI: filepath.ToSlash(out), Format: format},
		Execution: executionSpec{BatchTarget: "1KiB", MaxRecordBytes: "64KiB", MemoryBudget: "64MiB"},
	}
}

// summary mirrors the documented summary.json contract (report_version 2)
// independently of the producer's Go types.
type summary struct {
	ReportVersion           int                       `json:"report_version"`
	Scope                   string                    `json:"scope"`
	OutputMode              string                    `json:"output_mode"`
	Verification            string                    `json:"verification"`
	TrustPolicy             string                    `json:"trust_policy"`
	IngestionVerification   string                    `json:"ingestion_verification"`
	ProvenancePositionBasis string                    `json:"provenance_position_basis"`
	Coverage                vmCoverage                `json:"coverage"`
	RuleCoverage            map[string]vmRuleCoverage `json:"rule_coverage"`
	RowOutput               map[string]vmRowOutput    `json:"row_output"`
	RunID                   string                    `json:"run_id"`
	Status                  string                    `json:"status"`
	Validation              string                    `json:"validation"`
	Total                   uint64                    `json:"total"`
	Passed                  uint64                    `json:"pass"`
	Warned                  uint64                    `json:"warn"`
	Errored                 uint64                    `json:"error"`
	RowsScanned             uint64                    `json:"rows_scanned"`
	RuleCounts              map[string]uint64         `json:"rule_counts"`
	IngestCounts            map[string]uint64         `json:"ingest_counts"`
	Unevaluable             map[string]uint64         `json:"unevaluable"`
	NotApplicable           map[string]uint64         `json:"not_applicable"`
	Stats                   map[string]any            `json:"stats"`
	DatasetResults          []datasetResult           `json:"dataset_results"`
	DatasetIssues           []issue                   `json:"dataset_issues"`
	SchemaIssues            []issue                   `json:"schema_issues"`
	DurationSeconds         float64                   `json:"duration_seconds"`
	Passes                  int                       `json:"passes"`
	ManagedPeak             int64                     `json:"managed_peak_bytes"`
	ManifestURI             string                    `json:"manifest_uri,omitempty"`
}
type vmCoverage struct {
	ValueRows               uint64  `json:"value_rows"`
	MetadataClaimedRows     uint64  `json:"metadata_claimed_rows"`
	AccountedRows           uint64  `json:"accounted_rows"`
	RowGroupsScanned        *uint64 `json:"row_groups_scanned"`
	RowGroupsStatisticsOnly *uint64 `json:"row_groups_statistics_only"`
	RowGroupsTotal          *uint64 `json:"row_groups_total"`
}
type vmRuleCoverage struct {
	ValueRows           uint64 `json:"value_rows"`
	MetadataClaimedRows uint64 `json:"metadata_claimed_rows"`
}
type vmRowOutput struct {
	Policy  string `json:"policy"`
	Emitted uint64 `json:"emitted"`
	Omitted uint64 `json:"omitted"`
}
type datasetResult struct {
	Code          string `json:"code"`
	Metric        string `json:"metric"`
	Value         any    `json:"value"`
	Status        string `json:"status"`
	Reason        string `json:"reason,omitempty"`
	NotApplicable uint64 `json:"not_applicable"`
	Unevaluable   uint64 `json:"unevaluable"`
}
type issue struct {
	Source        string  `json:"source"`
	SourceOrdinal uint64  `json:"source_ordinal"`
	Row           uint64  `json:"row"`
	GlobalRow     uint64  `json:"global_row"`
	Line          uint64  `json:"line,omitempty"`
	RowGroup      *uint64 `json:"row_group,omitempty"`
	RowInGroup    uint64  `json:"row_in_group,omitempty"`
	PositionBasis string  `json:"position_basis,omitempty"`
	Code          string  `json:"code"`
	Severity      string  `json:"severity"`
	Origin        string  `json:"origin"`
	Scope         string  `json:"scope"`
	Column        string  `json:"column,omitempty"`
	Message       string  `json:"message,omitempty"`
	Reason        string  `json:"reason,omitempty"`
	Count         uint64  `json:"count,omitempty"`
}

type runResult struct {
	root, output   string
	format         string // output.format, selecting the bucket reader
	stdout, stderr []byte
	code           int
	manifests      []string
	summary        summary
	manifest       map[string]any
	issues         []issue
}

func put(t testing.TB, path, body string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// callContext bounds one CLI call by the test's own deadline, keeping a margin
// so a hung CLI fails with its stderr rather than the global test panic.
func callContext(t testing.TB) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(5 * time.Minute)
	if d, ok := t.(interface{ Deadline() (time.Time, bool) }); ok {
		if outer, ok := d.Deadline(); ok {
			margin := min(time.Until(outer)/10, 30*time.Second)
			if limit := outer.Add(-margin); limit.Before(deadline) {
				deadline = limit
			}
		}
	}
	return context.WithDeadline(context.Background(), deadline)
}

// command prepares a CLI process bound to the per-call deadline. Callers must
// check ctx.Err() after Wait to distinguish a deadline kill from a CLI exit.
func command(t testing.TB, args ...string) (*exec.Cmd, context.Context, context.CancelFunc) {
	ctx, cancel := callContext(t)
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.WaitDelay = 5 * time.Second
	return cmd, ctx, cancel
}
func invoke(t testing.TB, args ...string) ([]byte, []byte, int) {
	t.Helper()
	cmd, ctx, cancel := command(t, args...)
	defer cancel()
	var out, errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("CLI %v exceeded test deadline: %s", args, errout.String())
	}
	code := exitOK
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("CLI %v: %v\nstderr=%s", args, err, errout.String())
		}
		code = exit.ExitCode()
	}
	return out.Bytes(), errout.Bytes(), code
}

// scenario is one CLI validation over the default local pipeline.
type scenario struct {
	// input is written to a fresh input.<inputType>. inputFile instead names
	// an existing file; the run then happens in that file's directory, so
	// relative references resolve beside it.
	input     string
	inputFile string
	inputType string   // input.type; csv when empty
	format    string   // output.format; csv when empty
	config    []string // YAML fragments merged over the default pipeline, in order
}

// run validates s with the default pipeline plus s.config and collects the
// published run.
func run(t testing.TB, s scenario) *runResult {
	t.Helper()
	inputType := cmp.Or(s.inputType, "csv")
	path := s.inputFile
	switch {
	case path == "":
		path = put(t, filepath.Join(t.TempDir(), "input."+inputType), s.input)
	case s.input != "":
		t.Fatalf("scenario sets both input and inputFile %s", path)
	}
	root := filepath.Dir(path)
	p := defaultPipeline(path, inputType, filepath.Join(root, "output"), cmp.Or(s.format, "csv")).with(t, s.config...)
	if p.Input.NullValues == nil {
		p.Input.NullValues = nulls("")
	}
	return runPipeline(t, root, p)
}

// runPipeline writes p, invokes the CLI and collects any committed result.
// It performs setup and decoding only; each test owns its behavioral assertions.
func runPipeline(t testing.TB, root string, p *pipeline, flags ...string) *runResult {
	t.Helper()
	path := p.write(t, filepath.Join(root, "pipeline.yml"))
	stdout, stderr, code := invoke(t, append([]string{"validate", "-p", path}, flags...)...)
	return collect(t, &runResult{
		root:   root,
		output: filepath.FromSlash(p.Output.URI),
		format: p.Output.Format,
		stdout: stdout,
		stderr: stderr,
		code:   code,
	})
}
func collect(t testing.TB, r *runResult) *runResult {
	t.Helper()
	if len(r.manifests) == 0 {
		r.manifests, _ = filepath.Glob(filepath.Join(r.output, "*", "manifest.json"))
	}
	if len(r.manifests) != 1 {
		return r
	}
	decodeFile(t, r.manifests[0], &r.manifest)
	dir := filepath.Dir(r.manifests[0])
	decodeFile(t, filepath.Join(dir, "summary.json"), &r.summary)
	for _, path := range files(t, filepath.Join(dir, "issues", "*.ndjson")) {
		r.issues = append(r.issues, readIssues(t, path)...)
	}
	return r
}
func readIssues(t testing.TB, path string) []issue {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var issues []issue
	dec := json.NewDecoder(f)
	for {
		var value issue
		if err := dec.Decode(&value); err == io.EOF {
			return issues
		} else if err != nil {
			t.Fatal(err)
		}
		issues = append(issues, value)
	}
}
func decodeFile(t testing.TB, path string, v any) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		t.Fatal(err)
	}
}
func files(t testing.TB, pattern string) []string {
	t.Helper()
	paths, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	return paths
}
func (r *runResult) dir(t testing.TB) string {
	t.Helper()
	paths := r.manifests
	if len(paths) == 0 {
		paths = files(t, filepath.Join(r.output, "*", "manifest.json"))
	}
	if len(paths) != 1 {
		t.Fatalf("manifests = %v, want exactly one", paths)
	}
	return filepath.Dir(paths[0])
}

// rows reads every CSV part of a bucket as header-keyed records.
func (r *runResult) rows(t testing.TB, bucket string) []map[string]string {
	t.Helper()
	var rows []map[string]string
	for _, path := range files(t, filepath.Join(r.dir(t), bucket, "*.csv")) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		reader := csv.NewReader(f)
		header, err := reader.Read()
		if err != nil {
			t.Fatal(err)
		}
		for {
			values, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			row := map[string]string{}
			for i, k := range header {
				row[k] = values[i]
			}
			rows = append(rows, row)
		}
		_ = f.Close()
	}
	return rows
}

// column extracts one field from every record, in order.
func column(rows []map[string]string, name string) []string {
	values := make([]string, len(rows))
	for i, row := range rows {
		values[i] = row[name]
	}
	return values
}

func rowNumbers(values ...uint64) []string {
	rows := make([]string, len(values))
	for i, value := range values {
		rows[i] = strconv.FormatUint(value, 10)
	}
	return rows
}

// counts are the summary's row totals per bucket.
type counts struct{ total, pass, warn, error uint64 }

func countsOf(r *runResult) counts {
	return counts{total: r.summary.Total, pass: r.summary.Passed, warn: r.summary.Warned, error: r.summary.Errored}
}

// issueAt identifies a streamed issue by its global row (0 for issues without
// a row, such as dataset rules) and code.

type issueAt struct {
	globalRow uint64
	code      string
}

func issuesOf(r *runResult) []issueAt {
	issues := make([]issueAt, 0, len(r.issues))
	for _, value := range r.issues {
		issues = append(issues, issueAt{value.GlobalRow, value.Code})
	}
	return issues
}

// checkIssues spells the issues of a rule coded CHECK failing the given rows.
func checkIssues(rows []uint64) []issueAt {
	var issues []issueAt
	for _, row := range rows {
		issues = append(issues, issueAt{row, "CHECK"})
	}
	return issues
}

// globalRows returns __valet_global_row for each published row of bucket. CSV
// rows must also carry the remaining provenance columns.
func (r *runResult) globalRows(t testing.TB, bucket string) []string {
	t.Helper()
	switch r.format {
	case "csv":
		var rows []string
		for _, row := range r.rows(t, bucket) {
			rows = append(rows, row["__valet_global_row"])
		}
		return rows
	case "parquet":
		return parquetGlobalRows(t, r, bucket)
	}
	t.Fatalf("no bucket reader for output format %q", r.format)
	return nil
}

func metadataError(dir string, manifest map[string]any) error {
	for _, key := range []string{"inputs", "rules", "artifacts", "summary"} {
		ref, ok := manifest[key].(map[string]any)
		if !ok {
			return fmt.Errorf("manifest %s = %v, want a reference object", key, manifest[key])
		}
		uri, _ := ref["uri"].(string)
		if uri == "" {
			return fmt.Errorf("manifest %s uri is empty", key)
		}
		if strings.HasPrefix(uri, "s3://") {
			uri = filepath.Join(dir, filepath.Base(uri))
		} else if !filepath.IsAbs(uri) {
			uri = filepath.Join(dir, uri)
		}
		body, err := os.ReadFile(uri)
		if err != nil {
			return fmt.Errorf("manifest %s: %w", key, err)
		}
		digest := sha256.Sum256(body)
		if want := hex.EncodeToString(digest[:]); ref["sha256"] != want {
			return fmt.Errorf("manifest %s sha256 = %v, want %s", key, ref["sha256"], want)
		}
		size, ok := ref["size"].(json.Number)
		if !ok || size.String() != strconv.Itoa(len(body)) {
			return fmt.Errorf("manifest %s size = %v, want %d", key, ref["size"], len(body))
		}
		if key == "inputs" || key == "artifacts" {
			lines := bytes.Count(body, []byte("\n"))
			count, ok := ref["count"].(json.Number)
			if !ok || count.String() != strconv.Itoa(lines) {
				return fmt.Errorf("manifest %s count = %v, want %d streamed records", key, ref["count"], lines)
			}
		}
	}
	return nil
}
