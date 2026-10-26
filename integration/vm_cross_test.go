//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func vmCrossOUTPipeline(t testing.TB, root, mode, format string, workers int) *pipeline {
	t.Helper()
	paths := vmFixtureOUT(t, root, "csv")
	p := defaultPipeline(paths[0], "csv", filepath.Join(root, "output"), format).with(t, vmOutConfig)
	p.Source.URIs = paths
	p.Input.NullValues = nulls("")
	p.Output.Mode = mode
	p.Output.PartSize = "256B"
	p.Execution.Workers = workers
	p.Execution.BatchTarget = "64B"
	return p
}
func vmCrossAssertOUT(t testing.TB, r *runResult, mode string) {
	t.Helper()
	if r.code != exitValidationFailed {
		t.Fatalf("exit=%d stderr=%s", r.code, r.stderr)
	}
	if countsOf(r) != (counts{total: 8, pass: 2, warn: 2, error: 4}) {
		t.Fatalf("F-OUT counts=%+v", countsOf(r))
	}
	wantIssues := []issueAt{{2, "POSITIVE"}, {3, "STATUS_OK"}, {4, "POSITIVE"}, {4, "STATUS_OK"}, {5, "POSITIVE"}, {6, "INGEST_TYPE"}, {8, "STATUS_OK"}}
	if !reflect.DeepEqual(issuesOf(r), wantIssues) {
		t.Fatalf("F-OUT issues=%+v want %+v", issuesOf(r), wantIssues)
	}
	if r.summary.Unevaluable["POSITIVE"] != 1 || r.summary.RuleCounts["POSITIVE"] != 3 || r.summary.RuleCounts["STATUS_OK"] != 3 {
		t.Fatalf("wrong rule outcomes: %+v", r.summary)
	}
	for _, rule := range []string{"POSITIVE", "STATUS_OK"} {
		c := r.summary.RuleCoverage[rule]
		if c.ValueRows != 8 || c.MetadataClaimedRows != 0 {
			t.Fatalf("rule %s coverage=%+v", rule, c)
		}
	}
	vmAssertArtifacts(t, r)
	for bucket, indices := range map[string][]int{"pass": {0, 6}, "warn": {2, 7}, "error": {1, 3, 4, 5}} {
		want := indices
		if mode == "report" || (mode == "failures" && bucket == "pass") {
			want = nil
		}
		rows := vmRows(t, r, bucket)
		if len(rows) != len(want) {
			t.Fatalf("mode=%s %s rows=%d want %d", mode, bucket, len(rows), len(want))
		}
		for j, i := range want {
			row := rows[j]
			literal := vmOutLiterals[i]
			amount := literal[1]
			if i == 5 {
				amount = `\N`
			}
			source := filepath.Join(r.root, fmt.Sprintf("out-%d.csv", i/4))
			if r.format == "csv" {
				source = strings.ReplaceAll(source, `\`, `\\`)
			}
			for name, value := range map[string]string{"id": literal[0], "amount": amount, "status": literal[2], "__valet_source_ordinal": strconv.Itoa(i/4 + 1), "__valet_row": strconv.Itoa(i%4 + 1), "__valet_global_row": strconv.Itoa(i + 1), "__valet_source": source} {
				if row[name] != value {
					t.Fatalf("%s row %d %s=%q want %q", bucket, j, name, row[name], value)
				}
			}
		}
		policy := r.summary.RowOutput[bucket]
		wantPolicy := "emit"
		if mode == "report" || (mode == "failures" && bucket == "pass") {
			wantPolicy = "omit_by_policy"
		}
		if policy.Policy != wantPolicy || policy.Emitted != uint64(len(want)) || policy.Omitted != uint64(len(indices)-len(want)) {
			t.Fatalf("mode=%s %s policy=%+v", mode, bucket, policy)
		}
	}
	rejects := readRejects(t, r)
	wantRejects := 1
	if mode == "report" {
		wantRejects = 0
	}
	if len(rejects) != wantRejects {
		t.Fatalf("raw rejects=%d want %d", len(rejects), wantRejects)
	}
	if len(rejects) > 0 && !bytes.Contains(rejects[0]["raw"], []byte("oops")) {
		t.Fatal("raw reject lost invalid lexeme")
	}
	for _, is := range r.issues {
		if is.SourceOrdinal < 1 || is.SourceOrdinal > 2 || is.Row < 1 || is.Row > 4 || is.GlobalRow != (is.SourceOrdinal-1)*4+is.Row {
			t.Fatalf("wrong issue provenance %+v", is)
		}
	}
}
func TestVMCross01ModesWorkers(t *testing.T) {
	for _, mode := range []string{"all", "failures", "report"} {
		for _, workers := range []int{1, 4, 8} {
			for _, format := range []string{"csv", "parquet"} {
				t.Run(fmt.Sprintf("VM-CROSS-01/%s/workers%d/%s", mode, workers, format), func(t *testing.T) {
					root := t.TempDir()
					p := vmCrossOUTPipeline(t, root, mode, format, workers)
					r := runPipeline(t, root, p, "--json-output", "-", "--log-format", "json", "--log-level", "debug")
					vmCrossAssertOUT(t, r, mode)
					var printed summary
					vmJSON(t, r.stdout, &printed)
					if printed.RunID != r.summary.RunID {
						t.Fatal("stdout belongs to stale run")
					}
				})
			}
		}
	}
}
func vmCrossAssertSTAT(t testing.TB, r *runResult, mode, trust string) {
	t.Helper()
	if r.code != exitValidationFailed {
		t.Fatalf("F-STAT exit=%d stderr=%s", r.code, r.stderr)
	}
	vmAssertArtifacts(t, r)
	value, claimed, pass := uint64(16), uint64(0), uint64(13)
	if trust == "trusted" && mode != "all" {
		value, claimed, pass = 12, 4, 9
	}
	if r.summary.Total != value || r.summary.Passed != pass || r.summary.Errored != 3 || r.summary.Warned != 0 || r.summary.Coverage.ValueRows != value || r.summary.Coverage.MetadataClaimedRows != claimed || r.summary.Coverage.AccountedRows != 16 {
		t.Fatalf("mode=%s trust=%s counts=%+v coverage=%+v", mode, trust, countsOf(r), r.summary.Coverage)
	}
	c := r.summary.Coverage
	if c.RowGroupsTotal == nil || *c.RowGroupsTotal != 4 || c.RowGroupsScanned == nil || *c.RowGroupsScanned != 4-claimed/4 || c.RowGroupsStatisticsOnly == nil || *c.RowGroupsStatisticsOnly != claimed/4 {
		t.Fatalf("wrong group partition %+v", c)
	}
	want := []issueAt{{6, "NOT_NULL"}, {9, "POSITIVE"}, {10, "POSITIVE"}}
	if !reflect.DeepEqual(issuesOf(r), want) {
		t.Fatalf("F-STAT issues=%+v", issuesOf(r))
	}
	for _, rule := range []string{"NOT_NULL", "POSITIVE"} {
		if r.summary.RuleCoverage[rule] != (vmRuleCoverage{ValueRows: value, MetadataClaimedRows: claimed}) {
			t.Fatalf("%s coverage %+v", rule, r.summary.RuleCoverage[rule])
		}
	}
	policy := "none"
	if trust == "trusted" {
		policy = "producer_statistics"
	}
	if r.summary.TrustPolicy != policy {
		t.Fatalf("trust=%s", r.summary.TrustPolicy)
	}
	verification, basis := "values", "values"
	if claimed > 0 {
		verification, basis = "metadata_assisted", "metadata_prefix"
	}
	if r.summary.Verification != verification || r.summary.ProvenancePositionBasis != basis {
		t.Fatalf("verification=%s basis=%s", r.summary.Verification, r.summary.ProvenancePositionBasis)
	}
	for _, is := range r.issues {
		group := uint64(1)
		inGroup := uint64(2)
		if is.GlobalRow >= 9 {
			group = 2
			inGroup = is.GlobalRow - 8
		}
		if is.RowGroup == nil || *is.RowGroup != group || is.RowInGroup != inGroup || is.PositionBasis != basis {
			t.Fatalf("wrong issue coordinates %+v", is)
		}
	}
	for bucket, want := range map[string]int{"pass": 13, "warn": 0, "error": 3} {
		if mode == "report" || (mode == "failures" && bucket == "pass") {
			want = 0
		}
		rows := vmRows(t, r, bucket)
		if len(rows) != want {
			t.Fatalf("%s rows=%d want %d", bucket, len(rows), want)
		}
		if bucket == "error" && len(rows) > 0 {
			for i, v := range []string{`\N`, "-1", "0"} {
				if rows[i]["amount"] != v || rows[i]["__valet_global_row"] != []string{"6", "9", "10"}[i] {
					t.Fatalf("error row %d=%v", i, rows[i])
				}
			}
		}
	}
	evidence := vmEvidence(t, r)
	if trust == "off" {
		if len(evidence) != 0 {
			t.Fatal("off policy emitted proof ledger")
		}
		return
	}
	if len(evidence) != 4 {
		t.Fatalf("evidence groups=%d", len(evidence))
	}
	for i, e := range evidence {
		action := "scan"
		if i == 0 && claimed > 0 {
			action = "skip"
		}
		if e["action"] != action || fmt.Sprint(e["row_group"]) != strconv.Itoa(i) {
			t.Fatalf("evidence group %d=%v", i, e)
		}
	}
}
func TestVMCross02ModesTrust(t *testing.T) {
	for _, mode := range []string{"all", "failures", "report"} {
		for _, trust := range []string{"off", "trusted"} {
			t.Run("VM-CROSS-02/"+mode+"/"+trust, func(t *testing.T) {
				root := t.TempDir()
				input := vmFixtureSTAT(t, root)
				p := defaultPipeline(input, "parquet", filepath.Join(root, "output"), "parquet").with(t, vmStatConfig)
				p.Output.Mode = mode
				p.Execution.Workers = 4
				r := runPipeline(t, root, p, "--parquet-stats", trust, "--log-level", "error")
				vmCrossAssertSTAT(t, r, mode, trust)
				if trust == "trusted" {
					vmAssertEvidence(t, r, vmInspectParquet(t, input))
				}
			})
		}
	}
}
func TestVMCross03StorageDirections(t *testing.T) {
	for _, remoteInput := range []bool{false, true} {
		for _, remoteOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("VM-CROSS-03/sourceS3=%t/outputS3=%t", remoteInput, remoteOutput), func(t *testing.T) {
				root := t.TempDir()
				input := vmFixtureSTAT(t, root)
				descriptor := vmInspectParquet(t, input)
				if !remoteInput && !remoteOutput {
					p := defaultPipeline(input, "parquet", filepath.Join(root, "output"), "parquet").with(t, vmStatConfig)
					p.Output.Mode = "report"
					r := runPipeline(t, root, p, "--parquet-stats", "trusted", "--log-level", "error")
					vmCrossAssertSTAT(t, r, "report", "trusted")
					vmAssertEvidence(t, r, descriptor)
					return
				}
				e := minioEnvironment(t)
				source := input
				if remoteInput {
					body, err := os.ReadFile(input)
					if err != nil {
						t.Fatal(err)
					}
					e.put(t, "input.parquet", string(body))
					source = e.uri("input.parquet")
				}
				proxy := vmNewProxy(t, e)
				sourcePath := "/" + e.bucket + "/" + e.prefix + "input.parquet"
				if remoteInput {
					proxy.DenyPayload(sourcePath, descriptor, 0)
				}
				proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
					if (r.Method == "PUT" || r.Method == "POST") && (strings.Contains(r.URL.Path, "/pass/") || strings.Contains(r.URL.Path, "/warn/") || strings.Contains(r.URL.Path, "/error/") || strings.Contains(r.URL.Path, "/rejects/")) {
						http.Error(w, "report-only row write forbidden", 403)
						return true
					}
					return false
				}
				out := filepath.Join(root, "output")
				if remoteOutput {
					out = e.uri("output")
				}
				p := defaultPipeline(source, "parquet", out, "parquet").with(t, vmStatConfig)
				p.Source.S3 = e.s3(proxy.URL)
				p.Output.S3 = e.s3(proxy.URL)
				p.Output.Mode = "report"
				p.Execution.MemoryBudget = "128MiB"
				path := p.write(t, filepath.Join(root, "pipeline.yml"))
				stdout, stderr, code := invoke(t, "validate", "-p", path, "--parquet-stats", "trusted", "--log-level", "error")
				localOut := out
				if remoteOutput {
					localOut = filepath.Join(root, "download")
					e.download(t, "output", localOut)
				}
				r := collect(t, &runResult{root: root, output: localOut, format: "parquet", stdout: stdout, stderr: stderr, code: code})
				vmCrossAssertSTAT(t, r, "report", "trusted")
				vmAssertEvidence(t, r, descriptor)
				for _, request := range proxy.Requests() {
					if request.Status == 403 {
						t.Fatalf("forbidden backend operation attempted: %+v", request)
					}
					if remoteInput && request.Path == sourcePath && request.Method == "GET" && request.IfMatch == "" && request.VersionID == "" {
						t.Fatalf("unconditional source read %+v", request)
					}
				}
				if remoteInput {
					var pinned uint64
					for _, req := range proxy.Requests() {
						if req.Path == sourcePath && req.Method == "HEAD" {
							pinned++
						}
					}
					if pinned < 2 {
						t.Fatalf("missing final source identity HEAD: %+v", proxy.Requests())
					}
				}
				if remoteOutput {
					for _, key := range e.keys(t, "output") {
						for _, bucket := range []string{"/pass/", "/warn/", "/error/", "/rejects/"} {
							if strings.Contains(key, bucket) {
								t.Fatalf("prohibited object persisted %s", key)
							}
						}
					}
				}
			})
		}
	}
}
func vmCrossPipeRun(t testing.TB, root string, p *pipeline, brokenStdout, brokenStderr bool) *runResult {
	t.Helper()
	path := p.write(t, filepath.Join(root, "pipeline.yml"))
	cmd, ctx, cancel := command(t, "validate", "-p", path, "--json-output", "-", "--log-format", "json", "--log-level", "debug")
	defer cancel()
	var out, errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout
	if brokenStdout {
		rd, wr, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		_ = rd.Close()
		defer wr.Close()
		cmd.Stdout = wr
	}
	if brokenStderr {
		rd, wr, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		_ = rd.Close()
		defer wr.Close()
		cmd.Stderr = wr
	}
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("pipe case deadline: %s", errout.String())
	}
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatal(err)
		}
		code = ee.ExitCode()
	}
	return collect(t, &runResult{root: root, output: p.Output.URI, format: p.Output.Format, code: code, stdout: out.Bytes(), stderr: errout.Bytes()})
}
func TestVMCross04FailurePrecedence(t *testing.T) {
	for _, mode := range []string{"all", "failures", "report"} {
		for _, presentation := range []string{"normal", "stderr-closed", "stdout-closed", "both-closed"} {
			t.Run("VM-CROSS-04/"+mode+"/"+presentation, func(t *testing.T) {
				root := t.TempDir()
				p := vmCrossOUTPipeline(t, root, mode, "csv", 4)
				p.with(t, "dataset_rules: [{code: TOO_FEW, metric: row_count, min: 9, level: error}]")
				r := vmCrossPipeRun(t, root, p, strings.Contains(presentation, "stdout") || presentation == "both-closed", strings.Contains(presentation, "stderr") || presentation == "both-closed")
				want := 1
				if presentation == "stdout-closed" || presentation == "both-closed" {
					want = 3
				}
				if r.code != want {
					t.Fatalf("exit=%d want %d stderr=%s", r.code, want, r.stderr)
				}
				vmAssertArtifacts(t, r)
				if r.summary.Errored != 4 || r.summary.Validation != "fail" || len(r.summary.DatasetIssues) != 1 || r.summary.DatasetIssues[0].Code != "TOO_FEW" {
					t.Fatalf("row/dataset precedence %+v", r.summary)
				}
			})
		}
		t.Run("VM-CROSS-04/"+mode+"/schema-rejection-stderr-closed", func(t *testing.T) {
			root := t.TempDir()
			path := put(t, filepath.Join(root, "missing.csv"), "id\nA\n")
			p := defaultPipeline(path, "csv", filepath.Join(root, "output"), "csv").with(t, "input: {schema: {columns: {required: {type: int64, required: true}}}}")
			p.Output.Mode = mode
			r := vmCrossPipeRun(t, root, p, false, true)
			if r.code != 1 || r.summary.Total != 0 || len(r.summary.SchemaIssues) == 0 {
				t.Fatalf("schema rejection exit=%d summary=%+v", r.code, r.summary)
			}
			vmAssertArtifacts(t, r)
		})
	}
}
func TestVMCross05RepeatedJobsAndCleanup(t *testing.T) {
	t.Run("VM-CROSS-05/local-repeated", func(t *testing.T) {
		root := t.TempDir()
		p := vmCrossOUTPipeline(t, root, "report", "csv", 8)
		sentinel := put(t, filepath.Join(p.Output.URI, "unrelated-user-object"), "never replace me")
		path := p.write(t, filepath.Join(root, "pipeline.yml"))
		seen := map[string]bool{}
		roots := map[string]bool{}
		for range 3 {
			stdout, stderr, code := invoke(t, "validate", "-p", path)
			if code != 1 {
				t.Fatalf("exit=%d stderr=%s", code, stderr)
			}
			var current string
			for _, m := range files(t, filepath.Join(p.Output.URI, "*", "manifest.json")) {
				if !seen[m] {
					if current != "" {
						t.Fatal("one invocation published multiple manifests")
					}
					current = m
				}
			}
			if current == "" {
				t.Fatal("new invocation reused stale manifest")
			}
			seen[current] = true
			r := collect(t, &runResult{root: root, output: p.Output.URI, format: "csv", stdout: stdout, stderr: stderr, code: code, manifests: []string{current}})
			vmCrossAssertOUT(t, r, "report")
			if roots[r.summary.RunID] || r.summary.RunID == "" {
				t.Fatal("run ID reused")
			}
			roots[r.summary.RunID] = true
		}
		body, err := os.ReadFile(sentinel)
		if err != nil || string(body) != "never replace me" {
			t.Fatalf("user object changed: %q %v", body, err)
		}
		if len(seen) != 3 {
			t.Fatal("runs not unique")
		}
	})
	t.Run("VM-CROSS-05/MinIO-timeout-cleanup", func(t *testing.T) {
		e := minioEnvironment(t)
		e.put(t, "input.csv", fixtureA)
		e.put(t, "unrelated-user-object", "preserve")
		barrier := vmNewBarrier()
		t.Cleanup(barrier.Release)
		proxy := vmNewProxy(t, e)
		proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/input.csv") {
				_ = barrier.Block(r.Context())
				http.Error(w, "cancelled", 503)
				return true
			}
			return false
		}
		root := t.TempDir()
		p := defaultPipeline(e.uri("input.csv"), "csv", e.uri("output"), "csv").with(t, rulesA)
		p.Source.S3 = e.s3(proxy.URL)
		p.Output.S3 = e.s3(proxy.URL)
		p.Execution.MemoryBudget = "128MiB"
		path := p.write(t, filepath.Join(root, "pipeline.yml"))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cmd := exec.CommandContext(ctx, binaryPath, "validate", "-p", path)
		cmd.WaitDelay = 5 * time.Second
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		waitCtx, waitCancel := callContext(t)
		defer waitCancel()
		if err := barrier.WaitForProcess(waitCtx, done); err != nil {
			cancel()
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				<-done
			}
			t.Fatalf("%v; stderr=%s", err, stderr.String())
		}
		// Start the deadline only after observing the actual blocked backend read.
		expired, stop := context.WithTimeout(waitCtx, time.Millisecond)
		<-expired.Done()
		stop()
		cancel()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("timed-out process succeeded")
			}
		case <-waitCtx.Done():
			_ = cmd.Process.Kill()
			<-done
			t.Fatal("timed-out process did not terminate")
		}
		barrier.Release()
		for _, key := range e.keys(t, "output") {
			if strings.HasSuffix(key, "/manifest.json") {
				t.Fatalf("timed-out run published %s", key)
			}
		}
		if body := e.getObject(t, "unrelated-user-object"); body != "preserve" {
			t.Fatal("unrelated object changed")
		}
	})
	if runtime.GOOS != "windows" {
		t.Run("VM-CROSS-05/MinIO-signal", vmCrossSignal)
	}
}

// os.Interrupt is sent only on native Unix; Windows has no equivalent graceful signal.
func vmCrossSignal(t *testing.T) {
	e := minioEnvironment(t)
	e.put(t, "input.csv", fixtureA)
	barrier := vmNewBarrier()
	t.Cleanup(barrier.Release)
	proxy := vmNewProxy(t, e)
	proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/input.csv") {
			_ = barrier.Block(r.Context())
			http.Error(w, "cancelled", 503)
			return true
		}
		return false
	}
	root := t.TempDir()
	p := defaultPipeline(e.uri("input.csv"), "csv", e.uri("output"), "csv")
	p.Source.S3 = e.s3(proxy.URL)
	p.Output.S3 = e.s3(proxy.URL)
	p.Execution.MemoryBudget = "128MiB"
	path := p.write(t, filepath.Join(root, "pipeline.yml"))
	cmd, ctx, cancel := command(t, "validate", "-p", path)
	defer cancel()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	if err := barrier.WaitForProcess(ctx, done); err != nil {
		cancel()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			<-done
		}
		t.Fatalf("%v; stderr=%s", err, stderr.String())
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		_ = cmd.Process.Kill()
		<-done
		t.Fatal(err)
	}
	err := <-done
	if ctx.Err() != nil {
		t.Fatal("signal cancellation exceeded deadline")
	}
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 130 {
		t.Fatalf("signal exit=%v stderr=%s", err, stderr.String())
	}
	barrier.Release()
	for _, key := range e.keys(t, "output") {
		if strings.HasSuffix(key, "/manifest.json") {
			t.Fatal("signal published manifest")
		}
	}
}
func TestVMCross06NativeGeneratedWorkloads(t *testing.T) {
	info, err := buildinfo.ReadFile(binaryPath)
	if err != nil {
		t.Fatalf("read native CLI build provenance: %v", err)
	}
	settings := map[string]string{}
	for _, s := range info.Settings {
		settings[s.Key] = s.Value
	}
	if settings["GOOS"] != runtime.GOOS || settings["GOARCH"] != runtime.GOARCH {
		t.Fatalf("not native executable: %v", settings)
	}
	if settings["-race"] != "true" && settings["CGO_ENABLED"] != "0" {
		t.Fatal("release gate requires the no-cgo native artifact; race artifacts may use cgo")
	}
	t.Logf("VM-CROSS-06 binary=%s target=%s/%s toolchain=%s race=%s cgo=%s", binaryPath, runtime.GOOS, runtime.GOARCH, info.GoVersion, settings["-race"], settings["CGO_ENABLED"])
	sizes := []int{32, 65536}
	if raw := os.Getenv("VALET_VM_SCALE_ROWS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 65536 {
			t.Fatalf("VALET_VM_SCALE_ROWS must be >=65536: %q", raw)
		}
		sizes[1] = n
	}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("VM-CROSS-06/rows%d", n), func(t *testing.T) {
			root := t.TempDir()
			schema := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
			input := filepath.Join(root, "generated.parquet")
			f, err := os.Create(input)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(
				parquet.WithDataPageSize(16<<10),
				parquet.WithDictionaryDefault(false),
				parquet.WithBatchSize(512),
				parquet.WithMaxRowGroupLength(4096),
			), pqarrow.DefaultWriterProps())
			if err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			// Bound generator memory independently of requested scale.
			for start := 0; start < n; start += 4096 {
				end := min(start+4096, n)
				b := array.NewInt64Builder(memory.DefaultAllocator)
				for i := start; i < end; i++ {
					b.Append(int64(i + 1))
				}
				a := b.NewArray()
				b.Release()
				record := array.NewRecordBatch(schema, []arrow.Array{a}, int64(end-start))
				a.Release()
				err = writer.Write(record)
				record.Release()
				if err != nil {
					_ = writer.Close()
					t.Fatal(err)
				}
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			var observed int
			vmVisitParquet(t, input, func(rec arrow.RecordBatch, i int) {
				observed++
				if rec.Column(0).ValueStr(i) != strconv.Itoa(observed) {
					t.Fatalf("fixture row %d not literal", observed)
				}
			})
			if observed != n {
				t.Fatalf("fixture rows=%d", observed)
			}
			for _, mode := range []string{"all", "failures", "report"} {
				for _, trust := range []string{"off", "trusted"} {
					p := defaultPipeline(input, "parquet", filepath.Join(root, mode+"-"+trust), "parquet").with(t, vmStatConfig)
					p.Output.Mode = mode
					p.Execution.Workers = 8
					p.Execution.MemoryBudget = "128MiB"
					r := runPipeline(t, root, p, "--parquet-stats", trust, "--log-format", "json", "--log-level", "debug")
					if r.code != 0 {
						t.Fatalf("%s/%s exit=%d stderr=%s", mode, trust, r.code, r.stderr)
					}
					vmAssertArtifacts(t, r)
					value, claimed := uint64(n), uint64(0)
					if mode != "all" && trust == "trusted" {
						value, claimed = 0, uint64(n)
					}
					if r.summary.Coverage.ValueRows != value || r.summary.Coverage.MetadataClaimedRows != claimed || r.summary.Coverage.AccountedRows != uint64(n) {
						t.Fatalf("generated coverage %+v", r.summary.Coverage)
					}
					emitted := 0
					for _, artifact := range vmArtifacts(t, r) {
						if artifact.Kind != "pass" {
							continue
						}
						vmVisitParquet(t, vmResolve(t, r, artifact.URI), func(rec arrow.RecordBatch, i int) {
							emitted++
							want := strconv.Itoa(emitted)
							for _, name := range []string{"amount", "__valet_row", "__valet_global_row"} {
								if arrowColumn(t, rec, name).ValueStr(i) != want {
									t.Fatalf("generated row %d field %s differs", emitted, name)
								}
							}
						})
					}
					want := 0
					if mode == "all" {
						want = n
					}
					if emitted != want {
						t.Fatalf("emitted rows=%d want %d", emitted, want)
					}
				}
			}
		})
	}
}
