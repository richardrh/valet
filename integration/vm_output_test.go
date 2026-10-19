//go:build integration

package integration_test

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func vmOutputPipeline(t testing.TB, root, inputType, format string) *pipeline {
	paths := vmFixtureOUT(t, root, inputType)
	p := defaultPipeline(paths[0], inputType, filepath.Join(root, "output"), format).with(t, vmOutConfig)
	p.Source.URIs = paths
	return p
}

func vmCheckOUT(t testing.TB, r *runResult, mode string) {
	t.Helper()
	for bucket, n := range map[string]uint64{"pass": 2, "warn": 2, "error": 4, "rejects": 1} {
		policy := "emit"
		emitted, omitted := n, uint64(0)
		if mode == "report" || (mode == "failures" && bucket == "pass") {
			policy = "omit_by_policy"
			emitted, omitted = 0, n
		}
		got := r.summary.RowOutput[bucket]
		if got.Policy != policy || got.Emitted != emitted || got.Omitted != omitted {
			t.Errorf("%s materialization=%+v want %s/%d/%d", bucket, got, policy, emitted, omitted)
		}
	}
	if r.code != 1 {
		t.Fatalf("exit=%d stderr=%s", r.code, r.stderr)
	}
	if got := countsOf(r); got != (counts{8, 2, 2, 4}) {
		t.Fatalf("counts=%+v", got)
	}
	if r.summary.ReportVersion != 2 || r.manifest["output_mode"] != mode {
		t.Fatalf("version/mode: %v", r.manifest)
	}
	vmAssertArtifacts(t, r)
	wantIssues := []issueAt{{2, "POSITIVE"}, {3, "STATUS_OK"}, {4, "POSITIVE"}, {4, "STATUS_OK"}, {5, "POSITIVE"}, {6, "INGEST_TYPE"}, {8, "STATUS_OK"}}
	if !reflect.DeepEqual(issuesOf(r), wantIssues) {
		t.Errorf("issues=%v want=%v", issuesOf(r), wantIssues)
	}
	for _, issue := range r.issues {
		if issue.GlobalRow == 0 {
			t.Error("row issue has no row")
			continue
		}
		if issue.Row != (issue.GlobalRow-1)%4+1 || issue.SourceOrdinal != (issue.GlobalRow-1)/4+1 {
			t.Errorf("issue source coordinates=%+v", issue)
		}
	}
	if r.summary.RuleCounts["POSITIVE"] != 3 || r.summary.RuleCounts["STATUS_OK"] != 3 || r.summary.IngestCounts["INGEST_TYPE"] != 1 {
		t.Errorf("issue populations rules=%v ingest=%v", r.summary.RuleCounts, r.summary.IngestCounts)
	}
	if r.summary.Unevaluable["POSITIVE"] != 1 {
		t.Errorf("unevaluable=%v", r.summary.Unevaluable)
	}
	var s map[string]any
	decodeFile(t, filepath.Join(r.dir(t), "summary.json"), &s)
	for _, field := range []string{"output_mode", "verification", "trust_policy", "coverage", "row_output", "provenance_position_basis"} {
		if !reflect.DeepEqual(s[field], r.manifest[field]) {
			t.Errorf("summary/manifest %s mismatch: %v / %v", field, s[field], r.manifest[field])
		}
	}
	coverage := s["rule_coverage"].(map[string]any)
	for _, path := range files(t, filepath.Join(r.dir(t), "issues", "*.ndjson")) {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(body, []byte("oops")) {
			t.Error("ordinary issue disclosed rejected value")
		}
	}
	for _, code := range []string{"POSITIVE", "STATUS_OK"} {
		c := coverage[code].(map[string]any)
		if fmt.Sprint(c["value_rows"]) != "8" || fmt.Sprint(c["metadata_claimed_rows"]) != "0" {
			t.Errorf("%s coverage=%v", code, c)
		}
	}
	for bucket, ids := range map[string][]string{"pass": {"A", "G"}, "warn": {"C", "H"}, "error": {"B", "D", "E", "F"}} {
		if mode == "report" || (mode == "failures" && bucket == "pass") {
			ids = nil
		}
		rows := vmRows(t, r, bucket)
		if !reflect.DeepEqual(column(rows, "id"), ids) && !(len(rows) == 0 && len(ids) == 0) {
			t.Fatalf("%s IDs=%v want=%v", bucket, column(rows, "id"), ids)
		}
		for _, row := range rows {
			id := row["id"]
			index := strings.Index("ABCDEFGH", id)
			wantAmount := []string{"10", "-1", "5", "-2", "0", `\N`, "7", "2"}[index]
			if row["amount"] != wantAmount {
				t.Errorf("%s amount=%q want=%q", id, row["amount"], wantAmount)
			}
			wantStatus := "ok"
			if id == "C" || id == "D" || id == "H" {
				wantStatus = "pending"
			}
			if row["status"] != wantStatus {
				t.Errorf("%s status=%q want=%q", id, row["status"], wantStatus)
			}
			if got, want := filepath.Base(row["__valet_source"]), fmt.Sprintf("out-%d.%s", index/4, strings.TrimPrefix(filepath.Ext(row["__valet_source"]), ".")); got != want {
				t.Errorf("%s source=%q", id, row["__valet_source"])
			}
			for name, want := range map[string]string{"__valet_global_row": fmt.Sprint(index + 1), "__valet_row": fmt.Sprint(index%4 + 1), "__valet_source_ordinal": fmt.Sprint(index/4 + 1)} {
				if row[name] != want {
					t.Errorf("%s %s=%q want=%q", id, name, row[name], want)
				}
			}
		}
	}
	rejects := files(t, filepath.Join(r.dir(t), "rejects", "*"))
	if mode == "report" {
		if len(rejects) != 0 {
			t.Errorf("report wrote rejects: %v", rejects)
		}
		for _, bucket := range []string{"pass", "warn", "error"} {
			if entries, _ := os.ReadDir(filepath.Join(r.dir(t), bucket)); len(entries) > 0 {
				t.Errorf("omitted %s files=%v", bucket, entries)
			}
		}
	} else {
		if len(rejects) != 1 {
			t.Fatalf("reject files=%v", rejects)
		}
		b, err := os.ReadFile(rejects[0])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(b, []byte("oops")) || !bytes.Contains(b, []byte("invalid")) {
			t.Errorf("reject lost original/state: %s", b)
		}
		if lines := bytes.Count(bytes.TrimSpace(b), []byte("\n")) + 1; lines != 1 {
			t.Errorf("reject records=%d", lines)
		}
	}
}

func TestVMOutput(t *testing.T) {
	for _, tc := range []struct{ id, mode string }{{"VM-OUT-01", "all"}, {"VM-OUT-02", "failures"}, {"VM-OUT-03", "report"}} {
		t.Run(tc.id, func(t *testing.T) {
			for _, format := range []string{"csv", "parquet"} {
				t.Run(format, func(t *testing.T) {
					root := t.TempDir()
					p := vmOutputPipeline(t, root, "csv", format)
					flags := []string{}
					if tc.mode != "all" {
						flags = append(flags, "--output-mode", tc.mode)
					}
					vmCheckOUT(t, runPipeline(t, root, p, flags...), tc.mode)
				})
			}
		})
	}
	t.Run("VM-OUT-04", func(t *testing.T) {
		for _, typ := range []string{"csv", "ndjson", "json", "parquet"} {
			for _, mode := range []string{"all", "failures", "report"} {
				t.Run(typ+"/"+mode, func(t *testing.T) {
					root := t.TempDir()
					vmCheckOUT(t, runPipeline(t, root, vmOutputPipeline(t, root, typ, "parquet"), "--output-mode", mode), mode)
				})
			}
		}
	})
	t.Run("VM-OUT-05", func(t *testing.T) {
		for _, mode := range []string{"all", "failures", "report"} {
			t.Run(mode, func(t *testing.T) {
				r := run(t, scenario{input: "id\nA\nB\n", config: []string{"output: {mode: " + mode + "}", "dataset_rules: [{code: COUNT, metric: row_count, min: 3, level: error}]"}})
				if r.code != 1 || countsOf(r) != (counts{2, 2, 0, 0}) || len(r.summary.DatasetIssues) != 1 || len(r.summary.DatasetResults) != 1 {
					t.Fatalf("dataset failure: code=%d summary=%+v stderr=%s", r.code, r.summary, r.stderr)
				}
				if len(vmRows(t, r, "error")) != 0 {
					t.Fatal("invented error row")
				}
				vmAssertArtifacts(t, r)
			})
		}
	})
	t.Run("VM-OUT-06", func(t *testing.T) {
		for _, mode := range []string{"all", "failures", "report"} {
			t.Run(mode, func(t *testing.T) {
				r := run(t, scenario{input: "other\nA\n", config: []string{"output: {mode: " + mode + "}", "input: {schema: {columns: {required: {type: int64, required: true}}}}"}})
				if r.code != 1 || r.summary.Total != 0 || len(r.summary.SchemaIssues) == 0 {
					t.Fatalf("schema rejection code=%d summary=%+v stderr=%s", r.code, r.summary, r.stderr)
				}
				for _, bucket := range []string{"pass", "warn", "error"} {
					if len(vmRows(t, r, bucket)) != 0 {
						t.Fatal("schema rejection emitted rows")
					}
				}
				vmAssertArtifacts(t, r)
			})
		}
	})
	t.Run("VM-OUT-07", func(t *testing.T) {
		for _, mode := range []string{"all", "failures", "report"} {
			t.Run(mode, func(t *testing.T) {
				root := t.TempDir()
				a := put(t, filepath.Join(root, "a.csv"), "key,region\nA,X\nB,Y\n")
				b := put(t, filepath.Join(root, "b.csv"), "key,region\nC,Z\nA,X\n")
				ref := put(t, filepath.Join(root, "ref.csv"), "key,region\nA,X\nB,Y\n")
				p := defaultPipeline(a, "csv", filepath.Join(root, "out"), "parquet")
				p.Source.URIs = []string{a, b}
				p.with(t, fragment(t, obj{"output": obj{"mode": mode}, "rules": []obj{{"code": "UNIQUE", "level": "error", "column": "key", "checks": []obj{{"type": "is_unique"}}}, {"code": "REF", "level": "error", "checks": []obj{{"type": "referential_integrity_composite", "columns": []string{"key", "region"}, "reference_file": ref, "reference_columns": []string{"key", "region"}}}}}}))
				r := runPipeline(t, root, p)
				if r.code != 1 || !reflect.DeepEqual(issuesOf(r), []issueAt{{1, "UNIQUE"}, {3, "REF"}, {4, "UNIQUE"}}) || countsOf(r) != (counts{4, 1, 0, 3}) {
					t.Fatalf("cross-source result code=%d counts=%+v issues=%v stderr=%s", r.code, countsOf(r), issuesOf(r), r.stderr)
				}
				vmAssertArtifacts(t, r)
			})
		}
	})
	t.Run("VM-OUT-08", func(t *testing.T) {
		for _, pair := range [][2]string{{"report", "all"}, {"all", "failures"}, {"failures", "report"}} {
			t.Run(pair[0]+"/"+pair[1], func(t *testing.T) {
				root := t.TempDir()
				p := vmOutputPipeline(t, root, "csv", "csv").with(t, "output: {mode: "+pair[0]+"}")
				vmCheckOUT(t, runPipeline(t, root, p, "--output-mode", pair[1]), pair[1])
			})
		}
		for _, value := range []string{"", "ALL", "unknown"} {
			root := t.TempDir()
			p := defaultPipeline(filepath.Join(root, "absent"), "csv", filepath.Join(root, "out"), "csv")
			path := p.write(t, filepath.Join(root, "p.yml"))
			_, stderr, code := invoke(t, "validate", "-p", path, "--output-mode="+value)
			if code != 2 || strings.Contains(string(stderr), "no such file") {
				t.Errorf("invalid mode %q code=%d stderr=%s", value, code, stderr)
			}
		}
		for _, command := range []string{"schema"} {
			_, _, code := invoke(t, command, "--output-mode=report")
			if code != 2 {
				t.Errorf("scope %s exit=%d", command, code)
			}
		}
	})
	t.Run("VM-OUT-12", func(t *testing.T) {
		for _, mode := range []string{"all", "failures", "report"} {
			for _, kind := range []string{"empty", "warn", "error"} {
				t.Run(mode+"/"+kind, func(t *testing.T) {
					input := "x\n"
					level := "warn"
					n := uint64(0)
					code := 0
					if kind != "empty" {
						input += "bad\nbad\n"
						n = 2
					}
					if kind == "error" {
						level = "error"
						code = 1
					}
					r := run(t, scenario{input: input, config: []string{"output: {mode: " + mode + "}", "input: {schema: {columns: {x: {type: string}}}}", "rules: [{code: X, column: x, level: " + level + ", checks: [{type: equals, value: good}]}]"}})
					if r.code != code || r.summary.Total != n || uint64(len(r.issues)) != n {
						t.Fatalf("code=%d summary=%+v issues=%v", r.code, r.summary, r.issues)
					}
					for _, bucket := range []string{"pass", "warn", "error"} {
						want := 0
						if n > 0 && bucket == kind && mode != "report" {
							want = 2
						}
						if len(vmRows(t, r, bucket)) != want {
							t.Errorf("%s rows mismatch", bucket)
						}
						if want == 0 && len(files(t, filepath.Join(r.dir(t), bucket, "*"))) != 0 {
							t.Errorf("empty %s part", bucket)
						}
					}
					vmAssertArtifacts(t, r)
				})
			}
		}
	})
}

func TestVMOutputStorage(t *testing.T) {
	t.Run("VM-OUT-09", func(t *testing.T) {
		for _, mode := range []string{"all", "failures", "report"} {
			t.Run(mode, func(t *testing.T) {
				e := minioEnvironment(t)
				proxy := vmNewProxy(t, e)
				var mu sync.Mutex
				var denied []string
				proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
					if (r.Method == "PUT" || r.Method == "POST") && (strings.Contains(r.URL.Path, "/pass/") || (mode == "report" && (strings.Contains(r.URL.Path, "/warn/") || strings.Contains(r.URL.Path, "/error/") || strings.Contains(r.URL.Path, "/rejects/")))) {
						mu.Lock()
						denied = append(denied, r.URL.Path)
						mu.Unlock()
						http.Error(w, "denied", 403)
						return true
					}
					return false
				}
				root := t.TempDir()
				p := vmOutputPipeline(t, root, "csv", "parquet")
				p.Output.URI = e.uri("output")
				p.Output.S3 = e.s3(proxy.URL)
				path := p.write(t, filepath.Join(root, "p.yml"))
				_, stderr, code := invoke(t, "validate", "-p", path, "--output-mode", mode)
				want := 1
				if mode == "all" {
					want = 2
				}
				if code != want {
					t.Fatalf("code=%d want=%d stderr=%s", code, want, stderr)
				}
				mu.Lock()
				attempts := len(denied)
				mu.Unlock()
				if (mode == "all") != (attempts > 0) {
					t.Errorf("denied attempts=%d", attempts)
				}
				if mode == "all" {
					for _, k := range e.keys(t, "output") {
						if strings.HasSuffix(k, "manifest.json") {
							t.Fatal("manifest after failed required write")
						}
					}
				} else {
					local := filepath.Join(root, "download")
					e.download(t, "output", local)
					r := collect(t, &runResult{output: local, format: "parquet", code: code})
					vmCheckOUT(t, r, mode)
				}
			})
		}
	})
	t.Run("VM-OUT-10", func(t *testing.T) {
		for _, mode := range []string{"all", "failures", "report"} {
			for _, target := range []string{"summary.json", "artifacts", "manifest.json"} {
				t.Run(mode+"/"+target, func(t *testing.T) {
					e := minioEnvironment(t)
					proxy := vmNewProxy(t, e)
					proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
						if (r.Method == "PUT" || r.Method == "POST") && strings.Contains(r.URL.Path, target) {
							http.Error(w, "denied", 403)
							return true
						}
						return false
					}
					root := t.TempDir()
					p := vmOutputPipeline(t, root, "csv", "csv")
					p.Output.URI = e.uri("output")
					p.Output.S3 = e.s3(proxy.URL)
					path := p.write(t, filepath.Join(root, "p.yml"))
					_, stderr, code := invoke(t, "validate", "-p", path, "--output-mode", mode)
					denied := false
					for _, request := range proxy.Requests() {
						if strings.Contains(request.Path, target) && request.Status == 403 {
							denied = true
						}
					}
					if code != 2 || !denied {
						t.Fatalf("code=%d denied=%v stderr=%s requests=%+v", code, denied, stderr, proxy.Requests())
					}
					for _, k := range e.keys(t, "output") {
						if strings.HasSuffix(k, "manifest.json") {
							t.Fatal("manifest after publication failure")
						}
					}
				})
			}
			t.Run(mode+"/closed-stdout", func(t *testing.T) {
				if runtime.GOOS == "windows" {
					t.Skip("Unix pipe semantics")
				}
				root := t.TempDir()
				p := vmOutputPipeline(t, root, "csv", "parquet")
				path := p.write(t, filepath.Join(root, "p.yml"))
				cmd, ctx, cancel := command(t, "validate", "-p", path, "--output-mode", mode, "--json-output", "-")
				defer cancel()
				rd, wr, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				rd.Close()
				cmd.Stdout = wr
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				err = cmd.Run()
				wr.Close()
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				if err == nil || cmd.ProcessState.ExitCode() != 3 {
					t.Fatalf("exit=%v stderr=%s", cmd.ProcessState, stderr.String())
				}
				r := collect(t, &runResult{output: p.Output.URI, format: "parquet"})
				if len(r.manifests) != 1 {
					t.Fatal("stdout failure lost committed manifest")
				}
				vmAssertArtifacts(t, r)
			})
		}
	})
	t.Run("VM-OUT-11", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("Unix signals")
		}
		for _, signal := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
			for _, stage := range []string{"input", "multipart"} {
				t.Run(fmt.Sprintf("%s/%d", stage, signal), func(t *testing.T) {
					e := minioEnvironment(t)
					e.put(t, "input.csv", "id\n"+strings.Repeat(strings.Repeat("v", 1024)+"\n", 40000))
					proxy := vmNewProxy(t, e)
					barrier := vmNewBarrier()
					proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
						block := stage == "input" && r.Method == "GET" && strings.HasSuffix(r.URL.Path, "input.csv")
						block = block || (stage == "multipart" && r.Method == "PUT" && r.URL.Query().Get("partNumber") != "")
						if block {
							if err := barrier.Block(r.Context()); err != nil {
								return true
							}
						}
						return false
					}
					root := t.TempDir()
					p := defaultPipeline(e.uri("input.csv"), "csv", e.uri("output"), "csv")
					p.Source.S3 = e.s3(proxy.URL)
					p.Output.S3 = e.s3(proxy.URL)
					p.Output.PartSize = "64MiB"
					p.Execution.MemoryBudget = "128MiB"
					path := p.write(t, filepath.Join(root, "p.yml"))
					cmd, ctx, cancel := command(t, "validate", "-p", path)
					defer cancel()
					var stderr bytes.Buffer
					cmd.Stderr = &stderr
					if err := cmd.Start(); err != nil {
						t.Fatal(err)
					}
					done := make(chan error, 1)
					go func() { done <- cmd.Wait(); close(done) }()
					defer func() {
						barrier.Release()
						select {
						case <-done:
						default:
							_ = cmd.Process.Kill()
							<-done
						}
					}()
					select {
					case <-barrier.Entered:
					case err := <-done:
						t.Fatalf("child exited before %s barrier: %v stderr=%s requests=%+v", stage, err, stderr.String(), proxy.Requests())
					case <-ctx.Done():
						t.Fatal("barrier not reached")
					}
					if err := cmd.Process.Signal(signal); err != nil {
						t.Fatal(err)
					}
					barrier.Release()
					select {
					case <-done:
					case <-time.After(15 * time.Second):
						t.Fatal("signal termination hung")
					}
					if cmd.ProcessState.ExitCode() != 128+int(signal) {
						t.Fatalf("exit=%d stderr=%s", cmd.ProcessState.ExitCode(), stderr.String())
					}
					for _, key := range e.keys(t, "output") {
						if strings.HasSuffix(key, "manifest.json") {
							t.Fatal("cancelled run committed")
						}
					}
				})
			}
		}
	})
}

func TestVMOutputConfigurationBoundaries(t *testing.T) {
	t.Run("VM-OUT-08/config-invalid-before-access", func(t *testing.T) {
		for _, value := range []string{"''", "UNKNOWN"} {
			root := t.TempDir()
			p := defaultPipeline(filepath.Join(root, "missing.csv"), "csv", filepath.Join(root, "out"), "csv")
			p.with(t, "output: {mode: "+value+"}")
			path := p.write(t, filepath.Join(root, "p.yml"))
			_, stderr, code := invoke(t, "validate", "-p", path)
			if code != 2 || strings.Contains(string(stderr), "no such file") {
				t.Fatalf("invalid config code=%d stderr=%s", code, stderr)
			}
			if _, err := os.Stat(p.Output.URI); !os.IsNotExist(err) {
				t.Fatalf("invalid config accessed output: %v", err)
			}
		}
	})
	t.Run("VM-OUT-08/plan-policy-without-execution", func(t *testing.T) {
		root := t.TempDir()
		p := vmOutputPipeline(t, root, "parquet", "parquet")
		path := p.write(t, filepath.Join(root, "p.yml"))
		out, stderr, code := invoke(t, "plan", "-p", path, "--output-mode", "report", "--parquet-stats", "trusted")
		if code != 0 {
			t.Fatalf("plan code=%d stderr=%s", code, stderr)
		}
		var result map[string]any
		vmJSON(t, out, &result)
		if result["scope"] != "plan" {
			t.Fatalf("plan scope=%v", result)
		}
		if _, err := os.Stat(p.Output.URI); !os.IsNotExist(err) {
			t.Fatalf("plan published output: %v", err)
		}
	})
}

func TestVMOutputGroupPrepass(t *testing.T) {
	t.Run("VM-OUT-07/groups-and-ordered-tuples", func(t *testing.T) {
		for _, mode := range []string{"all", "failures", "report"} {
			t.Run(mode, func(t *testing.T) {
				root := t.TempDir()
				schema := arrow.NewSchema([]arrow.Field{{Name: "key", Type: arrow.BinaryTypes.String}, {Name: "region", Type: arrow.BinaryTypes.String}}, nil)
				paths := []string{}
				for source, rows := range [][][2]string{{{"A", "X"}, {"B", "Y"}}, {{"C", "Z"}, {"A", "X"}}} {
					var groups []arrow.RecordBatch
					for _, row := range rows {
						b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
						b.Field(0).(*array.StringBuilder).Append(row[0])
						b.Field(1).(*array.StringBuilder).Append(row[1])
						groups = append(groups, b.NewRecordBatch())
						b.Release()
					}
					path := filepath.Join(root, fmt.Sprintf("source%d.parquet", source))
					vmWriteParquet(t, path, schema, groups)
					for _, group := range groups {
						group.Release()
					}
					paths = append(paths, path)
				}
				ref := put(t, filepath.Join(root, "reference.csv"), "key,region\nA,X\nB,Y\nZ,C\n")
				p := defaultPipeline(paths[0], "parquet", filepath.Join(root, "out"), "parquet")
				p.Source.URIs = paths
				p.with(t, fragment(t, obj{"rules": []obj{
					{"code": "UNIQUE", "level": "error", "column": "key", "checks": []obj{{"type": "is_unique"}}},
					{"code": "REF", "level": "error", "checks": []obj{{"type": "referential_integrity_composite", "columns": []string{"key", "region"}, "reference_file": ref, "reference_columns": []string{"key", "region"}}}},
				}}))
				r := runPipeline(t, root, p, "--output-mode", mode)
				if r.code != 1 || countsOf(r) != (counts{4, 1, 0, 3}) || !reflect.DeepEqual(issuesOf(r), []issueAt{{1, "UNIQUE"}, {3, "REF"}, {4, "UNIQUE"}}) {
					t.Fatalf("group prepass code=%d counts=%+v issues=%v stderr=%s", r.code, countsOf(r), issuesOf(r), r.stderr)
				}
				if r.summary.Passes < 2 {
					t.Errorf("prepass was not disclosed: passes=%d", r.summary.Passes)
				}
				want := []string{"1", "3", "4"}
				if mode == "report" {
					want = nil
				}
				got := column(vmRows(t, r, "error"), "__valet_global_row")
				if !reflect.DeepEqual(got, want) && !(len(got) == 0 && len(want) == 0) {
					t.Errorf("error order=%v want=%v", got, want)
				}
				vmAssertArtifacts(t, r)
			})
		}
	})
}
