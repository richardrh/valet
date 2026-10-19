//go:build integration

package integration_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func vmLogRecords(t testing.TB, raw []byte) []map[string]any {
	t.Helper()
	s := bufio.NewScanner(bytes.NewReader(raw))
	s.Buffer(make([]byte, 4096), 4<<20)
	var records []map[string]any
	var seq int64
	run := ""
	for s.Scan() {
		m := vmOneJSON(t, s.Bytes())
		for _, k := range []string{"event", "level", "time", "msg"} {
			if _, ok := m[k].(string); !ok {
				t.Fatalf("missing envelope %s: %v", k, m)
			}
		}
		if m["log_version"] != json.Number("1") {
			t.Fatalf("log version: %v", m)
		}
		if _, err := time.Parse(time.RFC3339Nano, m["time"].(string)); err != nil {
			t.Fatal(err)
		}
		if id, ok := m["run_id"].(string); ok && id != "" {
			if run != "" && id != run {
				t.Fatal("mixed run IDs")
			}
			run = id
			n := vmLogInt(t, m, "seq")
			if n <= seq {
				t.Fatalf("nonmonotonic sequence %d <= %d", n, seq)
			}
			seq = n
			if m["command"] == nil {
				t.Fatal("missing command")
			}
		}
		records = append(records, m)
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}
func vmLogInt(t testing.TB, m map[string]any, key string) int64 {
	t.Helper()
	v, ok := m[key].(json.Number)
	if !ok {
		t.Fatalf("missing numeric %s: %v", key, m)
	}
	n, err := v.Int64()
	if err != nil || n < 0 {
		t.Fatalf("invalid %s=%v", key, v)
	}
	return n
}
func vmTerminal(t testing.TB, logs []byte, code int, committed, complete bool) map[string]any {
	t.Helper()
	var terminal map[string]any
	for _, m := range vmLogRecords(t, logs) {
		if m["event"] == "run.finished" {
			if terminal != nil {
				t.Fatal("duplicate terminal")
			}
			terminal = m
		}
		if m["event"] == "run.phase" {
			vmLogInt(t, m, "work_ns")
			vmLogInt(t, m, "wait_ns")
		}
	}
	if terminal == nil {
		t.Fatalf("missing terminal: %s", logs)
	}
	if vmLogInt(t, terminal, "exit_code") != int64(code) || terminal["manifest_committed"] != committed || terminal["counters_complete"] != complete {
		t.Fatalf("terminal truth %v", terminal)
	}
	return terminal
}
func vmLogPipeline(t testing.TB, root string) *pipeline {
	t.Helper()
	src := put(t, filepath.Join(root, "input.csv"), "amount\n1\n2\n")
	return defaultPipeline(src, "csv", filepath.Join(root, "result"), "csv").with(t, `input: {schema: {columns: {amount: {type: int64}}}}
rules: [{code: POSITIVE, column: amount, level: error, checks: [{type: valid_range, min: 0, exclusive_min: true}]}]
`)
}
func TestVMLogs(t *testing.T) {
	t.Run("VM-LOG-01", func(t *testing.T) {
		root := t.TempDir()
		r := runPipeline(t, root, vmLogPipeline(t, root), "--json-output", "-", "--log-format", "json")
		if r.code != 0 {
			t.Fatalf("exit %d: %s", r.code, r.stderr)
		}
		v := vmOneJSON(t, r.stdout)
		if v["report_version"] != json.Number("2") {
			t.Fatal("not version 2")
		}
		m := vmTerminal(t, r.stderr, 0, true, true)
		if m["run_id"] != v["run_id"] || vmLogInt(t, m, "value_rows") != 2 {
			t.Fatalf("result/log disagreement %v %v", v, m)
		}
		vmAssertArtifacts(t, r)
	})
	t.Run("VM-LOG-02", func(t *testing.T) {
		for _, workers := range []int{4, 8} {
			t.Run(fmt.Sprint(workers), func(t *testing.T) {
				root := t.TempDir()
				p := vmLogPipeline(t, root)
				p.Execution.Workers = workers
				var paths []string
				for i := range 8 {
					paths = append(paths, put(t, filepath.Join(root, fmt.Sprintf("input-%d.csv", i)), "amount\n1\n2\n"))
				}
				p.Source.URIs = paths
				p.Execution.BatchTarget = "64B"
				r := runPipeline(t, root, p, "--json-output", "-", "--log-format", "json", "--log-level", "debug")
				if r.code != 0 {
					t.Fatalf("workers exit %d: %s", r.code, r.stderr)
				}
				m := vmTerminal(t, r.stderr, 0, true, true)
				if vmLogInt(t, m, "value_rows") != 16 {
					t.Fatal("worker multiplied row counts")
				}
				vmAssertArtifacts(t, r)
			})
		}
	})
	t.Run("VM-LOG-04", func(t *testing.T) {
		for _, failure := range []string{"validation", "operational"} {
			t.Run(failure, func(t *testing.T) {
				root := t.TempDir()
				p := vmLogPipeline(t, root)
				want := 1
				if failure == "validation" {
					put(t, p.Source.URIs[0], "amount\n-1\n")
				} else {
					p.Source.URIs = []string{filepath.Join(root, "missing.csv")}
					want = 2
				}
				r := runPipeline(t, root, p, "--json-output", "-", "--log-format", "json")
				if r.code != want {
					t.Fatalf("exit %d: %s", r.code, r.stderr)
				}
				vmTerminal(t, r.stderr, want, want == 1, want == 1)
				if want == 1 {
					vmAssertArtifacts(t, r)
				} else if matches, _ := filepath.Glob(filepath.Join(p.Output.URI, "*", "manifest.json")); len(matches) != 0 {
					t.Fatal("operational failure committed")
				}
			})
		}
	})
	t.Run("VM-LOG-05", func(t *testing.T) {
		for _, cmd := range []string{"schema", "plan"} {
			for _, format := range []string{"json", "text"} {
				if cmd == "plan" && format == "text" {
					continue
				}
				t.Run(cmd+format, func(t *testing.T) {
					root := t.TempDir()
					src := vmFixtureFAST(t, root)
					p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv")
					path := p.write(t, filepath.Join(root, "p.yml"))
					args := []string{cmd, "-p", path, "--log-format", "json"}
					if cmd == "schema" {
						args = append(args, "--format", format)
					}
					out, stderr, code := invoke(t, args...)
					if code != 0 {
						t.Fatalf("%s exit=%d %s", cmd, code, stderr)
					}
					m := vmTerminal(t, stderr, 0, false, true)
					if m["scope"] != cmd || m["command"] != cmd {
						t.Fatalf("wrong command scope %v", m)
					}
					for _, event := range vmLogRecords(t, stderr) {
						if event["event"] == "run.phase" && (event["phase"] == "encode" || event["phase"] == "publish") {
							t.Fatalf("invented data work %v", event)
						}
					}
					if format == "json" {
						v := vmOneJSON(t, out)
						if v["run_id"] != m["run_id"] {
							t.Fatal("uncorrelated result")
						}
					}
					if _, err := os.Stat(p.Output.URI); !os.IsNotExist(err) {
						t.Fatal("non-data command published")
					}
				})
			}
		}
	})
	t.Run("VM-LOG-06", func(t *testing.T) {
		for _, level := range []string{"debug", "info", "warn", "error"} {
			t.Run(level, func(t *testing.T) {
				root := t.TempDir()
				p := defaultPipeline(vmFixtureFAST(t, root), "parquet", filepath.Join(root, "out"), "csv").with(t, vmStatConfig)
				r := runPipeline(t, root, p, "--json-output", "-", "--output-mode", "report", "--parquet-stats", "trusted", "--log-format", "json", "--log-level", level)
				if r.code != 0 {
					t.Fatalf("exit %d %s", r.code, r.stderr)
				}
				decision, trust := false, false
				for _, m := range vmLogRecords(t, r.stderr) {
					decision = decision || m["event"] == "parquet.stats_decision"
					trust = trust || m["event"] == "validation.trust"
					l := strings.ToLower(m["level"].(string))
					if level == "error" && l != "error" {
						t.Fatalf("filter leaked %v", m)
					}
					if level == "warn" && l != "warn" && l != "error" {
						t.Fatalf("filter leaked %v", m)
					}
				}
				if decision != (level == "debug") || trust != (level != "error") {
					t.Fatalf("filter debug=%v trust=%v", decision, trust)
				}
				if len(vmEvidence(t, r)) != 2 {
					t.Fatal("logs changed persisted evidence")
				}
				vmAssertArtifacts(t, r)
			})
		}
	})
	t.Run("VM-LOG-07", func(t *testing.T) {
		for _, level := range []string{"info", "debug"} {
			root := t.TempDir()
			p := vmLogPipeline(t, root)
			put(t, p.Source.URIs[0], "amount\nRAW_CELL_SECRET_123\n")
			p.with(t, `input: {schema: {columns: {amount: {type: string}}}}
rules: [{code: PRIVATE, column: amount, level: error, message: CUSTOM_SECRET_123, checks: [{type: equals, value: RULE_LITERAL_SECRET_123}]}]`)
			r := runPipeline(t, root, p, "--json-output", "-", "--log-format", "json", "--log-level", level)
			if r.code != 1 {
				t.Fatalf("secret fixture exit %d %s", r.code, r.stderr)
			}
			vmLogRecords(t, r.stderr)
			for _, secret := range []string{"RAW_CELL_SECRET_123", "CUSTOM_SECRET_123", "RULE_LITERAL_SECRET_123"} {
				if bytes.Contains(r.stderr, []byte(secret)) {
					t.Fatalf("log leaked %s", secret)
				}
			}
		}
	})
	t.Run("VM-LOG-09", func(t *testing.T) {
		e := minioEnvironment(t)
		root := t.TempDir()
		e.put(t, "source.csv", "amount\n1\n2\n")
		proxy := vmNewProxy(t, e)
		proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "PUT" && strings.Contains(r.URL.Path, "summary") {
				http.Error(w, "PUBLICATION_BODY_SECRET", 403)
				return true
			}
			return false
		}
		p := vmLogPipeline(t, root)
		p.Source.URIs = []string{e.uri("source.csv")}
		p.Source.S3 = e.s3(proxy.URL)
		p.Output.URI = e.uri("out/")
		p.Output.S3 = e.s3(proxy.URL)
		path := p.write(t, filepath.Join(root, "p.yml"))
		_, stderr, code := invoke(t, "validate", "-p", path, "--json-output", "-", "--log-format", "json")
		if code != 2 {
			t.Fatalf("publication failure exit %d %s", code, stderr)
		}
		vmTerminal(t, stderr, 2, false, false)
		for _, k := range e.keys(t, "out/") {
			if strings.HasSuffix(k, "manifest.json") {
				t.Fatal("failed publication committed")
			}
		}
	})
	t.Run("VM-LOG-11", func(t *testing.T) {
		for _, failure := range []bool{false, true} {
			root := t.TempDir()
			e := minioEnvironment(t)
			e.put(t, "source.csv", "amount\n1\n2\n")
			proxy := vmNewProxy(t, e)
			if failure {
				proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method == "GET" {
						http.Error(w, "SDK_BODY_SECRET_123 credential=SDK_CREDENTIAL_SECRET", 403)
						return true
					}
					return false
				}
			}
			p := vmLogPipeline(t, root)
			p.Source.URIs = []string{e.uri("source.csv")}
			p.Source.S3 = e.s3(proxy.URL)
			path := p.write(t, filepath.Join(root, "p.yml"))
			_, stderr, code := invoke(t, "validate", "-p", path, "--json-output", "-", "--log-format", "json", "--log-level", "debug")
			want := 0
			if failure {
				want = 2
			}
			if code != want {
				t.Fatalf("SDK exit %d %s", code, stderr)
			}
			vmTerminal(t, stderr, want, !failure, !failure)
			for _, secret := range []string{"SDK_BODY_SECRET_123", "SDK_CREDENTIAL_SECRET", "Response has no supported checksum"} {
				if bytes.Contains(stderr, []byte(secret)) {
					t.Fatalf("SDK diagnostics leaked %q: %s", secret, stderr)
				}
			}
		}
	})
	t.Run("VM-LOG-12", func(t *testing.T) {
		e := minioEnvironment(t)
		root := t.TempDir()
		groups := make([][]*int64, 256)
		for i := range groups {
			groups[i] = []*int64{new(int64(i + 1)), new(int64(i + 2))}
		}
		src := vmIntFixture(t, root, "many-groups", groups)
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		var first map[string]any
		var firstRequests []string
		for _, level := range []string{"debug", "error"} {
			e.put(t, "source.parquet", string(raw))
			proxy := vmNewProxy(t, e)
			proxy.DenyPayload("/"+e.bucket+"/"+e.prefix+"source.parquet", vmInspectParquet(t, src))
			p := defaultPipeline(e.uri("source.parquet"), "parquet", filepath.Join(root, level), "csv").with(t, vmStatConfig)
			p.Source.S3 = e.s3(proxy.URL)
			p.Execution.MemoryBudget = "32MiB"
			r := runPipeline(t, filepath.Join(root, level+"-config"), p, "--json-output", "-", "--output-mode", "report", "--parquet-stats", "trusted", "--log-format", "json", "--log-level", level)
			if r.code != 0 {
				t.Fatalf("diagnostic budget exit %d %s", r.code, r.stderr)
			}
			v := vmOneJSON(t, r.stdout)
			if r.summary.ManagedPeak > 32<<20 {
				t.Fatal("managed budget exceeded")
			}
			if r.summary.Total != 0 || r.summary.Coverage.MetadataClaimedRows != 512 || r.summary.Coverage.AccountedRows != 512 {
				t.Fatalf("wrong literal coverage %+v", r.summary.Coverage)
			}
			if level == "debug" {
				m := vmTerminal(t, r.stderr, 0, true, true)
				if vmLogInt(t, m, "metadata_claimed_rows") != 512 || vmLogInt(t, m, "source_payload_bytes_read") != 0 {
					t.Fatalf("wrong metadata log %v", m)
				}
			}
			var requests []string
			for _, q := range proxy.Requests() {
				requests = append(requests, q.Method+" "+q.Path+" "+q.Range)
			}
			if first != nil {
				for _, k := range []string{"total", "pass", "warn", "error", "coverage", "verification", "trust_policy"} {
					if !reflect.DeepEqual(first[k], v[k]) {
						t.Fatalf("logging changed %s", k)
					}
				}
				if !reflect.DeepEqual(firstRequests, requests) {
					t.Fatalf("logging changed source access %v != %v", firstRequests, requests)
				}
			}
			first = v
			firstRequests = requests
			vmAssertArtifacts(t, r)
		}
	})
}

func TestVMLogActiveFailure(t *testing.T) {
	for _, kind := range []string{"input", "resource"} {
		t.Run("VM-LOG-09/"+kind, func(t *testing.T) {
			root := t.TempDir()
			p := vmLogPipeline(t, root)
			p.Execution.Workers = 4
			good := put(t, filepath.Join(root, "good.csv"), "amount\n1\n2\n")
			bad := "amount\n\"unterminated\n"
			if kind == "resource" {
				bad = "amount\n" + strings.Repeat("PRIVATE_OVERSIZED_ROW", 8192) + "\n"
			}
			p.Source.URIs = []string{good, put(t, filepath.Join(root, "bad.csv"), bad)}
			r := runPipeline(t, root, p, "--json-output", "-", "--log-format", "json", "--log-level", "debug")
			if r.code != 2 {
				t.Fatalf("active %s failure exit %d: %s", kind, r.code, r.stderr)
			}
			terminal := vmTerminal(t, r.stderr, 2, false, false)
			if terminal["outcome"] == "pass" || terminal["outcome"] == "success" {
				t.Fatalf("false completion %v", terminal)
			}
			if bytes.Contains(r.stderr, []byte("PRIVATE_OVERSIZED_ROW")) {
				t.Fatal("resource failure leaked row")
			}
			if matches, _ := filepath.Glob(filepath.Join(p.Output.URI, "*", "manifest.json")); len(matches) != 0 {
				t.Fatal("active failure committed")
			}
		})
	}
}

func TestVMLogCommittedBytes(t *testing.T) {
	root := t.TempDir()
	r := runPipeline(t, root, vmLogPipeline(t, root), "--json-output", "-", "--log-format", "json")
	if r.code != 0 {
		t.Fatalf("run exit %d: %s", r.code, r.stderr)
	}
	vmAssertArtifacts(t, r)
	terminal := vmTerminal(t, r.stderr, 0, true, true)
	var size int64
	if err := filepath.Walk(r.dir(t), func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := vmLogInt(t, terminal, "result_bytes_committed"); got != size {
		t.Fatalf("VM-LOG-01 committed bytes=%d actual=%d", got, size)
	}
	if vmLogInt(t, terminal, "output_bytes_written") < size {
		t.Fatal("logical accepted bytes below committed bytes")
	}
}
