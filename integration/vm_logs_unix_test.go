//go:build integration && !windows

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func vmWaitProcess(t testing.TB, cmd *exec.Cmd, ctx context.Context) int {
	t.Helper()
	err := cmd.Wait()
	if ctx.Err() != nil {
		t.Fatalf("child timed out: %v", ctx.Err())
	}
	if err == nil {
		return 0
	}
	var e *exec.ExitError
	if !errors.As(err, &e) {
		t.Fatal(err)
	}
	return e.ExitCode()
}

type vmObservedChild struct {
	done chan struct{}
	err  error
}

// vmObserveBarrier joins the child exactly once, including failure before the
// request barrier is reached. The closed channel synchronizes stderr reads.
func vmObserveBarrier(t testing.TB, cmd *exec.Cmd, ctx context.Context, barrier *vmBarrier, logs *bytes.Buffer) *vmObservedChild {
	t.Helper()
	child := &vmObservedChild{done: make(chan struct{})}
	go func() { child.err = cmd.Wait(); close(child.done) }()
	t.Cleanup(func() {
		select {
		case <-child.done:
		default:
			_ = cmd.Process.Kill()
			<-child.done
		}
	})
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-barrier.Entered:
		return child
	case <-child.done:
		t.Fatalf("child exited before I/O barrier: %v; stderr=%s", child.err, logs.Bytes())
	case <-timer.C:
		_ = cmd.Process.Kill()
		<-child.done
		t.Fatalf("I/O barrier not reached within 30s; stderr=%s", logs.Bytes())
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-child.done
		t.Fatalf("I/O barrier deadline: %v; stderr=%s", ctx.Err(), logs.Bytes())
	}
	return nil
}

func vmObservedExit(t testing.TB, child *vmObservedChild, ctx context.Context) int {
	t.Helper()
	<-child.done
	if ctx.Err() != nil {
		t.Fatalf("child timed out: %v", ctx.Err())
	}
	if child.err == nil {
		return 0
	}
	var exit *exec.ExitError
	if !errors.As(child.err, &exit) {
		t.Fatal(child.err)
	}
	return exit.ExitCode()
}
func vmClosedPipe(t testing.TB) *os.File {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}
func TestVMLogPipes(t *testing.T) {
	for _, tc := range []struct {
		id, cmd, stream string
		want            int
	}{{"VM-LOG-08", "validate", "stderr", 0}, {"VM-LOG-04/post-commit", "validate", "stdout", 3}, {"VM-DIFF-10/broken-stdout", "schema", "stdout", 2}} {
		t.Run(tc.id, func(t *testing.T) {
			root := t.TempDir()
			p := vmLogPipeline(t, root)
			if tc.cmd == "schema" {
				p = defaultPipeline(vmFixtureFAST(t, root), "parquet", filepath.Join(root, "out"), "csv")
			}
			path := p.write(t, filepath.Join(root, "p.yml"))
			args := []string{tc.cmd, "-p", path, "--log-format", "json", "--log-level", "debug"}
			if tc.cmd == "validate" {
				args = append(args, "--json-output", "-")
			}
			cmd, ctx, cancel := command(t, args...)
			defer cancel()
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			if tc.stream == "stdout" {
				cmd.Stdout = vmClosedPipe(t)
			} else {
				cmd.Stderr = vmClosedPipe(t)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			code := vmWaitProcess(t, cmd, ctx)
			if code != tc.want {
				t.Fatalf("closed %s exit %d want %d: %s", tc.stream, code, tc.want, stderr.Bytes())
			}
			if tc.cmd == "validate" {
				r := &runResult{output: p.Output.URI, stdout: stdout.Bytes(), stderr: stderr.Bytes(), code: code}
				collect(t, r)
				vmAssertArtifacts(t, r)
				if tc.stream == "stderr" {
					v := vmOneJSON(t, stdout.Bytes())
					if v["validation"] != "pass" {
						t.Fatal("stderr failure changed validation")
					}
				} else {
					vmTerminal(t, stderr.Bytes(), 3, true, true)
				}
			} else {
				vmTerminal(t, stderr.Bytes(), 2, false, false)
				if _, err := os.Stat(p.Output.URI); !os.IsNotExist(err) {
					t.Fatal("schema presentation failure created data result")
				}
			}
		})
	}
}
func TestVMLogBarrier(t *testing.T) {
	t.Run("VM-LOG-03", func(t *testing.T) {
		e := minioEnvironment(t)
		root := t.TempDir()
		e.put(t, "source.csv", "amount\n1\n2\n")
		proxy := vmNewProxy(t, e)
		barrier := vmNewBarrier()
		t.Cleanup(barrier.Release)
		proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "source.csv") {
				if err := barrier.Block(r.Context()); err != nil {
					return true
				}
			}
			return false
		}
		p := vmLogPipeline(t, root)
		p.Source.URIs = []string{e.uri("source.csv")}
		p.Source.S3 = e.s3(proxy.URL)
		path := p.write(t, filepath.Join(root, "p.yml"))
		cmd, ctx, cancel := command(t, "validate", "-p", path, "--json-output", "-", "--log-format", "json")
		defer cancel()
		var out, logs bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &logs
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		child := vmObserveBarrier(t, cmd, ctx, barrier, &logs)
		held := time.Now() // This observed interval is inside a real backend operation, not a stage-start sleep.
		barrier.Release()
		stall := time.Since(held)
		code := vmObservedExit(t, child, ctx)
		if code != 0 {
			t.Fatalf("barrier exit %d %s", code, logs.Bytes())
		}
		terminal := vmTerminal(t, logs.Bytes(), 0, true, true)
		var backend int64
		for _, q := range proxy.Requests() {
			if q.Method == "GET" && q.Status >= 200 && q.Status < 300 {
				backend += q.Bytes
			}
		}
		if vmLogInt(t, terminal, "source_payload_bytes_read") != backend {
			t.Fatalf("backend byte counter=%v actual=%d", terminal, backend)
		}
		covered := false
		for _, m := range vmLogRecords(t, logs.Bytes()) {
			if m["event"] == "run.phase" && (m["phase"] == "decode" || m["phase"] == "input_hash" || m["phase"] == "metadata") {
				covered = covered || time.Duration(vmLogInt(t, m, "work_ns")) >= stall
			}
		}
		if !covered {
			t.Fatalf("no phase covers IO stall %s", stall)
		}
	})
	t.Run("VM-LOG-10", func(t *testing.T) {
		for _, sig := range []os.Signal{syscall.SIGINT, syscall.SIGTERM} {
			t.Run(sig.String(), func(t *testing.T) {
				e := minioEnvironment(t)
				root := t.TempDir()
				e.put(t, "source.csv", "amount\n1\n2\n")
				proxy := vmNewProxy(t, e)
				barrier := vmNewBarrier()
				t.Cleanup(barrier.Release)
				proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
					if r.Method == "GET" {
						if err := barrier.Block(r.Context()); err != nil {
							return true
						}
					}
					return false
				}
				p := vmLogPipeline(t, root)
				p.Source.URIs = []string{e.uri("source.csv")}
				p.Source.S3 = e.s3(proxy.URL)
				path := p.write(t, filepath.Join(root, "p.yml"))
				cmd, ctx, cancel := command(t, "validate", "-p", path, "--json-output", "-", "--log-format", "json")
				defer cancel()
				var out, logs bytes.Buffer
				cmd.Stdout = &out
				cmd.Stderr = &logs
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				child := vmObserveBarrier(t, cmd, ctx, barrier, &logs)
				if err := cmd.Process.Signal(sig); err != nil {
					t.Fatal(err)
				}
				code := vmObservedExit(t, child, ctx)
				want := 130
				if sig == syscall.SIGTERM {
					want = 143
				}
				if code != want {
					t.Fatalf("signal exit %d want %d: %s", code, want, logs.Bytes())
				}
				m := vmTerminal(t, logs.Bytes(), want, false, false)
				if !strings.Contains(strings.ToLower(m["outcome"].(string)), "cancel") && !strings.Contains(strings.ToLower(m["outcome"].(string)), "interrupt") {
					t.Fatalf("not cancellation: %v", m)
				}
				if files, _ := filepath.Glob(filepath.Join(p.Output.URI, "*", "manifest.json")); len(files) != 0 {
					t.Fatal("cancelled run committed")
				}
			})
		}
	})
}

func TestVMLogMemoryObservation(t *testing.T) {
	root := t.TempDir()
	groups := make([][]*int64, 512)
	for i := range groups {
		groups[i] = []*int64{new(int64(i + 1))}
	}
	p := defaultPipeline(vmIntFixture(t, root, "many", groups), "parquet", filepath.Join(root, "out"), "csv").with(t, vmStatConfig)
	p.Execution.MemoryBudget = "32MiB"
	path := p.write(t, filepath.Join(root, "p.yml"))
	cmd, ctx, cancel := command(t, "validate", "-p", path, "--json-output", "-", "--output-mode", "report", "--parquet-stats", "trusted", "--log-format", "json", "--log-level", "debug")
	defer cancel()
	var out, logs bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if code := vmWaitProcess(t, cmd, ctx); code != 0 {
		t.Fatalf("many-group run exit %d: %s", code, logs.Bytes())
	}
	m := vmTerminal(t, logs.Bytes(), 0, true, true)
	if vmLogInt(t, m, "managed_peak_bytes") > 32<<20 {
		t.Fatal("managed limit exceeded")
	}
	if usage, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		t.Logf("VM-LOG-12 observed child maxrss=%d (OS units; not a managed-memory cap)", usage.Maxrss)
	}
}
