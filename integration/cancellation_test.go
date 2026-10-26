//go:build integration

package integration_test

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// stalledObjectStore serves just enough path-style S3 for the CLI to pin
// s3://valet/input.csv (HEAD) and then holds every GET open until the
// client disconnects, so a run can be stopped mid-read without an object store.
func stalledObjectStore(t *testing.T) (endpoint string, reading <-chan struct{}) {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/valet/input.csv" {
			http.Error(w, "unexpected request "+r.Method+" "+r.URL.Path, http.StatusNotImplemented)
			return
		}
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(fixtureA)))
			w.Header().Set("ETag", `"stalled-fixture"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			once.Do(func() { close(started) })
			select {
			case <-r.Context().Done():
			case <-release:
			}
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})
	return server.URL, started
}

// interruptDuringRead runs validate against a stalled object read, applies stop
// once the read is in flight, asserts no terminal manifest was published and
// returns the CLI exit code.
func interruptDuringRead(t *testing.T, stop func(*os.Process) error) int {
	t.Helper()
	endpoint, reading := stalledObjectStore(t)
	root := t.TempDir()
	out := filepath.Join(root, "out")
	p := defaultPipeline("s3://valet/input.csv", "csv", out, "csv").with(t, rulesA)
	p.Source.S3 = &s3Spec{Region: "us-east-1", Endpoint: endpoint, PathStyle: true}
	path := p.write(t, filepath.Join(root, "pipeline.yml"))

	cmd, ctx, cancel := command(t, "validate", "-p", path)
	defer cancel()
	// Static throwaway credentials keep the SDK chain off the developer's
	// profiles and instance metadata.
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID=valet-test",
		"AWS_SECRET_ACCESS_KEY=valet-test",
		"AWS_CONFIG_FILE="+filepath.Join(root, "absent-aws-config"),
		"AWS_SHARED_CREDENTIALS_FILE="+filepath.Join(root, "absent-aws-credentials"),
		"AWS_EC2_METADATA_DISABLED=true",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-reading:
	case err := <-done:
		t.Fatalf("CLI exited before the stalled read: %v %s", err, output.String())
	case <-ctx.Done():
		<-done
		t.Fatal("CLI did not reach the stalled read before the test deadline")
	}
	if err := stop(cmd.Process); err != nil {
		_ = cmd.Process.Kill()
		<-done
		t.Fatal(err)
	}
	err := <-done
	if ctx.Err() != nil {
		t.Fatalf("stopped CLI did not exit before the test deadline: %s", output.String())
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("stopped CLI result=%v output=%s", err, output.String())
	}
	if manifests := files(t, filepath.Join(out, "*", "manifest.json")); len(manifests) != 0 {
		t.Fatalf("stopped run published terminal manifest %v", manifests)
	}
	t.Logf("exit=%d output=%s", exit.ExitCode(), output.String())
	return exit.ExitCode()
}
