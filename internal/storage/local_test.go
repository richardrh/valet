package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
)

func testStore(t *testing.T, limit int64, endpoint string) (*Store, *resource.Budget) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent"))
	budget := resource.New(limit)
	input := config.S3{Region: "us-east-1", Endpoint: endpoint, PathStyle: true}
	store, err := New(context.Background(), input, config.S3{}, budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("store.Close() at cleanup = %v, want nil", err)
		}
		if n := budget.Used(); n != 0 {
			t.Errorf("budget used at cleanup = %d, want 0", n)
		}
	})
	return store, budget
}

func fixture(t *testing.T, name, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

// assertAbsent checks name does not exist; what describes why it must not.
func assertAbsent(t *testing.T, name, what string) {
	t.Helper()
	if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s: Stat(%s) err = %v, want os.ErrNotExist", what, name, err)
	}
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}

func uris(objects []Object) []string {
	out := make([]string, len(objects))
	for i, object := range objects {
		out[i] = object.URI
	}
	return out
}

func TestLocalDiscoveryOrderingAndBudget(t *testing.T) {
	root := t.TempDir()
	fixture(t, filepath.Join(root, "z.csv"), "z\n")
	fixture(t, filepath.Join(root, "a.csv.gz"), "gzip")
	fixture(t, filepath.Join(root, "sub", "b.csv"), "b\n")
	fixture(t, filepath.Join(root, "ignored.json"), "[]")
	store, budget := testStore(t, 1<<20, "")
	objects, err := store.Discover(context.Background(), config.Source{Prefix: root, Recursive: true, Filter: "csv"})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		filepath.Join(root, "a.csv.gz"),
		filepath.Join(root, "sub", "b.csv"),
		filepath.Join(root, "z.csv"),
	}
	if got := uris(objects); !slices.Equal(got, want) {
		t.Fatalf("discovered URIs = %q, want %q (filtered, recursive, sorted)", got, want)
	}

	t.Run("discovery metadata is charged", func(t *testing.T) {
		if budget.Used() == 0 {
			t.Error("budget used after Discover = 0, want metadata charged")
		}
	})

	t.Run("explicit uris keep listed order", func(t *testing.T) {
		listed := []string{objects[2].URI, objects[0].URI}
		explicit, err := store.Discover(context.Background(), config.Source{URIs: listed})
		if err != nil {
			t.Fatal(err)
		}
		if got := uris(explicit); !slices.Equal(got, listed) {
			t.Errorf("explicit URIs = %q, want %q", got, listed)
		}
	})

	t.Run("duplicate explicit uri is rejected", func(t *testing.T) {
		duplicated := []string{objects[0].URI, objects[0].URI}
		if _, err := store.Discover(context.Background(), config.Source{URIs: duplicated}); err == nil {
			t.Errorf("Discover(%q) err = nil, want duplicate rejection", duplicated)
		}
	})
}

func TestLocalDiscoveryResourceFailure(t *testing.T) {
	// A 20KiB budget cannot hold discovery state for this many files.
	const files = 20
	root := t.TempDir()
	for i := range files {
		fixture(t, filepath.Join(root, strings.Repeat("x", i+1)+".csv"), "a\n")
	}
	store, _ := testStore(t, 20*1024, "")
	_, err := store.Discover(context.Background(), config.Source{Prefix: root, Filter: "csv"})
	assertResourceLimit(t, "Discover", err)
}

func TestLocalReplayDetectsSameMetadataMutation(t *testing.T) {
	name := filepath.Join(t.TempDir(), "source.csv")
	fixture(t, name, "first\n")
	store, _ := testStore(t, 1<<20, "")
	obj, err := store.Stat(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.Open(context.Background(), obj)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}

	// Same size and mtime, different bytes.
	fixture(t, name, "other\n")
	if err = os.Chtimes(name, obj.ModTime, obj.ModTime); err != nil {
		t.Fatal(err)
	}
	r, err = store.Open(context.Background(), obj)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, r)
	closeErr := r.Close()
	if readErr == nil && closeErr == nil {
		t.Fatal("replay read and Close errors = nil, want same-metadata mutation detected")
	}
}

func TestLocalRangeReplayDetectsMutation(t *testing.T) {
	name := filepath.Join(t.TempDir(), "source.parquet")
	fixture(t, name, "abcdef")
	store, _ := testStore(t, 1<<20, "")
	obj, err := store.Stat(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	r, err := store.OpenAt(context.Background(), obj)
	if err != nil {
		t.Fatal(err)
	}
	var b [2]byte
	if _, err = r.ReadAt(b[:], 2); err != nil {
		t.Fatalf("ReadAt(off 2): %v", err)
	}
	if got := string(b[:]); got != "cd" {
		t.Errorf("ReadAt(off 2) = %q, want %q", got, "cd")
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}

	// Same size and mtime, one byte changed.
	fixture(t, name, "abcxef")
	if err = os.Chtimes(name, obj.ModTime, obj.ModTime); err != nil {
		t.Fatal(err)
	}
	r, err = store.OpenAt(context.Background(), obj)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err == nil {
		t.Fatal("range replay Close err = nil, want mutation detected")
	}
}

func TestMetadataRangeCloseDoesNotDigestWholeFile(t *testing.T) {
	name := filepath.Join(t.TempDir(), "source.parquet")
	fixture(t, name, strings.Repeat("x", 1<<20))
	store, _ := testStore(t, 2<<20, "")
	obj, err := store.Stat(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.OpenAtMetadata(context.Background(), obj)
	if err != nil {
		t.Fatal(err)
	}
	var footer [8]byte
	if _, err = reader.ReadAt(footer[:], obj.Size-int64(len(footer))); err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	digested := store.snapshots[obj.URI].digested
	store.mu.Unlock()
	if digested {
		t.Error("snapshot digested after metadata-only range Close = true, want false")
	}
}

func TestLocalPublicationAbortAndExistingOutput(t *testing.T) {
	t.Run("output appears on Close and is never overwritten", func(t *testing.T) {
		store, _ := testStore(t, 1<<20, "")
		name := filepath.Join(t.TempDir(), "run", "manifest.json")
		w, err := store.Create(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write([]byte("complete")); err != nil {
			t.Fatal(err)
		}
		assertAbsent(t, name, "before Close")
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "complete" {
			t.Errorf("published output = %q, want %q", got, "complete")
		}
		if err = store.Put(context.Background(), name, []byte("overwrite")); err == nil {
			t.Error("Put over existing output err = nil, want refusal")
		}
	})

	t.Run("aborted output leaves nothing behind", func(t *testing.T) {
		store, _ := testStore(t, 1<<20, "")
		root := t.TempDir()
		name := filepath.Join(root, "aborted.json")
		w, err := store.Create(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write([]byte("partial")); err != nil {
			t.Fatal(err)
		}
		if err = w.Abort(); err != nil {
			t.Fatal(err)
		}
		assertAbsent(t, name, "after Abort")
		if got := dirNames(t, root); len(got) != 0 {
			t.Errorf("entries after Abort = %q, want none", got)
		}
	})

	t.Run("store close discards pending output", func(t *testing.T) {
		store, _ := testStore(t, 1<<20, "")
		root := t.TempDir()
		w, err := store.Create(context.Background(), filepath.Join(root, "pending.json"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = w.Write([]byte("partial")); err != nil {
			t.Fatal(err)
		}
		if err = store.Close(); err != nil {
			t.Fatal(err)
		}
		if got := dirNames(t, root); len(got) != 0 {
			t.Errorf("entries after store.Close = %q, want none", got)
		}
	})
}

func TestLocalPublicationSyncFailureRemovesPublishedTarget(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "manifest.json")
	store, _ := testStore(t, 1<<20, "")
	original := syncDirectoryFn
	syncDirectoryFn = func(string) error { return errors.New("injected directory sync failure") }
	defer func() { syncDirectoryFn = original }()

	writer, err := store.Create(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write([]byte("terminal")); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err == nil || !strings.Contains(err.Error(), "directory sync") {
		t.Fatalf("writer.Close() = %v, want directory sync failure", err)
	}
	assertAbsent(t, target, "after failed directory sync")
}

func TestLocalCancelledWriteDoesNotPublish(t *testing.T) {
	store, _ := testStore(t, 1<<20, "")
	ctx, cancel := context.WithCancel(context.Background())
	name := filepath.Join(t.TempDir(), "manifest.json")
	w, err := store.Create(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err = w.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("writer.Close() = %v, want context.Canceled", err)
	}
	assertAbsent(t, name, "after cancelled Close")
}

func TestJoinPreservesSchemeAndBase(t *testing.T) {
	base := filepath.Join(t.TempDir(), "output")
	for _, tc := range []struct {
		name  string
		base  string
		parts []string
		want  string
	}{
		{
			name:  "s3 escapes key and drops dot segments from parts",
			base:  "s3://bucket/base path/",
			parts: []string{"run", "../manifest.json"},
			want:  "s3://bucket/base%20path/run/manifest.json",
		},
		{
			name:  "local parts cannot climb above base",
			base:  base,
			parts: []string{"../manifest.json"},
			want:  filepath.Join(base, "manifest.json"),
		},
	} {
		if got := Join(tc.base, tc.parts...); got != tc.want {
			t.Errorf("%s: Join(%q, %q) = %q, want %q", tc.name, tc.base, tc.parts, got, tc.want)
		}
	}
	// S3 discovery compares decoded object keys, not their escaped spelling.
	if !uriLess("s3://bucket/a%20b.csv", "s3://bucket/a!b.csv") {
		t.Error(`uriLess("a%20b.csv", "a!b.csv") = false, want true`)
	}
}

func TestURILessTreatsMalformedEscapeAsLiteral(t *testing.T) {
	// "%gg" is not an escape, so it compares as '%' (0x25) after '!' (0x21);
	// treating 'g' as a hex digit would decode it to 0x10 and sort it first.
	const bang, malformed = "s3://bucket/!", "s3://bucket/%gg"
	if !uriLess(bang, malformed) {
		t.Errorf("uriLess(%q, %q) = false, want true", bang, malformed)
	}
	if uriLess(malformed, bang) {
		t.Errorf("uriLess(%q, %q) = true, want false", malformed, bang)
	}
}

func TestLogURIRedactsCredentialsAndQuery(t *testing.T) {
	for in, want := range map[string]string{
		"https://user:secret@host/key?X-Amz-Signature=abc#frag": "https://host/key",
		"s3://bucket/a%20b.csv?versionId=1":                     "s3://bucket/a%20b.csv",
		"/tmp/out/run":                                          "/tmp/out/run",
	} {
		if got := LogURI(in).LogValue().String(); got != want {
			t.Errorf("LogURI(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCreateRunRootIsExclusiveLocally(t *testing.T) {
	store, _ := testStore(t, 1<<20, "")
	root := filepath.Join(t.TempDir(), "out", "run")
	if err := store.CreateRunRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRunRoot(context.Background(), root); err == nil {
		t.Error("second CreateRunRoot on local root err = nil, want existing-directory error")
	}
	// S3 has no directories; run uniqueness comes from conditional object creation.
	if err := store.CreateRunRoot(context.Background(), "s3://bucket/out/run"); err != nil {
		t.Errorf("CreateRunRoot(s3) = %v, want nil", err)
	}
}

func TestConcurrentLocalPublicationNeverReplacesWinner(t *testing.T) {
	store, _ := testStore(t, 1<<20, "")
	target := filepath.Join(t.TempDir(), "manifest.json")
	const count = 32
	writers := make([]Writer, count)
	for i := range count {
		writer, err := store.Create(context.Background(), target)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = fmt.Fprintf(writer, "writer-%d", i); err != nil {
			t.Fatal(err)
		}
		writers[i] = writer
	}
	type result struct {
		index int
		err   error
	}
	results := make(chan result, count)
	start := make(chan struct{})
	for i, writer := range writers {
		go func() { <-start; results <- result{i, writer.Close()} }()
	}
	close(start)

	winner := -1
	for range count {
		result := <-results
		switch {
		case result.err == nil && winner != -1:
			t.Fatalf("writers %d and %d both published, want exactly one", winner, result.index)
		case result.err == nil:
			winner = result.index
		case !errors.Is(result.err, os.ErrExist):
			t.Errorf("writer %d Close = %v, want nil or os.ErrExist", result.index, result.err)
		}
	}
	if winner < 0 {
		t.Fatal("no writer published, want exactly one")
	}
	body, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("writer-%d", winner); string(body) != want {
		t.Errorf("published body = %q, want winner's %q", body, want)
	}
	if got := dirNames(t, filepath.Dir(target)); !slices.Equal(got, []string{"manifest.json"}) {
		t.Errorf("entries after publication = %q, want only manifest.json", got)
	}
}

// Cancellation becomes visible only on the post-sync check, not the first check.
type cancelAtSecondCheck struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelAtSecondCheck) Err() error {
	c.checks++
	if c.checks == 2 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestLocalCancellationAfterSyncPreventsPublication(t *testing.T) {
	store, _ := testStore(t, 1<<20, "")
	target := filepath.Join(t.TempDir(), "manifest.json")
	writer, err := store.Create(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer.(*localWriter).ctx = &cancelAtSecondCheck{Context: ctx, cancel: cancel}
	if err = writer.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("writer.Close() = %v, want post-sync context.Canceled", err)
	}
	assertAbsent(t, target, "after post-sync cancellation")
	if got := dirNames(t, filepath.Dir(target)); len(got) != 0 {
		t.Errorf("entries after cancelled Close = %q, want none", got)
	}
}

func TestJoinPreservesOpaqueS3Prefix(t *testing.T) {
	for _, prefix := range []string{"team//results/", "team/./results/", "team/../results/", "team///"} {
		base := "s3://bucket/" + prefix
		if got, want := Join(base, "run", "../manifest.json"), base+"run/manifest.json"; got != want {
			t.Errorf("Join(%q, run, ../manifest.json) = %q, want %q", base, got, want)
		}
	}
}

type observedLocalOutput struct {
	file     *os.File
	maxWrite int
	writeErr error
	syncErr  error
	closeErr error
	calls    []string
}

func (f *observedLocalOutput) Write(p []byte) (int, error) {
	f.calls = append(f.calls, "write")
	if f.maxWrite >= 0 {
		p = p[:min(len(p), f.maxWrite)]
	}
	n, err := f.file.Write(p)
	return n, errors.Join(err, f.writeErr)
}
func (f *observedLocalOutput) Sync() error {
	f.calls = append(f.calls, "sync")
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.file.Sync()
}
func (f *observedLocalOutput) Close() error {
	f.calls = append(f.calls, "close")
	return errors.Join(f.file.Close(), f.closeErr)
}

func TestLocalSmallWritesFlushBeforeSync(t *testing.T) {
	store, budget := testStore(t, 1<<20, "")
	target := filepath.Join(t.TempDir(), "rows.ndjson")
	raw, err := store.Create(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	writer := raw.(*localWriter)
	file := &observedLocalOutput{file: writer.file.(*os.File), maxWrite: -1}
	writer.file = file
	const line = "{\"id\":1}\n"
	for range 1000 {
		if n, err := writer.Write([]byte(line)); n != len(line) || err != nil {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
	if len(file.calls) != 0 {
		t.Fatalf("small writes reached file before flush: %v", file.calls)
	}
	assertAbsent(t, target, "before buffered publication")
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"write", "sync", "close"}; !slices.Equal(file.calls, want) {
		t.Errorf("file operations = %v, want %v", file.calls, want)
	}
	content, err := os.ReadFile(target)
	if err != nil || string(content) != strings.Repeat(line, 1000) {
		t.Errorf("published bytes incorrect: size=%d err=%v", len(content), err)
	}
	if budget.Used() != 0 {
		t.Errorf("budget after Close = %d", budget.Used())
	}
}

func TestLocalBufferedFinalizationFailuresNeverPublish(t *testing.T) {
	injected := errors.New("injected local failure")
	for _, tc := range []struct {
		name                              string
		maxWrite                          int
		writeErr, syncErr, closeErr, want error
		calls                             []string
	}{
		{name: "partial-error", maxWrite: 3, writeErr: injected, want: injected, calls: []string{"write", "close"}},
		{name: "short-write", maxWrite: 3, want: io.ErrShortWrite, calls: []string{"write", "close"}},
		{name: "zero-write", maxWrite: 0, want: io.ErrShortWrite, calls: []string{"write", "close"}},
		{name: "sync", maxWrite: -1, syncErr: injected, want: injected, calls: []string{"write", "sync", "close"}},
		{name: "close", maxWrite: -1, closeErr: injected, want: injected, calls: []string{"write", "sync", "close"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, budget := testStore(t, 1<<20, "")
			target := filepath.Join(t.TempDir(), "rows.ndjson")
			raw, err := store.Create(context.Background(), target)
			if err != nil {
				t.Fatal(err)
			}
			writer := raw.(*localWriter)
			file := &observedLocalOutput{file: writer.file.(*os.File), maxWrite: tc.maxWrite, writeErr: tc.writeErr, syncErr: tc.syncErr, closeErr: tc.closeErr}
			writer.file = file
			if n, err := writer.Write([]byte("buffered")); n != 8 || err != nil {
				t.Fatalf("buffered Write = %d, %v", n, err)
			}
			if err := writer.Close(); !errors.Is(err, tc.want) {
				t.Fatalf("Close = %v, want %v", err, tc.want)
			}
			if err := writer.Close(); !errors.Is(err, tc.want) {
				t.Errorf("repeated Close = %v, want sticky failure", err)
			}
			if !slices.Equal(file.calls, tc.calls) {
				t.Errorf("file operations = %v, want %v", file.calls, tc.calls)
			}
			assertAbsent(t, target, "after failed buffered publication")
			if names := dirNames(t, filepath.Dir(target)); len(names) != 0 {
				t.Errorf("failed output retained files: %v", names)
			}
			if budget.Used() != 0 {
				t.Errorf("budget after failed Close = %d", budget.Used())
			}
		})
	}
}

func TestLocalWriteReportsOnlyAcceptedPrefixOnFlushFailure(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint("buffered=", buffered), func(t *testing.T) {
			store, _ := testStore(t, 1<<20, "")
			target := filepath.Join(t.TempDir(), "rows.ndjson")
			raw, err := store.Create(context.Background(), target)
			if err != nil {
				t.Fatal(err)
			}
			writer := raw.(*localWriter)
			file := &observedLocalOutput{file: writer.file.(*os.File), maxWrite: 3, writeErr: io.ErrClosedPipe}
			writer.file = file
			input, want := strings.Repeat("x", localWriteBuffer), 3
			if buffered {
				if n, err := writer.Write([]byte(strings.Repeat("x", localWriteBuffer-1))); n != localWriteBuffer-1 || err != nil {
					t.Fatalf("initial buffered Write = %d, %v", n, err)
				}
				input, want = "xyz", 1
			}
			if n, err := writer.Write([]byte(input)); n != want || !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("Write = %d, %v; want accepted prefix %d", n, err, want)
			}
			if n, err := writer.Write([]byte("later")); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
				t.Errorf("Write after failure = %d, %v", n, err)
			}
			if err := writer.Close(); !errors.Is(err, io.ErrClosedPipe) {
				t.Errorf("Close = %v", err)
			}
			assertAbsent(t, target, "after partial write")
		})
	}
}

func TestLocalBufferAdaptsToLowBudget(t *testing.T) {
	store, budget := testStore(t, writerRegistryCharge, "")
	target := filepath.Join(t.TempDir(), "output")
	writer, err := store.Create(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := writer.Write([]byte("direct")); n != 6 || err != nil {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Peak() != writerRegistryCharge {
		t.Errorf("budget peak = %d, want unbuffered registry only", budget.Peak())
	}
}

func TestLocalReaderAtConcurrentRanges(t *testing.T) {
	content := strings.Repeat("0123456789abcdef", 8192)
	target := filepath.Join(t.TempDir(), "source.parquet")
	fixture(t, target, content)
	store, _ := testStore(t, 1<<20, "")
	obj, err := store.Stat(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.OpenAt(context.Background(), obj)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 32 {
				offset := int64(worker*101 + i*31)
				var buffer [257]byte
				n, err := reader.ReadAt(buffer[:], offset)
				if n != len(buffer) || err != nil || string(buffer[:]) != content[offset:offset+int64(len(buffer))] {
					t.Errorf("ReadAt(%d) = %d, %v, wrong=%t", offset, n, err, string(buffer[:]) != content[offset:offset+int64(len(buffer))])
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}
