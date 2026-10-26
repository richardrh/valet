package storage

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
)

// Wire-level S3 names the fake servers check and answer with.
const (
	headerIfNoneMatch = "If-None-Match"
	headerSSE         = "X-Amz-Server-Side-Encryption"
	headerSSEKMSKeyID = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
	headerWriteID     = "X-Amz-Meta-Valet-Write-Id"
	headerSHA256      = "X-Amz-Meta-Valet-Sha256"

	testUploadID           = "upload-one"
	initiateUploadResponse = `<InitiateMultipartUploadResult><UploadId>` + testUploadID +
		`</UploadId></InitiateMultipartUploadResult>`
	completeUploadResponse = `<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`

	// tailSize spills a write past one full part, so it uploads as two parts.
	tailSize    = 17
	twoPartSize = uploadPartSize + tailSize
)

func initiatesUpload(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Query().Has("uploads")
}

func targetsUpload(r *http.Request, uploadID string) bool {
	return r.URL.Query().Get("uploadId") == uploadID
}

func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	w.WriteHeader(status)
	_, _ = io.WriteString(w, "<Error><Code>"+code+"</Code><Message>"+message+"</Message></Error>")
}

// multipartOps counts the multipart operations a fake server observed.
type multipartOps struct {
	uploads, parts, completes, aborts int32
}

func writeLarge(t *testing.T, w Writer, n int) error {
	t.Helper()
	var block [8192]byte
	for n > 0 {
		count := min(n, len(block))
		written, err := w.Write(block[:count])
		if err != nil {
			return err
		}
		if written != count {
			t.Fatalf("Write = %d bytes, nil error; want %d", written, count)
		}
		n -= count
	}
	return nil
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func dropHTTPConnection(t *testing.T, writer http.ResponseWriter) {
	t.Helper()
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		t.Fatal("test server cannot hijack connections")
	}
	connection, _, err := hijacker.Hijack()
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.Close()
}

func assertClosedWithoutLeaks(t *testing.T, store *Store, budget *resource.Budget) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Errorf("store.Close() = %v, want nil", err)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after store.Close = %d, want 0", got)
	}
}

// committedObject is a fake S3 object that remembers the identity metadata of
// the PUT the server committed, so HEAD can answer reconciliation probes.
// Callers hold mu while serving a request.
type committedObject struct {
	mu      sync.Mutex
	stored  bool
	size    int
	writeID string
	digest  string
}

func (o *committedObject) commit(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	o.stored, o.size = true, len(body)
	o.writeID = r.Header.Get(headerWriteID)
	o.digest = r.Header.Get(headerSHA256)
}

func (o *committedObject) serveHead(w http.ResponseWriter) {
	w.Header().Set("Content-Length", strconv.Itoa(o.size))
	w.Header().Set(headerWriteID, o.writeID)
	w.Header().Set(headerSHA256, o.digest)
	w.WriteHeader(http.StatusOK)
}

func TestS3SmallPutUsesOutputSettings(t *testing.T) {
	inputServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("input endpoint got %s %s, want no output traffic", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer inputServer.Close()
	var calls atomic.Int32
	outputServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPut || r.URL.Path != "/results/run/manifest.json" {
			t.Errorf("request = %s %s, want PUT /results/run/manifest.json", r.Method, r.URL.Path)
		}
		for header, want := range map[string]string{
			headerIfNoneMatch: "*",
			headerSSE:         "aws:kms",
			headerSSEKMSKeyID: "key-arn",
		} {
			if got := r.Header.Get(header); got != want {
				t.Errorf("%s = %q, want %q", header, got, want)
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read PUT body: %v", err)
		}
		if string(body) != "manifest" {
			t.Errorf("PUT body = %q, want %q", body, "manifest")
		}
		w.Header().Set("ETag", `"written"`)
	}))
	defer outputServer.Close()
	// Establish isolated standard-chain credentials, then use independent output config.
	_, _ = testStore(t, 16<<20, inputServer.URL)
	budget := resource.New(16 << 20)
	input := config.S3{Region: "us-east-1", Endpoint: inputServer.URL, PathStyle: true}
	output := config.S3{
		Region:    "eu-west-1",
		Endpoint:  outputServer.URL,
		PathStyle: true,
		SSE:       "aws:kms",
		KMSKeyID:  "key-arn",
	}
	store, err := New(context.Background(), input, output, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Put(context.Background(), "s3://results/run/manifest.json", []byte("manifest")); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("output PUT calls = %d, want 1", got)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after Put = %d, want 0", got)
	}
}

func TestS3MultipartCompletesInOrder(t *testing.T) {
	var uploads, parts, completes, aborts atomic.Int32
	wantPartSizes := []int64{uploadPartSize, tailSize}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case initiatesUpload(r):
			uploads.Add(1)
			_, _ = io.WriteString(w, initiateUploadResponse)
		case r.Method == http.MethodPut && targetsUpload(r, testUploadID):
			number := parts.Add(1)
			if got, want := q.Get("partNumber"), strconv.Itoa(int(number)); got != want {
				t.Errorf("upload %d partNumber = %s, want %s", number, got, want)
			}
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				t.Error(err)
			}
			if int(number) > len(wantPartSizes) {
				t.Errorf("part %d uploaded, want %d parts", number, len(wantPartSizes))
			} else if want := wantPartSizes[number-1]; n != want {
				t.Errorf("part %d size = %d, want %d", number, n, want)
			}
			w.Header().Set("ETag", `"part-`+q.Get("partNumber")+`"`)
		case r.Method == http.MethodPost && targetsUpload(r, testUploadID):
			completes.Add(1)
			if got := r.Header.Get(headerIfNoneMatch); got != "*" {
				t.Errorf("completion %s = %q, want %q", headerIfNoneMatch, got, "*")
			}
			body, _ := io.ReadAll(r.Body)
			for _, part := range []string{"<PartNumber>1</PartNumber>", "<PartNumber>2</PartNumber>"} {
				if !strings.Contains(string(body), part) {
					t.Errorf("completion body lacks %s:\n%s", part, body)
				}
			}
			_, _ = io.WriteString(w, completeUploadResponse)
		case r.Method == http.MethodDelete:
			aborts.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 16<<20, server.URL)
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	if err = writeLarge(t, writer, twoPartSize); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	got := multipartOps{
		uploads:   uploads.Load(),
		parts:     parts.Load(),
		completes: completes.Load(),
		aborts:    aborts.Load(),
	}
	if want := (multipartOps{uploads: 1, parts: 2, completes: 1, aborts: 0}); got != want {
		t.Errorf("multipart operations = %+v, want %+v", got, want)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after Close = %d, want 0", got)
	}
}

func TestS3MultipartFailureAndCancellationAbort(t *testing.T) {
	type finishFunc func(t *testing.T, store *Store, writer Writer, cancel context.CancelFunc)
	for _, tc := range []struct {
		name string
		// failParts makes the server reject part uploads.
		failParts               bool
		wantWriteErr            bool
		finish                  finishFunc
		wantCompletionAttempted bool
	}{
		{name: "part", failParts: true, wantWriteErr: true},
		{
			name: "complete",
			finish: func(t *testing.T, _ *Store, writer Writer, _ context.CancelFunc) {
				if err := writer.Close(); err == nil {
					t.Fatal("writer.Close() = nil, want completion failure")
				}
			},
			wantCompletionAttempted: true,
		},
		{
			name: "cancel",
			finish: func(t *testing.T, _ *Store, writer Writer, cancel context.CancelFunc) {
				cancel()
				if err := writer.Close(); !errors.Is(err, context.Canceled) {
					t.Fatalf("writer.Close() = %v, want context.Canceled", err)
				}
			},
		},
		{
			name: "store-close",
			finish: func(t *testing.T, store *Store, _ Writer, _ context.CancelFunc) {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var aborted, completed atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				switch {
				case initiatesUpload(r):
					_, _ = io.WriteString(w, initiateUploadResponse)
				case r.Method == http.MethodDelete:
					aborted.Add(1)
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodPut && tc.failParts:
					_, _ = io.Copy(io.Discard, r.Body)
					writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "injected part failure")
				case r.Method == http.MethodPut:
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("ETag", `"part"`)
				case r.Method == http.MethodPost:
					completed.Add(1)
					writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "injected completion failure")
				case r.Method == http.MethodHead:
					w.WriteHeader(http.StatusNotFound)
				default:
					t.Errorf("unexpected method %s", r.Method)
				}
			}))
			defer server.Close()
			store, budget := testStore(t, 16<<20, server.URL)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writer, err := store.Create(ctx, "s3://bucket/output")
			if err != nil {
				t.Fatal(err)
			}

			err = writeLarge(t, writer, twoPartSize)
			if gotErr := err != nil; gotErr != tc.wantWriteErr {
				t.Fatalf("writeLarge err = %v, want error: %t", err, tc.wantWriteErr)
			}
			if tc.finish != nil {
				tc.finish(t, store, writer, cancel)
			}

			if got := aborted.Load(); got != 1 {
				t.Errorf("abort requests = %d, want 1", got)
			}
			if got := completed.Load(); (got > 0) != tc.wantCompletionAttempted {
				t.Errorf("completion requests = %d, want attempted: %t", got, tc.wantCompletionAttempted)
			}
			if got := budget.Used(); got != 0 {
				t.Errorf("budget used after abort = %d, want 0", got)
			}
		})
	}
}

func TestS3UploadBudgetFailureBeforeBufferAllocation(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	// Too small even for the guaranteed response/cleanup workspace.
	store, budget := testStore(t, 1<<20, server.URL)
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Write([]byte("first byte"))
	assertResourceLimit(t, "Write", err)
	if got := requests.Load(); got != 0 {
		t.Errorf("requests before buffer reservation = %d, want 0", got)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after failed Write = %d, want 0", got)
	}
}

func TestS3BudgetExhaustionStillAbortsMultipart(t *testing.T) {
	var aborted, parts, completed atomic.Int32
	var budget *resource.Budget
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := budget.Used(); got < sdkResponseCharge {
			t.Errorf("budget used during %s = %d, want SDK response workspace >= %d", r.Method, got, sdkResponseCharge)
		}
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case initiatesUpload(r):
			_, _ = io.WriteString(w, initiateUploadResponse)
		case r.Method == http.MethodPut:
			parts.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"part"`)
		case r.Method == http.MethodDelete:
			aborted.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost:
			completed.Add(1)
			_, _ = io.WriteString(w, completeUploadResponse)
		default:
			t.Errorf("unexpected request: %s", r.Method)
		}
	}))
	defer server.Close()
	store, b := testStore(t, 16<<20, server.URL)
	budget = b
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	if err = writeLarge(t, writer, twoPartSize); err != nil {
		t.Fatal(err)
	}
	// Exhaust the remaining capacity before the final part metadata reservation.
	held := budget.Limit() - budget.Used()
	if err = budget.Reserve(held, "test competing managed state"); err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	budget.Release(held)

	assertResourceLimit(t, "writer.Close()", err)
	got := multipartOps{parts: parts.Load(), completes: completed.Load(), aborts: aborted.Load()}
	if want := (multipartOps{parts: 1, completes: 0, aborts: 1}); got != want {
		t.Errorf("multipart operations = %+v, want %+v", got, want)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after abort = %d, want 0", got)
	}
}

func TestS3PutPreservesConfiguredOpaquePrefix(t *testing.T) {
	const wantPath = "/bucket/team//./results/run/manifest.json"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wantPath {
			t.Errorf("PUT path = %q, want %q", r.URL.Path, wantPath)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"written"`)
	}))
	defer server.Close()
	store, _ := testStore(t, 8<<20, server.URL)
	uri := Join("s3://bucket/team//./results/", "run", "manifest.json")
	if err := store.Put(context.Background(), uri, []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

func TestConditionalPutReconcilesLostSuccess(t *testing.T) {
	var object committedObject
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		object.mu.Lock()
		defer object.mu.Unlock()
		switch {
		case r.Method == http.MethodPut && !object.stored:
			// Commit, then lose the response so the client must reconcile.
			object.commit(r)
			dropHTTPConnection(t, w)
		case r.Method == http.MethodPut:
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
		case r.Method == http.MethodHead:
			object.serveHead(w)
		default:
			t.Errorf("unexpected request %s", r.Method)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 32<<20, server.URL)
	if err := store.Put(context.Background(), "s3://bucket/result.json", []byte("committed")); err != nil {
		t.Fatal(err)
	}
	if !object.stored {
		t.Error("server committed no PUT, want one")
	}
	if object.writeID == "" {
		t.Errorf("committed PUT %s = empty, want write identity", headerWriteID)
	}
	if object.digest == "" {
		t.Errorf("committed PUT %s = empty, want content digest", headerSHA256)
	}
	assertClosedWithoutLeaks(t, store, budget)
}

func TestConditionalPutReconcilesAfterContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var object committedObject
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		object.mu.Lock()
		defer object.mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			// Commit, then cancel the caller and lose the response.
			object.commit(r)
			cancel()
			dropHTTPConnection(t, w)
		case http.MethodHead:
			object.serveHead(w)
		default:
			t.Errorf("unexpected request %s", r.Method)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 32<<20, server.URL)
	if err := store.Put(ctx, "s3://bucket/manifest.json", []byte("committed")); err != nil {
		t.Fatal(err)
	}
	if !object.stored {
		t.Fatal("server committed no PUT, want the canceled request committed")
	}
	assertClosedWithoutLeaks(t, store, budget)
}

func TestAmbiguousMultipartCreateAbortsEveryUpload(t *testing.T) {
	const lostUploadID = "lost-upload"
	const listUploadsResponse = `<ListMultipartUploadsResult><Bucket>bucket</Bucket><IsTruncated>false</IsTruncated>` +
		`<Upload><Key>output</Key><UploadId>` + lostUploadID + `</UploadId></Upload></ListMultipartUploadsResult>`
	var aborted atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case initiatesUpload(r):
			dropHTTPConnection(t, w)
		case r.Method == http.MethodGet && r.URL.Query().Has("uploads"):
			_, _ = io.WriteString(w, listUploadsResponse)
		case r.Method == http.MethodDelete && targetsUpload(r, lostUploadID):
			aborted.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 12<<20, server.URL)
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	if err = writeLarge(t, writer, uploadPartSize+1); err == nil {
		t.Fatal("writeLarge err = nil, want ambiguous multipart creation failure")
	}
	if got := aborted.Load(); got != 1 {
		t.Errorf("aborted uploads = %d, want 1", got)
	}
	assertClosedWithoutLeaks(t, store, budget)
}

// A lost CompleteMultipartUpload response is reconciled by HEAD alone: the
// object's write identity and length prove it is this upload. The object is
// never read back, so reconciliation time does not grow with its size.
func TestMultipartCompleteReconcilesByWriteIdentity(t *testing.T) {
	for _, tc := range []struct {
		name      string
		headID    func(sent string) string
		headSize  int
		published bool
	}{
		{name: "matching identity and length", headID: func(sent string) string { return sent }, headSize: twoPartSize, published: true},
		{name: "different identity", headID: func(string) string { return "someone-else" }, headSize: twoPartSize},
		{name: "different length", headID: func(sent string) string { return sent }, headSize: twoPartSize - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			writeID := ""
			completed := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/xml")
				switch {
				case initiatesUpload(r):
					writeID = r.Header.Get(headerWriteID)
					_, _ = io.WriteString(w, initiateUploadResponse)
				case r.Method == http.MethodPut && targetsUpload(r, testUploadID):
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("ETag", `"part"`)
				case r.Method == http.MethodPost && targetsUpload(r, testUploadID) && !completed:
					// Complete, then lose the response so the client must reconcile.
					completed = true
					dropHTTPConnection(t, w)
				case r.Method == http.MethodPost && targetsUpload(r, testUploadID):
					w.WriteHeader(http.StatusNotFound)
					_, _ = io.WriteString(w, `<Error><Code>NoSuchUpload</Code></Error>`)
				case r.Method == http.MethodDelete && targetsUpload(r, testUploadID):
					w.WriteHeader(http.StatusNotFound)
					_, _ = io.WriteString(w, `<Error><Code>NoSuchUpload</Code></Error>`)
				case r.Method == http.MethodHead:
					w.Header().Set("Content-Length", strconv.Itoa(tc.headSize))
					w.Header().Set("ETag", `"complete"`)
					w.Header().Set(headerWriteID, tc.headID(writeID))
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodGet:
					t.Error("reconciliation read the object back; HEAD must be enough")
					w.WriteHeader(http.StatusBadRequest)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			store, budget := testStore(t, 32<<20, server.URL)
			writer, err := store.Create(context.Background(), "s3://bucket/output")
			if err != nil {
				t.Fatal(err)
			}
			if err = writeLarge(t, writer, twoPartSize); err != nil {
				t.Fatal(err)
			}
			err = writer.Close()
			if !completed {
				t.Error("server completed no upload, want one completion before reconciliation")
			}
			if writeID == "" {
				t.Errorf("multipart create %s = empty, want write identity", headerWriteID)
			}
			_, _, digested := writer.(CompletedDigester).CompletedDigest()
			if !tc.published {
				if err == nil || digested {
					t.Errorf("Close() = %v, digest reported %t; want an error and no completed digest", err, digested)
				}
				assertClosedWithoutLeaks(t, store, budget)
				return
			}
			if err != nil {
				t.Fatalf("Close() = %v, want reconciled success", err)
			}
			hasher := sha256.New()
			if _, err := io.CopyN(hasher, zeroReader{}, twoPartSize); err != nil {
				t.Fatal(err)
			}
			var expected [32]byte
			hasher.Sum(expected[:0])
			if size, sum, ok := writer.(CompletedDigester).CompletedDigest(); !ok || size != twoPartSize || sum != expected {
				t.Errorf("reconciled completed digest=(%d,%x,%t), want (%d,%x,true)", size, sum, ok, twoPartSize, expected)
			}
			assertClosedWithoutLeaks(t, store, budget)
		})
	}
}

func TestS3CompletedDigestAndSinglePutBoundaries(t *testing.T) {
	for _, size := range []int{0, 1, uploadPartSize} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			var puts atomic.Int32
			uploaded := make(chan [32]byte, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut || r.URL.Query().Has("uploadId") {
					t.Errorf("single-object boundary made unexpected request: %s %s", r.Method, r.URL)
				}
				puts.Add(1)
				hasher := sha256.New()
				n, err := io.Copy(hasher, r.Body)
				if n != int64(size) || err != nil {
					t.Errorf("PUT bytes = %d, %v; want %d", n, err, size)
				}
				var sum [32]byte
				hasher.Sum(sum[:0])
				uploaded <- sum
				w.Header().Set("ETag", `"written"`)
			}))
			defer server.Close()
			store, budget := testStore(t, 16<<20, server.URL)
			writer, err := store.Create(context.Background(), "s3://bucket/output")
			if err != nil {
				t.Fatal(err)
			}
			digester, ok := writer.(CompletedDigester)
			if !ok {
				t.Fatal("S3 writer lacks completed digest capability")
			}
			if err := writeLarge(t, writer, size); err != nil {
				t.Fatal(err)
			}
			if _, _, ok := digester.CompletedDigest(); ok {
				t.Fatal("open writer claims authoritative completed digest")
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			wantHash := <-uploaded
			gotSize, gotHash, ok := digester.CompletedDigest()
			if !ok || gotSize != int64(size) || gotHash != wantHash {
				t.Errorf("completed digest = (%d,%x,%t), want (%d,%x,true)", gotSize, gotHash, ok, size, wantHash)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if puts.Load() != 1 || budget.Used() != 0 {
				t.Errorf("PUTs=%d retained=%d", puts.Load(), budget.Used())
			}
		})
	}
}

func TestS3AbortedWriterNeverClaimsDigestOrUploads(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	store, budget := testStore(t, sdkResponseCharge+(128<<10), server.URL)
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := writer.Write([]byte("small")); n != 5 || err != nil {
		t.Fatalf("low-budget small Write = %d, %v", n, err)
	}
	if err := writer.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := writer.(CompletedDigester).CompletedDigest(); ok {
		t.Fatal("aborted writer claims completed digest")
	}
	if err := writer.Close(); !errors.Is(err, ErrAborted) {
		t.Errorf("Close after Abort = %v, want ErrAborted so callers never treat it as published", err)
	}
	if requests.Load() != 0 || budget.Used() != 0 {
		t.Errorf("aborted requests=%d retained=%d", requests.Load(), budget.Used())
	}
}

func TestS3MultipartConcurrencyCompletesInPartOrder(t *testing.T) {
	firstArrived := make(chan struct{})
	secondFinished := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseFirst) })
	var active, peak atomic.Int32
	completeBody := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case initiatesUpload(r):
			_, _ = io.WriteString(w, initiateUploadResponse)
		case r.Method == http.MethodPut:
			number, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			running := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); running > old && !peak.CompareAndSwap(old, running); old = peak.Load() {
			}
			n, err := io.Copy(io.Discard, r.Body)
			want := int64(uploadPartSize)
			if number == 3 {
				want = 1
			}
			if n != want || err != nil {
				t.Errorf("part %d bytes = %d, %v; want %d", number, n, err, want)
			}
			if number == 1 {
				close(firstArrived)
				select {
				case <-releaseFirst:
				case <-r.Context().Done():
					return
				}
			} else if number == 2 {
				close(secondFinished)
			}
			w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, number))
		case r.Method == http.MethodPost:
			body, _ := io.ReadAll(r.Body)
			completeBody <- string(body)
			_, _ = io.WriteString(w, completeUploadResponse)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 64<<20, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer, err := store.Create(ctx, "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Abort()
	writeResult := make(chan error, 1)
	go func() {
		_, err := io.CopyN(writer, zeroReader{}, 2*uploadPartSize+1)
		writeResult <- err
	}()
	for _, arrived := range []<-chan struct{}{firstArrived, secondFinished} {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			releaseOnce.Do(func() { close(releaseFirst) })
			cancel()
			<-writeResult
			t.Fatal("second part did not finish while first part was blocked")
		}
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	if err := <-writeResult; err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if peak.Load() < 2 || peak.Load() > maxConcurrentUploads {
		t.Errorf("peak simultaneous parts=%d, want 2..%d", peak.Load(), maxConcurrentUploads)
	}
	var completion struct {
		Parts []struct {
			Number int    `xml:"PartNumber"`
			ETag   string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.Unmarshal([]byte(<-completeBody), &completion); err != nil {
		t.Fatal(err)
	}
	if len(completion.Parts) != 3 {
		t.Fatalf("completion parts=%+v", completion.Parts)
	}
	for i, part := range completion.Parts {
		if part.Number != i+1 || part.ETag != fmt.Sprintf(`"part-%d"`, i+1) {
			t.Errorf("completion part %d = %+v", i, part)
		}
	}
	if _, _, ok := writer.(CompletedDigester).CompletedDigest(); !ok {
		t.Error("successful multipart writer lacks authoritative digest")
	}
	if budget.Used() != 0 {
		t.Errorf("retained multipart budget=%d", budget.Used())
	}
}

func TestS3MultipartUsesLargerGeometryWhenBudgetAllows(t *testing.T) {
	var sizesMu sync.Mutex
	sizes := make(map[int]int64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case initiatesUpload(r):
			_, _ = io.WriteString(w, initiateUploadResponse)
		case r.Method == http.MethodPut:
			number, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				t.Error(err)
			}
			sizesMu.Lock()
			sizes[number] = n
			sizesMu.Unlock()
			w.Header().Set("ETag", `"part"`)
		case r.Method == http.MethodPost:
			_, _ = io.WriteString(w, completeUploadResponse)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 512<<20, server.URL)
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(writer, zeroReader{}, preferredUploadPartSize+1); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	sizesMu.Lock()
	defer sizesMu.Unlock()
	if len(sizes) != 2 || sizes[1] != preferredUploadPartSize || sizes[2] != 1 {
		t.Errorf("larger-geometry part sizes=%v", sizes)
	}
	if budget.Used() != 0 {
		t.Errorf("retained larger-geometry budget=%d", budget.Used())
	}
}

func TestS3UploadBudgetFailureReportsOnlyStagedPrefix(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	store, budget := testStore(t, sdkResponseCharge+writerRegistryCharge+(128<<10), server.URL)
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	n, err := writer.Write(make([]byte, 256<<10))
	assertResourceLimit(t, "bounded Write", err)
	if n != 64<<10 {
		t.Errorf("Write accepted=%d, want staged prefix %d", n, 64<<10)
	}
	if _, _, ok := writer.(CompletedDigester).CompletedDigest(); ok {
		t.Error("failed Write claims completed digest")
	}
	if requests.Load() != 0 || budget.Used() != 0 {
		t.Errorf("failed staging requests=%d retained=%d", requests.Load(), budget.Used())
	}
}

func TestS3MultipartPartCountBoundary(t *testing.T) {
	budget := resource.New(partETagCharge)
	writer := &s3Writer{store: &Store{budget: budget}, parts: make([]types.CompletedPart, 9999, 10000)}
	index, err := writer.preparePart()
	if err != nil || index != 9999 || len(writer.parts) != 10000 || *writer.parts[index].PartNumber != 10000 {
		t.Fatalf("last permitted part = %d, %v, count=%d", index, err, len(writer.parts))
	}
	if _, err := writer.preparePart(); err == nil || len(writer.parts) != 10000 {
		t.Fatalf("part beyond S3 limit = %v, count=%d", err, len(writer.parts))
	}
	budget.Release(writer.etagCharge)
}

func TestS3ConcurrentPartFailureCancelsJoinsAndAborts(t *testing.T) {
	firstArrived := make(chan struct{})
	stopServer := make(chan struct{})
	var stopOnce sync.Once
	defer stopOnce.Do(func() { close(stopServer) })
	var aborts, completes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case initiatesUpload(r):
			_, _ = io.WriteString(w, initiateUploadResponse)
		case r.Method == http.MethodPut:
			number, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			_, _ = io.Copy(io.Discard, r.Body)
			switch number {
			case 1:
				close(firstArrived)
				select {
				case <-r.Context().Done():
				case <-stopServer:
				}
			case 2:
				select {
				case <-firstArrived:
				case <-stopServer:
					return
				}
				writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "injected concurrent part failure")
			default:
				w.Header().Set("ETag", `"tail"`)
			}
		case r.Method == http.MethodDelete:
			aborts.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost:
			completes.Add(1)
			_, _ = io.WriteString(w, completeUploadResponse)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 64<<20, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	writer, err := store.Create(ctx, "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := io.CopyN(writer, zeroReader{}, 2*uploadPartSize+1)
	closeErr := writer.Close()
	stopOnce.Do(func() { close(stopServer) })
	err = errors.Join(writeErr, closeErr)
	if err == nil || !strings.Contains(err.Error(), "injected concurrent part failure") {
		t.Fatalf("concurrent failure lost operational cause: %v", err)
	}
	if aborts.Load() != 1 || completes.Load() != 0 {
		t.Errorf("failed upload aborts=%d completes=%d", aborts.Load(), completes.Load())
	}
	if _, _, ok := writer.(CompletedDigester).CompletedDigest(); ok {
		t.Error("failed concurrent upload claims completed digest")
	}
	if budget.Used() != 0 {
		t.Errorf("retained failed concurrent upload budget=%d", budget.Used())
	}
}

func TestS3MultipartGeometryAdaptsToCompetingManagedState(t *testing.T) {
	var sizesMu sync.Mutex
	sizes := make(map[int]int64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case initiatesUpload(r):
			_, _ = io.WriteString(w, initiateUploadResponse)
		case r.Method == http.MethodPut:
			number, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				t.Error(err)
			}
			sizesMu.Lock()
			sizes[number] = n
			sizesMu.Unlock()
			w.Header().Set("ETag", `"part"`)
		case r.Method == http.MethodPost:
			_, _ = io.WriteString(w, completeUploadResponse)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 512<<20, server.URL)
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	// The first small write selected preferred geometry while the budget was
	// empty; leave only enough headroom for a smaller legal streaming part.
	held := budget.Limit() - budget.Used() - (12 << 20)
	if err := budget.Reserve(held, "test competing managed state"); err != nil {
		t.Fatal(err)
	}
	defer budget.Release(held)
	if err := writeLarge(t, writer, 8<<20); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	sizesMu.Lock()
	defer sizesMu.Unlock()
	if len(sizes) != 2 || sizes[1] != 8<<20 || sizes[2] != 1 {
		t.Errorf("adapted multipart sizes=%v, want 8MiB plus one-byte tail", sizes)
	}
	if budget.Used() != held {
		t.Errorf("writer retained charges after adaptive Close: used=%d held=%d", budget.Used(), held)
	}
}

func TestS3SerialMultipartFitsTenMiBBudget(t *testing.T) {
	var sizesMu sync.Mutex
	sizes := make(map[int]int64)
	var uploads, completes, aborts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case initiatesUpload(r):
			uploads.Add(1)
			_, _ = io.WriteString(w, initiateUploadResponse)
		case r.Method == http.MethodPut:
			number, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				t.Error(err)
			}
			sizesMu.Lock()
			sizes[number] = n
			sizesMu.Unlock()
			w.Header().Set("ETag", `"part"`)
		case r.Method == http.MethodPost:
			completes.Add(1)
			_, _ = io.WriteString(w, completeUploadResponse)
		case r.Method == http.MethodDelete:
			aborts.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	store, budget := testStore(t, 10<<20, server.URL)
	writer, err := store.Create(context.Background(), "s3://bucket/output")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeLarge(t, writer, uploadPartSize+1); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	sizesMu.Lock()
	defer sizesMu.Unlock()
	if len(sizes) != 2 || sizes[1] != uploadPartSize || sizes[2] != 1 {
		t.Errorf("serial multipart sizes=%v, want legal 5MiB part and one-byte tail", sizes)
	}
	if uploads.Load() != 1 || completes.Load() != 1 || aborts.Load() != 0 {
		t.Errorf("serial operations: create=%d complete=%d abort=%d", uploads.Load(), completes.Load(), aborts.Load())
	}
	if budget.Used() != 0 {
		t.Errorf("serial multipart retained budget=%d", budget.Used())
	}
}
