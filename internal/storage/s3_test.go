package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
)

// Identity of the fake object served by objectHeaders.
const (
	headerIfMatch   = "If-Match"
	headerVersionID = "X-Amz-Version-Id"
	snapshotETag    = `"snapshot"`
	snapshotVersion = "version-one"
)

var objectTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func objectHeaders(w http.ResponseWriter, size int, version bool) {
	w.Header().Set("Content-Length", strconv.Itoa(size))
	w.Header().Set("ETag", snapshotETag)
	w.Header().Set("Last-Modified", objectTime.Format(http.TimeFormat))
	if version {
		w.Header().Set(headerVersionID, snapshotVersion)
	}
}

func assertResourceLimit(t *testing.T, operation string, err error) {
	t.Helper()
	var limit *resource.ResourceLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("%s err = %v, want *resource.ResourceLimitError", operation, err)
	}
}

func TestS3ConditionalStreamingAndRanges(t *testing.T) {
	const content = "abcdef"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); !strings.Contains(auth, "Credential=test-access/") {
			t.Errorf("Authorization = %q, want signature by test-access credential chain", auth)
		}
		if r.URL.Path != "/bucket/key.csv" {
			t.Errorf("path = %s, want path-style /bucket/key.csv", r.URL.Path)
		}
		if r.Method == http.MethodHead {
			objectHeaders(w, len(content), true)
			return
		}
		if got := r.Header.Get(headerIfMatch); got != snapshotETag {
			t.Errorf("read %s = %q, want %q", headerIfMatch, got, snapshotETag)
		}
		if got := r.URL.Query().Get("versionId"); got != snapshotVersion {
			t.Errorf("read versionId = %q, want %q", got, snapshotVersion)
		}
		if span := r.Header.Get("Range"); span != "" {
			var start, end int
			if _, err := fmt.Sscanf(span, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= len(content) || end < start {
				t.Errorf("invalid range %q", span)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			objectHeaders(w, end-start+1, true)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, content[start:end+1])
			return
		}
		objectHeaders(w, len(content), true)
		_, _ = io.WriteString(w, content)
	}))
	defer server.Close()
	store, budget := testStore(t, 16<<20, server.URL)
	obj, err := store.Stat(context.Background(), "s3://bucket/key.csv")
	if err != nil {
		t.Fatal(err)
	}
	before := budget.Used()

	body, err := store.Open(context.Background(), obj)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if err = body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("streamed body = %q, want %q", got, content)
	}

	reader, err := store.OpenAt(context.Background(), obj, ReadVerified)
	if err != nil {
		t.Fatal(err)
	}
	var p [3]byte
	n, err := reader.ReadAt(p[:], 2)
	if err != nil {
		t.Fatalf("ReadAt(off 2): %v", err)
	}
	if got := string(p[:n]); got != "cde" {
		t.Errorf("ReadAt(off 2) = %q, want %q", got, "cde")
	}
	n, err = reader.ReadAt(p[:], 5)
	if err != io.EOF {
		t.Errorf("ReadAt(off 5) err = %v, want io.EOF", err)
	}
	if got := string(p[:n]); got != "f" {
		t.Errorf("ReadAt(off 5) = %q, want %q", got, "f")
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}

	if got := budget.Used(); got != before {
		t.Errorf("budget used after closing readers = %d, want %d", got, before)
	}
}

func TestS3RejectsChangedAndIgnoredRange(t *testing.T) {
	openStream := func(t *testing.T, store *Store, obj Object) error {
		r, err := store.Open(context.Background(), obj)
		if err == nil {
			r.Close()
		}
		return err
	}
	readRange := func(t *testing.T, store *Store, obj Object) error {
		r, err := store.OpenAt(context.Background(), obj, ReadVerified)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		var b [3]byte
		_, err = r.ReadAt(b[:], 0)
		return err
	}
	for _, tc := range []struct {
		name string
		// respond answers the conditional GET.
		respond func(w http.ResponseWriter)
		read    func(t *testing.T, store *Store, obj Object) error
	}{
		{
			name: "changed",
			respond: func(w http.ResponseWriter) {
				objectHeaders(w, 3, false)
				w.Header().Set("ETag", `"changed"`)
				_, _ = io.WriteString(w, "abc")
			},
			read: openStream,
		},
		{
			// A full 200 response to a range request.
			name: "ignored-range",
			respond: func(w http.ResponseWriter) {
				objectHeaders(w, 3, false)
				_, _ = io.WriteString(w, "abc")
			},
			read: readRange,
		},
		{
			name: "precondition",
			respond: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = io.WriteString(w, "<Error><Code>PreconditionFailed</Code></Error>")
			},
			read: openStream,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					objectHeaders(w, 3, false)
					return
				}
				if got := r.Header.Get(headerIfMatch); got != snapshotETag {
					t.Errorf("read %s = %q, want %q", headerIfMatch, got, snapshotETag)
				}
				tc.respond(w)
			}))
			defer server.Close()
			store, _ := testStore(t, 16<<20, server.URL)
			obj, err := store.Stat(context.Background(), "s3://bucket/key")
			if err != nil {
				t.Fatal(err)
			}
			if err = tc.read(t, store, obj); err == nil {
				t.Fatal("read err = nil, want rejection")
			}
		})
	}
}

func TestS3PaginatedDiscoverySortsKeys(t *testing.T) {
	const firstPage = `<ListBucketResult><IsTruncated>true</IsTruncated>` +
		`<NextContinuationToken>page-two</NextContinuationToken>` +
		`<Contents><Key>input/z.csv</Key></Contents>` +
		`<Contents><Key>input/ignore.txt</Key></Contents>` +
		`</ListBucketResult>`
	const secondPage = `<ListBucketResult><IsTruncated>false</IsTruncated>` +
		`<Contents><Key>input/a!.csv</Key></Contents>` +
		`<Contents><Key>input/a .csv</Key></Contents>` +
		`</ListBucketResult>`
	var pages atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			objectHeaders(w, 1, false)
			return
		}
		query := r.URL.Query()
		for param, want := range map[string]string{"list-type": "2", "max-keys": "100", "prefix": "input/"} {
			if got := query.Get(param); got != want {
				t.Errorf("listing %s = %q, want %q (query %s)", param, got, want, r.URL.RawQuery)
			}
		}
		pages.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		switch token := query.Get("continuation-token"); token {
		case "":
			_, _ = io.WriteString(w, firstPage)
		case "page-two":
			_, _ = io.WriteString(w, secondPage)
		default:
			t.Errorf("continuation-token = %q, want page-two", token)
			_, _ = io.WriteString(w, secondPage)
		}
	}))
	defer server.Close()
	store, _ := testStore(t, 16<<20, server.URL)
	source := config.Source{Prefix: "s3://bucket/input/", Recursive: true, Filter: "csv"}
	objects, err := store.Discover(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, object := range objects {
		got = append(got, object.URI)
	}
	// Sorted by decoded key: ' ' (0x20) before '!' (0x21) before 'z'.
	want := []string{"s3://bucket/input/a%20.csv", "s3://bucket/input/a%21.csv", "s3://bucket/input/z.csv"}
	if !slices.Equal(got, want) {
		t.Errorf("discovered URIs = %q, want %q", got, want)
	}
	if got := pages.Load(); got != 2 {
		t.Errorf("listing pages = %d, want 2", got)
	}
}

func TestS3OversizedListingResponseIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(metadataLimit+1))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	store, _ := testStore(t, 16<<20, server.URL)
	source := config.Source{Prefix: "s3://bucket/input/", Recursive: true, Filter: "csv"}
	_, err := store.Discover(context.Background(), source)
	if err == nil {
		t.Fatalf("Discover err = nil, want rejection of listing over %d bytes", metadataLimit)
	}
}

func TestS3LargeHeadDoesNotBufferObject(t *testing.T) {
	const size int64 = 1 << 40
	// Stat may keep only small, bounded per-object state after decoding.
	const maxRetained = 16 << 10
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("Stat sent %s, want HEAD only", r.Method)
		}
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.Header().Set("ETag", `"large"`)
	}))
	defer server.Close()
	store, budget := testStore(t, 8<<20, server.URL)
	obj, err := store.Stat(context.Background(), "s3://bucket/large.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if obj.Size != size {
		t.Errorf("Stat size = %d, want %d", obj.Size, size)
	}
	if got := budget.Peak(); got != sdkResponseCharge {
		t.Errorf("budget peak = %d, want one SDK response charge %d", got, sdkResponseCharge)
	}
	if got := budget.Used(); got > maxRetained {
		t.Errorf("budget used after Stat = %d, want at most %d", got, maxRetained)
	}
}

func TestS3TruncatedStreamFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		objectHeaders(w, 10, false)
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, "short")
		}
	}))
	defer server.Close()
	store, _ := testStore(t, 8<<20, server.URL)
	obj, err := store.Stat(context.Background(), "s3://bucket/truncated.csv")
	if err != nil {
		t.Fatal(err)
	}
	body, err := store.Open(context.Background(), obj)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, body)
	closeErr := body.Close()
	if readErr == nil && closeErr == nil {
		t.Fatal("read and Close errors = nil, want truncation detected by either")
	}
}

func TestS3ResponseBudgetIsReservedBeforeRequests(t *testing.T) {
	object := Object{URI: "s3://bucket/key", Size: 1, ETag: snapshotETag}
	for _, tc := range []struct {
		name string
		call func(store *Store) error
	}{
		{name: "head", call: func(store *Store) error {
			_, err := store.Stat(context.Background(), object.URI)
			return err
		}},
		{name: "get", call: func(store *Store) error {
			reader, err := store.Open(context.Background(), object)
			if reader != nil {
				reader.Close()
			}
			return err
		}},
		{name: "range", call: func(store *Store) error {
			reader, err := store.OpenAt(context.Background(), object, ReadVerified)
			if err != nil {
				return err
			}
			defer reader.Close()
			var b [1]byte
			_, err = reader.ReadAt(b[:], 0)
			return err
		}},
		{name: "put", call: func(store *Store) error {
			return putObject(context.Background(), store, object.URI, []byte("body"))
		}},
		{name: "list", call: func(store *Store) error {
			_, err := store.Discover(context.Background(), config.Source{Prefix: "s3://bucket/prefix", Filter: "csv"})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer server.Close()
			// One byte short of the response decoding workspace.
			store, budget := testStore(t, sdkResponseCharge-1, server.URL)
			assertResourceLimit(t, tc.name, tc.call(store))
			if got := requests.Load(); got != 0 {
				t.Errorf("requests = %d, want 0 before response workspace is reserved", got)
			}
			if got := budget.Used(); got != 0 {
				t.Errorf("budget used after failure = %d, want 0", got)
			}
		})
	}
}

func TestS3LargeErrorResponseHasManagedWorkspace(t *testing.T) {
	var budget *resource.Budget
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := budget.Used(); got < sdkResponseCharge {
			t.Errorf("budget used during request = %d, want SDK error workspace >= %d", got, sdkResponseCharge)
		}
		w.Header().Set("Content-Type", "application/xml")
		// Just under metadataLimit, so the SDK decodes the whole error body.
		message := strings.Repeat("x", int(metadataLimit)-100)
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", message)
	}))
	defer server.Close()
	store, b := testStore(t, 8<<20, server.URL)
	budget = b
	transport, ok := store.input.Options().HTTPClient.(*boundedHTTPClient)
	if !ok || transport.inner == nil {
		t.Fatalf("input HTTP client = %T, want initialized *boundedHTTPClient", store.input.Options().HTTPClient)
	}
	reader, err := store.Open(context.Background(), Object{URI: "s3://bucket/key", Size: 1, ETag: snapshotETag})
	if reader != nil {
		reader.Close()
	}
	if err == nil {
		t.Fatal("Open err = nil, want S3 error response")
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("requests = %d, want 1", got)
	}
	if got := budget.Peak(); got < sdkResponseCharge {
		t.Errorf("budget peak = %d, want at least %d", got, sdkResponseCharge)
	}
	if got := budget.Used(); got != 0 {
		t.Errorf("budget used after error = %d, want 0", got)
	}
}

func TestS3ReaderAtCoalescesPinnedConcurrentRangesAndTail(t *testing.T) {
	content := strings.Repeat("0123456789abcdef", 64<<10)
	var gets atomic.Int32
	var rangesMu sync.Mutex
	var ranges []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			objectHeaders(w, len(content), true)
			return
		}
		gets.Add(1)
		if r.Header.Get(headerIfMatch) != snapshotETag || r.URL.Query().Get("versionId") != snapshotVersion {
			t.Errorf("unpinned cached GET: headers=%v query=%s", r.Header, r.URL.RawQuery)
		}
		span := r.Header.Get("Range")
		rangesMu.Lock()
		ranges = append(ranges, span)
		rangesMu.Unlock()
		var start, end int
		if _, err := fmt.Sscanf(span, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= len(content) || end < start {
			t.Errorf("invalid cached range %q", span)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		objectHeaders(w, end-start+1, true)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, content[start:end+1])
	}))
	defer server.Close()
	store, budget := testStore(t, 16<<20, server.URL)
	obj, err := store.Stat(context.Background(), "s3://bucket/source.parquet")
	if err != nil {
		t.Fatal(err)
	}
	before := budget.Used()
	reader, err := store.OpenAt(context.Background(), obj, ReadVerified)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, offset := range []int64{int64(worker * 100), int64(len(content) - 1024 + worker)} {
				var buffer [32]byte
				n, err := reader.ReadAt(buffer[:], offset)
				if n != len(buffer) || err != nil || string(buffer[:]) != content[offset:offset+32] {
					t.Errorf("ReadAt(%d) = %d, %v with incorrect bytes", offset, n, err)
				}
			}
		}()
	}
	wg.Wait()
	if gets.Load() != 2 {
		t.Errorf("cached GETs = %d, want one normal window and one tail", gets.Load())
	}
	rangesMu.Lock()
	for _, span := range ranges {
		var start, end int
		_, _ = fmt.Sscanf(span, "bytes=%d-%d", &start, &end)
		if end-start+1 > s3RangeWindow {
			t.Errorf("range exceeds bounded window: %s", span)
		}
	}
	rangesMu.Unlock()
	var end [8]byte
	if n, err := reader.ReadAt(end[:], obj.Size-3); n != 3 || err != io.EOF {
		t.Errorf("tail boundary ReadAt = %d, %v; want 3, EOF", n, err)
	}
	if n, err := reader.ReadAt(nil, obj.Size); n != 0 || err != nil {
		t.Errorf("empty EOF ReadAt = %d, %v", n, err)
	}
	if n, err := reader.ReadAt(end[:], -1); n != 0 || err == nil {
		t.Errorf("negative ReadAt = %d, %v", n, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != before {
		t.Errorf("cache charges after Close = %d, want %d", budget.Used(), before)
	}
	if n, err := reader.ReadAt(end[:], 0); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Errorf("closed ReadAt = %d, %v", n, err)
	}
}

func TestS3RangeCacheFallsBackWithoutStealingResponseBudget(t *testing.T) {
	var gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gets.Add(1)
		if got := r.Header.Get("Range"); got != "bytes=17-20" {
			t.Errorf("low-budget range = %q, want exact caller range", got)
		}
		objectHeaders(w, 4, false)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 17-20/%d", 1<<20))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "data")
	}))
	defer server.Close()
	store, budget := testStore(t, sdkResponseCharge+s3RangeReaderCharge+s3BodyCharge, server.URL)
	reader, err := store.OpenAt(context.Background(), Object{URI: "s3://bucket/source.parquet", Size: 1 << 20, ETag: snapshotETag}, ReadVerified)
	if err != nil {
		t.Fatal(err)
	}
	var data [4]byte
	if n, err := reader.ReadAt(data[:], 17); n != 4 || err != nil || string(data[:]) != "data" {
		t.Fatalf("low-budget ReadAt = %d, %v, %q", n, err, data[:])
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 1 || budget.Used() != 0 {
		t.Errorf("low-budget reads=%d retained=%d", gets.Load(), budget.Used())
	}
}

func TestS3MetadataReaderNeverReadsAheadIntoPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if span := r.Header.Get("Range"); span != "bytes=1048568-1048575" {
			t.Errorf("metadata GET reads beyond requested footer: %q", span)
		}
		objectHeaders(w, 8, false)
		w.Header().Set("Content-Range", "bytes 1048568-1048575/1048576")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "footer!!")
	}))
	defer server.Close()
	store, budget := testStore(t, 8<<20, server.URL)
	reader, err := store.OpenAt(context.Background(), Object{URI: "s3://bucket/source.parquet", Size: 1 << 20, ETag: snapshotETag}, ReadMetadata)
	if err != nil {
		t.Fatal(err)
	}
	var footer [8]byte
	if n, err := reader.ReadAt(footer[:], (1<<20)-8); n != 8 || err != nil || string(footer[:]) != "footer!!" {
		t.Fatalf("metadata ReadAt = %d, %v, %q", n, err, footer[:])
	}
	if budget.Used() != s3RangeReaderCharge {
		t.Errorf("metadata reader retained cache: used=%d", budget.Used())
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestS3RangeCacheNeverPublishesFailedFill(t *testing.T) {
	var gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		number := gets.Add(1)
		objectHeaders(w, s3RangeWindow, false)
		if number == 1 {
			w.Header().Set("ETag", `"different-object"`)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", s3RangeWindow-1, 1<<20))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.CopyN(w, zeroReader{}, s3RangeWindow)
	}))
	defer server.Close()
	store, budget := testStore(t, 8<<20, server.URL)
	reader, err := store.OpenAt(context.Background(), Object{URI: "s3://bucket/source.parquet", Size: 1 << 20, ETag: snapshotETag}, ReadVerified)
	if err != nil {
		t.Fatal(err)
	}
	buffer := []byte("untouched")
	if n, err := reader.ReadAt(buffer, 31); n != 0 || err == nil || string(buffer) != "untouched" {
		t.Fatalf("invalid cached fill exposed bytes: n=%d err=%v bytes=%q", n, err, buffer)
	}
	if n, err := reader.ReadAt(buffer, 31); n != len(buffer) || err != nil || string(buffer) != strings.Repeat("\x00", len(buffer)) {
		t.Fatalf("fresh pinned fill = %d, %v, %q", n, err, buffer)
	}
	if gets.Load() != 2 {
		t.Errorf("failed cache fill was reused: GETs=%d", gets.Load())
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 0 {
		t.Errorf("failed-fill budget retained=%d", budget.Used())
	}
}

func TestS3RangeCacheGrowthRespectsSharedBudgetOverlap(t *testing.T) {
	const oldWindow = s3RangeWindow / 2
	const objectSize = 2*s3RangeWindow + oldWindow
	for _, shortBy := range []int64{0, 1} {
		t.Run(fmt.Sprintf("headroom-short-by-%d", shortBy), func(t *testing.T) {
			spans := make(chan string, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				span := r.Header.Get("Range")
				spans <- span
				var start, end int
				if _, err := fmt.Sscanf(span, "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= objectSize || end < start {
					t.Errorf("invalid range %q", span)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				objectHeaders(w, end-start+1, false)
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, objectSize))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.CopyN(w, zeroReader{}, int64(end-start+1))
			}))
			defer server.Close()
			store, budget := testStore(t, 8<<20, server.URL)
			reader, err := store.OpenAt(context.Background(), Object{URI: "s3://bucket/source.parquet", Size: objectSize, ETag: snapshotETag}, ReadVerified)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			var buffer [16]byte
			if n, err := reader.ReadAt(buffer[:], 2*s3RangeWindow); n != len(buffer) || err != nil {
				t.Fatalf("initial small cache ReadAt = %d, %v", n, err)
			}
			if span := <-spans; span != fmt.Sprintf("bytes=%d-%d", 2*s3RangeWindow, objectSize-1) {
				t.Fatalf("initial cache range=%q", span)
			}
			if used := budget.Used(); used != s3RangeReaderCharge+oldWindow {
				t.Fatalf("initial reader charge=%d, want old window retained", used)
			}
			// Keep the old cache live while leaving exactly the charge for
			// a complete replacement plus pinned-response workspace.
			held := budget.Limit() - budget.Used() - s3RangeWindow - sdkResponseCharge - s3BodyCharge + shortBy
			if err := budget.Reserve(held, "test competing managed state"); err != nil {
				t.Fatal(err)
			}
			defer budget.Release(held)
			if n, err := reader.ReadAt(buffer[:], 0); n != len(buffer) || err != nil {
				t.Fatalf("shared-budget cache growth ReadAt = %d, %v", n, err)
			}
			wantSpan := fmt.Sprintf("bytes=0-%d", s3RangeWindow-1)
			retained := int64(s3RangeWindow)
			if shortBy != 0 {
				wantSpan = "bytes=0-15"
				retained = oldWindow
			}
			if span := <-spans; span != wantSpan {
				t.Errorf("replacement range=%q, want %q", span, wantSpan)
			}
			if used := budget.Used(); used != held+s3RangeReaderCharge+retained {
				t.Errorf("replacement reader charge=%d, want %d", used, held+s3RangeReaderCharge+retained)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if used := budget.Used(); used != held {
				t.Errorf("cache charges after Close=%d, want competing state only %d", used, held)
			}
		})
	}
}
