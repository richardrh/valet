package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/richardrh/valet/internal/config"
)

const metadataLimit int64 = 1 << 20

// Covers the capped response, buffer growth/copies, XML decoding and bounded headers.
const sdkResponseCharge int64 = 4*metadataLimit + 256*1024

type boundedHTTPClient struct{ inner aws.HTTPClient }

func (c *boundedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	response, err := c.inner.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			clone := *ue
			if u, e := url.Parse(clone.URL); e == nil {
				u.RawQuery = ""
				u.Fragment = ""
				u.User = nil
				clone.URL = u.String()
			}
			return nil, &clone
		}
		return nil, err
	}
	q := req.URL.Query()
	metadata := req.Method != "GET" || q.Has("list-type") || q.Has("uploads") || q.Has("uploadId") || response.StatusCode >= 300
	if metadata {
		// HEAD Content-Length describes the object, not a metadata response body.
		if req.Method != "HEAD" && response.ContentLength > metadataLimit {
			response.Body.Close()
			return nil, errors.New("S3 metadata response exceeds bounded size")
		}
		response.Body = &cappedBody{ReadCloser: response.Body, remaining: metadataLimit}
	}
	return response, nil
}

type cappedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.remaining == 0 {
		var extra [1]byte
		n, err := b.ReadCloser.Read(extra[:])
		if n != 0 {
			return 0, errors.New("S3 metadata response exceeds bounded size")
		}
		return 0, err
	}
	p = p[:min(int64(len(p)), b.remaining)]
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}

func newS3(ctx context.Context, settings config.S3, logger *slog.Logger) (*s3.Client, error) {
	// Request/response wire logging stays off (ClientLogMode 0): it would echo
	// signed headers. Only the SDK's own Logf diagnostics reach the logger, at
	// debug level, so the SDK's default standard logger never writes
	// unstructured lines to stderr.
	sdkLogger := logging.LoggerFunc(func(classification logging.Classification, format string, v ...any) {
		if logger.Enabled(context.Background(), slog.LevelDebug) {
			logger.Debug("aws sdk", slog.String("phase", "storage"), slog.String("classification", string(classification)))
		}
	})
	options := []func(*awsconfig.LoadOptions) error{awsconfig.WithRetryMaxAttempts(3), awsconfig.WithLogger(sdkLogger), awsconfig.WithClientLogMode(0)}
	if settings.Region != "" {
		options = append(options, awsconfig.WithRegion(settings.Region))
	}
	if settings.Profile != "" {
		options = append(options, awsconfig.WithSharedConfigProfile(settings.Profile))
	}
	if settings.Endpoint != "" {
		u, err := url.Parse(settings.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("S3 endpoint requires an HTTP(S) URL without credentials or query")
		}
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = awshttp.NewBuildableClient()
	}
	if client, ok := cfg.HTTPClient.(*awshttp.BuildableClient); ok {
		cfg.HTTPClient = client.WithTransportOptions(func(transport *http.Transport) {
			transport.MaxResponseHeaderBytes = 64 * 1024
			transport.MaxConnsPerHost = 4
			transport.MaxIdleConnsPerHost = 4
		})
	}
	cfg.HTTPClient = &boundedHTTPClient{inner: cfg.HTTPClient}
	cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	// WhenRequired: payload checksums are validated only when an operation
	// requires them. Pinned reads are instead verified by exact Content-Length,
	// ETag/version match and Content-Range, so a response without a checksum
	// header (the SDK's "no supported checksum" debug note) is not an error.
	cfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = settings.PathStyle
		if settings.Endpoint != "" {
			o.BaseEndpoint = aws.String(settings.Endpoint)
		}
	}), nil
}
func validS3Identity(obj Object) error {
	if obj.Size < 0 || ((obj.VersionID == "" || obj.VersionID == "null") && obj.ETag == "") {
		return errors.New("S3 target lacks a version or ETag required for pinned reads")
	}
	return nil
}
func (s *Store) discoverS3(ctx context.Context, src config.Source, add func(string) error) error {
	bucket, prefix, err := s3URI(src.Prefix)
	if err != nil {
		return err
	}
	var token *string
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		const pageCharge = sdkResponseCharge
		if err := s.budget.Reserve(pageCharge, "S3 discovery page"); err != nil {
			return err
		}
		input := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token, MaxKeys: aws.Int32(100)}
		if !src.Recursive {
			input.Delimiter = aws.String("/")
		}
		page, e := s.input.ListObjectsV2(ctx, input)
		if e != nil {
			s.budget.Release(pageCharge)
			return fmt.Errorf("list S3 objects: %w", e)
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if key == "" || strings.HasSuffix(key, "/") || !matchesFilter(key, src.Filter) {
				continue
			}
			uri := objectURI(bucket, key)
			if e = add(uri); e != nil {
				break
			}
		}
		more := aws.ToBool(page.IsTruncated)
		next := aws.ToString(page.NextContinuationToken)
		if e == nil && more && (next == "" || next == aws.ToString(token)) {
			e = errors.New("S3 listing did not advance its continuation token")
		}
		// Bound the only response value carried into the next request independently.
		if e == nil && more && len(next) > 16384 {
			e = errors.New("S3 listing continuation token exceeds bound")
		}
		if e == nil && more && token == nil {
			e = s.budget.Reserve(s3ContinuationCharge, "S3 continuation token")
			if e == nil {
				defer s.budget.Release(s3ContinuationCharge)
			}
		}
		if e == nil && more {
			next = strings.Clone(next)
		}
		s.budget.Release(pageCharge)
		if e != nil {
			return e
		}
		if !more {
			return nil
		}
		token = &next
	}
}

// openS3 reads the pinned object; when ranged, only [offset, offset+length).
// metadata attributes the bytes read to Parquet metadata rather than payload.
func (s *Store) openS3(ctx context.Context, obj Object, ranged bool, offset, length int64, metadata bool) (io.ReadCloser, error) {
	if err := validS3Identity(obj); err != nil {
		return nil, err
	}
	bucket, key, err := s3URI(obj.URI)
	if err != nil {
		return nil, err
	}
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if obj.VersionID != "" {
		input.VersionId = aws.String(obj.VersionID)
	}
	if obj.ETag != "" {
		input.IfMatch = aws.String(obj.ETag)
	}
	var span string
	if ranged {
		span = strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+length-1, 10)
		input.Range = aws.String("bytes=" + span)
	}
	if err = s.budget.Reserve(sdkResponseCharge, "S3 GET response"); err != nil {
		return nil, err
	}
	defer s.budget.Release(sdkResponseCharge)
	result, err := s.input.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("read pinned S3 object: %w", err)
	}
	if result.ContentLength == nil || *result.ContentLength != length || (obj.ETag != "" && aws.ToString(result.ETag) != obj.ETag) || (obj.VersionID != "" && aws.ToString(result.VersionId) != obj.VersionID) || (!obj.ModTime.IsZero() && !aws.ToTime(result.LastModified).Equal(obj.ModTime)) {
		result.Body.Close()
		return nil, changed(obj.URI)
	}
	if ranged && aws.ToString(result.ContentRange) != "bytes "+span+"/"+strconv.FormatInt(obj.Size, 10) {
		result.Body.Close()
		return nil, errors.New("S3 target did not honor the pinned byte range")
	}
	if err = s.budget.Reserve(s3BodyCharge, "S3 streaming reader"); err != nil {
		result.Body.Close()
		return nil, err
	}
	return &s3Body{body: result.Body, remaining: length, store: s, ctx: ctx, metadata: metadata}, nil
}

type s3Body struct {
	body      io.ReadCloser
	remaining int64
	store     *Store
	ctx       context.Context
	metadata  bool
	closed    bool
	err       error
}

func (r *s3Body) Read(p []byte) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}
	n, err := r.body.Read(p)
	recordBackendRead(r.ctx, n, r.metadata)
	r.remaining -= int64(n)
	if r.remaining < 0 || (err == io.EOF && r.remaining != 0) {
		r.err = io.ErrUnexpectedEOF
		return n, r.err
	}
	if err != nil && err != io.EOF {
		r.err = err
	}
	return n, err
}
func (r *s3Body) Close() error {
	if r.closed {
		return r.err
	}
	r.closed = true
	r.err = errors.Join(r.err, r.body.Close())
	r.store.budget.Release(s3BodyCharge)
	return r.err
}

const (
	s3RangeWindow = 256 << 10
	s3TailWindow  = 64 << 10
)

type s3RangeCache struct {
	data   []byte
	offset int64
	valid  bool
}

type s3ReaderAt struct {
	store    *Store
	ctx      context.Context
	obj      Object
	mu       sync.RWMutex
	cacheMu  sync.Mutex
	window   s3RangeCache
	tail     s3RangeCache
	coalesce bool
	metadata bool
	closed   bool
}

func (r *s3ReaderAt) readRange(p []byte, offset int64) (int, error) {
	body, err := r.store.openS3(r.ctx, r.obj, true, offset, int64(len(p)), r.metadata)
	if err != nil {
		return 0, err
	}
	n, err := io.ReadFull(body, p)
	return n, errors.Join(err, body.Close())
}

func (r *s3ReaderAt) cachedRead(p []byte, offset int64) (int, error) {
	// One fill per window coalesces concurrent neighboring reads. Large reads
	// bypass this lock and stream directly into the caller's owned buffer.
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	for _, cache := range []*s3RangeCache{&r.tail, &r.window} {
		if cache.valid && offset >= cache.offset && int64(len(p)) <= int64(len(cache.data))-(offset-cache.offset) {
			return copy(p, cache.data[offset-cache.offset:]), nil
		}
	}
	cache := &r.window
	start := offset / s3RangeWindow * s3RangeWindow
	length := min(int64(s3RangeWindow), r.obj.Size-start)
	tailStart := max(int64(0), r.obj.Size-s3TailWindow)
	if offset >= tailStart {
		cache, start, length = &r.tail, tailStart, r.obj.Size-tailStart
	} else if int64(len(p)) > length-(offset-start) {
		start, length = offset, min(int64(s3RangeWindow), r.obj.Size-offset)
	}
	if int64(cap(cache.data)) < length {
		// Cache allocation is optional, and must not consume the workspace
		// needed to validate the pinned GET response.
		available := r.store.budget.Limit() - r.store.budget.Used()
		if length > available-sdkResponseCharge-s3BodyCharge || r.store.budget.Reserve(length, "S3 range cache") != nil {
			return r.readRange(p, offset)
		}
		oldCharge := int64(cap(cache.data))
		next := make([]byte, int(length))
		cache.data = next
		r.store.budget.Release(oldCharge)
	}
	cache.valid = false
	cache.data = cache.data[:int(length)]
	if _, err := r.readRange(cache.data, start); err != nil {
		return 0, err
	}
	cache.offset, cache.valid = start, true
	return copy(p, cache.data[offset-start:]), nil
}

func (r *s3ReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	if offset < 0 {
		return 0, errors.New("negative read offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if offset >= r.obj.Size {
		return 0, io.EOF
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	length := min(int64(len(p)), r.obj.Size-offset)
	var n int
	var err error
	if r.coalesce && length <= s3TailWindow {
		n, err = r.cachedRead(p[:length], offset)
	} else {
		n, err = r.readRange(p[:length], offset)
	}
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (r *s3ReaderAt) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		r.store.budget.Release(s3RangeReaderCharge + int64(cap(r.window.data)) + int64(cap(r.tail.data)))
		r.window.data, r.tail.data = nil, nil
	}
	return nil
}
