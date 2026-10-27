// Package storage provides bounded, identity-pinned local and S3 storage.
package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
)

type Object struct {
	URI       string
	Size      int64
	ETag      string
	VersionID string
	ModTime   time.Time
	// Identity and coverage describe input verification, never content digests.
	IdentityMethod        string
	PayloadVerification   string
	IngestionVerification string
}
type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

// Writer publishes one object. Close publishes it; Abort discards it. A
// Writer is used by one goroutine at a time.
type Writer interface {
	io.Writer
	Close() error
	Abort() error
}

// ErrAborted is returned by Close and Write after Abort, so an aborted object
// is never mistaken for a published one.
var ErrAborted = errors.New("storage: output was aborted")

// CompletedDigester exposes an authoritative digest only after successful
// publication. Writers implementing it hash exactly the bytes accepted by Write;
// failed, aborted, and still-open writers return ok=false.
type CompletedDigester interface {
	CompletedDigest() (size int64, sha256 [32]byte, ok bool)
}

// Budget charges held by storage. Each reserve and its release use the same
// constant so the pair cannot drift.
const (
	writerRegistryCharge      int64 = 512 // tracked writer list node
	localReaderCharge         int64 = 512 // local reader state and digest
	localDigestBuffer               = 32 * 1024
	s3RangeReaderCharge       int64 = 256   // pinned S3 range reader handle
	s3BodyCharge              int64 = 128   // streaming S3 body wrapper
	s3ContinuationCharge      int64 = 32768 // cloned continuation token (bounded to 16 KiB) plus slack
	snapshotBaseCharge        int64 = 1024  // file metadata, snapshot and map growth slack per identity
	discoveryObjectCharge     int64 = 128   // one Object slot in the discovery list
	discoveryIndexEntryCharge int64 = 64    // one duplicate-set entry for a URI list
	directoryWalkCharge       int64 = 16384 // one open directory and its ReadDir batch
	completedPartCharge       int64 = 128   // one multipart CompletedPart slot
	completionXMLBase               = 4096
	completionXMLPerPart            = 1024
)

// Store owns discovery metadata and outstanding writers. Close it after readers.
type Store struct {
	budget         *resource.Budget
	logger         *slog.Logger
	input, output  *s3.Client
	outputSettings config.S3
	mu             sync.Mutex
	snapshots      map[string]*snapshot
	writers        *writerLink
	retained       int64
	closed         bool
}
type snapshot struct {
	object          Object
	info            os.FileInfo
	digest          [32]byte
	digested        bool
	parquetMetadata bool
	footerOffset    int64
}
type writerLink struct {
	writer Writer
	next   *writerLink
}

// New opens the input and output clients. A nil logger discards discovery,
// publication and abort events.
func New(ctx context.Context, input, output config.S3, budget *resource.Budget, logger *slog.Logger) (*Store, error) {
	if budget == nil {
		return nil, errors.New("storage requires a resource budget")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if output == (config.S3{}) {
		output = input
	}
	in, err := newS3(ctx, input, logger)
	if err != nil {
		return nil, err
	}
	out := in
	if output != input {
		out, err = newS3(ctx, output, logger)
		if err != nil {
			return nil, err
		}
	}
	return &Store{budget: budget, logger: logger, input: in, output: out, outputSettings: output}, nil
}

func (s *Store) remember(obj Object, info os.FileInfo) (*snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, os.ErrClosed
	}
	if p := s.snapshots[obj.URI]; p != nil {
		if p.object.ETag == obj.ETag && p.object.VersionID == obj.VersionID && p.object.Size == obj.Size && p.object.ModTime.Equal(obj.ModTime) {
			return p, nil
		}
		return nil, changed(obj.URI)
	}
	// Includes file metadata, snapshot, and conservative hash-table growth slack.
	n := snapshotBaseCharge + int64(len(obj.URI)+len(obj.ETag)+len(obj.VersionID))
	if err := s.budget.Reserve(n, "storage identities"); err != nil {
		return nil, err
	}
	// Clone strings so SDK response backing storage is not retained.
	obj.URI = strings.Clone(obj.URI)
	obj.ETag = strings.Clone(obj.ETag)
	obj.VersionID = strings.Clone(obj.VersionID)
	if s.snapshots == nil {
		s.snapshots = make(map[string]*snapshot)
	}
	p := &snapshot{object: obj, info: info}
	s.snapshots[obj.URI] = p
	s.retained += n
	return p, nil
}
func (s *Store) track(w Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if err := s.budget.Reserve(writerRegistryCharge, "storage writer registry"); err != nil {
		return err
	}
	s.writers = &writerLink{writer: w, next: s.writers}
	return nil
}
func (s *Store) untrack(w Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p := &s.writers; *p != nil; p = &(*p).next {
		if (*p).writer == w {
			*p = (*p).next
			s.budget.Release(writerRegistryCharge)
			return
		}
	}
}

// Close aborts writers still open, for runs that failed before closing them,
// and releases retained identities. Callers must have stopped using every
// writer first: writers are not safe for concurrent use.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	writers := s.writers
	s.writers = nil
	retained := s.retained
	s.retained = 0
	s.snapshots = nil
	s.mu.Unlock()
	var err error
	for p := writers; p != nil; p = p.next {
		err = errors.Join(err, p.writer.Abort())
		s.budget.Release(writerRegistryCharge)
	}
	s.budget.Release(retained)
	return err
}

func s3URI(uri string) (bucket, key string, err error) {
	u, e := url.Parse(uri)
	if e != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", errors.New("invalid S3 URI (expected s3://bucket/key without credentials or query)")
	}
	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}
func isS3(uri string) bool { return strings.HasPrefix(uri, "s3://") }
func localPath(uri string) (string, error) {
	if strings.Contains(uri, "://") {
		return "", errors.New("unsupported storage URI scheme")
	}
	return filepath.Abs(uri)
}

// Join appends relative components without permitting them to escape the base.
func Join(base string, elements ...string) string {
	if isS3(base) {
		bucket, key, err := s3URI(base)
		if err != nil {
			return base
		}
		for _, e := range elements {
			component := strings.TrimPrefix(path.Clean("/"+strings.ReplaceAll(e, "\\", "/")), "/")
			if component == "" {
				continue
			}
			// Existing key bytes are opaque: repeated slashes and dot segments matter.
			if key != "" && !strings.HasSuffix(key, "/") {
				key += "/"
			}
			key += component
		}
		return objectURI(bucket, key)
	}
	for _, e := range elements {
		base = filepath.Join(base, filepath.FromSlash(strings.TrimPrefix(path.Clean("/"+strings.ReplaceAll(e, "\\", "/")), "/")))
	}
	return base
}

func (s *Store) Stat(ctx context.Context, uri string) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	var obj Object
	var info os.FileInfo
	var err error
	if isS3(uri) {
		var bucket, key string
		bucket, key, err = s3URI(uri)
		if err != nil {
			return Object{}, err
		}
		if key == "" {
			return Object{}, errors.New("S3 object key is empty")
		}
		if err = s.budget.Reserve(sdkResponseCharge, "S3 HEAD response"); err != nil {
			return Object{}, err
		}
		result, headErr := s.input.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		s.budget.Release(sdkResponseCharge)
		if headErr != nil {
			return Object{}, fmt.Errorf("head S3 object: %w", headErr)
		}
		if result.ContentLength == nil || *result.ContentLength < 0 {
			return Object{}, errors.New("S3 object has no valid content length")
		}
		obj = Object{URI: objectURI(bucket, key), Size: *result.ContentLength, ETag: aws.ToString(result.ETag), VersionID: aws.ToString(result.VersionId), ModTime: aws.ToTime(result.LastModified)}
		err = validS3Identity(obj)
	} else {
		var p string
		p, err = localPath(uri)
		if err == nil {
			info, err = os.Stat(p)
		}
		if err == nil {
			if !info.Mode().IsRegular() {
				return Object{}, errors.New("input is not a regular file")
			}
			obj = Object{URI: p, Size: info.Size(), ModTime: info.ModTime(), ETag: fmt.Sprintf("\"%x-%x\"", info.Size(), info.ModTime().UnixNano())}
		}
	}
	if err != nil {
		return Object{}, err
	}
	obj.IdentityMethod = "local_stat"
	if isS3(obj.URI) {
		obj.IdentityMethod = "s3_etag"
		if obj.VersionID != "" && obj.VersionID != "null" {
			obj.IdentityMethod = "s3_version"
		}
	}
	obj.PayloadVerification = "full"
	obj.IngestionVerification = "full"
	snap, err := s.remember(obj, info)
	if err != nil {
		return Object{}, err
	}
	return snap.object, nil
}

func (s *Store) Discover(ctx context.Context, src config.Source) ([]Object, error) {
	if (len(src.URIs) > 0) == (src.Prefix != "") {
		return nil, errors.New("source requires exactly one of uris or prefix")
	}
	var objects []Object
	var capacityBytes int64
	// A URI list is deduplicated through a set sized once from the list, so
	// duplicate detection stays linear and still fails at the second occurrence.
	var seen map[string]struct{}
	if src.Prefix == "" {
		indexCharge := int64(len(src.URIs)) * discoveryIndexEntryCharge
		if err := s.budget.Reserve(indexCharge, "discovery duplicate index"); err != nil {
			return nil, err
		}
		defer s.budget.Release(indexCharge)
		seen = make(map[string]struct{}, len(src.URIs))
	}
	appendObject := func(uri string) error {
		obj, err := s.Stat(ctx, uri)
		if err != nil {
			return err
		}
		if seen != nil {
			if _, dup := seen[obj.URI]; dup {
				return fmt.Errorf("duplicate source: %s", obj.URI)
			}
			seen[obj.URI] = struct{}{}
		}
		if len(objects) == cap(objects) {
			capacity := max(8, cap(objects)*2)
			n := int64(capacity) * discoveryObjectCharge
			if err := s.budget.Reserve(n, "discovery object list"); err != nil {
				return err
			}
			next := make([]Object, len(objects), capacity)
			copy(next, objects)
			s.budget.Release(capacityBytes)
			objects = next
			capacityBytes = n
		}
		objects = append(objects, obj)
		return nil
	}
	var err error
	if len(src.URIs) > 0 {
		for _, uri := range src.URIs {
			if err = appendObject(uri); err != nil {
				break
			}
		}
	} else if src.Filter == "" {
		err = errors.New("prefix discovery requires a format filter")
	} else if isS3(src.Prefix) {
		err = s.discoverS3(ctx, src, appendObject)
	} else {
		var root string
		root, err = localPath(src.Prefix)
		if err == nil {
			err = s.walkLocal(ctx, root, src.Recursive, src.Filter, appendObject)
		}
	}
	if err != nil {
		s.budget.Release(capacityBytes)
		return nil, err
	}
	if src.Prefix != "" {
		sort.Slice(objects, func(i, j int) bool { return uriLess(objects[i].URI, objects[j].URI) })
		for i := 1; i < len(objects); i++ {
			if objects[i-1].URI == objects[i].URI {
				s.budget.Release(capacityBytes)
				return nil, fmt.Errorf("duplicate source: %s", objects[i].URI)
			}
		}
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.budget.Release(capacityBytes)
		return nil, os.ErrClosed
	}
	s.retained += capacityBytes
	s.mu.Unlock()
	var total int64
	for _, obj := range objects {
		total += obj.Size
	}
	source := src.Prefix
	if source == "" && len(src.URIs) == 1 {
		source = src.URIs[0]
	}
	s.logger.Debug("storage discovery complete", slog.String("phase", "discovery"), slog.Any("source", LogURI(source)), slog.Int("objects", len(objects)), slog.Int64("bytes", total))
	return objects, nil
}
func matchesFilter(name, filter string) bool {
	name = strings.ToLower(name)
	filter = strings.ToLower(filter)
	if strings.ContainsAny(filter, "*?[") {
		ok, _ := path.Match(filter, path.Base(name))
		return ok
	}
	filter = strings.TrimPrefix(filter, ".")
	return strings.HasSuffix(name, "."+filter) || (filter != "parquet" && strings.HasSuffix(name, "."+filter+".gz"))
}

// ReadDir batches avoid filepath.WalkDir's unbounded per-directory allocation.
func (s *Store) walkLocal(ctx context.Context, root string, recursive bool, filter string, add func(string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	const charge = directoryWalkCharge
	if err := s.budget.Reserve(charge, "directory discovery"); err != nil {
		return err
	}
	defer s.budget.Release(charge)
	dir, err := openLocalInput(root, true)
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(16)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			p := filepath.Join(root, entry.Name())
			if entry.IsDir() {
				if recursive {
					if err := s.walkLocal(ctx, p, true, filter, add); err != nil {
						return err
					}
				}
				continue
			}
			if entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			if matchesFilter(entry.Name(), filter) {
				if err := add(p); err != nil {
					return err
				}
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}
func (s *Store) Open(ctx context.Context, obj Object) (io.ReadCloser, error) {
	if isS3(obj.URI) {
		return s.openS3(ctx, obj, false, 0, obj.Size, false)
	}
	f, snap, err := s.pinnedFile(ctx, obj)
	if err != nil {
		return nil, err
	}
	if err = s.budget.Reserve(localReaderCharge, "local digest reader"); err != nil {
		f.Close()
		return nil, err
	}
	return &localReader{file: f, store: s, snap: snap, ctx: ctx, hash: sha256.New()}, nil
}

// ReadMode selects how an OpenAt reader verifies content and attributes bytes.
type ReadMode int

const (
	// ReadVerified reads with S3 read-ahead and re-verifies a full-content
	// digest at Close.
	ReadVerified ReadMode = iota
	// ReadSelective reads exact payload ranges without a whole-file hash. It is
	// valid only for explicitly disclosed partial verification.
	ReadSelective
	// ReadMetadata opens exact metadata ranges without read-ahead or a full
	// digest. Callers publishing selective results must subsequently VerifyIdentity.
	ReadMetadata
	// ReadMetadataVerified preserves the ordinary close-time full-content
	// baseline while keeping metadata range requests exact.
	ReadMetadataVerified
)

func (s *Store) OpenAt(ctx context.Context, obj Object, mode ReadMode) (ReadAtCloser, error) {
	verifyDigest := mode == ReadVerified || mode == ReadMetadataVerified
	metadata := mode == ReadMetadata || mode == ReadMetadataVerified
	if isS3(obj.URI) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validS3Identity(obj); err != nil {
			return nil, err
		}
		if err := s.budget.Reserve(s3RangeReaderCharge, "S3 range reader"); err != nil {
			return nil, err
		}
		return &s3ReaderAt{store: s, ctx: ctx, obj: obj, coalesce: verifyDigest && !metadata, metadata: metadata}, nil
	}
	reader, err := s.openLocalAt(ctx, obj, verifyDigest, metadata)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// CreateRunRoot claims a unique run directory. S3 has no directories and run
// uniqueness is enforced by conditional object creation, so it is a no-op there.
// Locally the parent is created as needed and the run directory exclusively.
func (s *Store) CreateRunRoot(ctx context.Context, uri string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if isS3(uri) {
		_, _, err := s3URI(uri)
		return err
	}
	root, err := localPath(uri)
	if err != nil {
		return err
	}
	if err = mkdirAllDurable(filepath.Dir(root), 0755); err != nil {
		return fmt.Errorf("create output root parent: %w", err)
	}
	if err = os.Mkdir(root, 0755); err != nil {
		return fmt.Errorf("create unique run directory: %w", err)
	}
	if err = syncDirectoryFn(filepath.Dir(root)); err != nil {
		return fmt.Errorf("sync output root parent: %w", err)
	}
	return nil
}
func (s *Store) Create(ctx context.Context, uri string) (Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isS3(uri) {
		return s.createS3(ctx, uri)
	}
	return s.createLocal(ctx, uri)
}

// Compare canonical S3 keys rather than their percent-escaped URI spelling.
func uriLess(a, b string) bool {
	if !isS3(a) || !isS3(b) {
		return a < b
	}
	next := func(s string, index *int) byte {
		value := s[*index]
		(*index)++
		if value == '%' && *index+1 < len(s) {
			// A malformed escape compares as the literal '%' byte.
			if v, err := strconv.ParseUint(s[*index:*index+2], 16, 8); err == nil {
				value = byte(v)
				*index += 2
			}
		}
		return value
	}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		x, y := next(a, &i), next(b, &j)
		if x != y {
			return x < y
		}
	}
	return i == len(a) && j < len(b)
}

// objectURI is the canonical s3:// spelling of bucket and key.
func objectURI(bucket, key string) string {
	return (&url.URL{Scheme: "s3", Host: bucket, Path: "/" + key}).String()
}

// LogURI returns a slog value for uri with userinfo, query and fragment
// removed, so signed URLs and embedded credentials never reach logs.
func LogURI(uri string) slog.LogValuer { return logURI(uri) }

type logURI string

func (u logURI) LogValue() slog.Value {
	s := string(u)
	if !strings.Contains(s, "://") {
		if i := strings.IndexAny(s, "?#"); i >= 0 {
			s = s[:i]
		}
		return slog.StringValue(s)
	}
	parsed, err := url.Parse(s)
	if err != nil {
		// Never echo an unparseable URI that may carry secrets.
		return slog.StringValue("<unparseable uri>")
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawFragment = ""
	return slog.StringValue(parsed.String())
}
