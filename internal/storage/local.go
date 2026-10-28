package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/richardrh/valet/internal/diagnostics"
)

func changed(uri string) error { return fmt.Errorf("source identity changed: %s", uri) }
func checkedLocalInput(file *os.File, directory bool) (*os.File, error) {
	info, err := file.Stat()
	if err == nil && ((directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular())) {
		err = &os.PathError{Op: "open", Path: file.Name(), Err: errors.New("input has an unsupported file type")}
	}
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}
func (s *Store) pinnedFile(ctx context.Context, obj Object) (*os.File, *snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	p, err := localPath(obj.URI)
	if err != nil {
		return nil, nil, err
	}
	f, err := openLocalInput(p, false)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != obj.Size || !info.ModTime().Equal(obj.ModTime) {
		f.Close()
		return nil, nil, changed(obj.URI)
	}
	snap, err := s.remember(obj, info)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if !os.SameFile(snap.info, info) {
		f.Close()
		return nil, nil, changed(obj.URI)
	}
	return f, snap, nil
}
func verifyFile(f *os.File, snap *snapshot) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != snap.object.Size || !info.ModTime().Equal(snap.object.ModTime) || !os.SameFile(snap.info, info) {
		return changed(snap.object.URI)
	}
	// Also detect replacement of the path while the old file descriptor is open.
	current, err := os.Stat(snap.object.URI)
	if err != nil {
		return err
	}
	if !os.SameFile(current, info) {
		return changed(snap.object.URI)
	}
	return nil
}

var syncDirectoryFn = syncDirectory

// SyncParentDirectory makes a completed local path publication durable.
func SyncParentDirectory(path string) error {
	return syncDirectoryFn(filepath.Dir(path))
}

func mkdirAllDurable(name string, mode os.FileMode) error {
	name = filepath.Clean(name)
	var missing []string
	for current := name; ; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return &os.PathError{Op: "mkdir", Path: current, Err: errors.New("path component is not a directory")}
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return err
		}
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], mode); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := syncDirectoryFn(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) checkDigest(snap *snapshot, digest []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.digested {
		var sum [32]byte
		copy(sum[:], digest)
		if sum != snap.digest {
			return changed(snap.object.URI)
		}
	} else {
		copy(snap.digest[:], digest)
		snap.digested = true
	}
	return nil
}

type localReader struct {
	file      *os.File
	store     *Store
	snap      *snapshot
	ctx       context.Context
	hash      hash.Hash
	read      int64
	finalized bool
	closed    bool
	err       error
}

func (r *localReader) finish() error {
	if r.finalized {
		return r.err
	}
	r.finalized = true
	if r.read != r.snap.object.Size {
		r.err = changed(r.snap.object.URI)
		return r.err
	}
	r.err = verifyFile(r.file, r.snap)
	if r.err == nil {
		var digest [32]byte
		r.err = r.store.checkDigest(r.snap, r.hash.Sum(digest[:0]))
	}
	return r.err
}
func (r *localReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, os.ErrClosed
	}
	if r.err != nil {
		return 0, r.err
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	span := diagnostics.Start(r.ctx, diagnostics.InputHash)
	n, err := r.file.Read(p)
	recordBackendRead(r.ctx, n, false)
	r.read += int64(n)
	_, _ = r.hash.Write(p[:n])
	diagnostics.AddBytes(r.ctx, diagnostics.InputHash, uint64(n))
	if errors.Is(err, io.EOF) {
		span.End(nil)
	} else {
		span.End(err)
	}
	if r.read > r.snap.object.Size {
		r.err = changed(r.snap.object.URI)
		return n, r.err
	}
	if err == io.EOF {
		if e := r.finish(); e != nil {
			return n, e
		}
	}
	return n, err
}
func (r *localReader) Close() error {
	if r.closed {
		return r.err
	}
	r.closed = true
	if r.read == r.snap.object.Size {
		r.err = errors.Join(r.err, r.finish())
	} else {
		r.err = errors.Join(r.err, verifyFile(r.file, r.snap))
	}
	r.err = errors.Join(r.err, r.file.Close())
	r.store.budget.Release(localReaderCharge)
	return r.err
}

type localReaderAt struct {
	mu           sync.RWMutex
	file         *os.File
	store        *Store
	snap         *snapshot
	ctx          context.Context
	verifyDigest bool
	metadata     bool
	closed       bool
	err          error
}

func (s *Store) openLocalAt(ctx context.Context, obj Object, verifyDigest, metadata bool) (*localReaderAt, error) {
	f, snap, err := s.pinnedFile(ctx, obj)
	if err != nil {
		return nil, err
	}
	if err = s.budget.Reserve(localReaderCharge, "local range reader"); err != nil {
		f.Close()
		return nil, err
	}
	return &localReaderAt{file: f, store: s, snap: snap, ctx: ctx, verifyDigest: verifyDigest, metadata: metadata}, nil
}
func (r *localReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	// Identity is pinned at open and re-verified with a full digest at Close;
	// per-call stats would double the syscalls of Parquet's many small reads.
	n, err := r.file.ReadAt(p, offset)
	recordBackendRead(r.ctx, n, r.metadata)
	return n, err
}
func (r *localReaderAt) digest() (err error) {
	span := diagnostics.Start(r.ctx, diagnostics.InputHash)
	defer func() { span.End(err) }()
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if err := verifyFile(r.file, r.snap); err != nil {
		return err
	}
	const size = localDigestBuffer
	if err := r.store.budget.Reserve(size, "local range replay digest"); err != nil {
		return err
	}
	defer r.store.budget.Release(size)
	buffer := make([]byte, size)
	h := sha256.New()
	r.store.mu.Lock()
	parquetMetadata, footerOffset := r.snap.parquetMetadata, r.snap.footerOffset
	r.store.mu.Unlock()
	var offset int64
	for offset < r.snap.object.Size {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		n, err := r.file.ReadAt(buffer[:min(int64(len(buffer)), r.snap.object.Size-offset)], offset)
		if n > 0 {
			var metadataBytes int64
			if parquetMetadata {
				end := offset + int64(n)
				metadataBytes = max(0, min(end, 4)-offset) + max(0, end-max(offset, footerOffset))
			}
			diagnostics.AddCounter(r.ctx, diagnostics.SourceMetadataBytesRead, uint64(metadataBytes))
			diagnostics.AddCounter(r.ctx, diagnostics.SourcePayloadBytesRead, uint64(int64(n)-metadataBytes))
			diagnostics.AddBytes(r.ctx, diagnostics.InputHash, uint64(n))
		}
		if n > 0 {
			_, _ = h.Write(buffer[:n])
			offset += int64(n)
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	if err := verifyFile(r.file, r.snap); err != nil {
		return err
	}
	var digest [32]byte
	return r.store.checkDigest(r.snap, h.Sum(digest[:0]))
}
func (r *localReaderAt) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.err
	}
	r.closed = true
	if r.verifyDigest {
		r.err = r.digest()
	} else {
		r.err = verifyFile(r.file, r.snap)
	}
	r.err = errors.Join(r.err, r.file.Close())
	r.store.budget.Release(localReaderCharge)
	return r.err
}

const localWriteBuffer = 64 << 10

type localOutput interface {
	io.Writer
	Sync() error
	Close() error
}

type localWriter struct {
	store        *Store
	ctx          context.Context
	file         localOutput
	target, temp string
	size         int64
	buffer       []byte
	done         bool
	err          error
}

func (s *Store) createLocal(ctx context.Context, uri string) (Writer, error) {
	target, err := localPath(uri)
	if err != nil {
		return nil, err
	}
	if _, err = os.Lstat(target); err == nil {
		return nil, fmt.Errorf("output already exists: %s", target)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err = mkdirAllDurable(filepath.Dir(target), 0755); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(target), ".valet-*")
	if err != nil {
		return nil, err
	}
	w := &localWriter{store: s, ctx: ctx, file: f, target: target, temp: f.Name()}
	if err = s.track(w); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return w, nil
}
func (w *localWriter) Write(p []byte) (int, error) {
	if w.done {
		if w.err == ErrAborted {
			return 0, ErrAborted
		}
		return 0, os.ErrClosed
	}
	if w.err != nil {
		return 0, w.err
	}
	if err := w.ctx.Err(); err != nil {
		w.err = err
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if w.buffer == nil && len(p) < localWriteBuffer {
		// Buffering is optional: small budgets still support direct writes.
		capacity := min(int64(localWriteBuffer), (w.store.budget.Limit()-w.store.budget.Used())/4)
		if capacity >= 4096 && w.store.budget.Reserve(capacity, "local output buffer") == nil {
			w.buffer = make([]byte, 0, int(capacity))
		}
	}
	written := 0
	for len(p) != 0 {
		if len(w.buffer) == 0 && (cap(w.buffer) == 0 || len(p) >= cap(w.buffer)) {
			n, err := w.file.Write(p)
			if err == nil && n != len(p) {
				err = io.ErrShortWrite
			}
			w.size += int64(n)
			w.err = err
			return written + n, err
		}
		if len(w.buffer) == cap(w.buffer) {
			if err := w.flush(); err != nil {
				return written, err
			}
		}
		n := min(len(p), cap(w.buffer)-len(w.buffer))
		w.buffer = append(w.buffer, p[:n]...)
		w.size += int64(n)
		written += n
		p = p[n:]
	}
	return written, nil
}
func (w *localWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}
	n, err := w.file.Write(w.buffer)
	if err == nil && n != len(w.buffer) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
		return err
	}
	w.buffer = w.buffer[:0]
	return nil
}
func (w *localWriter) releaseBuffer() {
	w.store.budget.Release(int64(cap(w.buffer)))
	w.buffer = nil
}
func (w *localWriter) Close() error {
	if w.done {
		return w.err
	}
	published := false
	if w.err == nil {
		w.err = w.ctx.Err()
	}
	if w.err == nil {
		w.err = w.flush()
	}
	if w.err == nil {
		w.err = w.file.Sync()
	}
	w.err = errors.Join(w.err, w.file.Close())
	// Cancellation during Sync/Close must be observed before the commit boundary.
	if w.err == nil {
		w.err = w.ctx.Err()
	}
	if w.err == nil {
		w.err = publishLocalNoReplace(w.temp, w.target)
		published = w.err == nil
	}
	if w.err == nil {
		w.err = syncDirectoryFn(filepath.Dir(w.target))
	}
	if w.err != nil {
		if published {
			rollbackErr := os.Remove(w.target)
			if rollbackErr == nil {
				rollbackErr = syncDirectoryFn(filepath.Dir(w.target))
			}
			w.err = errors.Join(w.err, rollbackErr)
		} else {
			w.err = errors.Join(w.err, os.Remove(w.temp))
		}
	} else {
		w.store.logger.Debug("object published", slog.Any("uri", LogURI(w.target)), slog.Int64("bytes", w.size))
	}
	w.releaseBuffer()
	w.done = true
	w.store.untrack(w)
	return w.err
}
func (w *localWriter) Abort() error {
	if w.done {
		return nil
	}
	w.done = true
	w.err = ErrAborted
	err := errors.Join(w.file.Close(), os.Remove(w.temp))
	w.releaseBuffer()
	w.store.untrack(w)
	return err
}
