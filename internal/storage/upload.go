package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	uploadPartSize                   = 5 * 1024 * 1024
	preferredUploadPartSize          = 32 * 1024 * 1024
	maxConcurrentUploads             = 4
	uploadTaskCharge           int64 = 8 << 10 // worker stack, task, and completion channel
	partETagCharge             int64 = 1024
	writeIDMetadataKey               = "valet-write-id"
	writeHashMetadataKey             = "valet-sha256"
	reconciliationDigestBuffer       = 32 << 10
)

type uploadTask struct {
	done   chan struct{}
	buffer []byte
	index  int
	etag   string
	err    error
}

type s3Writer struct {
	store                   *Store
	ctx                     context.Context
	cancel                  context.CancelFunc
	bucket, key             string
	uploadID, writeID       string
	digest                  hash.Hash
	buffer                  []byte
	spare                   []byte
	partSize                int
	pending                 [maxConcurrentUploads]*uploadTask
	pendingCount            int
	parts                   []types.CompletedPart
	partsCharge, etagCharge int64
	responseCharge          int64
	size                    int64
	completed               [32]byte
	published               bool
	done                    bool
	err                     error
}

func (s *Store) createS3(ctx context.Context, uri string) (Writer, error) {
	bucket, key, err := s3URI(uri)
	if err != nil {
		return nil, err
	}
	if key == "" {
		return nil, errors.New("S3 output key is empty")
	}
	writeID, err := newS3WriteID()
	if err != nil {
		return nil, err
	}
	child, cancel := context.WithCancel(ctx)
	w := &s3Writer{store: s, ctx: child, cancel: cancel, bucket: bucket, key: key, writeID: writeID, digest: sha256.New()}
	if err = s.track(w); err != nil {
		cancel()
		return nil, err
	}
	return w, nil
}
func (w *s3Writer) reserveResponse() error {
	if w.responseCharge != 0 {
		return nil
	}
	// Keep one response workspace until finalization. It is reused by owner
	// requests and guarantees abort capacity even after budget exhaustion.
	if err := w.store.reserve(sdkResponseCharge, "S3 upload and cleanup response"); err != nil {
		return err
	}
	w.responseCharge = sdkResponseCharge
	return nil
}
func (w *s3Writer) growBuffer() error {
	if w.partSize == 0 {
		available := w.store.budget.Limit() - w.store.budget.Used()
		w.partSize = uploadPartSize
		if available >= 256<<20 {
			w.partSize = preferredUploadPartSize
		} else if available >= 96<<20 {
			w.partSize = preferredUploadPartSize / 2
		}
	}
	capacity := min(w.partSize, max(64<<10, cap(w.buffer)*2))
	if capacity >= w.partSize/2 {
		capacity = w.partSize
	}
	if cap(w.buffer) == 0 && w.partSize == uploadPartSize {
		available := w.store.budget.Limit() - w.store.budget.Used()
		// A legal serial part can fit even when growing a small staging
		// buffer into it cannot: both allocations must be charged during
		// the copy. Choose the full legal buffer before accepting bytes.
		if available >= uploadPartSize && available < 2*uploadPartSize {
			capacity = uploadPartSize
		}
	}
	// Geometry is only a preference until the first multipart request. If
	// competing managed state grew since the first small Write, use a legal
	// smaller part rather than requiring the originally preferred buffer.
	if len(w.parts) == 0 && int64(capacity) > w.store.budget.Limit()-w.store.budget.Used() && capacity > uploadPartSize {
		if cap(w.buffer) >= uploadPartSize {
			w.partSize = cap(w.buffer)
			return w.flush()
		}
		w.partSize, capacity = uploadPartSize, uploadPartSize
	}
	charge := int64(capacity)
	if err := w.store.reserve(charge, "S3 upload buffer"); err != nil {
		return err
	}
	next := make([]byte, len(w.buffer), capacity)
	copy(next, w.buffer)
	w.store.budget.Release(int64(cap(w.buffer)))
	w.buffer = next
	return nil
}
func (w *s3Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, os.ErrClosed
	}
	if w.err != nil {
		return 0, w.err
	}
	if err := w.ctx.Err(); err != nil {
		return 0, w.fail(err)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := w.collect(false); err != nil {
		return 0, w.fail(err)
	}
	if err := w.reserveResponse(); err != nil {
		return 0, w.fail(err)
	}
	written := 0
	for len(p) > 0 {
		if err := w.ctx.Err(); err != nil {
			return written, w.fail(err)
		}
		if len(w.buffer) == cap(w.buffer) {
			var err error
			if cap(w.buffer) == w.partSize && w.partSize != 0 {
				err = w.flush()
			} else {
				err = w.growBuffer()
			}
			if err != nil {
				return written, w.fail(err)
			}
		}
		n := min(len(p), cap(w.buffer)-len(w.buffer))
		w.buffer = append(w.buffer, p[:n]...)
		_, _ = w.digest.Write(p[:n])
		p = p[n:]
		written += n
		w.size += int64(n)
	}
	return written, nil
}
func (w *s3Writer) preparePart() (int, error) {
	if len(w.parts) >= 10000 {
		return 0, errors.New("S3 multipart object exceeds 10000 bounded parts; reduce output part size")
	}
	required := partETagCharge
	if len(w.parts) == cap(w.parts) {
		required += int64(min(10000, max(8, cap(w.parts)*2))) * completedPartCharge
	}
	if required > w.store.budget.Limit()-w.store.budget.Used() {
		w.store.budget.Release(int64(cap(w.spare)))
		w.spare = nil
	}
	if len(w.parts) == cap(w.parts) {
		capacity := min(10000, max(8, cap(w.parts)*2))
		charge := int64(capacity) * completedPartCharge
		if err := w.store.reserve(charge, "S3 completed part metadata"); err != nil {
			return 0, err
		}
		parts := make([]types.CompletedPart, len(w.parts), capacity)
		copy(parts, w.parts)
		w.store.budget.Release(w.partsCharge)
		w.parts = parts
		w.partsCharge = charge
	}
	if err := w.store.reserve(partETagCharge, "S3 part ETag"); err != nil {
		return 0, err
	}
	w.etagCharge += partETagCharge
	index := len(w.parts)
	w.parts = append(w.parts, types.CompletedPart{PartNumber: aws.Int32(int32(index + 1))})
	return index, nil
}
func (w *s3Writer) startUpload() error {
	if w.uploadID != "" {
		return nil
	}
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(w.bucket), Key: aws.String(w.key),
		Metadata: map[string]string{writeIDMetadataKey: w.writeID},
	}
	input.ServerSideEncryption, input.SSEKMSKeyId = w.store.outputSSE()
	response, err := w.store.output.CreateMultipartUpload(w.ctx, input, func(options *s3.Options) {
		options.Retryer = aws.NopRetryer{}
	})
	if err != nil {
		cleanupErr := w.store.abortMultipartUploadsForKeyReserved(w.ctx, w.bucket, w.key)
		return errors.Join(fmt.Errorf("create S3 multipart upload: %w", err), cleanupErr)
	}
	if response.UploadId == nil || *response.UploadId == "" {
		cleanupErr := w.store.abortMultipartUploadsForKeyReserved(w.ctx, w.bucket, w.key)
		return errors.Join(errors.New("S3 returned no multipart upload ID"), cleanupErr)
	}
	w.uploadID = *response.UploadId
	if len(w.uploadID) > 16384 {
		return errors.New("S3 upload ID exceeds bound")
	}
	if err := w.store.reserve(int64(len(w.uploadID)), "S3 upload identity"); err != nil {
		return err
	}
	w.etagCharge += int64(len(w.uploadID))
	w.uploadID = strings.Clone(w.uploadID)
	return nil
}
func (w *s3Writer) upload(buffer []byte, index int) (string, error) {
	response, err := w.store.output.UploadPart(w.ctx, &s3.UploadPartInput{
		Bucket: aws.String(w.bucket), Key: aws.String(w.key), UploadId: aws.String(w.uploadID),
		PartNumber: aws.Int32(int32(index + 1)), ContentLength: aws.Int64(int64(len(buffer))),
		Body: bytes.NewReader(buffer),
	})
	if err != nil {
		return "", fmt.Errorf("upload S3 part: %w", err)
	}
	etag := aws.ToString(response.ETag)
	if etag == "" || len(etag) > int(partETagCharge) {
		return "", errors.New("S3 returned an invalid part ETag")
	}
	return strings.Clone(etag), nil
}
func (w *s3Writer) collect(all bool) error {
	var first error
	for w.pendingCount != 0 {
		task := w.pending[0]
		if all {
			<-task.done
		} else {
			select {
			case <-task.done:
			default:
				return first
			}
		}
		if task.err != nil {
			if first == nil || errors.Is(first, context.Canceled) && !errors.Is(task.err, context.Canceled) {
				first = task.err
			}
			w.cancel()
			all = true
		}
		if task.err == nil {
			w.parts[task.index].ETag = aws.String(task.etag)
		}
		charge := int64(sdkResponseCharge) + uploadTaskCharge
		if w.spare == nil {
			w.spare = task.buffer[:0]
		} else {
			charge += int64(cap(task.buffer))
		}
		w.store.budget.Release(charge)
		copy(w.pending[:], w.pending[1:w.pendingCount])
		w.pendingCount--
		w.pending[w.pendingCount] = nil
	}
	return first
}
func (w *s3Writer) flush() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if err := w.collect(false); err != nil {
		return err
	}
	if w.pendingCount == len(w.pending) {
		// Join in part-number order, not scheduler completion order.
		task := w.pending[0]
		<-task.done
		if err := w.collect(false); err != nil {
			return err
		}
	}
	index, err := w.preparePart()
	if err != nil {
		return err
	}
	if err = w.startUpload(); err != nil {
		return err
	}
	// Reserve the next staging buffer, task, and response before starting a
	// worker. Under low budgets reuse the owner's response slot synchronously.
	charge := int64(sdkResponseCharge) + uploadTaskCharge
	if w.spare == nil {
		charge += int64(w.partSize)
	}
	if w.pendingCount < len(w.pending) && w.store.reserve(charge, "S3 concurrent upload") == nil {
		task := &uploadTask{done: make(chan struct{}), buffer: w.buffer, index: index}
		if w.spare != nil {
			w.buffer, w.spare = w.spare, nil
		} else {
			w.buffer = make([]byte, 0, w.partSize)
		}
		w.pending[w.pendingCount] = task
		w.pendingCount++
		go func() {
			task.etag, task.err = w.upload(task.buffer, task.index)
			if task.err != nil {
				w.cancel()
			}
			close(task.done)
		}()
		return nil
	}
	etag, err := w.upload(w.buffer, index)
	if err != nil {
		return err
	}
	w.parts[index].ETag = aws.String(etag)
	w.buffer = w.buffer[:0]
	return nil
}
func (w *s3Writer) releaseBuffer() {
	w.store.budget.Release(int64(cap(w.buffer)))
	w.buffer = nil
	w.store.budget.Release(int64(cap(w.spare)))
	w.spare = nil
}
func (w *s3Writer) release() {
	w.releaseBuffer()
	w.store.budget.Release(w.partsCharge + w.etagCharge + w.responseCharge)
	w.partsCharge, w.etagCharge, w.responseCharge = 0, 0, 0
	w.parts = nil
	w.done = true
	w.cancel()
	w.store.untrack(w)
}
func (w *s3Writer) abortUpload() error {
	if w.uploadID == "" {
		return nil
	}
	// All workers have joined before cleanup, so its reserved response workspace
	// cannot be in use and no late UploadPart can race the abort.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 10*time.Second)
	defer cancel()
	_, err := w.store.output.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(w.bucket), Key: aws.String(w.key), UploadId: aws.String(w.uploadID)})
	w.uploadID = ""
	w.store.logger.Warn("S3 multipart upload aborted", slog.Any("uri", LogURI(objectURI(w.bucket, w.key))), errAttr(err))
	if err != nil {
		return fmt.Errorf("abort S3 multipart upload: %w", err)
	}
	return nil
}
func (w *s3Writer) fail(err error) error {
	w.cancel()
	uploadErr := w.collect(true)
	if errors.Is(err, context.Canceled) && uploadErr != nil && !errors.Is(uploadErr, context.Canceled) {
		err = uploadErr
	}
	w.releaseBuffer()
	w.err = errors.Join(err, w.abortUpload())
	w.release()
	return w.err
}
func (w *s3Writer) Close() error {
	if w.done {
		return w.err
	}
	if err := w.ctx.Err(); err != nil {
		return w.fail(err)
	}
	if err := w.reserveResponse(); err != nil {
		return w.fail(err)
	}
	if w.uploadID == "" {
		if err := w.store.putS3ObjectReserved(w.ctx, w.bucket, w.key, w.buffer, w.writeID, w.objectDigest()); err != nil {
			return w.fail(err)
		}
	} else {
		if len(w.buffer) > 0 {
			if err := w.flush(); err != nil {
				return w.fail(err)
			}
		}
		if err := w.collect(true); err != nil {
			return w.fail(err)
		}
		w.releaseBuffer()
		// SDK completion XML grows with part count; account encoding and growth copies.
		xmlCharge := int64(completionXMLBase+len(w.parts)*completionXMLPerPart) + 8*w.etagCharge
		if err := w.store.reserve(xmlCharge, "S3 completion XML"); err != nil {
			return w.fail(err)
		}
		_, err := w.store.output.CompleteMultipartUpload(w.ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(w.bucket), Key: aws.String(w.key), UploadId: aws.String(w.uploadID), IfNoneMatch: aws.String("*"), MultipartUpload: &types.CompletedMultipartUpload{Parts: w.parts}})
		w.store.budget.Release(xmlCharge)
		if err != nil {
			reconciled, reconcileErr := w.store.reconcileS3ObjectReserved(w.ctx, w.bucket, w.key, w.writeID, w.objectDigest(), w.size)
			if !reconciled {
				return w.fail(errors.Join(fmt.Errorf("complete S3 multipart upload: %w", err), reconcileErr))
			}
		}
		w.uploadID = ""
	}
	w.digest.Sum(w.completed[:0])
	w.published = true
	w.store.logger.Debug("object uploaded", slog.Any("uri", LogURI(objectURI(w.bucket, w.key))), slog.Int64("bytes", w.size), slog.Int("parts", len(w.parts)))
	w.release()
	return nil
}
func (w *s3Writer) Abort() error {
	if w.done {
		return nil
	}
	w.cancel()
	_ = w.collect(true)
	w.releaseBuffer()
	err := w.abortUpload()
	w.release()
	return err
}

func (w *s3Writer) CompletedDigest() (int64, [32]byte, bool) {
	return w.size, w.completed, w.published
}

func (w *s3Writer) objectDigest() string {
	var sum [32]byte
	return hex.EncodeToString(w.digest.Sum(sum[:0]))
}

func (s *Store) putS3Object(ctx context.Context, bucket, key string, body []byte, writeID, digest string) error {
	if err := s.reserve(sdkResponseCharge, "S3 PUT response"); err != nil {
		return err
	}
	defer s.budget.Release(sdkResponseCharge)
	return s.putS3ObjectReserved(ctx, bucket, key, body, writeID, digest)
}

func (s *Store) putS3ObjectReserved(ctx context.Context, bucket, key string, body []byte, writeID, digest string) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: aws.String("*"),
		Metadata: map[string]string{writeIDMetadataKey: writeID, writeHashMetadataKey: digest},
	}
	input.ServerSideEncryption, input.SSEKMSKeyId = s.outputSSE()
	if _, err := s.output.PutObject(ctx, input); err != nil {
		reconciled, reconcileErr := s.reconcileS3ObjectReserved(ctx, bucket, key, writeID, digest, int64(len(body)))
		if reconciled {
			return nil
		}
		return errors.Join(fmt.Errorf("put S3 object: %w", err), reconcileErr)
	}
	return nil
}

func newS3WriteID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate S3 write identity: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func s3BodyDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (s *Store) reconcileS3ObjectReserved(parent context.Context, bucket, key, writeID, digest string, size int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer cancel()
	head, err := s.output.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return false, fmt.Errorf("reconcile S3 object: %w", err)
	}
	if head.ContentLength == nil || *head.ContentLength != size || head.Metadata[writeIDMetadataKey] != writeID {
		return false, errors.New("S3 object exists with a different write identity")
	}
	if digest == "" || head.Metadata[writeHashMetadataKey] == digest {
		return true, nil
	}
	if err := s.reserve(reconciliationDigestBuffer, "S3 reconciliation digest"); err != nil {
		return false, err
	}
	defer s.budget.Release(reconciliationDigestBuffer)
	get, err := s.output.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), IfMatch: head.ETag,
	})
	if err != nil {
		return false, fmt.Errorf("read S3 object for reconciliation: %w", err)
	}
	hasher := sha256.New()
	buffer := make([]byte, reconciliationDigestBuffer)
	_, copyErr := io.CopyBuffer(hasher, get.Body, buffer)
	closeErr := get.Body.Close()
	if copyErr != nil || closeErr != nil {
		return false, errors.Join(copyErr, closeErr)
	}
	if hex.EncodeToString(hasher.Sum(nil)) != digest {
		return false, errors.New("S3 object exists with a different content digest")
	}
	return true, nil
}

// abortMultipartUploadsForKeyReserved reuses the writer's held response
// reservation, so ambiguous cleanup cannot fail while reserving another
// response buffer after the non-idempotent create request.
func (s *Store) abortMultipartUploadsForKeyReserved(parent context.Context, bucket, key string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
	defer cancel()
	var keyMarker, uploadMarker *string
	for {
		page, err := s.output.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket: aws.String(bucket), Prefix: aws.String(key), MaxUploads: aws.Int32(1000),
			KeyMarker: keyMarker, UploadIdMarker: uploadMarker,
		})
		if err != nil {
			return fmt.Errorf("list ambiguous S3 multipart uploads: %w", err)
		}
		for _, upload := range page.Uploads {
			if aws.ToString(upload.Key) != key || aws.ToString(upload.UploadId) == "" {
				continue
			}
			if _, err = s.output.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: upload.UploadId,
			}); err != nil {
				return fmt.Errorf("abort ambiguous S3 multipart upload: %w", err)
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return nil
		}
		nextKey, nextUpload := aws.ToString(page.NextKeyMarker), aws.ToString(page.NextUploadIdMarker)
		if nextKey == "" || nextKey == aws.ToString(keyMarker) && nextUpload == aws.ToString(uploadMarker) {
			return errors.New("S3 multipart reconciliation listing did not advance")
		}
		keyMarker, uploadMarker = aws.String(nextKey), aws.String(nextUpload)
	}
}

// outputSSE returns the configured output server-side encryption settings in
// the form shared by multipart and single-object uploads.
func (s *Store) outputSSE() (types.ServerSideEncryption, *string) {
	var key *string
	if s.outputSettings.KMSKeyID != "" {
		key = aws.String(s.outputSettings.KMSKeyID)
	}
	return types.ServerSideEncryption(s.outputSettings.SSE), key
}
