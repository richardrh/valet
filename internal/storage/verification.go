package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/richardrh/valet/internal/diagnostics"
)

type metadataReadKey struct{}

func metadataReadContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, metadataReadKey{}, true)
}
func isMetadataRead(ctx context.Context) bool { v, _ := ctx.Value(metadataReadKey{}).(bool); return v }
func recordBackendRead(ctx context.Context, n int) {
	if n <= 0 {
		return
	}
	counter := diagnostics.SourcePayloadBytesRead
	phase := diagnostics.Decode
	if isMetadataRead(ctx) {
		counter = diagnostics.SourceMetadataBytesRead
		phase = diagnostics.Metadata
	}
	diagnostics.AddCounter(ctx, counter, uint64(n))
	diagnostics.AddBytes(ctx, phase, uint64(n))
}

// SetParquetMetadataRange records already-guarded header/footer locations for
// classifying later full-file hash reads. It performs no additional I/O.
func (s *Store) SetParquetMetadataRange(obj Object, footerOffset int64) error {
	if obj.Size < 12 || footerOffset < 4 || footerOffset > obj.Size-8 {
		return errors.New("invalid Parquet metadata byte range")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.snapshots[obj.URI]
	if snap == nil || snap.object.Size != obj.Size || snap.object.ETag != obj.ETag || snap.object.VersionID != obj.VersionID || !snap.object.ModTime.Equal(obj.ModTime) {
		return changed(obj.URI)
	}
	if snap.parquetMetadata && snap.footerOffset != footerOffset {
		return changed(obj.URI)
	}
	snap.parquetMetadata, snap.footerOffset = true, footerOffset
	return nil
}

func parquetMetadataBytes(offset, length, footerOffset int64) int64 {
	end := offset + length
	header := max(int64(0), min(end, int64(4))-offset)
	footer := max(int64(0), end-max(offset, footerOffset))
	return header + footer
}

// VerifyIdentity revalidates the pinned source without reading payload. Immutable
// S3 versions are checked by version, not against whichever version is current.
func (s *Store) VerifyIdentity(ctx context.Context, obj Object) (err error) {
	span := diagnostics.Start(ctx, diagnostics.Metadata)
	defer func() { span.End(err) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !isS3(obj.URI) {
		f, snap, err := s.pinnedFile(ctx, obj)
		if err != nil {
			return err
		}
		return errors.Join(verifyFile(f, snap), f.Close())
	}
	if err := validS3Identity(obj); err != nil {
		return err
	}
	bucket, key, err := s3URI(obj.URI)
	if err != nil {
		return err
	}
	if err = s.reserve(sdkResponseCharge, "S3 final identity response"); err != nil {
		return err
	}
	defer s.budget.Release(sdkResponseCharge)
	input := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if obj.VersionID != "" {
		input.VersionId = aws.String(obj.VersionID)
	}
	if obj.ETag != "" {
		input.IfMatch = aws.String(obj.ETag)
	}
	result, err := s.input.HeadObject(ctx, input)
	if err != nil {
		return fmt.Errorf("revalidate pinned S3 object: %w", err)
	}
	if result.ContentLength == nil || *result.ContentLength != obj.Size || (obj.ETag != "" && aws.ToString(result.ETag) != obj.ETag) || (obj.VersionID != "" && aws.ToString(result.VersionId) != obj.VersionID) || (!obj.ModTime.IsZero() && !aws.ToTime(result.LastModified).Equal(obj.ModTime)) {
		return changed(obj.URI)
	}
	return nil
}

// EstablishBaseline records or checks the ordinary local full-content digest.
// Selective callers must not invoke this for an object with skipped payload.
func (s *Store) EstablishBaseline(ctx context.Context, obj Object) error {
	if isS3(obj.URI) {
		return s.VerifyIdentity(ctx, obj)
	}
	reader, err := s.openLocalAt(ctx, obj, true)
	if err != nil {
		return err
	}
	return reader.Close()
}
