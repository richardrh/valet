//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func vmIOPipeline(t testing.TB, root string, sources ...string) *pipeline {
	t.Helper()
	p := defaultPipeline(sources[0], "parquet", filepath.Join(root, "output"), "csv")
	p.Source.URIs = sources
	return p.with(t, vmStatConfig, "input: {parquet: {statistics: trusted}}\noutput: {mode: report}\n")
}

func vmIOFailure(t testing.TB, r *runResult) {
	t.Helper()
	if r.code != exitOperational || len(r.manifests) != 0 {
		t.Fatalf("operational failure must not commit: exit=%d manifests=%v stderr=%s", r.code, r.manifests, r.stderr)
	}
	if len(bytes.TrimSpace(r.stderr)) == 0 {
		t.Fatal("operational failure has no diagnostic")
	}
}

func vmIONumber(t testing.TB, v any) uint64 {
	t.Helper()
	s := fmt.Sprint(v)
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("expected unsigned JSON integer, got %#v: %v", v, err)
	}
	return n
}

func vmIOCoverage(t testing.TB, r *runResult, values, claimed, scanned, skipped uint64) {
	t.Helper()
	if len(r.manifests) != 1 {
		t.Fatalf("missing completed result: exit=%d stderr=%s", r.code, r.stderr)
	}
	vmAssertArtifacts(t, r)
	var s map[string]any
	vmJSON(t, r.stdout, &s)
	for _, report := range []map[string]any{s, r.manifest} {
		c, ok := report["coverage"].(map[string]any)
		if !ok {
			t.Fatalf("missing coverage: %#v", report)
		}
		for key, want := range map[string]uint64{"value_rows": values, "metadata_claimed_rows": claimed, "accounted_rows": values + claimed, "row_groups_scanned": scanned, "row_groups_statistics_only": skipped, "row_groups_total": scanned + skipped} {
			if got := vmIONumber(t, c[key]); got != want {
				t.Errorf("coverage.%s=%d, want %d", key, got, want)
			}
		}
		if report["trust_policy"] != "producer_statistics" {
			t.Errorf("trust policy=%v, want producer_statistics", report["trust_policy"])
		}
	}
	if r.summary.Total != values {
		t.Errorf("decoded total=%d, want %d", r.summary.Total, values)
	}
	for _, code := range []string{"POSITIVE", "NOT_NULL"} {
		got, ok := r.summary.RuleCoverage[code]
		if !ok || got.ValueRows != values || got.MetadataClaimedRows != claimed {
			t.Errorf("rule coverage %s=%+v, want value=%d metadata=%d", code, got, values, claimed)
		}
	}
	wantVerification := "values"
	if claimed > 0 {
		wantVerification = "metadata_assisted"
	}
	if r.summary.Verification != wantVerification {
		t.Errorf("verification=%q, want %q", r.summary.Verification, wantVerification)
	}
	wantIngestion := "full"
	if claimed > 0 {
		wantIngestion = "partial"
		if values == 0 {
			wantIngestion = "not_evaluated"
		}
	}
	if r.summary.IngestionVerification != wantIngestion {
		t.Errorf("ingestion=%q, want %q", r.summary.IngestionVerification, wantIngestion)
	}
}

// The child is started before a barrier is awaited. Only the test goroutine
// mutates fixtures and reports failures; timeout never masquerades as exit 2.
func vmIOBarrierRun(t *testing.T, root string, p *pipeline, barrier *vmBarrier, mutate func()) *runResult {
	t.Helper()
	path := p.write(t, filepath.Join(root, "pipeline.yml"))
	base, baseCancel := callContext(t)
	defer baseCancel()
	ctx, cancel := context.WithTimeout(base, 45*time.Second)
	cmd := exec.CommandContext(ctx, binaryPath, "validate", "-p", path, "--json-output", "-", "--log-format", "json", "--log-level", "error")
	cmd.WaitDelay = 5 * time.Second
	defer cancel()
	defer barrier.Release()
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() { done <- cmd.Wait(); close(joined) }()
	defer func() { barrier.Release(); cancel(); <-joined }()
	select {
	case <-barrier.Entered:
		mutate()
		barrier.Release()
	case err := <-done:
		t.Fatalf("child exited before identity barrier: %v stderr=%s", err, stderr.String())
	case <-ctx.Done():
		barrier.Release()
		<-done
		t.Fatalf("child timed out before identity barrier: %s", stderr.String())
	}
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		barrier.Release()
		<-done
		t.Fatalf("child timed out after identity barrier: %s", stderr.String())
	}
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("waiting for CLI: %v", err)
		}
		code = exit.ExitCode()
	}
	if ctx.Err() != nil {
		t.Fatalf("CLI deadline, not an operational result: %s", stderr.String())
	}
	return collect(t, &runResult{root: root, output: filepath.FromSlash(p.Output.URI), format: p.Output.Format, stdout: stdout.Bytes(), stderr: stderr.Bytes(), code: code})
}

func vmIOHoldResponse(resp *http.Response, barrier *vmBarrier) error {
	// Capture the real old response before mutation; no synthesized S3 success.
	body, err := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return barrier.Block(resp.Request.Context())
}

func vmIORead(t testing.TB, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func vmIOValues(t testing.TB, path string, want []string) {
	t.Helper()
	var got []string
	readParquet(t, path, func(record arrow.RecordBatch, row int) {
		column := arrowColumn(t, record, "amount")
		if column.IsNull(row) {
			got = append(got, "null")
		} else {
			got = append(got, column.ValueStr(row))
		}
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("independent fixture values=%v, want %v", got, want)
	}
}

// Shared bucket configuration may already retain versions. Remove only our
// isolated prefix's versions before objectEnv's ordinary current-object cleanup.
func vmIOEnvironment(t *testing.T) *objectEnv {
	t.Helper()
	e := minioEnvironment(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pages := s3.NewListObjectVersionsPaginator(e.client, &s3.ListObjectVersionsInput{Bucket: aws.String(e.bucket), Prefix: aws.String(e.prefix)})
		remove := func(key, version *string) {
			if !strings.HasPrefix(aws.ToString(key), e.prefix) {
				t.Errorf("refusing cleanup outside owned prefix: %q", aws.ToString(key))
				return
			}
			if _, err := e.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(e.bucket), Key: key, VersionId: version}); err != nil {
				t.Errorf("delete owned-prefix version: %v", err)
			}
		}
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				t.Errorf("list owned-prefix versions: %v", err)
				return
			}
			for _, v := range page.Versions {
				remove(v.Key, v.VersionId)
			}
			for _, v := range page.DeleteMarkers {
				remove(v.Key, v.VersionId)
			}
		}
	})
	return e
}

// A versioned test must own the bucket itself, not merely a prefix. Its explicit
// permission is separate from permission to create an ordinary shared bucket.
func vmIOVersionedEnvironment(t *testing.T) *objectEnv {
	t.Helper()
	base := vmIOEnvironment(t)
	if os.Getenv("VALET_S3_CREATE_VERSIONED_BUCKET") != "1" {
		t.Skip("versioned MinIO NOT RUN: set VALET_S3_CREATE_VERSIONED_BUCKET=1 to permit an owned versioned bucket")
	}
	bucket := "valet-vm-" + strings.ReplaceAll(strings.Trim(base.prefix, "/"), "/", "-")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := base.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create owned versioned bucket: %v", err)
	}
	e := &objectEnv{client: base.client, bucket: bucket, endpoint: base.endpoint, region: base.region, prefix: "owned/"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		uploads := s3.NewListMultipartUploadsPaginator(e.client, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
		for uploads.HasMorePages() {
			page, err := uploads.NextPage(ctx)
			if err != nil {
				t.Errorf("list owned uploads: %v", err)
				break
			}
			for _, u := range page.Uploads {
				t.Errorf("owned versioned run leaked multipart upload %s", aws.ToString(u.Key))
				if _, err := e.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: u.Key, UploadId: u.UploadId}); err != nil {
					t.Errorf("abort owned upload: %v", err)
				}
			}
		}
		versions := s3.NewListObjectVersionsPaginator(e.client, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		for versions.HasMorePages() {
			page, err := versions.NextPage(ctx)
			if err != nil {
				t.Errorf("list owned versions: %v", err)
				break
			}
			for _, v := range page.Versions {
				if _, err := e.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: v.Key, VersionId: v.VersionId}); err != nil {
					t.Errorf("delete owned version: %v", err)
				}
			}
			for _, v := range page.DeleteMarkers {
				if _, err := e.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: v.Key, VersionId: v.VersionId}); err != nil {
					t.Errorf("delete owned marker: %v", err)
				}
			}
		}
		if _, err := e.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Errorf("delete owned bucket: %v", err)
		}
	})
	if _, err := e.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable versioning on owned bucket: %v", err)
	}
	return e
}

func TestVMStatisticsIO(t *testing.T) {
	t.Run("VM-STAT-16", func(t *testing.T) {
		for _, versioned := range []bool{false, true} {
			for _, phase := range []string{"during_reads", "after_last_read"} {
				t.Run(fmt.Sprintf("versioned_%t/%s", versioned, phase), func(t *testing.T) {
					e := vmIOEnvironment(t)
					if versioned {
						e = vmIOVersionedEnvironment(t)
					}
					root := t.TempDir()
					input := vmFixtureFAST(t, root)
					desc := vmInspectParquet(t, input)
					vmIOValues(t, input, []string{"1", "2", "3", "4", "5", "6", "7", "8"})
					e.put(t, "source.parquet", string(vmIORead(t, input)))
					before, err := e.client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(e.bucket), Key: aws.String(e.prefix + "source.parquet")})
					if err != nil {
						t.Fatal(err)
					}
					if versioned && aws.ToString(before.VersionId) == "" {
						t.Fatal("owned versioned fixture has no version ID")
					}
					proxy := vmNewProxy(t, e)
					path := "/" + e.bucket + "/" + e.prefix + "source.parquet"
					proxy.DenyPayload(path, desc)
					barrier := vmNewBarrier()
					target := "bytes=0-3"
					if phase == "during_reads" {
						target = fmt.Sprintf("bytes=%d-%d", desc.Size-8, desc.Size-1)
					}
					var once sync.Once
					proxy.After = func(resp *http.Response) error {
						// Exercise ETag-only pinning even if the configured shared
						// test bucket happens to retain versions.
						if !versioned {
							resp.Header.Del("X-Amz-Version-Id")
						}
						if resp.Request.URL.Path == path && resp.Request.Method == http.MethodGet && resp.Request.Header.Get("Range") == target && resp.StatusCode == http.StatusPartialContent {
							var err error
							once.Do(func() { err = vmIOHoldResponse(resp, barrier) })
							return err
						}
						return nil
					}
					p := vmIOPipeline(t, root, e.uri("source.parquet"))
					p.Source.S3 = e.s3(proxy.URL)
					r := vmIOBarrierRun(t, root, p, barrier, func() {
						// Replace with a different, valid file. Old-version success must
						// therefore identify the original footer, never the replacement.
						replacement := vmFixtureSTAT(t, t.TempDir())
						e.put(t, "source.parquet", string(vmIORead(t, replacement)))
					})
					trace := proxy.Requests()
					var selected, final int
					for i, request := range trace {
						if request.Path != path {
							continue
						}
						if request.Method == http.MethodGet {
							selected++
							if request.IfMatch != aws.ToString(before.ETag) {
								t.Errorf("GET unpinned: %+v", request)
							}
							if versioned && request.VersionID != aws.ToString(before.VersionId) {
								t.Errorf("GET changed version: %+v", request)
							}
							if !versioned && request.VersionID != "" {
								t.Errorf("ETag-only run unexpectedly pinned a version: %+v", request)
							}
						}
						if request.Method == http.MethodHead && request.IfMatch == aws.ToString(before.ETag) && i > 0 {
							final++
							wantStatus := http.StatusOK
							if !versioned {
								wantStatus = http.StatusPreconditionFailed
							}
							if request.Status != wantStatus {
								t.Errorf("final pinned identity response=%d, want %d", request.Status, wantStatus)
							}
							if versioned && request.VersionID != aws.ToString(before.VersionId) {
								t.Errorf("final HEAD not version-specific: %+v", request)
							}
						}
					}
					if selected == 0 {
						t.Fatal("identity experiment made no selected reads")
					}
					if phase == "after_last_read" && selected != 3 {
						t.Errorf("last-response experiment performed %d GETs, want only trailer/footer/header; trace=%+v", selected, trace)
					}
					if !versioned {
						vmIOFailure(t, r)
						if phase == "after_last_read" && final != 1 {
							t.Errorf("final conditional HEAD count=%d, want 1; trace=%+v", final, trace)
						}
						return
					}
					if r.code != exitOK {
						t.Fatalf("immutable pinned version should remain valid: exit=%d stderr=%s", r.code, r.stderr)
					}
					if final != 1 {
						t.Errorf("final version-specific HEAD count=%d, want 1; trace=%+v", final, trace)
					}
					vmIOCoverage(t, r, 0, 8, 0, 2)
					vmAssertEvidence(t, r, desc)
					evidence := vmEvidence(t, r)
					if len(evidence) != 2 {
						t.Fatalf("evidence count=%d, want 2", len(evidence))
					}
					for _, decision := range evidence {
						if decision["footer_sha256"] != desc.FooterSHA256 {
							t.Errorf("old-version footer not retained: %#v", decision)
						}
						identity, ok := decision["identity"].(map[string]any)
						if !ok || identity["version_id"] != aws.ToString(before.VersionId) || identity["etag"] != aws.ToString(before.ETag) || decision["identity_method"] != "s3_version" {
							t.Errorf("evidence omits exact pinned identity: %#v", decision)
						}
						if vmIONumber(t, decision["footer_offset"]) != uint64(desc.Footer.Start) || vmIONumber(t, decision["footer_length"]) != uint64(desc.Footer.End-desc.Footer.Start-8) {
							t.Errorf("wrong independently reproducible footer range: %#v", decision)
						}
					}
					var manifest struct {
						Inputs vmReference `json:"inputs"`
					}
					decodeFile(t, r.manifests[0], &manifest)
					var inputIdentity map[string]any
					vmJSON(t, vmReadReference(t, r, manifest.Inputs), &inputIdentity)
					if inputIdentity["version_id"] != aws.ToString(before.VersionId) || inputIdentity["identity_method"] != "s3_version" || inputIdentity["payload_verification"] == "full" {
						t.Errorf("input index misrepresents old selective version: %#v", inputIdentity)
					}
				})
			}
		}
	})

	t.Run("VM-STAT-17", func(t *testing.T) {
		for _, mutation := range []string{"size", "mtime", "inode"} {
			t.Run(mutation, func(t *testing.T) {
				e := vmIOEnvironment(t)
				root := t.TempDir()
				local := vmFixtureFAST(t, root)
				body := vmIORead(t, local)
				info, err := os.Stat(local)
				if err != nil {
					t.Fatal(err)
				}
				remote := vmFixtureFAST(t, t.TempDir())
				desc := vmInspectParquet(t, remote)
				e.put(t, "later.parquet", string(vmIORead(t, remote)))
				proxy := vmNewProxy(t, e)
				path := "/" + e.bucket + "/" + e.prefix + "later.parquet"
				proxy.DenyPayload(path, desc)
				barrier := vmNewBarrier()
				var once sync.Once
				proxy.After = func(resp *http.Response) error {
					if resp.Request.URL.Path == path && resp.Request.Method == http.MethodGet {
						var err error
						once.Do(func() { err = barrier.Block(resp.Request.Context()) })
						return err
					}
					return nil
				}
				p := vmIOPipeline(t, root, local, e.uri("later.parquet"))
				p.Source.S3 = e.s3(proxy.URL)
				r := vmIOBarrierRun(t, root, p, barrier, func() {
					switch mutation {
					case "size":
						if err := os.WriteFile(local, append(body, 0), 0600); err != nil {
							t.Fatal(err)
						}
					case "mtime":
						if err := os.Chtimes(local, info.ModTime().Add(time.Hour), info.ModTime().Add(time.Hour)); err != nil {
							t.Fatal(err)
						}
					case "inode":
						replacement := put(t, filepath.Join(root, "replacement.parquet"), string(body))
						if err := os.Chtimes(replacement, info.ModTime(), info.ModTime()); err != nil {
							t.Fatal(err)
						}
						if err := os.Rename(replacement, local); err != nil {
							t.Fatal(err)
						}
					}
				})
				vmIOFailure(t, r)
			})
		}
	})

	t.Run("VM-STAT-18", func(t *testing.T) {
		t.Run("evidence_survives_error_logging", func(t *testing.T) {
			root := t.TempDir()
			input := vmFixtureSTAT(t, root)
			desc := vmInspectParquet(t, input)
			r := runPipeline(t, root, vmIOPipeline(t, root, input), "--json-output", "-", "--log-format", "json", "--log-level", "error")
			if r.code != exitValidationFailed {
				t.Fatalf("exit=%d stderr=%s", r.code, r.stderr)
			}
			if len(bytes.TrimSpace(r.stderr)) != 0 {
				t.Errorf("successful publication with row failures logged below error filter: %s", r.stderr)
			}
			vmIOCoverage(t, r, 12, 4, 3, 1)
			vmAssertEvidence(t, r, desc)
			decisions := vmEvidence(t, r)
			if len(decisions) != 4 {
				t.Fatalf("decision count=%d, want 4", len(decisions))
			}
			for i, decision := range decisions {
				want := "scan"
				if i == 0 {
					want = "skip"
				}
				if vmIONumber(t, decision["row_group"]) != uint64(i) || decision["action"] != want || decision["footer_sha256"] != desc.FooterSHA256 {
					t.Errorf("decision %d lacks exact persisted audit identity: %#v", i, decision)
				}
			}
		})
		for _, denied := range []string{"evidence/", "artifacts.ndjson"} {
			t.Run("deny_"+strings.ReplaceAll(denied, "/", "_"), func(t *testing.T) {
				e := vmIOEnvironment(t)
				root := t.TempDir()
				input := vmFixtureFAST(t, root)
				proxy := vmNewProxy(t, e)
				var attempts int
				var mu sync.Mutex
				proxy.Before = func(w http.ResponseWriter, req *http.Request) bool {
					if (req.Method == http.MethodPut || req.Method == http.MethodPost) && strings.Contains(req.URL.Path, denied) {
						mu.Lock()
						attempts++
						mu.Unlock()
						http.Error(w, "denied audit publication", http.StatusForbidden)
						return true
					}
					return false
				}
				p := vmIOPipeline(t, root, input)
				p.Output.URI, p.Output.S3 = e.uri("output"), e.s3(proxy.URL)
				path := p.write(t, filepath.Join(root, "pipeline.yml"))
				stdout, stderr, code := invoke(t, "validate", "-p", path, "--json-output", "-", "--log-level", "error")
				out := filepath.Join(root, "download")
				e.download(t, "output", out)
				r := collect(t, &runResult{root: root, output: out, format: "csv", stdout: stdout, stderr: stderr, code: code})
				vmIOFailure(t, r)
				mu.Lock()
				n := attempts
				mu.Unlock()
				if n == 0 {
					t.Fatalf("no attempted publication matched denial %q; requests=%+v", denied, proxy.Requests())
				}
				for _, key := range e.keys(t, "output") {
					if strings.HasSuffix(key, "/manifest.json") {
						t.Errorf("failed partial audit ledger has consumable manifest %s", key)
					}
				}
			})
		}
	})

	t.Run("VM-STAT-21", vmIOResourceCases)
	t.Run("VM-STAT-22", vmIOEmptyAndSources)
}

func vmIOResourceCases(t *testing.T) {
	t.Run("diagnostic_workspace_exhaustion", func(t *testing.T) {
		e := vmIOEnvironment(t)
		root := t.TempDir()
		input := vmFixtureFAST(t, root)
		e.put(t, "source.parquet", string(vmIORead(t, input)))
		proxy := vmNewProxy(t, e)
		p := vmIOPipeline(t, root, e.uri("source.parquet"))
		p.Source.S3, p.Execution.MemoryBudget = e.s3(proxy.URL), "32KiB"
		r := runPipeline(t, root, p, "--json-output", "-", "--log-format", "json", "--log-level", "debug")
		vmIOFailure(t, r)
		if len(proxy.Requests()) != 0 {
			t.Errorf("diagnostic resource failure accessed source: %+v", proxy.Requests())
		}
		if !bytes.Contains(bytes.ToLower(r.stderr), []byte("resource")) {
			t.Errorf("diagnostic allocation failure not resource-classified: %s", r.stderr)
		}
	})

	t.Run("footer_resource_exhaustion", func(t *testing.T) {
		root := t.TempDir()
		input := vmFixtureFAST(t, root)
		vmRewriteFooter(t, input, func(meta *metadata.FileMetaData) {
			kv := meta.KeyValueMetadata()
			if err := kv.Append("resource-marker", strings.Repeat("x", 1<<20)); err != nil {
				t.Fatal(err)
			}
			meta.FileMetaData.KeyValueMetadata = kv
		})
		p := vmIOPipeline(t, root, input)
		p.Execution.MemoryBudget = "32MiB"
		r := runPipeline(t, root, p, "--json-output", "-", "--log-level", "error")
		vmIOFailure(t, r)
		if !bytes.Contains(bytes.ToLower(r.stderr), []byte("budget")) && !bytes.Contains(bytes.ToLower(r.stderr), []byte("resource")) {
			t.Errorf("footer failure not resource classified: %s", r.stderr)
		}
	})

	t.Run("proof_budget_and_bounded_logging", func(t *testing.T) {
		e := vmIOEnvironment(t)
		root := t.TempDir()
		input := filepath.Join(root, "many.parquet")
		schema := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.AppendValues([]int64{1, 2, 3, 4}, nil)
		values := builder.NewArray()
		builder.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, 4)
		values.Release()
		defer record.Release()
		groups := make([]arrow.RecordBatch, 256)
		for i := range groups {
			groups[i] = record
		}
		vmWriteParquet(t, input, schema, groups)
		desc := vmInspectParquet(t, input)
		e.put(t, "many.parquet", string(vmIORead(t, input)))
		var baseline []map[string]any
		var access []string
		for _, level := range []string{"error", "debug"} {
			proxy := vmNewProxy(t, e)
			path := "/" + e.bucket + "/" + e.prefix + "many.parquet"
			proxy.DenyPayload(path, desc)
			runRoot := t.TempDir()
			p := vmIOPipeline(t, runRoot, e.uri("many.parquet"))
			p.Source.S3, p.Execution.MemoryBudget = e.s3(proxy.URL), "32MiB"
			r := runPipeline(t, runRoot, p, "--json-output", "-", "--log-format", "json", "--log-level", level)
			if r.code != exitOK {
				t.Fatalf("bounded %s logging should fit budget: exit=%d stderr=%s", level, r.code, r.stderr)
			}
			vmIOCoverage(t, r, 0, 1024, 0, 256)
			vmAssertEvidence(t, r, desc)
			if r.summary.ManagedPeak > 32<<20 {
				t.Errorf("managed peak exceeds configured budget: %d", r.summary.ManagedPeak)
			}
			decisions := vmEvidence(t, r)
			for _, decision := range decisions {
				delete(decision, "run_id")
			}
			var got []string
			for _, request := range proxy.Requests() {
				got = append(got, request.Method+" "+request.Range+" "+request.IfMatch+" "+request.VersionID)
			}
			if baseline == nil {
				baseline, access = decisions, got
			} else if !reflect.DeepEqual(baseline, decisions) || !reflect.DeepEqual(access, got) {
				t.Error("logging changes exact decisions or backend reads")
			}
		}
		proxy := vmNewProxy(t, e)
		proxy.DenyPayload("/"+e.bucket+"/"+e.prefix+"many.parquet", desc)
		runRoot := t.TempDir()
		p := vmIOPipeline(t, runRoot, e.uri("many.parquet"))
		p.Source.S3, p.Execution.MemoryBudget = e.s3(proxy.URL), "32MiB"
		rules := make([]any, 1024)
		for i := range rules {
			rules[i] = obj{"code": fmt.Sprintf("PROOF_%04d", i), "column": "amount", "level": "error", "checks": []any{obj{"type": "valid_range", "min": 0}}}
		}
		p.with(t, fragment(t, obj{"rules": rules}))
		r := runPipeline(t, runRoot, p, "--json-output", "-", "--log-format", "json", "--log-level", "debug")
		vmIOFailure(t, r)
		lower := bytes.ToLower(r.stderr)
		if !bytes.Contains(lower, []byte("budget")) && !bytes.Contains(lower, []byte("resource")) {
			t.Errorf("proof exhaustion not resource classified: %s", r.stderr)
		}
		for _, request := range proxy.Requests() {
			if request.Status == http.StatusForbidden {
				t.Errorf("proof exhaustion secretly attempted payload fallback: %+v", request)
			}
		}
	})

	t.Run("denied_unknown_proof_fallback", func(t *testing.T) {
		e := vmIOEnvironment(t)
		root := t.TempDir()
		input := vmFixtureFAST(t, root)
		vmRewriteFooter(t, input, func(meta *metadata.FileMetaData) {
			for _, group := range meta.RowGroups {
				for _, column := range group.Columns {
					column.MetaData.Statistics.NullCount = nil
				}
			}
		})
		desc := vmInspectParquet(t, input)
		e.put(t, "unknown.parquet", string(vmIORead(t, input)))
		proxy := vmNewProxy(t, e)
		proxy.DenyPayload("/"+e.bucket+"/"+e.prefix+"unknown.parquet", desc)
		p := vmIOPipeline(t, root, e.uri("unknown.parquet"))
		p.Source.S3 = e.s3(proxy.URL)
		r := runPipeline(t, root, p, "--json-output", "-", "--log-level", "error")
		vmIOFailure(t, r)
		denied := false
		for _, request := range proxy.Requests() {
			if request.Method == http.MethodGet && request.Status == http.StatusForbidden {
				denied = true
			}
		}
		if !denied {
			t.Fatalf("unknown proof did not attempt required fallback read: %+v", proxy.Requests())
		}
	})

	t.Run("aggregate_declared_count_overflow", func(t *testing.T) {
		root := t.TempDir()
		var sources []string
		for range 3 {
			input := vmFixtureFAST(t, t.TempDir())
			vmRewriteFooter(t, input, func(meta *metadata.FileMetaData) {
				meta.RowGroups = meta.RowGroups[:1]
				meta.NumRows, meta.RowGroups[0].NumRows = 1<<63-1, 1<<63-1
				for _, column := range meta.RowGroups[0].Columns {
					column.MetaData.NumValues = 1<<63 - 1
				}
			})
			sources = append(sources, input)
		}
		r := runPipeline(t, root, vmIOPipeline(t, root, sources...), "--json-output", "-", "--log-level", "error")
		vmIOFailure(t, r)
	})
}

func vmIOEmptyAndSources(t *testing.T) {
	root := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	empty := filepath.Join(root, "empty.parquet")
	vmWriteParquet(t, empty, schema, nil)
	emptyTail := filepath.Join(root, "empty-tail.parquet")
	vmWriteParquet(t, emptyTail, schema, nil)
	t.Run("empty_file_only", func(t *testing.T) {
		runRoot := t.TempDir()
		r := runPipeline(t, runRoot, vmIOPipeline(t, runRoot, empty), "--json-output", "-", "--log-level", "error")
		if r.code != exitOK {
			t.Fatalf("empty file exit=%d stderr=%s", r.code, r.stderr)
		}
		vmIOCoverage(t, r, 0, 0, 0, 0)
		if len(vmEvidence(t, r)) != 0 || len(r.issues) != 0 {
			t.Error("empty file invented row-group evidence or issues")
		}
		for _, bucket := range []string{"pass", "warn", "error"} {
			if len(vmRows(t, r, bucket)) != 0 {
				t.Errorf("empty file invented %s rows", bucket)
			}
		}
		if r.summary.Verification != "values" || r.summary.ProvenancePositionBasis != "values" {
			t.Errorf("empty file claims unobserved metadata rows: %+v", r.summary)
		}
	})
	fast := vmFixtureFAST(t, t.TempDir())
	vmRewriteFooter(t, fast, func(meta *metadata.FileMetaData) {
		// Clone via public metadata serialization so no existing group's columns
		// are aliased and accidentally zeroed along with the inserted empty group.
		raw, err := meta.Serialize(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		clone, err := metadata.NewFileMetaData(raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		group := clone.RowGroups[0]
		group.NumRows, group.TotalByteSize = 0, 0
		for _, column := range group.Columns {
			column.MetaData.NumValues = 0
			column.MetaData.TotalCompressedSize, column.MetaData.TotalUncompressedSize = 0, 0
			column.MetaData.DataPageOffset = 4
			column.MetaData.DictionaryPageOffset = nil
			column.MetaData.Statistics = nil
		}
		meta.RowGroups = append(meta.RowGroups, nil)
		copy(meta.RowGroups[2:], meta.RowGroups[1:len(meta.RowGroups)-1])
		meta.RowGroups[1] = group
	})
	stat := vmFixtureSTAT(t, t.TempDir())
	for _, mode := range []string{"failures", "report"} {
		t.Run(mode, func(t *testing.T) {
			runRoot := t.TempDir()
			p := vmIOPipeline(t, runRoot, empty, fast, stat, emptyTail)
			p.with(t, "output: {mode: "+mode+"}\n")
			r := runPipeline(t, runRoot, p, "--json-output", "-", "--log-level", "error")
			if r.code != exitValidationFailed {
				t.Fatalf("exit=%d stderr=%s", r.code, r.stderr)
			}
			vmIOCoverage(t, r, 12, 12, 4, 3)
			if r.manifest["provenance_position_basis"] != "metadata_prefix" {
				t.Errorf("missing metadata-prefix disclosure: %#v", r.manifest)
			}
			if r.summary.Passed != 9 || r.summary.Errored != 3 || len(r.issues) != 3 {
				t.Errorf("literal decoded outcomes wrong: %+v issues=%+v", r.summary, r.issues)
			}
			var observed []map[string]any
			for _, path := range files(t, filepath.Join(r.dir(t), "issues", "*.ndjson")) {
				f, err := os.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				dec := json.NewDecoder(f)
				dec.UseNumber()
				for {
					var value map[string]any
					err := dec.Decode(&value)
					if err == io.EOF {
						break
					}
					if err != nil {
						f.Close()
						t.Fatal(err)
					}
					observed = append(observed, value)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if len(observed) != 3 {
				t.Fatalf("issue records=%d, want 3", len(observed))
			}
			for i, want := range []struct{ row, global, group, inGroup uint64 }{{6, 14, 1, 2}, {9, 17, 2, 1}, {10, 18, 2, 2}} {
				got := observed[i]
				for key, expected := range map[string]uint64{"source_ordinal": 3, "row": want.row, "global_row": want.global, "row_group": want.group, "row_in_group": want.inGroup} {
					if vmIONumber(t, got[key]) != expected {
						t.Errorf("issue %d %s=%v, want %d", i, key, got[key], expected)
					}
				}
				if got["position_basis"] != "metadata_prefix" || got["source"] != stat {
					t.Errorf("issue position/source claim wrong: %#v", got)
				}
				wantCode := "POSITIVE"
				if i == 0 {
					wantCode = "NOT_NULL"
				}
				if got["code"] != wantCode {
					t.Errorf("issue %d code=%v, want %s", i, got["code"], wantCode)
				}
			}
			decisions := vmEvidence(t, r)
			if len(decisions) != 7 {
				t.Fatalf("evidence groups=%d, want 7", len(decisions))
			}
			for i, want := range []struct {
				source, group, rows uint64
				action              string
			}{{2, 0, 4, "skip"}, {2, 1, 0, "scan"}, {2, 2, 4, "skip"}, {3, 0, 4, "skip"}, {3, 1, 4, "scan"}, {3, 2, 4, "scan"}, {3, 3, 4, "scan"}} {
				got := decisions[i]
				if vmIONumber(t, got["source_ordinal"]) != want.source || vmIONumber(t, got["row_group"]) != want.group || vmIONumber(t, got["declared_rows"]) != want.rows || got["action"] != want.action {
					t.Errorf("evidence source/group order %d: %#v, want %+v", i, got, want)
				}
			}
			vmAssertEvidence(t, r, vmInspectParquet(t, empty), vmInspectParquet(t, fast), vmInspectParquet(t, stat), vmInspectParquet(t, emptyTail))
			if len(vmRows(t, r, "pass")) != 0 || len(vmRows(t, r, "warn")) != 0 {
				t.Error("skipped/value passing rows were materialized in omission mode")
			}
			if mode == "failures" {
				rows := vmRows(t, r, "error")
				if len(rows) != 3 {
					t.Fatalf("error rows=%d, want 3", len(rows))
				}
				for i, want := range []string{`\N`, "-1", "0"} {
					if rows[i]["amount"] != want {
						t.Errorf("error value %d=%q, want %q", i, rows[i]["amount"], want)
					}
				}
				for i, want := range []struct{ row, global uint64 }{{6, 14}, {9, 17}, {10, 18}} {
					for key, expected := range map[string]uint64{"__valet_source_ordinal": 3, "__valet_row": want.row, "__valet_global_row": want.global} {
						if rows[i][key] != strconv.FormatUint(expected, 10) {
							t.Errorf("error row %d %s=%q, want %d", i, key, rows[i][key], expected)
						}
					}
				}
			} else if rows := vmRows(t, r, "error"); len(rows) != 0 {
				t.Errorf("report invented row artifacts: %v", rows)
			}
		})
	}
}
