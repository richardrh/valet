//go:build integration

package integration_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	gocmp "github.com/google/go-cmp/cmp"
)

// objectMemoryBudget is the execution memory budget of every object-store run.
const objectMemoryBudget = 128 << 20

type objectEnv struct {
	client                           *s3.Client
	bucket, endpoint, region, prefix string
}

// objectTarget selects the bucket an objectEnv isolates a prefix in.
type objectTarget struct {
	endpoint     string // S3-compatible endpoint; empty for AWS
	bucket       string
	createBucket bool // create the bucket when it does not exist
}

// minioEnvironment isolates a prefix in the S3-compatible bucket named by
// VALET_S3_ENDPOINT and VALET_S3_BUCKET, skipping when either is unset.
func minioEnvironment(t *testing.T) *objectEnv {
	t.Helper()
	endpoint, bucket := os.Getenv("VALET_S3_ENDPOINT"), os.Getenv("VALET_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("MinIO NOT RUN: set VALET_S3_ENDPOINT and VALET_S3_BUCKET")
	}
	return newObjectEnv(t, objectTarget{
		endpoint:     endpoint,
		bucket:       bucket,
		createBucket: os.Getenv("VALET_S3_CREATE_BUCKET") == "1",
	})
}

// awsEnvironment isolates a prefix in VALET_AWS_BUCKET, skipping unless
// VALET_AWS_INTEGRATION=1.
func awsEnvironment(t *testing.T) *objectEnv {
	t.Helper()
	if os.Getenv("VALET_AWS_INTEGRATION") != "1" {
		t.Skip("AWS NOT RUN: explicitly set VALET_AWS_INTEGRATION=1 and VALET_AWS_BUCKET")
	}
	bucket := os.Getenv("VALET_AWS_BUCKET")
	if bucket == "" {
		t.Fatal("AWS requested but VALET_AWS_BUCKET is absent")
	}
	return newObjectEnv(t, objectTarget{bucket: bucket})
}

func newObjectEnv(t *testing.T, target objectTarget) *objectEnv {
	t.Helper()
	region := cmp.Or(os.Getenv("AWS_REGION"), "us-east-1")
	// The test's own fixture client: validate checksums only when required so
	// the SDK does not print unstructured notices into the test output.
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(region),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired))
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if target.endpoint != "" {
			o.BaseEndpoint = aws.String(target.endpoint)
			o.UsePathStyle = true
		}
	})
	if target.createBucket {
		_, err := client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(target.bucket)})
		if err != nil {
			bucket := &s3.CreateBucketInput{Bucket: aws.String(target.bucket)}
			if _, err = client.CreateBucket(context.Background(), bucket); err != nil {
				t.Fatalf("creating explicitly requested bucket: %v", err)
			}
		}
	}
	var id [12]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	e := &objectEnv{
		client:   client,
		bucket:   target.bucket,
		endpoint: target.endpoint,
		region:   region,
		prefix:   "valet-acceptance/" + hex.EncodeToString(id[:]) + "/",
	}
	t.Cleanup(func() { e.cleanup(t) })
	return e
}
func (e *objectEnv) uri(key string) string { return "s3://" + e.bucket + "/" + e.prefix + key }
func (e *objectEnv) s3(endpoint string) *s3Spec {
	if endpoint == "" {
		return &s3Spec{Region: e.region}
	}
	return &s3Spec{Region: e.region, Endpoint: endpoint, PathStyle: true}
}
func (e *objectEnv) put(t testing.TB, key, body string) {
	t.Helper()
	_, err := e.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(e.bucket),
		Key:    aws.String(e.prefix + key),
		Body:   strings.NewReader(body),
	})
	if err != nil {
		t.Fatal(err)
	}
}
func (e *objectEnv) keys(t testing.TB, prefix string) []string {
	t.Helper()
	p := s3.NewListObjectsV2Paginator(e.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(e.bucket),
		Prefix: aws.String(e.prefix + prefix),
	})
	var keys []string
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys
}
func (e *objectEnv) cleanup(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p := s3.NewListObjectsV2Paginator(e.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(e.bucket),
		Prefix: aws.String(e.prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Errorf("cleaning isolated objects: %v", err)
			return
		}
		for _, o := range page.Contents {
			object := &s3.DeleteObjectInput{Bucket: aws.String(e.bucket), Key: o.Key}
			if _, err := e.client.DeleteObject(ctx, object); err != nil {
				t.Errorf("delete isolated object: %v", err)
			}
		}
	}
	uploads, err := e.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(e.bucket),
		Prefix: aws.String(e.prefix),
	})
	if err != nil {
		t.Errorf("checking leaked multipart uploads: %v", err)
		return
	}
	for _, u := range uploads.Uploads {
		t.Errorf("leaked multipart upload %s, want none", aws.ToString(u.Key))
		_, _ = e.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(e.bucket),
			Key:      u.Key,
			UploadId: u.UploadId,
		})
	}
}
func (e *objectEnv) download(t testing.TB, prefix, root string) {
	t.Helper()
	for _, key := range e.keys(t, prefix) {
		response, err := e.client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(e.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		relative := strings.TrimPrefix(key, e.prefix+prefix+"/")
		put(t, filepath.Join(root, filepath.FromSlash(relative)), string(body))
	}
}
func (e *objectEnv) getObject(t testing.TB, key string) string {
	t.Helper()
	response, err := e.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(e.bucket),
		Key:    aws.String(e.prefix + key),
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// objectScenario is one CLI validation with object storage on either side.
type objectScenario struct {
	source       string   // local path or s3:// URI
	inputType    string   // input.type; csv when empty
	format       string   // output.format; csv when empty
	endpoint     string   // S3 endpoint for source and output; empty for AWS
	remoteOutput bool     // publish into the bucket, then download for inspection
	config       []string // YAML fragments merged over the default pipeline, in order
}

func (e *objectEnv) run(t *testing.T, s objectScenario) *runResult {
	t.Helper()
	root := t.TempDir()
	out := filepath.Join(root, "out")
	output := out
	if s.remoteOutput {
		output = e.uri("output")
	}
	format := cmp.Or(s.format, "csv")
	p := defaultPipeline(s.source, cmp.Or(s.inputType, "csv"), output, format)
	p.Source.S3 = e.s3(s.endpoint)
	p.Output.S3 = e.s3(s.endpoint)
	p.Execution.MemoryBudget = "128MiB" // objectMemoryBudget
	p.Input.NullValues = nulls("")
	path := p.with(t, s.config...).write(t, filepath.Join(root, "pipeline.yml"))
	stdout, stderr, code := invoke(t, "validate", "-p", path)
	if s.remoteOutput {
		e.download(t, "output", out)
	}
	return collect(t, &runResult{root: root, output: out, format: format, stdout: stdout, stderr: stderr, code: code})
}

func TestE2E14S3CrossFormats(t *testing.T) {
	stageLocal := func(t *testing.T, _ *objectEnv) string {
		return put(t, filepath.Join(t.TempDir(), "input.csv"), fixtureA)
	}
	stageObject := func(t *testing.T, e *objectEnv) string {
		e.put(t, "input.csv", fixtureA)
		return e.uri("input.csv")
	}
	directions := []struct {
		name         string
		stage        func(*testing.T, *objectEnv) string // writes fixture A and returns the source
		remoteOutput bool
	}{
		{name: "local-to-MinIO", stage: stageLocal, remoteOutput: true},
		{name: "MinIO-to-local", stage: stageObject, remoteOutput: false},
		{name: "MinIO-to-MinIO", stage: stageObject, remoteOutput: true},
	}
	for _, direction := range directions {
		for _, format := range []string{"csv", "parquet"} {
			t.Run(direction.name+"/"+format, func(t *testing.T) {
				e := minioEnvironment(t)
				r := e.run(t, objectScenario{
					source:       direction.stage(t, e),
					format:       format,
					endpoint:     e.endpoint,
					remoteOutput: direction.remoteOutput,
					config:       []string{rulesA},
				})
				if r.code != exitValidationFailed {
					t.Fatalf("validate %s/%s exit = %d, want %d; stderr=%s", direction.name, format, r.code, exitValidationFailed, r.stderr)
				}
				if diff := gocmp.Diff(fixtureACounts, countsOf(r), gocmp.AllowUnexported(counts{})); diff != "" {
					t.Errorf("validate %s/%s counts diff (-want +got):\n%s", direction.name, format, diff)
				}
				if diff := gocmp.Diff(fixtureAIssues, issuesOf(r), gocmp.AllowUnexported(issueAt{})); diff != "" {
					t.Errorf("validate %s/%s issues diff (-want +got):\n%s", direction.name, format, diff)
				}
				for bucket, want := range map[string][]string{"pass": rowNumbers(1), "warn": rowNumbers(3), "error": rowNumbers(2, 4, 5)} {
					if diff := gocmp.Diff(want, r.globalRows(t, bucket)); diff != "" {
						t.Errorf("validate %s/%s %s rows diff (-want +got):\n%s", direction.name, format, bucket, diff)
					}
				}
			})
		}
	}
	t.Run("Parquet range reads", func(t *testing.T) {
		e := minioEnvironment(t)
		path := filepath.Join(t.TempDir(), "data.parquet")
		writeParquetFixture(t, path, 0)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		e.put(t, "input.parquet", string(body))
		var ranges atomic.Int64
		proxy := e.proxy(t, func(w http.ResponseWriter, r *http.Request) bool {
			if r.Method == "GET" && r.Header.Get("Range") != "" {
				ranges.Add(1)
			}
			return false
		})
		r := e.run(t, objectScenario{
			source:    e.uri("input.parquet"),
			inputType: "parquet",
			endpoint:  proxy.URL,
			config:    []string{rulesA},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate S3 Parquet range input exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if diff := gocmp.Diff(fixtureACounts, countsOf(r), gocmp.AllowUnexported(counts{})); diff != "" {
			t.Errorf("validate S3 Parquet range counts diff (-want +got):\n%s", diff)
		}
		if ranges.Load() == 0 {
			t.Errorf("signed range GETs = 0, want at least one")
		}
	})
	t.Run("S3 rename and reference", func(t *testing.T) {
		e := minioEnvironment(t)
		e.put(t, "mapping.csv", "source,target\nold,key\n")
		e.put(t, "reference.csv", "key\nA\n")
		e.put(t, "input.csv", "old\nA\nB\n")
		config := fragment(t, obj{
			"transforms": []obj{{"type": "rename_columns", "mapping_file": e.uri("mapping.csv")}},
			"rules": []obj{{"code": "REF", "level": "error", "column": "key", "checks": []obj{{
				"type":             "referential_integrity",
				"reference_file":   e.uri("reference.csv"),
				"reference_column": "key",
			}}}},
		})
		r := e.run(t, objectScenario{
			source:   e.uri("input.csv"),
			endpoint: e.endpoint,
			config:   []string{config},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate S3 rename/reference exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if diff := gocmp.Diff([]issueAt{{2, "REF"}}, issuesOf(r), gocmp.AllowUnexported(issueAt{})); diff != "" {
			t.Errorf("validate S3 rename/reference issues diff (-want +got):\n%s", diff)
		}
	})
}
func (e *objectEnv) proxy(t *testing.T, intercept func(http.ResponseWriter, *http.Request) bool) *httptest.Server {
	t.Helper()
	target, err := url.Parse(e.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !intercept(w, r) {
			proxy.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

const uniqueID = "rules: [{code: UNIQUE, level: error, column: id, checks: [{type: is_unique}]}]"

func TestE2E16S3ChangedReplayObject(t *testing.T) {
	e := minioEnvironment(t)
	e.put(t, "input.csv", "id\na\nb\na\n")
	var gets atomic.Int64
	var changed atomic.Bool
	// Replace the object as it is fetched a second time, so the run reads two versions.
	proxy := e.proxy(t, func(w http.ResponseWriter, r *http.Request) bool {
		isInputGet := r.Method == "GET" && strings.HasSuffix(r.URL.Path, "input.csv")
		if isInputGet && gets.Add(1) == 2 {
			_, err := e.client.PutObject(r.Context(), &s3.PutObjectInput{
				Bucket: aws.String(e.bucket),
				Key:    aws.String(e.prefix + "input.csv"),
				Body:   strings.NewReader("id\na\nc\na\n"),
			})
			if err != nil {
				http.Error(w, err.Error(), 500)
				return true
			}
			changed.Store(true)
		}
		return false
	})
	r := e.run(t, objectScenario{
		source:       e.uri("input.csv"),
		endpoint:     proxy.URL,
		remoteOutput: true,
		config:       []string{uniqueID},
	})
	if r.code != exitOperational {
		t.Errorf("validate changed S3 replay exit = %d, want %d", r.code, exitOperational)
	}
	if !changed.Load() {
		t.Fatalf("replay mutation fault was never exercised:\nstdout=%s\nstderr=%s", r.stdout, r.stderr)
	}
	if !strings.Contains(strings.ToLower(string(r.stderr)), "chang") && !strings.Contains(string(r.stderr), "412") {
		t.Errorf("stderr = %s, want an input-change diagnostic", r.stderr)
	}
}
func TestE2E17S3SlowSink(t *testing.T) {
	e := minioEnvironment(t)
	e.put(t, "input.csv", fixtureA)
	var delayed atomic.Int64
	proxy := e.proxy(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "PUT" && strings.Contains(r.URL.Path, "/output/") {
			delayed.Add(1)
			select {
			case <-time.After(20 * time.Millisecond):
			case <-r.Context().Done():
			}
		}
		return false
	})
	r := e.run(t, objectScenario{
		source:       e.uri("input.csv"),
		endpoint:     proxy.URL,
		remoteOutput: true,
		config:       []string{rulesA},
	})
	if r.code != exitValidationFailed {
		t.Fatalf("validate slow S3 sink exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
	}
	if diff := gocmp.Diff(fixtureACounts, countsOf(r), gocmp.AllowUnexported(counts{})); diff != "" {
		t.Errorf("validate slow S3 sink counts diff (-want +got):\n%s", diff)
	}
	for bucket, want := range map[string][]string{"pass": rowNumbers(1), "warn": rowNumbers(3), "error": rowNumbers(2, 4, 5)} {
		if diff := gocmp.Diff(want, r.globalRows(t, bucket)); diff != "" {
			t.Errorf("validate slow S3 sink %s rows diff (-want +got):\n%s", bucket, diff)
		}
	}
	if delayed.Load() == 0 {
		t.Errorf("delayed output PUTs = 0, want the slow sink path reached")
	}
	if r.summary.ManagedPeak > objectMemoryBudget {
		t.Errorf("managed_peak_bytes = %d, want at most %d", r.summary.ManagedPeak, objectMemoryBudget)
	}
}
func TestE2E18S3FinalUploadFailure(t *testing.T) {
	const unrelated = "preserve"
	e := minioEnvironment(t)
	e.put(t, "input.csv", fixtureA)
	e.put(t, "unrelated.txt", unrelated)
	var failures atomic.Int64
	proxy := e.proxy(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/manifest.json") {
			failures.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "<Error><Code>ServiceUnavailable</Code>"+
				"<Message>injected final publication fault</Message></Error>")
			return true
		}
		return false
	})
	failed := e.run(t, objectScenario{
		source:       e.uri("input.csv"),
		endpoint:     proxy.URL,
		remoteOutput: true,
		config:       []string{rulesA},
	})
	if failed.code != exitOperational {
		t.Errorf("validate failed manifest upload exit = %d, want %d", failed.code, exitOperational)
	}
	if failures.Load() == 0 {
		t.Fatalf("final manifest upload fault not reached:\nstdout=%s\nstderr=%s", failed.stdout, failed.stderr)
	}
	if got := e.getObject(t, "unrelated.txt"); got != unrelated {
		t.Errorf("unrelated object = %q, want %q", got, unrelated)
	}
}
func TestE2E15S3PaginatedDiscovery(t *testing.T) {
	// One more than the 1000-key ListObjectsV2 page.
	const objects = 1001
	e := minioEnvironment(t)
	wantIDs := make([]string, objects)
	for i := range objects {
		wantIDs[i] = fmt.Sprintf("%04d", i)
		e.put(t, "input/"+wantIDs[i]+".csv", "id\n"+wantIDs[i]+"\n")
	}
	root := t.TempDir()
	var pages atomic.Int64
	proxy := e.proxy(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Query().Get("list-type") == "2" {
			pages.Add(1)
		}
		return false
	})
	r := runPipeline(t, root, &pipeline{
		Source: sourceSpec{Prefix: e.uri("input/"), Filter: "csv", Recursive: true, S3: e.s3(proxy.URL)},
		Input:  inputSpec{Type: "csv", Mode: "raw_strings"},
		Output: outputSpec{URI: filepath.ToSlash(filepath.Join(root, "out")), Format: "csv"},
	})
	if r.code != exitOK {
		t.Fatalf("validate paginated S3 discovery exit = %d, want %d; stderr=%s", r.code, exitOK, r.stderr)
	}
	if got := countsOf(r); got != (counts{total: objects, pass: objects}) {
		t.Errorf("validate paginated S3 discovery counts = %+v, want total:%d pass:%d", got, objects, objects)
	}
	if got := column(r.rows(t, "pass"), "id"); !slices.Equal(got, wantIDs) {
		t.Errorf("pass ids in object order = %q, want %q", got, wantIDs)
	}
	if got := pages.Load(); got < 2 {
		t.Errorf("list pages = %d, want at least 2", got)
	}
}
func TestE2E24S3AWSIsolatedRun(t *testing.T) {
	e := awsEnvironment(t)
	e.put(t, "input.csv", fixtureA)
	r := e.run(t, objectScenario{
		source:       e.uri("input.csv"),
		remoteOutput: true,
		config:       []string{rulesA},
	})
	if r.code != exitValidationFailed {
		t.Fatalf("validate AWS fixture exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
	}
	if diff := gocmp.Diff(fixtureACounts, countsOf(r), gocmp.AllowUnexported(counts{})); diff != "" {
		t.Errorf("validate AWS fixture counts diff (-want +got):\n%s", diff)
	}
	if diff := gocmp.Diff(fixtureAIssues, issuesOf(r), gocmp.AllowUnexported(issueAt{})); diff != "" {
		t.Errorf("validate AWS fixture issues diff (-want +got):\n%s", diff)
	}
	t.Run("conditional GET", func(t *testing.T) {
		// Independent conditional GET proves the actual AWS service contract. The
		// engine's replay condition is additionally exercised by the uniqueness run.
		key := e.prefix + "input.csv"
		head, err := e.client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(e.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
		response, err := e.client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket:  aws.String(e.bucket),
			Key:     aws.String(key),
			IfMatch: head.ETag,
			Range:   aws.String("bytes=0-4"),
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		rejected, conditionErr := e.client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket:  aws.String(e.bucket),
			Key:     aws.String(key),
			IfMatch: aws.String("\"not-the-current-etag\""),
		})
		if conditionErr == nil {
			_ = rejected.Body.Close()
			t.Errorf("GET with a stale If-Match succeeded, want a precondition failure")
		}
	})
	t.Run("engine uniqueness replay", func(t *testing.T) {
		separate := awsEnvironment(t)
		separate.put(t, "input.csv", "id\na\nb\na\n")
		replay := separate.run(t, objectScenario{
			source:       separate.uri("input.csv"),
			remoteOutput: true,
			config:       []string{uniqueID},
		})
		if replay.code != exitValidationFailed {
			t.Fatalf("validate AWS uniqueness replay exit = %d, want %d; stderr=%s", replay.code, exitValidationFailed, replay.stderr)
		}
		if diff := gocmp.Diff(counts{total: 3, pass: 1, error: 2}, countsOf(replay), gocmp.AllowUnexported(counts{})); diff != "" {
			t.Errorf("validate AWS uniqueness replay counts diff (-want +got):\n%s", diff)
		}
		if diff := gocmp.Diff([]issueAt{{1, "UNIQUE"}, {3, "UNIQUE"}}, issuesOf(replay), gocmp.AllowUnexported(issueAt{})); diff != "" {
			t.Errorf("validate AWS uniqueness replay issues diff (-want +got):\n%s", diff)
		}
		if replay.summary.Passes < 2 {
			t.Errorf("passes = %d, want at least 2 (replay)", replay.summary.Passes)
		}
	})
	t.Run("SSE-S3 CLI output", func(t *testing.T) { awsEncryptionRun(t, e, "AES256", "") })
	if kms := os.Getenv("VALET_AWS_KMS_KEY_ID"); kms != "" {
		t.Run("SSE-KMS CLI output", func(t *testing.T) { awsEncryptionRun(t, e, "aws:kms", kms) })
	} else {
		t.Log("AWS SSE-KMS NOT RUN: VALET_AWS_KMS_KEY_ID absent")
	}
}
func awsEncryptionRun(t *testing.T, e *objectEnv, mode, kms string) {
	t.Helper()
	root := t.TempDir()
	destination := "encrypted-" + strings.ReplaceAll(mode, ":", "-")
	p := &pipeline{
		Source: sourceSpec{URIs: []string{e.uri("input.csv")}, S3: e.s3("")},
		Input:  inputSpec{Type: "csv", Mode: "raw_strings", NullValues: nulls("")},
		Output: outputSpec{
			URI:    e.uri(destination),
			Format: "csv",
			S3:     &s3Spec{Region: e.region, SSE: mode, KMSKeyID: kms},
		},
	}
	path := p.with(t, rulesA).write(t, filepath.Join(root, "pipeline.yml"))
	_, stderr, code := invoke(t, "validate", "-p", path)
	if code != exitValidationFailed {
		t.Fatalf("encryption CLI exit = %d, want %d; stderr=%s", code, exitValidationFailed, stderr)
	}
	manifests := 0
	for _, key := range e.keys(t, destination) {
		head, err := e.client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(e.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
		if head.ServerSideEncryption != types.ServerSideEncryption(mode) {
			t.Errorf("%s encryption = %s, want %s", key, head.ServerSideEncryption, mode)
		}
		if strings.HasSuffix(key, "/manifest.json") {
			manifests++
		}
	}
	if manifests != 1 {
		t.Errorf("encrypted manifests = %d, want 1", manifests)
	}
}
