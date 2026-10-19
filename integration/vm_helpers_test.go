//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// vmInterval is a half-open backend byte interval, never a reader-cache interval.
type vmInterval struct{ Start, End int64 }

func (a vmInterval) intersects(b vmInterval) bool { return a.Start < b.End && b.Start < a.End }

type vmParquetDescriptor struct {
	Size         int64
	Footer       vmInterval
	FooterSHA256 string
	Groups       [][]vmInterval
	Metadata     *metadata.FileMetaData
}

func vmInspectParquet(t testing.TB, path string) vmParquetDescriptor {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size := stat.Size()
	if size < 12 {
		t.Fatalf("short Parquet fixture %s", path)
	}
	var header [4]byte
	var trailer [8]byte
	if _, err = f.ReadAt(header[:], 0); err != nil {
		t.Fatal(err)
	}
	if _, err = f.ReadAt(trailer[:], size-8); err != nil {
		t.Fatal(err)
	}
	if string(header[:]) != "PAR1" || string(trailer[4:]) != "PAR1" {
		t.Fatalf("invalid fixture magic: %s", path)
	}
	n := int64(binary.LittleEndian.Uint32(trailer[:4]))
	start := size - 8 - n
	if start < 4 {
		t.Fatalf("invalid footer length %d", n)
	}
	footer := make([]byte, n)
	if _, err = f.ReadAt(footer, start); err != nil {
		t.Fatal(err)
	}
	m, err := metadata.NewFileMetaData(footer, nil)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(footer)
	d := vmParquetDescriptor{Size: size, Footer: vmInterval{start, size}, FooterSHA256: hex.EncodeToString(sum[:]), Metadata: m}
	for _, g := range m.RowGroups {
		var chunks []vmInterval
		for _, c := range g.Columns {
			x := c.MetaData
			off := x.DataPageOffset
			if x.DictionaryPageOffset != nil && *x.DictionaryPageOffset < off {
				off = *x.DictionaryPageOffset
			}
			chunks = append(chunks, vmInterval{off, off + x.TotalCompressedSize})
		}
		d.Groups = append(d.Groups, chunks)
	}
	return d
}
func vmWriteParquet(t testing.TB, path string, schema *arrow.Schema, groups []arrow.RecordBatch, props ...parquet.WriterProperty) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(props...), pqarrow.DefaultWriterProps())
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	for _, r := range groups {
		if err = w.Write(r); err != nil {
			_ = w.Close()
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
}

// vmVisitParquet independently streams Arrow record batches instead of loading a table.
func vmVisitParquet(t testing.TB, path string, visit func(arrow.RecordBatch, int)) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := file.NewParquetReader(f)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	defer reader.Close()
	ar, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 4096}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	records, err := ar.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer records.Release()
	for records.Next() {
		record := records.RecordBatch()
		for i := range int(record.NumRows()) {
			visit(record, i)
		}
	}
	if err := records.Err(); err != nil {
		t.Fatal(err)
	}
}
func vmRewriteFooter(t testing.TB, path string, change func(*metadata.FileMetaData)) {
	t.Helper()
	d := vmInspectParquet(t, path)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	prefix := append([]byte(nil), body[:d.Footer.Start]...)
	change(d.Metadata)
	footer, err := d.Metadata.Serialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	out := append(prefix, footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	out = append(out, []byte("PAR1")...)
	if !bytes.Equal(body[:d.Footer.Start], out[:d.Footer.Start]) {
		t.Fatal("footer rewrite changed payload")
	}
	if err = os.WriteFile(path, out, 0600); err != nil {
		t.Fatal(err)
	}
}

const vmOutConfig = `input:
  on_type_error: row_error
  schema:
    columns:
      id: {type: string}
      amount: {type: int64}
      status: {type: string}
rules:
  - code: POSITIVE
    level: error
    column: amount
    checks: [{type: valid_range, min: 0, exclusive_min: true}]
  - code: STATUS_OK
    level: warn
    column: status
    checks: [{type: equals, value: ok}]
`
const vmStatConfig = `input:
  schema:
    columns:
      amount: {type: int64}
rules:
  - code: POSITIVE
    level: error
    column: amount
    checks: [{type: valid_range, min: 0, exclusive_min: true}]
  - code: NOT_NULL
    level: error
    column: amount
    checks: [{type: not_null}]
`

var vmOutLiterals = [][3]string{{"A", "10", "ok"}, {"B", "-1", "ok"}, {"C", "5", "pending"}, {"D", "-2", "pending"}, {"E", "0", "ok"}, {"F", "oops", "ok"}, {"G", "7", "ok"}, {"H", "2", "pending"}}

func vmFixtureOUT(t testing.TB, root, inputType string) []string {
	t.Helper()
	var paths []string
	for source := range 2 {
		path := filepath.Join(root, fmt.Sprintf("out-%d.%s", source, inputType))
		rows := vmOutLiterals[source*4 : source*4+4]
		switch inputType {
		case "csv":
			var b bytes.Buffer
			w := csv.NewWriter(&b)
			_ = w.Write([]string{"id", "amount", "status"})
			for _, row := range rows {
				_ = w.Write(row[:])
			}
			w.Flush()
			if err := w.Error(); err != nil {
				t.Fatal(err)
			}
			put(t, path, b.String())
		case "json", "ndjson":
			objects := make([]map[string]string, 0, 4)
			for _, row := range rows {
				objects = append(objects, map[string]string{"id": row[0], "amount": row[1], "status": row[2]})
			}
			var b bytes.Buffer
			enc := json.NewEncoder(&b)
			if inputType == "json" {
				if err := enc.Encode(objects); err != nil {
					t.Fatal(err)
				}
			} else {
				for _, row := range objects {
					if err := enc.Encode(row); err != nil {
						t.Fatal(err)
					}
				}
			}
			put(t, path, b.String())
		case "parquet":
			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}, {Name: "amount", Type: arrow.BinaryTypes.String}, {Name: "status", Type: arrow.BinaryTypes.String}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			for _, row := range rows {
				for c := range row {
					b.Field(c).(*array.StringBuilder).Append(row[c])
				}
			}
			r := b.NewRecordBatch()
			b.Release()
			vmWriteParquet(t, path, schema, []arrow.RecordBatch{r})
			r.Release()
		default:
			t.Fatalf("unknown F-OUT type %q", inputType)
		}
		paths = append(paths, path)
	}
	return paths
}
func vmIntFixture(t testing.TB, root, name string, groups [][]*int64) string {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	var records []arrow.RecordBatch
	for _, g := range groups {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		for _, x := range g {
			if x == nil {
				b.AppendNull()
			} else {
				b.Append(*x)
			}
		}
		a := b.NewArray()
		b.Release()
		r := array.NewRecordBatch(schema, []arrow.Array{a}, int64(len(g)))
		a.Release()
		records = append(records, r)
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()
	path := filepath.Join(root, name+".parquet")
	vmWriteParquet(t, path, schema, records, parquet.WithDictionaryDefault(false))
	return path
}
func vmFixtureFAST(t testing.TB, root string) string {
	return vmIntFixture(t, root, "fast", [][]*int64{{new(int64(1)), new(int64(2)), new(int64(3)), new(int64(4))}, {new(int64(5)), new(int64(6)), new(int64(7)), new(int64(8))}})
}
func vmFixtureSTAT(t testing.TB, root string) string {
	path := vmIntFixture(t, root, "stat", [][]*int64{{new(int64(1)), new(int64(2)), new(int64(3)), new(int64(4))}, {new(int64(5)), nil, new(int64(6)), new(int64(7))}, {new(int64(-1)), new(int64(0)), new(int64(8)), new(int64(9))}, {new(int64(10)), new(int64(11)), new(int64(12)), new(int64(13))}})
	vmRewriteFooter(t, path, func(m *metadata.FileMetaData) { m.RowGroups[3].Columns[0].MetaData.Statistics = nil })
	return path
}
func vmFixtureLIE(t testing.TB, root string) string {
	path := vmIntFixture(t, root, "lie", [][]*int64{{nil, new(int64(-1)), new(int64(11)), new(int64(12))}})
	var observed []string
	vmVisitParquet(t, path, func(r arrow.RecordBatch, i int) {
		a := r.Column(0)
		if a.IsNull(i) {
			observed = append(observed, "null")
		} else {
			observed = append(observed, a.ValueStr(i))
		}
	})
	if strings.Join(observed, ",") != "null,-1,11,12" {
		t.Fatalf("F-LIE payload=%v", observed)
	}
	vmRewriteFooter(t, path, func(m *metadata.FileMetaData) {
		s := m.RowGroups[0].Columns[0].MetaData.Statistics
		s.NullCount = new(int64(0))
		s.MinValue = binary.LittleEndian.AppendUint64(nil, 10)
		s.MaxValue = binary.LittleEndian.AppendUint64(nil, 20)
		s.Min = nil
		s.Max = nil
	})
	observed = nil
	vmVisitParquet(t, path, func(r arrow.RecordBatch, i int) {
		a := r.Column(0)
		if a.IsNull(i) {
			observed = append(observed, "null")
		} else {
			observed = append(observed, a.ValueStr(i))
		}
	})
	if strings.Join(observed, ",") != "null,-1,11,12" {
		t.Fatalf("rewritten F-LIE payload=%v", observed)
	}
	return path
}

// vmFixtureSCHEMA produces a payload marker with caller-selected independently typed fields.
func vmFixtureSCHEMA(t testing.TB, root, name string, schema *arrow.Schema) string {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i, f := range schema.Fields() {
		if f.Type.ID() == arrow.STRING {
			b.Field(i).(*array.StringBuilder).Append("VM_PRIVATE_PAYLOAD_MARKER")
		} else {
			b.Field(i).AppendNull()
		}
	}
	r := b.NewRecordBatch()
	defer r.Release()
	path := filepath.Join(root, name+".parquet")
	vmWriteParquet(t, path, schema, []arrow.RecordBatch{r})
	return path
}
func vmJSON(t testing.TB, body []byte, v any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		t.Fatalf("JSON: %v: %s", err, body)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		t.Fatalf("trailing JSON/log bytes: %v", err)
	}
}

type vmReference struct {
	URI    string `json:"uri"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	Count  uint64 `json:"count"`
}
type vmArtifact struct {
	vmReference
	Kind          string `json:"kind"`
	Format        string `json:"format"`
	ReportVersion int    `json:"report_version"`
}

func vmResolve(t testing.TB, r *runResult, uri string) string {
	t.Helper()
	dir := r.dir(t)
	if strings.HasPrefix(uri, "s3://") {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		marker := "/" + filepath.Base(dir) + "/"
		_, rel, ok := strings.Cut(u.Path, marker)
		if !ok {
			t.Fatalf("reference outside run: %s", uri)
		}
		return filepath.Join(dir, filepath.FromSlash(rel))
	}
	if !filepath.IsAbs(uri) {
		return filepath.Join(dir, filepath.FromSlash(uri))
	}
	return uri
}
func vmReadReference(t testing.TB, r *runResult, ref vmReference) []byte {
	t.Helper()
	body, err := os.ReadFile(vmResolve(t, r, ref.URI))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if int64(len(body)) != ref.Size || hex.EncodeToString(sum[:]) != ref.SHA256 {
		t.Fatalf("invalid size/hash for %s", ref.URI)
	}
	return body
}
func vmArtifacts(t testing.TB, r *runResult) []vmArtifact {
	t.Helper()
	var manifest struct {
		Artifacts vmReference `json:"artifacts"`
	}
	decodeFile(t, r.manifests[0], &manifest)
	body := vmReadReference(t, r, manifest.Artifacts)
	var out []vmArtifact
	d := json.NewDecoder(bytes.NewReader(body))
	for {
		var a vmArtifact
		if err := d.Decode(&a); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	if uint64(len(out)) != manifest.Artifacts.Count {
		t.Fatalf("artifact index count=%d want %d", len(out), manifest.Artifacts.Count)
	}
	return out
}
func vmRows(t testing.TB, r *runResult, bucket string) []map[string]string {
	t.Helper()
	var rows []map[string]string
	for _, a := range vmArtifacts(t, r) {
		if a.Kind != bucket {
			continue
		}
		path := vmResolve(t, r, a.URI)
		switch a.Format {
		case "parquet":
			vmVisitParquet(t, path, func(rec arrow.RecordBatch, i int) {
				row := map[string]string{}
				for j, f := range rec.Schema().Fields() {
					v := rec.Column(j)
					if v.IsNull(i) {
						row[f.Name] = `\N`
					} else {
						row[f.Name] = v.ValueStr(i)
					}
				}
				rows = append(rows, row)
			})
		case "csv":
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			rd := csv.NewReader(f)
			header, err := rd.Read()
			if err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			for {
				record, err := rd.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					_ = f.Close()
					t.Fatal(err)
				}
				row := map[string]string{}
				for i, key := range header {
					row[key] = record[i]
				}
				rows = append(rows, row)
			}
			_ = f.Close()
		default:
			t.Fatalf("unsupported row format %s", a.Format)
		}
	}
	return rows
}
func vmEvidence(t testing.TB, r *runResult) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, a := range vmArtifacts(t, r) {
		if a.Kind != "evidence" {
			continue
		}
		body := vmReadReference(t, r, a.vmReference)
		d := json.NewDecoder(bytes.NewReader(body))
		d.UseNumber()
		for {
			var item map[string]any
			if err := d.Decode(&item); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			out = append(out, item)
		}
	}
	return out
}
func vmAssertArtifacts(t testing.TB, r *runResult) {
	t.Helper()
	if len(r.manifests) != 1 {
		t.Fatalf("exit=%d manifests=%v stderr=%s", r.code, r.manifests, r.stderr)
	}
	if r.summary.ReportVersion != 2 {
		t.Fatalf("report_version=%d", r.summary.ReportVersion)
	}
	var manifest map[string]json.RawMessage
	decodeFile(t, r.manifests[0], &manifest)
	var version int
	vmJSON(t, manifest["report_version"], &version)
	if version != 2 {
		t.Fatalf("manifest report_version=%d", version)
	}
	for _, key := range []string{"inputs", "rules", "artifacts", "summary"} {
		var ref vmReference
		vmJSON(t, manifest[key], &ref)
		body := vmReadReference(t, r, ref)
		if key == "inputs" || key == "artifacts" {
			records := vmNDJSON(t, body)
			if uint64(len(records)) != ref.Count {
				t.Fatalf("%s records=%d count=%d", key, len(records), ref.Count)
			}
			if key == "inputs" {
				for i, record := range records {
					if fmt.Sprint(record["report_version"]) != "2" || fmt.Sprint(record["source_ordinal"]) != strconv.Itoa(i+1) {
						t.Fatalf("input version/order: %v", record)
					}
					for _, field := range []string{"identity_method", "payload_verification", "ingestion_verification"} {
						if record[field] == nil || record[field] == "" {
							t.Fatalf("input lacks %s: %v", field, record)
						}
					}
				}
			}
		} else {
			var document any
			vmJSON(t, body, &document)
		}
	}
	summaryJSON, err := json.Marshal(r.summary)
	if err != nil {
		t.Fatal(err)
	}
	var summaryFields map[string]json.RawMessage
	vmJSON(t, summaryJSON, &summaryFields)
	for _, key := range []string{"output_mode", "verification", "trust_policy", "coverage", "row_output"} {
		var x, y any
		vmJSON(t, manifest[key], &x)
		vmJSON(t, summaryFields[key], &y)
		a, _ := json.Marshal(x)
		b, _ := json.Marshal(y)
		if !bytes.Equal(a, b) {
			t.Fatalf("manifest/summary %s disagree: %s != %s", key, a, b)
		}
	}
	referenced := map[string]bool{}
	for _, a := range vmArtifacts(t, r) {
		if a.ReportVersion != 2 {
			t.Fatalf("artifact version=%d", a.ReportVersion)
		}
		body := vmReadReference(t, r, a.vmReference)
		referenced[vmResolve(t, r, a.URI)] = true
		var n uint64
		switch a.Format {
		case "ndjson":
			n = uint64(len(vmNDJSON(t, body)))
		case "csv":
			rd := csv.NewReader(bytes.NewReader(body))
			if _, err := rd.Read(); err != nil {
				t.Fatal(err)
			}
			for {
				_, err := rd.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				n++
			}
		case "parquet":
			vmVisitParquet(t, vmResolve(t, r, a.URI), func(arrow.RecordBatch, int) { n++ })
		default:
			t.Fatalf("unknown artifact format %q", a.Format)
		}
		if n != a.Count {
			t.Fatalf("artifact %s records=%d count=%d", a.URI, n, a.Count)
		}
	}
	for _, bucket := range []string{"pass", "warn", "error", "rejects", "issues", "evidence"} {
		err := filepath.WalkDir(filepath.Join(r.dir(t), bucket), func(path string, d os.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if !d.IsDir() && !referenced[path] {
				t.Errorf("unreferenced artifact %s", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
func vmNDJSON(t testing.TB, body []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(body), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var record map[string]any
		vmJSON(t, line, &record)
		records = append(records, record)
	}
	return records
}

// Descriptors are source-ordered; the serialized footer digest excludes its trailer.
func vmAssertEvidence(t testing.TB, r *runResult, descriptors ...vmParquetDescriptor) {
	t.Helper()
	if r.summary.Coverage.MetadataClaimedRows > 0 {
		want := "partial"
		if r.summary.Coverage.ValueRows == 0 {
			want = "not_evaluated"
		}
		if r.summary.IngestionVerification != want {
			t.Fatalf("ingestion_verification=%q want %q for value_rows=%d metadata_claimed_rows=%d",
				r.summary.IngestionVerification, want, r.summary.Coverage.ValueRows, r.summary.Coverage.MetadataClaimedRows)
		}
	}
	ledger := vmEvidence(t, r)
	next := 0
	for source, d := range descriptors {
		for group, g := range d.Metadata.RowGroups {
			if next >= len(ledger) {
				t.Fatal("missing group evidence")
			}
			e := ledger[next]
			next++
			for key, want := range map[string]string{
				"source_ordinal": strconv.Itoa(source + 1), "row_group": strconv.Itoa(group),
				"declared_rows": strconv.FormatInt(g.NumRows, 10),
				"footer_offset": strconv.FormatInt(d.Footer.Start, 10),
				"footer_length": strconv.FormatInt(d.Footer.End-d.Footer.Start-8, 10),
				"footer_sha256": d.FooterSHA256,
			} {
				if fmt.Sprint(e[key]) != want {
					t.Fatalf("evidence %s=%v want %s", key, e[key], want)
				}
			}
			if e["identity_method"] == "" || e["identity_method"] == nil {
				t.Fatal("evidence lacks identity method")
			}
			identity, ok := e["identity"].(map[string]any)
			if !ok || len(identity) == 0 {
				t.Fatal("evidence lacks pinned identity")
			}
			if e["action"] != "skip" && e["action"] != "scan" {
				t.Fatalf("unknown action %v", e["action"])
			}
			if e["action"] == "skip" {
				ingestion, ingestionPresent := e["ingestion_verification"].(string)
				payload, payloadPresent := e["payload_verification"].(string)
				if !ingestionPresent || !payloadPresent || ingestion == "" || payload == "" {
					t.Fatal("skipped group lacks explicit ingestion/payload non-verification")
				}
				if ingestion == "full" || ingestion == "values" || payload == "full" || payload == "decoded" {
					t.Fatal("skipped payload falsely verified")
				}
				checks, ok := e["checks"].([]any)
				if !ok {
					t.Fatal("skip lacks check proof list")
				}
				for _, raw := range checks {
					check, ok := raw.(map[string]any)
					if !ok || check["claim"] != "pass" {
						t.Fatalf("skip contains non-pass proof %v", raw)
					}
					fields, ok := check["fields"].([]any)
					if !ok || len(fields) == 0 {
						t.Fatalf("skip lacks evidence field names %v", raw)
					}
				}
			}
		}
	}
	if next != len(ledger) {
		t.Fatalf("extra evidence groups: got %d want %d", len(ledger), next)
	}
}

type vmRequest struct {
	Method, Path, Range, IfMatch, VersionID, Operation string
	Status                                             int
	Bytes                                              int64
}
type vmProxy struct {
	URL string
	// Before returns true only when it handled the request; false forwards to MinIO.
	Before   func(http.ResponseWriter, *http.Request) bool
	After    func(*http.Response) error
	mu       sync.Mutex
	requests []vmRequest
	denials  map[string][]vmInterval
	sizes    map[string]int64
}
type vmCountingWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *vmCountingWriter) WriteHeader(n int) {
	if n >= 100 && n < 200 {
		w.ResponseWriter.WriteHeader(n)
		return
	}
	if w.status != 0 {
		return
	}
	w.status = n
	w.ResponseWriter.WriteHeader(n)
}
func (w *vmCountingWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, e := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, e
}
func (w *vmCountingWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func vmNewProxy(t *testing.T, e *objectEnv) *vmProxy {
	t.Helper()
	target, err := url.Parse(e.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	p := &vmProxy{denials: map[string][]vmInterval{}, sizes: map[string]int64{}}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.ModifyResponse = func(r *http.Response) error {
		if p.After != nil {
			return p.After(r)
		}
		return nil
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		x := vmRequest{Method: r.Method, Path: r.URL.Path, Range: r.Header.Get("Range"), IfMatch: r.Header.Get("If-Match"), VersionID: r.URL.Query().Get("versionId"), Operation: vmOperation(r)}
		cw := &vmCountingWriter{ResponseWriter: w}
		defer func() {
			x.Status = cw.status
			if x.Status == 0 {
				x.Status = 200
			}
			x.Bytes = cw.bytes
			p.mu.Lock()
			p.requests = append(p.requests, x)
			p.mu.Unlock()
		}()
		p.mu.Lock()
		ranges := append([]vmInterval(nil), p.denials[r.URL.Path]...)
		size := p.sizes[r.URL.Path]
		p.mu.Unlock()
		if r.Method == "GET" && len(ranges) > 0 {
			interval, ok := vmHTTPRange(r.Header.Get("Range"), size)
			deny := !ok
			for _, bad := range ranges {
				deny = deny || interval.intersects(bad)
			}
			if deny {
				http.Error(cw, "forbidden payload interval", http.StatusForbidden)
				return
			}
		}
		if p.Before != nil && p.Before(cw, r) {
			return
		}
		forward.ServeHTTP(cw, r)
	}))
	p.URL = server.URL
	t.Cleanup(server.Close)
	return p
}
func vmOperation(r *http.Request) string {
	q := r.URL.Query()
	switch {
	case q.Has("uploads") && r.Method == "POST":
		return "multipart.create"
	case q.Has("uploadId") && r.Method == "DELETE":
		return "multipart.abort"
	case q.Has("uploadId") && r.Method == "POST":
		return "multipart.complete"
	case q.Has("partNumber"):
		return "multipart.part"
	default:
		return r.Method
	}
}
func vmHTTPRange(value string, size int64) (vmInterval, bool) {
	if value == "" {
		return vmInterval{0, size}, true
	}
	s, ok := strings.CutPrefix(value, "bytes=")
	if !ok || strings.Contains(s, ",") {
		return vmInterval{}, false
	}
	a, b, ok := strings.Cut(s, "-")
	if !ok {
		return vmInterval{}, false
	}
	if a == "" {
		n, e := strconv.ParseInt(b, 10, 64)
		return vmInterval{max(0, size-n), size}, e == nil && n > 0
	}
	start, e := strconv.ParseInt(a, 10, 64)
	if e != nil {
		return vmInterval{}, false
	}
	end := size
	if b != "" {
		n, e := strconv.ParseInt(b, 10, 64)
		if e != nil || n == int64(^uint64(0)>>1) {
			return vmInterval{}, false
		}
		end = n + 1
	}
	return vmInterval{start, end}, start >= 0 && end > start && end <= size
}
func (p *vmProxy) DenyPayload(path string, d vmParquetDescriptor, groups ...int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sizes[path] = d.Size
	if len(groups) == 0 {
		p.denials[path] = []vmInterval{{4, d.Footer.Start}}
		return
	}
	for _, g := range groups {
		p.denials[path] = append(p.denials[path], d.Groups[g]...)
	}
}
func (p *vmProxy) Requests() []vmRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]vmRequest(nil), p.requests...)
}

type vmBarrier struct {
	Entered     <-chan struct{}
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func vmNewBarrier() *vmBarrier {
	c := make(chan struct{})
	return &vmBarrier{Entered: c, entered: c, release: make(chan struct{})}
}
func (b *vmBarrier) Release() { b.releaseOnce.Do(func() { close(b.release) }) }
func (b *vmBarrier) Block(ctx context.Context) error {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (b *vmBarrier) Wait(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	select {
	case <-b.Entered:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForProcess also detects CLI startup/argument failures before the barrier.
// An early-exit error means done was consumed; callers must not wait a second time.
func (b *vmBarrier) WaitForProcess(ctx context.Context, done <-chan error) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	select {
	case <-b.Entered:
		return nil
	case err := <-done:
		return fmt.Errorf("CLI exited before reaching backend barrier: %v", err)
	case <-ctx.Done():
		return fmt.Errorf("backend barrier not reached: %w", ctx.Err())
	}
}

// vmSortedKeys permits exact object absence assertions without assuming part names.
func vmSortedKeys(e *objectEnv, t testing.TB, prefix string) []string {
	keys := e.keys(t, prefix)
	sort.Strings(keys)
	return keys
}
