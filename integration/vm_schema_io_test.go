//go:build integration

package integration_test

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestVMSchemaIO(t *testing.T) {
	t.Run("VM-DIFF-04", func(t *testing.T) {
		for _, collision := range []bool{false, true} {
			name := "rename"
			if collision {
				name = "collision"
			}
			t.Run(name, func(t *testing.T) {
				root := t.TempDir()
				fields := []arrow.Field{{Name: "old", Type: arrow.PrimitiveTypes.Int32}}
				if collision {
					fields = append(fields, arrow.Field{Name: "new", Type: arrow.PrimitiveTypes.Int64})
				}
				src := vmSchemaFile(t, filepath.Join(root, "rename.parquet"), fields)
				p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv").with(t, `transforms: [{type: rename_columns, mapping: {old: new}}]
input: {schema: {columns: {new: {type: int64}}}}
`)
				if collision {
					path := p.write(t, filepath.Join(root, "p.yml"))
					_, stderr, code := invoke(t, "schema", "-p", path)
					if code != 2 || len(stderr) == 0 {
						t.Fatalf("collision code=%d stderr=%s", code, stderr)
					}
					return
				}
				v, _ := vmSchemaInvoke(t, p, root, "json", 1)
				d := vmDifferences(t, v)
				if len(d) != 1 {
					t.Fatalf("rename differences %v", d)
				}
				m := d[0].(map[string]any)
				if m["column"] != "new" || m["source_name"] != "old" {
					t.Fatalf("rename provenance %v", m)
				}
				_, text := vmSchemaInvoke(t, p, root, "text", 1)
				if !strings.Contains(text, "old") || !strings.Contains(text, "new") {
					t.Fatal("text lost rename")
				}
			})
		}
	})
	t.Run("VM-DIFF-06", func(t *testing.T) {
		e := minioEnvironment(t)
		root := t.TempDir()
		src := vmFixtureFAST(t, root)
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		e.put(t, "source.parquet", string(raw))
		proxy := vmNewProxy(t, e)
		proxy.DenyPayload("/"+e.bucket+"/"+e.prefix+"source.parquet", vmInspectParquet(t, src))
		proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
			if strings.Contains(r.URL.Path, "reference") || r.Method == "PUT" || r.Method == "POST" || r.Method == "DELETE" {
				http.Error(w, "FORBIDDEN_REFERENCE_OUTPUT", 403)
				return true
			}
			return false
		}
		p := defaultPipeline(e.uri("source.parquet"), "parquet", e.uri("output/"), "parquet")
		p.Source.S3 = e.s3(proxy.URL)
		p.Output.S3 = e.s3(proxy.URL)
		p.with(t, fragment(t, obj{"rules": []obj{{"code": "REF", "column": "amount", "level": "error", "checks": []obj{{"type": "referential_integrity", "reference_file": e.uri("reference.csv"), "reference_column": "amount"}}}}}))
		for _, format := range []string{"json", "text"} {
			path := p.write(t, filepath.Join(root, "p.yml"))
			out, stderr, code := invoke(t, "schema", "-p", path, "--format", format)
			if code != 0 {
				t.Fatalf("metadata-only %d: %s", code, stderr)
			}
			if format == "json" {
				v := vmOneJSON(t, out)
				if v["rows_checked"].(interface{ String() string }).String() != "0" {
					t.Fatal("schema checked data")
				}
			}
		}
		for _, r := range proxy.Requests() {
			if strings.Contains(r.Path, "reference") || r.Method == "PUT" || r.Method == "POST" {
				t.Fatalf("forbidden request %+v", r)
			}
		}
		if keys := e.keys(t, "output/"); len(keys) != 0 {
			t.Fatalf("schema wrote %v", keys)
		}
	})
	t.Run("VM-DIFF-07", func(t *testing.T) {
		root := t.TempDir()
		src := vmFixtureFAST(t, root)
		d := vmInspectParquet(t, src)
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		footer := append([]byte(nil), raw[d.Footer.Start:]...)
		for _, group := range d.Groups {
			for _, r := range group {
				for i := r.Start; i < r.End; i++ {
					raw[i] = 0xff
				}
			}
		}
		if !bytes.Equal(footer, raw[d.Footer.Start:]) {
			t.Fatal("fixture changed footer")
		}
		if err = os.WriteFile(src, raw, 0600); err != nil {
			t.Fatal(err)
		}
		p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv")
		v, _ := vmSchemaInvoke(t, p, root, "json", 0)
		if v["rows_checked"].(interface{ String() string }).String() != "0" {
			t.Fatal("corrupt payload checked")
		}
		_, text := vmSchemaInvoke(t, p, root, "text", 0)
		if !strings.Contains(strings.ToLower(text), "not") && !strings.Contains(strings.ToLower(text), "unverified") {
			t.Fatal("missing unverified disclosure")
		}
		path := p.write(t, filepath.Join(root, "validate.yml"))
		_, stderr, code := invoke(t, "validate", "-p", path, "--json-output", "-")
		if code != 2 {
			t.Fatalf("full scan accepted corrupt payload: %d %s", code, stderr)
		}
	})
	t.Run("VM-DIFF-10/later-object", func(t *testing.T) {
		root := t.TempDir()
		src := vmFixtureFAST(t, root)
		p := defaultPipeline(src, "parquet", filepath.Join(root, "out"), "csv")
		p.Source.URIs = append(p.Source.URIs, filepath.Join(root, "missing-PAYLOAD_SECRET.parquet"))
		path := p.write(t, filepath.Join(root, "p.yml"))
		_, stderr, code := invoke(t, "schema", "-p", path, "--log-format", "json")
		if code != 2 {
			t.Fatalf("later source exit %d: %s", code, stderr)
		}
		if _, err := os.Stat(p.Output.URI); !os.IsNotExist(err) {
			t.Fatal("schema failure created data result")
		}
	})
}

func TestVMSchemaInvalidPresentationBeforeAccess(t *testing.T) {
	e := minioEnvironment(t)
	root := t.TempDir()
	proxy := vmNewProxy(t, e)
	proxy.Before = func(w http.ResponseWriter, r *http.Request) bool {
		http.Error(w, "NO_ACCESS_PERMITTED", 403)
		return true
	}
	p := defaultPipeline(e.uri("never-read.parquet"), "parquet", e.uri("never-write/"), "parquet")
	p.Source.S3 = e.s3(proxy.URL)
	p.Output.S3 = e.s3(proxy.URL)
	path := p.write(t, filepath.Join(root, "p.yml"))
	for _, format := range []string{"", "unknown"} {
		_, stderr, code := invoke(t, "schema", "-p", path, "--format="+format, "--log-format", "json")
		if code != 2 || !bytes.Contains(bytes.ToLower(stderr), []byte("format")) {
			t.Fatalf("VM-DIFF-08 invalid presentation exit=%d %s", code, stderr)
		}
	}
	if requests := proxy.Requests(); len(requests) != 0 {
		t.Fatalf("presentation configuration accessed storage: %v", requests)
	}
}
