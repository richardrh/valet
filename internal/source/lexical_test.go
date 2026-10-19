package source

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// pointerColumn declares one projected column for walker tests.
type pointerColumn struct{ name, path string }

// decodeRecord projects a single JSON object through the production walker.
func decodeRecord(raw string, columns []pointerColumn, policy jsonPolicy) (lexicalRecord, error) {
	root := &projection{}
	names := make([]string, len(columns))
	for i, c := range columns {
		root.insert(jsontext.Pointer(c.path), i)
		names[i] = c.name
	}
	dec := jsontext.NewDecoder(strings.NewReader(raw))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return lexicalRecord{}, errors.New("not an object")
	}
	w := jsonWalker{dec: dec, root: root, columns: names, maxDepth: 64, policy: policy}
	rec, err := w.record()
	if err != nil {
		return rec, err
	}
	if _, err = dec.ReadToken(); !errors.Is(err, io.EOF) {
		return rec, errors.New("trailing data")
	}
	return rec, nil
}

var idColumn = []pointerColumn{{name: "id", path: "/id"}}

func TestJSONPointerEscapesAndArrayIndices(t *testing.T) {
	// "/a~1b/~0key/1" is RFC 6901 for key "a/b", then key "~key", then index 1.
	raw := `{"a/b":{"~key":[0,9007199254740993]},"unused":{"x":true}}`
	rec, err := decodeRecord(raw, []pointerColumn{{name: "value", path: "/a~1b/~0key/1"}}, jsonPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.values[0].text; got != "9007199254740993" {
		t.Errorf("projected text = %q, want %q", got, "9007199254740993")
	}
	if got := rec.values[0].kind; got != kindNumber {
		t.Errorf("projected kind = %v, want %v", got, kindNumber)
	}
	if want := []string{"/unused"}; !slices.Equal(rec.extras, want) {
		t.Errorf("extras = %v, want %v", rec.extras, want)
	}
}

func TestJSONExtraPathsAreEscapedPointers(t *testing.T) {
	raw := `{"id":1,"nest":{"a/b":{"~":1}}}`
	columns := []pointerColumn{{name: "id", path: "/id"}, {name: "x", path: "/nest/x"}}
	rec, err := decodeRecord(raw, columns, jsonPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	// "/" inside a key is escaped as "~1"; reporting stops at the first undeclared key.
	if want := []string{"/nest/a~1b"}; !slices.Equal(rec.extras, want) {
		t.Errorf("extras = %v, want %v", rec.extras, want)
	}
}

func TestJSONIgnoredLongAncestorDoesNotExpandPaths(t *testing.T) {
	// One undeclared 16KiB key holding 128 children, then the selected id.
	var raw strings.Builder
	raw.WriteString(`{"` + strings.Repeat("x", 16<<10) + `":{`)
	for i := range 128 {
		if i > 0 {
			raw.WriteByte(',')
		}
		raw.WriteString(`"` + string(rune('A'+i/26)) + string(rune('a'+i%26)) + `":0`)
	}
	raw.WriteString(`},"id":1}`)
	// Expanding every child path would reserve 128 copies of the 16KiB key.
	const reserveCeiling = 128 << 10

	for _, tc := range []struct {
		name         string
		ignoreExtras bool
		maxReserved  int64
		wantExtras   int
	}{
		{name: "ignored extras reserve and retain nothing", ignoreExtras: true, maxReserved: 0, wantExtras: 0},
		{
			name:         "reported extras keep the subtree as one path",
			ignoreExtras: false,
			maxReserved:  reserveCeiling,
			wantExtras:   1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reserved int64
			policy := jsonPolicy{ignoreExtras: tc.ignoreExtras, reserve: func(n int64) error {
				reserved += n
				if reserved > reserveCeiling {
					return errors.New("path reservation unexpectedly amplified")
				}
				return nil
			}}
			rec, err := decodeRecord(raw.String(), idColumn, policy)
			if err != nil {
				t.Fatal(err)
			}
			if got := rec.values[0].text; got != "1" {
				t.Errorf("id = %q, want %q", got, "1")
			}
			if reserved > tc.maxReserved {
				t.Errorf("reserved = %d bytes, want <= %d", reserved, tc.maxReserved)
			}
			if got := len(rec.extras); got != tc.wantExtras {
				t.Errorf("extra paths = %d, want %d", got, tc.wantExtras)
			}
		})
	}
}

func TestJSONPathReservationPrecedesRetention(t *testing.T) {
	refused := errors.New("path budget refused")
	_, err := decodeRecord(`{"unexpected":1}`, idColumn, jsonPolicy{reserve: func(int64) error { return refused }})
	if !errors.Is(err, refused) {
		t.Fatalf("decode error = %v, want %v", err, refused)
	}
}

func TestJSONSelectedObjectDoesNotCascadeExtras(t *testing.T) {
	rec, err := decodeRecord(`{"id":{"nested":{"unexpected":1}}}`, idColumn, jsonPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.values[0].kind; got != kindObject {
		t.Errorf("id kind = %v, want %v", got, kindObject)
	}
	if len(rec.extras) != 0 {
		t.Errorf("extras = %v, want none (selected value's children are not extras)", rec.extras)
	}
}

func TestIgnoredJSONStillRejectsDuplicateKeys(t *testing.T) {
	raw := `{"ignored":{"duplicate":1,"duplicate":2}}`
	if _, err := decodeRecord(raw, idColumn, jsonPolicy{ignoreExtras: true}); err == nil {
		t.Fatal("decode error = nil, want duplicate key error")
	}
}

func TestJSONDepthLimitAppliesToIgnoredValues(t *testing.T) {
	// 65 nested arrays inside the root object exceed maxDepth 64.
	raw := `{"ignored":` + strings.Repeat("[", 65) + strings.Repeat("]", 65) + `}`
	if _, err := decodeRecord(raw, idColumn, jsonPolicy{ignoreExtras: true}); err == nil {
		t.Fatal("decode error = nil, want max_depth error")
	}
}

func TestRecordLimiterStopsAtLimit(t *testing.T) {
	l := &recordLimiter{r: strings.NewReader(strings.Repeat("x", 100)), limit: 10}
	n, err := io.Copy(io.Discard, l)
	if err == nil {
		t.Error("copy error = nil, want limit error")
	}
	if !l.exceeded {
		t.Error("exceeded = false, want true")
	}
	// One byte past the limit is read to detect the overflow.
	if n != 11 {
		t.Errorf("bytes read = %d, want 11", n)
	}
	l.startRecord(n)
	if _, err = io.CopyN(io.Discard, l, 10); err != nil {
		t.Fatalf("next record copy error = %v, want nil", err)
	}
}

// fuzzDataset is the minimal dataset the lexical inputs need.
func fuzzDataset(kind inputKind, explicit bool) *Dataset {
	d := &Dataset{
		cfg:         &config.Config{},
		kind:        kind,
		budget:      resource.New(64 << 20),
		recordLimit: 4096,
		batchTarget: 4096,
		maxDepth:    8,
	}
	if explicit {
		d.cfg.Input.Schema = &config.Schema{}
		d.projection = &projection{}
		d.projection.insert("/id", 0)
		d.projection.insert("/nested/0/x", 1)
		d.selectionNames = []string{"id", "x"}
	}
	return d
}

func fuzzStream(t *testing.T, d *Dataset, input []byte, fixed int64, tape bool) *textStream {
	t.Helper()
	obj := storage.Object{URI: "fuzz", Size: int64(len(input))}
	src, err := newTextStream(d, obj, io.NopCloser(bytes.NewReader(input)), fixed, tape)
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func FuzzJSONInput(f *testing.F) {
	f.Add([]byte(`[{"id":9007199254740993},{"nested":[{"x":"y"}]}]`), true, true)
	f.Add([]byte("{\"id\":null}\n\n{\"id\":[1,2]}\n"), false, true)
	f.Add([]byte("{\"a\":1,\"b\":\"x\"}\n{\"a\":2}"), false, false)
	f.Add([]byte(`[{"a":1,"a":2}]`), true, false)
	f.Fuzz(func(t *testing.T, input []byte, array, explicit bool) {
		kind := inputNDJSON
		if array {
			kind = inputJSON
		}
		d := fuzzDataset(kind, explicit)
		j, err := newJSONInput(d, fuzzStream(t, d, input, jsonDecoderFixedBytes, true))
		if err != nil {
			t.Fatal(err)
		}
		var previous uint64
		for range 1000 {
			rec, err := j.next(context.Background())
			if err != nil {
				break
			}
			if !jsontext.Value(rec.raw).IsValid() || rec.raw[0] != '{' {
				t.Fatalf("raw record = %q, want one JSON object", rec.raw)
			}
			if int64(len(rec.raw)) > d.recordLimit {
				t.Fatalf("record size = %d bytes, want <= %d", len(rec.raw), d.recordLimit)
			}
			if rec.line < previous {
				t.Fatalf("record line = %d after line %d, want non-decreasing", rec.line, previous)
			}
			// NDJSON allows one record per line; array elements may share a line.
			if !array && rec.line == previous {
				t.Fatalf("NDJSON record line = %d, same as previous record", rec.line)
			}
			previous = rec.line
		}
		if err := j.close(); err != nil {
			t.Fatal(err)
		}
		if n := d.budget.Used(); n != 0 {
			t.Fatalf("budget used after close = %d, want 0", n)
		}
	})
}

func FuzzCSVInput(f *testing.F) {
	// BOM, CRLF record ends and a bare LF inside a quoted field.
	f.Add([]byte("\ufeffid,text\r\n1,\"a,b\"\r\n2,\"line1\nline2\"\r\n"), byte(','))
	f.Add([]byte("a;b\n1;\"x;y\"\n"), byte(';'))
	f.Add([]byte("a,b\n\n1,2\n"), byte(','))
	f.Fuzz(func(t *testing.T, input []byte, comma byte) {
		// Quote, line breaks and non-ASCII bytes are not usable delimiters.
		if comma >= utf8.RuneSelf || strings.ContainsRune("\"\r\n", rune(comma)) {
			comma = ','
		}
		d := fuzzDataset(inputCSV, false)
		defer func() {
			if n := d.budget.Used(); n != 0 {
				t.Errorf("budget used after close = %d, want 0", n)
			}
		}()
		c, err := newCSVInput(d, fuzzStream(t, d, input, textReadBufferBytes, false), csvOptions{comma: rune(comma)})
		if err != nil {
			return
		}
		var previous uint64
		for range 1000 {
			rec, err := c.next(context.Background())
			if err != nil {
				break
			}
			if len(rec.values) != len(c.header) {
				t.Fatalf("record fields = %d, want %d (header width)", len(rec.values), len(c.header))
			}
			if rec.line <= previous {
				t.Fatalf("record line = %d after line %d, want increasing", rec.line, previous)
			}
			for _, v := range rec.values {
				if !utf8.ValidString(v.text) {
					t.Fatalf("field %q is not valid UTF-8", v.text)
				}
			}
			previous = rec.line
		}
		if err = c.close(); err != nil {
			t.Fatal(err)
		}
	})
}
