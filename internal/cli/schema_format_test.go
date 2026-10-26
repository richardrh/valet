package cli

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/richardrh/valet/internal/source"
)

func TestSchemaTextEscapesNamesAndPreservesTypedAssociations(t *testing.T) {
	width, signed, precision, scale := 64, true, int32(19), int32(4)
	unit, timezone := "us", "Europe/Paris"
	actual := &source.SchemaAttributes{Type: "int64", BitWidth: &width, Signed: &signed}
	expected := &source.SchemaAttributes{Type: "decimal", Precision: &precision, Scale: &scale}
	uri := "https://user:secret@example.test/object?signature=secret#fragment"
	name := "列\n\x1b[31m"
	report := &source.SchemaReport{
		Compatible: false, ObjectsChecked: 2, DeclaredRows: 42,
		BaselineSource: uri, BaselineSourceOrdinal: 1,
		Objects: []source.SchemaObject{
			{SourceOrdinal: 1, URI: uri, Fields: []source.SchemaField{{Name: "time", Attributes: &source.SchemaAttributes{Type: "timestamp", Unit: &unit, Timezone: &timezone}}}},
			{SourceOrdinal: 2, URI: "second.parquet", Fields: []source.SchemaField{{Name: name, SourceName: "raw\rname", Attributes: actual}}},
		},
		Differences: []source.SchemaDifference{{SourceOrdinal: 2, Source: "second.parquet", Column: name,
			SourceName: "raw\rname", Kind: "type_mismatch", Severity: "warn", RuleCode: "TYPE\nRULE",
			ExpectationSource: "rule", Expected: expected, Actual: actual, Reason: "TYPE_MISMATCH"}},
		NotEvaluated: []source.SchemaCheck{{Code: "REF", Type: "referential_integrity", Reason: "no references are loaded"}},
	}
	var out bytes.Buffer
	if err := writeSchemaText(&out, report); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"Incompatible schema", "Rows checked: 0", "not_evaluated", "unverified",
		"first object", "not a user-declared schema", "[warn] type_mismatch", `rule="TYPE\nRULE"`,
		`original source name "raw\rname"`, `列\n\x1b[31m`, `type="decimal", precision=19, scale=4`,
		`type="int64", bit_width=64, signed=true`, `unit="us", timezone="Europe/Paris"`,
		`"referential_integrity"`, "no references are loaded", "does not establish non-null values"} {
		if !strings.Contains(text, want) {
			t.Errorf("text missing %q:\n%s", want, text)
		}
	}
	if strings.ContainsAny(text, "\x1b\r") || strings.Contains(text, "secret") || strings.Contains(text, "signature=") {
		t.Fatalf("text leaked credentials or terminal controls: %q", text)
	}
	if strings.Index(text, "Source 1:") > strings.Index(text, "Source 2:") {
		t.Fatalf("source order changed: %s", text)
	}
}

func TestSchemaTextPropagatesWriterFailures(t *testing.T) {
	report := &source.SchemaReport{Compatible: true}
	broken := errors.New("broken stdout")
	if err := writeSchemaText(schemaFormatWriter{err: broken}, report); !errors.Is(err, broken) {
		t.Fatalf("writer failure = %v", err)
	}
	if err := writeSchemaText(schemaFormatWriter{}, report); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short writer failure = %v", err)
	}
}

type schemaFormatWriter struct{ err error }

func (w schemaFormatWriter) Write([]byte) (int, error) { return 0, w.err }
