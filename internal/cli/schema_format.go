package cli

import (
	"errors"
	"fmt"
	"io"

	"github.com/richardrh/valet/internal/source"
	"github.com/richardrh/valet/internal/storage"
)

// writeSchemaText presents the canonical metadata-only report. It neither
// acquires source handles nor performs validation, reference or output I/O.
func writeSchemaText(w io.Writer, report *source.SchemaReport) error {
	if report == nil {
		return errors.New("cannot present a nil schema report")
	}
	out := schemaTextWriter{writer: w}
	if report.Compatible {
		out.printf("Compatible schema\n")
	} else {
		out.printf("Incompatible schema\n")
	}
	out.printf("Rows checked: %d; data validation: not_evaluated (not evaluated).\n", report.RowsChecked)
	out.printf("Metadata only: %d objects; %d footer-declared rows (unverified).\n", report.ObjectsChecked, report.DeclaredRows)
	out.printf("Payload decoding, integrity and value castability are not evaluated. Required column presence does not establish non-null values.\n")
	if report.BaselineSourceOrdinal != 0 {
		out.printf("Schema baseline: first object, source %d %q (not a user-declared schema).\n", report.BaselineSourceOrdinal, schemaTextURI(report.BaselineSource))
	} else {
		out.printf("Schema expectations: declared input schema and applicable metadata rules.\n")
	}
	// InspectSchema supplies canonical source/column/kind/rule order. Stream that
	// order without building another unbudgeted report or retaining source data.
	difference := 0
	for _, object := range report.Objects {
		out.printf("\nSource %d: %q\n", object.SourceOrdinal, schemaTextURI(object.URI))
		out.printf("  Footer-declared rows: %d (unverified)\n", object.DeclaredRows)
		for _, field := range object.Fields {
			out.printf("  Field %q", field.Name)
			if field.SourceName != "" && field.SourceName != field.Name {
				out.printf(" (original source name %q)", field.SourceName)
			}
			out.printf(": ")
			out.attributes(field.Attributes)
			out.printf("\n")
		}
		start := difference
		for difference < len(report.Differences) && report.Differences[difference].SourceOrdinal == object.SourceOrdinal {
			diff := &report.Differences[difference]
			out.printf("  [%s] %s column %q", diff.Severity, diff.Kind, diff.Column)
			if diff.SourceName != "" && diff.SourceName != diff.Column {
				out.printf(" (original source name %q)", diff.SourceName)
			}
			out.printf("; expectation_source=%s", diff.ExpectationSource)
			if diff.RuleCode != "" {
				out.printf("; rule=%q", diff.RuleCode)
			}
			out.printf("\n    expected: ")
			out.attributes(diff.Expected)
			out.printf("\n    actual: ")
			out.attributes(diff.Actual)
			out.printf("\n    reason: %q\n", diff.Reason)
			difference++
		}
		if difference == start {
			out.printf("  No policy-relevant schema differences.\n")
		}
	}
	out.printf("\nNot evaluated (zero value-checked rows):\n")
	for _, check := range report.NotEvaluated {
		out.printf("  %q %q: %q\n", check.Code, check.Type, check.Reason)
	}
	return out.err
}

type schemaTextWriter struct {
	writer io.Writer
	err    error
}

func (w *schemaTextWriter) printf(format string, args ...any) {
	if w.err == nil {
		_, w.err = fmt.Fprintf(w, format, args...)
	}
}

func (w *schemaTextWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *schemaTextWriter) attributes(a *source.SchemaAttributes) {
	if a == nil {
		w.printf("null (absent or no typed expectation)")
		return
	}
	w.printf("type=%q", a.Type)
	if a.BitWidth != nil {
		w.printf(", bit_width=%d", *a.BitWidth)
	}
	if a.Signed != nil {
		w.printf(", signed=%t", *a.Signed)
	}
	if a.Precision != nil {
		w.printf(", precision=%d", *a.Precision)
	}
	if a.Scale != nil {
		w.printf(", scale=%d", *a.Scale)
	}
	if a.Unit != nil {
		w.printf(", unit=%q", *a.Unit)
	}
	if a.Timezone != nil {
		w.printf(", timezone=%q", *a.Timezone)
	}
	if a.Nullable != nil {
		w.printf(", nullable=%t", *a.Nullable)
	}
	if a.Required != nil {
		w.printf(", required=%t (presence only)", *a.Required)
	}
	if a.PhysicalType != "" {
		w.printf(", physical_type=%q", a.PhysicalType)
	}
	if a.ArrowType != "" {
		w.printf(", arrow_type=%q", a.ArrowType)
	}
}

func schemaTextURI(uri string) string {
	return storage.LogURI(uri).LogValue().String()
}
