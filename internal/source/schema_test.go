package source

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

func schemaFixture(t *testing.T, name string, typ arrow.DataType) string {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: typ, Nullable: true}}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	switch b := builder.Field(0).(type) {
	case *array.Int64Builder:
		b.AppendValues([]int64{7, 9}, nil)
	case *array.StringBuilder:
		b.AppendValues([]string{"seven", "nine"}, nil)
	case *array.FixedSizeBinaryBuilder:
		b.Append([]byte{1, 2})
		b.Append([]byte{3, 4})
	default:
		t.Fatalf("unsupported test type %s", typ)
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	return writeParquetFile(t, record, compress.Codecs.Snappy)
}

func inspectSchemaFixture(t *testing.T, cfg *config.Config) *SchemaReport {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	budget := resource.New(cfg.BudgetBytes())
	store, err := storage.New(context.Background(), cfg.Source.S3, config.S3{}, budget)
	if err != nil {
		t.Fatal(err)
	}
	report, inspectErr := InspectSchema(context.Background(), cfg, store, budget)
	closeErr := store.Close()
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	t.Cleanup(func() {
		report.Release()
		if used := budget.Used(); used != 0 {
			t.Errorf("schema leaked %d managed bytes after release", used)
		}
	})
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if used := budget.Used(); used != report.retained || used == 0 {
		t.Fatalf("live report budget = %d, want retained %d", used, report.retained)
	}
	return report
}

func TestSchemaDoesNotDecodeCorruptedPayload(t *testing.T) {
	path := schemaFixture(t, "id", arrow.PrimitiveTypes.Int64)
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	footer := len(encoded) - 8 - int(binary.LittleEndian.Uint32(encoded[len(encoded)-8:]))
	if footer <= 4 {
		t.Fatal("fixture contains no payload")
	}
	for i := 4; i < footer; i++ {
		encoded[i] = 0xff
	}
	if err = os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Input: config.Input{Type: "parquet"}, Source: config.Source{URIs: []string{path}}}
	report := inspectSchemaFixture(t, cfg)
	if !report.Compatible || report.RowsChecked != 0 || report.DeclaredRows != 2 || report.ObjectsChecked != 1 || report.Validation != "not_evaluated" {
		t.Fatalf("metadata result = %+v", report)
	}
	// The same corruption must still fail normal row reading, not become an
	// implicit bypass of the validation decoder's malformed-input guards.
	budget := resource.New(cfg.BudgetBytes())
	store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ds, err := New(context.Background(), cfg, store, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	reader, err := ds.Open(context.Background())
	if err != nil {
		return
	}
	defer reader.Close()
	batch, err := reader.Next(context.Background())
	if batch != nil {
		batch.Release()
	}
	if err == nil {
		t.Fatal("corrupted payload unexpectedly decoded")
	}
}

func TestSchemaInspectsAllObjectsAndRenamesAgainstDeclaredTypes(t *testing.T) {
	first := schemaFixture(t, "raw", arrow.PrimitiveTypes.Int64)
	second := schemaFixture(t, "raw", arrow.BinaryTypes.String)
	cfg := &config.Config{Input: config.Input{Type: "parquet", Schema: &config.Schema{Columns: map[string]config.Column{"id": {Type: "int64", Required: true}}}},
		Source:     config.Source{URIs: []string{first, second}},
		Transforms: []config.Transform{{Type: "rename_columns", Mapping: map[string]string{"raw": "id"}}},
		Rules:      []config.Rule{{Code: "STRICT", Level: "error", Checks: []config.Check{{Type: "schema_matches", Params: map[string]any{"mode": "strict", "columns": map[string]any{"id": "int64"}}}}}}}
	report := inspectSchemaFixture(t, cfg)
	if report.Compatible || report.ObjectsChecked != 2 || report.DeclaredRows != 4 || report.RowsChecked != 0 {
		t.Fatalf("report = %+v", report)
	}
	var declaredMismatch, strictMismatch bool
	for _, issue := range report.SchemaIssues {
		if issue.Source != second || issue.Column != "id" {
			t.Fatalf("wrong mismatch provenance: %+v", issue)
		}
		if issue.Code == "INGEST_SCHEMA" {
			declaredMismatch = true
		}
		if issue.Code == "STRICT" && issue.Reason == "TYPE_MISMATCH" {
			strictMismatch = true
		}
	}
	if !declaredMismatch || !strictMismatch {
		t.Fatalf("missing actual-vs-target and strict mismatch: %+v", report.SchemaIssues)
	}
	for _, object := range report.Objects {
		if object.Fields[0].Name != "id" || object.Fields[0].SourceName != "raw" {
			t.Fatalf("rename metadata = %+v", object.Fields)
		}
	}
}

func TestSchemaRequiredExtraAndCastabilityAreDistinct(t *testing.T) {
	path := schemaFixture(t, "id", arrow.PrimitiveTypes.Int64)
	cfg := &config.Config{Input: config.Input{Type: "parquet"}, Source: config.Source{URIs: []string{path}}, Rules: []config.Rule{
		{Code: "CAST", Level: "error", Checks: []config.Check{{Type: "schema_matches", Params: map[string]any{"columns": map[string]any{"id": "int64"}}}}},
		{Code: "REQUIRED", Level: "error", Checks: []config.Check{{Type: "required_columns", Params: map[string]any{"columns": []any{"missing"}}}}},
		{Code: "EXTRA", Level: "error", Checks: []config.Check{{Type: "schema_matches", Params: map[string]any{"mode": "strict", "columns": map[string]any{"other": "string"}, "require_columns": false, "allow_extra_columns": false}}}},
	}}
	report := inspectSchemaFixture(t, cfg)
	if report.Compatible {
		t.Fatal("missing/extra columns incorrectly compatible")
	}
	foundCast, foundMissing, foundExtra := false, false, false
	for _, check := range report.Checks {
		if check.Code == "CAST" {
			foundCast = check.Status == "not_evaluated"
		}
	}
	for _, issue := range report.SchemaIssues {
		if issue.Code == "REQUIRED" && issue.Reason == "MISSING_COLUMN" {
			foundMissing = true
		}
		if issue.Code == "EXTRA" && issue.Reason == "UNEXPECTED_COLUMN" {
			foundExtra = true
		}
	}
	if !foundCast || !foundMissing || !foundExtra {
		t.Fatalf("incorrect gate outcomes: %+v / %+v", report.Checks, report.SchemaIssues)
	}
}

func TestSchemaS3ReadsOnlyPinnedFooterRanges(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "schema-test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "schema-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "absent"))
	encoded, err := os.ReadFile(schemaFixture(t, "id", arrow.PrimitiveTypes.Int64))
	if err != nil {
		t.Fatal(err)
	}
	footer := int64(len(encoded)-8) - int64(binary.LittleEndian.Uint32(encoded[len(encoded)-8:]))
	modified := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	var gets atomic.Int32
	var payloadReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket/rows.parquet" {
			t.Errorf("schema accessed rule reference or output: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"schema-object"`)
		w.Header().Set("Last-Modified", modified)
		w.Header().Set("X-Amz-Version-Id", "pinned-version")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(len(encoded)))
			return
		}
		if r.Method != http.MethodGet {
			t.Errorf("unexpected request %s", r.Method)
			w.WriteHeader(405)
			return
		}
		gets.Add(1)
		if r.Header.Get("If-Match") != `"schema-object"` || r.URL.Query().Get("versionId") != "pinned-version" {
			t.Error("metadata read is not identity-pinned")
		}
		var start, end int64
		if _, e := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); e != nil || (start < footer && !(start == 0 && end == 3)) || end < start || end >= int64(len(encoded)) {
			payloadReads.Add(1)
			t.Errorf("metadata command requested payload or unbounded bytes: %q (footer starts %d)", r.Header.Get("Range"), footer)
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(encoded)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(encoded[start : end+1])
	}))
	defer server.Close()
	cfg := &config.Config{Input: config.Input{Type: "parquet"}, Source: config.Source{URIs: []string{"s3://bucket/rows.parquet"}, S3: config.S3{Region: "us-east-1", Endpoint: server.URL, PathStyle: true}},
		Rules: []config.Rule{{Code: "REF", Level: "error", Column: "id", Checks: []config.Check{{Type: "referential_integrity", Params: map[string]any{"reference_file": "s3://bucket/never-read.csv", "reference_column": "id"}}}}}}
	// A row-local rule with a reference is explicitly not evaluated, never
	// loaded. Requests other than the object's HEAD, exact magic header or footer fail.
	report := inspectSchemaFixture(t, cfg)
	if !report.Compatible || report.RowsChecked != 0 || gets.Load() == 0 || payloadReads.Load() != 0 {
		t.Fatalf("report/access = %+v / gets %d payload %d", report, gets.Load(), payloadReads.Load())
	}
	found := false
	for _, check := range report.NotEvaluated {
		if check.Code == "REF" && strings.Contains(check.Reason, "no references") {
			found = true
		}
	}
	if !found {
		t.Fatal("reference check missing explicit not-evaluated result")
	}
}

func TestSchemaDeclaredPresenceExtrasAndAliases(t *testing.T) {
	path := schemaFixture(t, "id", arrow.PrimitiveTypes.Int64)
	for _, tc := range []struct {
		name, extras         string
		required, compatible bool
	}{
		{name: "optional absent and ignored extras", extras: "ignore", compatible: true},
		{name: "required absent", extras: "ignore", required: true},
		{name: "undeclared physical extra", extras: "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				Input:  config.Input{Type: "parquet", Schema: &config.Schema{ExtraFields: tc.extras, Columns: map[string]config.Column{"missing": {Type: "integer", Required: tc.required}}}},
				Source: config.Source{URIs: []string{path}},
				Rules:  []config.Rule{{Code: "ALIAS", Level: "error", Checks: []config.Check{{Type: "schema_matches", Params: map[string]any{"mode": "strict", "columns": map[string]any{"id": "integer"}}}}}},
			}
			report := inspectSchemaFixture(t, cfg)
			if report.Compatible != tc.compatible {
				t.Fatalf("compatible = %v, want %v: %+v", report.Compatible, tc.compatible, report.SchemaIssues)
			}
			if len(report.Checks) != 1 || report.Checks[0].Status != "pass" {
				t.Fatalf("alias not compared against actual physical type: %+v", report.Checks)
			}
		})
	}
}

func TestSchemaReportOwnsBudgetThroughPresentation(t *testing.T) {
	cfg := &config.Config{Input: config.Input{Type: "parquet"}, Source: config.Source{URIs: []string{schemaFixture(t, "id", arrow.PrimitiveTypes.Int64)}}}
	report := inspectSchemaFixture(t, cfg)
	budget := report.budget
	retained := report.retained
	if retained <= 0 || budget.Used() != retained {
		t.Fatalf("live report is not charged: retained=%d used=%d", retained, budget.Used())
	}
	// The store/dataset are already closed by the helper. Encoding must still
	// be possible, and the live report must prevent overcommitting its budget.
	encoded, err := json.Marshal(report)
	if err != nil || len(encoded) == 0 {
		t.Fatalf("encode live report: %v", err)
	}
	var limit *resource.ResourceLimitError
	if err = budget.Reserve(budget.Limit()-retained+1, "competing allocation"); !errors.As(err, &limit) {
		t.Fatalf("live report failed to enforce budget: %v", err)
	}
	report.Release()
	report.Release()
	if used := budget.Used(); used != 0 {
		t.Fatalf("idempotent release left %d bytes", used)
	}
}

func TestOversizedSchemaReportFailsWithoutLeakingReservations(t *testing.T) {
	cfg := &config.Config{Input: config.Input{Type: "parquet"}, Source: config.Source{URIs: []string{schemaFixture(t, "id", arrow.PrimitiveTypes.Int64)}}}
	for i := range 128 {
		cfg.Rules = append(cfg.Rules, config.Rule{Code: fmt.Sprintf("ROW_%d", i), Level: "error", Column: "id", Checks: []config.Check{{Type: "not_null"}}})
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	budget := resource.New(8 << 10)
	store, err := storage.New(context.Background(), config.S3{}, config.S3{}, budget)
	if err != nil {
		t.Fatal(err)
	}
	report, inspectErr := InspectSchema(context.Background(), cfg, store, budget)
	closeErr := store.Close()
	if report != nil {
		report.Release()
		t.Fatal("oversized report unexpectedly returned")
	}
	var limit *resource.ResourceLimitError
	if !errors.As(inspectErr, &limit) || limit.Phase != "schema report" {
		t.Fatalf("oversized report error = %v", inspectErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if used := budget.Used(); used != 0 {
		t.Fatalf("failed report leaked %d bytes", used)
	}
}

func TestSchemaBinaryRepresentationDoesNotRequireValueCasting(t *testing.T) {
	path := schemaFixture(t, "id", &arrow.FixedSizeBinaryType{ByteWidth: 2})
	cfg := &config.Config{
		Input:  config.Input{Type: "parquet", Schema: &config.Schema{Columns: map[string]config.Column{"id": {Type: "binary", Required: true}}}},
		Source: config.Source{URIs: []string{path}},
		Rules:  []config.Rule{{Code: "BINARY", Level: "error", Checks: []config.Check{{Type: "schema_matches", Params: map[string]any{"mode": "strict", "columns": map[string]any{"id": "binary"}}}}}},
	}
	report := inspectSchemaFixture(t, cfg)
	if !report.Compatible || report.Objects[0].Fields[0].PhysicalType != "FIXED_LEN_BYTE_ARRAY" {
		t.Fatalf("physical binary schema = %+v", report)
	}
}

func TestSchemaCanonicalDifferencesPreservePolicyAndNullSides(t *testing.T) {
	path := schemaFixture(t, "raw", arrow.PrimitiveTypes.Int64)
	cfg := &config.Config{
		Input: config.Input{Type: "parquet", Schema: &config.Schema{Columns: map[string]config.Column{
			"expected": {Type: "integer", Required: true},
		}}},
		Source:     config.Source{URIs: []string{path}},
		Transforms: []config.Transform{{Type: "rename_columns", Mapping: map[string]string{"raw": "renamed"}}},
		Rules: []config.Rule{{Code: "WARN_TYPE", Level: "warn", Checks: []config.Check{{
			Type: "schema_matches", Params: map[string]any{"mode": "strict", "columns": map[string]any{"renamed": "string"}},
		}}}},
	}
	report := inspectSchemaFixture(t, cfg)
	if len(report.Differences) != 3 {
		t.Fatalf("differences = %+v", report.Differences)
	}
	missing, mismatch, unexpected := report.Differences[0], report.Differences[1], report.Differences[2]
	if missing.Column != "expected" || missing.Kind != "missing" || missing.Actual != nil ||
		missing.Expected == nil || missing.Expected.Type != "int64" || missing.ExpectationSource != "input_schema" {
		t.Fatalf("missing difference = %+v", missing)
	}
	if mismatch.Column != "renamed" || mismatch.SourceName != "raw" || mismatch.Kind != "type_mismatch" ||
		mismatch.Severity != "warn" || mismatch.RuleCode != "WARN_TYPE" || mismatch.RuleOrdinal != 1 ||
		mismatch.ExpectationSource != "rule" || mismatch.Expected.Type != "string" ||
		mismatch.Actual.Type != "int64" || mismatch.Actual.BitWidth == nil || *mismatch.Actual.BitWidth != 64 ||
		mismatch.Actual.Signed == nil || !*mismatch.Actual.Signed {
		t.Fatalf("type mismatch = %+v", mismatch)
	}
	if unexpected.Column != "renamed" || unexpected.Kind != "unexpected" || unexpected.Expected != nil ||
		unexpected.Actual == nil || unexpected.SourceName != "raw" {
		t.Fatalf("unexpected difference = %+v", unexpected)
	}
	for _, diff := range report.Differences {
		if diff.SourceOrdinal != 1 || diff.Source != path {
			t.Fatalf("wrong source association: %+v", diff)
		}
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"actual":null`) || !strings.Contains(string(encoded), `"expected":null`) {
		t.Fatalf("absent types were fabricated: %s", encoded)
	}
}

func TestSchemaBaselineDifferencesStayOnLaterSource(t *testing.T) {
	first := schemaFixture(t, "raw", arrow.PrimitiveTypes.Int64)
	second := schemaFixture(t, "raw", arrow.BinaryTypes.String)
	cfg := &config.Config{
		Input: config.Input{Type: "parquet"}, Source: config.Source{URIs: []string{first, second}},
		Transforms: []config.Transform{{Type: "rename_columns", Mapping: map[string]string{"raw": "id"}}},
	}
	report := inspectSchemaFixture(t, cfg)
	if report.BaselineSource != first || report.BaselineSourceOrdinal != 1 || len(report.Differences) != 1 {
		t.Fatalf("baseline result = %+v", report)
	}
	diff := report.Differences[0]
	if diff.Source != second || diff.SourceOrdinal != 2 || diff.ExpectationSource != "baseline_object" ||
		diff.Column != "id" || diff.SourceName != "raw" || diff.Expected.Type != "int64" || diff.Actual.Type != "string" {
		t.Fatalf("baseline difference = %+v", diff)
	}
}

func TestSchemaWarningDifferencesDoNotRejectCompatibility(t *testing.T) {
	path := schemaFixture(t, "id", arrow.PrimitiveTypes.Int64)
	cfg := &config.Config{
		Input: config.Input{Type: "parquet"}, Source: config.Source{URIs: []string{path}},
		Rules: []config.Rule{
			{Code: "FIRST", Level: "warn", Checks: []config.Check{{Type: "schema_matches", Params: map[string]any{"mode": "strict", "columns": map[string]any{"id": "string"}}}}},
			{Code: "SECOND", Level: "warn", Checks: []config.Check{{Type: "schema_matches", Params: map[string]any{"mode": "strict", "columns": map[string]any{"id": "bool"}}}}},
			{Code: "PRESENCE", Level: "warn", Checks: []config.Check{{Type: "required_columns", Params: map[string]any{"columns": []any{"missing"}}}}},
		},
	}
	report := inspectSchemaFixture(t, cfg)
	if !report.Compatible || len(report.Differences) != 3 {
		t.Fatalf("warning result = %+v", report)
	}
	if report.Differences[0].RuleCode != "FIRST" || report.Differences[1].RuleCode != "SECOND" {
		t.Fatalf("rule declaration order lost: %+v", report.Differences)
	}
	presence := report.Differences[2]
	if presence.Expected != nil || presence.Actual != nil || presence.Kind != "missing" {
		t.Fatalf("untyped presence expectation fabricated a type: %+v", presence)
	}
}

func TestSchemaTypedAttributesPreserveLogicalParameters(t *testing.T) {
	unsigned := schemaAttributes(arrow.PrimitiveTypes.Uint16)
	if unsigned.BitWidth == nil || *unsigned.BitWidth != 16 || unsigned.Signed == nil || *unsigned.Signed {
		t.Fatalf("unsigned integer attributes = %+v", unsigned)
	}
	decimal := schemaAttributes(&arrow.Decimal128Type{Precision: 23, Scale: 7})
	if decimal.Precision == nil || *decimal.Precision != 23 || decimal.Scale == nil || *decimal.Scale != 7 {
		t.Fatalf("decimal attributes = %+v", decimal)
	}
	timestamp := schemaAttributes(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Europe/Paris"})
	if timestamp.Unit == nil || *timestamp.Unit != arrow.Microsecond.String() ||
		timestamp.Timezone == nil || *timestamp.Timezone != "Europe/Paris" {
		t.Fatalf("timestamp attributes = %+v", timestamp)
	}
	for _, name := range []string{"decimal", "datetime"} {
		attributes := schemaRuleAttributes(name)
		if attributes.Precision != nil || attributes.Scale != nil || attributes.Unit != nil || attributes.Timezone != nil {
			t.Fatalf("rule %s fabricated an unstated logical constraint: %+v", name, attributes)
		}
	}
}
