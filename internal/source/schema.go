package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/resource"
	"github.com/richardrh/valet/internal/storage"
)

// SchemaReport describes only footer/schema compatibility, never row validation.
// DeclaredRows is the footer claim, not an observed or validated row count.
type SchemaReport struct {
	ReportVersion         int                `json:"report_version"`
	RunID                 string             `json:"run_id"`
	Scope                 string             `json:"scope"`
	MetadataOnly          bool               `json:"metadata_only"`
	Validation            string             `json:"validation"`
	Compatible            bool               `json:"compatible"`
	RowsChecked           int64              `json:"rows_checked"`
	ObjectsChecked        int                `json:"objects_checked"`
	DeclaredRows          int64              `json:"declared_rows"`
	Objects               []SchemaObject     `json:"objects"`
	DeclaredSchema        []SchemaField      `json:"declared_schema"`
	SchemaIssues          []data.Issue       `json:"schema_issues"`
	Checks                []SchemaCheck      `json:"checks"`
	NotEvaluated          []SchemaCheck      `json:"not_evaluated"`
	Differences           []SchemaDifference `json:"differences"`
	BaselineSource        string             `json:"baseline_source,omitempty"`
	BaselineSourceOrdinal uint64             `json:"baseline_source_ordinal,omitempty"`
	budget                *resource.Budget
	retained              int64
}

// Release returns the report's managed reservations. It is idempotent; the
// report must not be used afterward. No source handles or dataset state remain
// attached to a completed report.
func (r *SchemaReport) Release() {
	if r == nil || r.budget == nil {
		return
	}
	r.budget.Release(r.retained)
	r.retained = 0
	r.budget = nil
	r.Objects = nil
	r.DeclaredSchema = nil
	r.SchemaIssues = nil
	r.Checks = nil
	r.NotEvaluated = nil
	r.Differences = nil
	r.BaselineSource = ""
}

func (r *SchemaReport) reserve(n int64) error {
	if err := r.budget.Reserve(n, "schema report"); err != nil {
		return err
	}
	r.retained += n
	return nil
}

// SchemaField preserves the actual source name separately from its renamed name.
type SchemaField struct {
	Name         string            `json:"name"`
	SourceName   string            `json:"source_name,omitempty"`
	Type         string            `json:"type"`
	ArrowType    string            `json:"arrow_type"`
	PhysicalType string            `json:"physical_type,omitempty"`
	Nullable     bool              `json:"nullable"`
	Required     bool              `json:"required,omitempty"`
	Attributes   *SchemaAttributes `json:"attributes"`
}

// SchemaObject is the metadata observed for one pinned input object.
type SchemaObject struct {
	SourceOrdinal uint64        `json:"source_ordinal"`
	URI           string        `json:"uri"`
	DeclaredRows  int64         `json:"declared_rows"`
	Fields        []SchemaField `json:"fields"`
}

// SchemaCheck explicitly separates metadata outcomes from unevaluated checks.
type SchemaCheck struct {
	Code   string `json:"code"`
	Type   string `json:"type"`
	Source string `json:"source,omitempty"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// SchemaAttributes records the logical domain without inventing absent
// constraints. A nil attribute means the expectation does not constrain it.
type SchemaAttributes struct {
	Type         string  `json:"type"`
	ArrowType    string  `json:"arrow_type,omitempty"`
	PhysicalType string  `json:"physical_type,omitempty"`
	Nullable     *bool   `json:"nullable"`
	Required     *bool   `json:"required"`
	BitWidth     *int    `json:"bit_width"`
	Signed       *bool   `json:"signed"`
	Precision    *int32  `json:"precision"`
	Scale        *int32  `json:"scale"`
	Unit         *string `json:"unit"`
	Timezone     *string `json:"timezone"`
}

// SchemaDifference is a policy-relevant mismatch, not a value-validation
// outcome. Expected/Actual are null when no typed schema exists on that side.
type SchemaDifference struct {
	SourceOrdinal     uint64            `json:"source_ordinal"`
	Source            string            `json:"source"`
	Column            string            `json:"column"`
	SourceName        string            `json:"source_name,omitempty"`
	Kind              string            `json:"kind"`
	Expected          *SchemaAttributes `json:"expected"`
	Actual            *SchemaAttributes `json:"actual"`
	Severity          string            `json:"severity"`
	RuleCode          string            `json:"rule_code,omitempty"`
	ExpectationSource string            `json:"expectation_source"`
	Reason            string            `json:"reason"`
	RuleOrdinal       int               `json:"rule_ordinal,omitempty"`
	CheckOrdinal      int               `json:"check_ordinal,omitempty"`
}

// InspectSchema reads Parquet footers only. It neither opens a row reader nor
// compiles checks, loads rule references, validates pages or hashes input data.
// cfg must already be validated; store and budget remain owned by the caller.
// The caller must Release the returned report after presentation.
func InspectSchema(ctx context.Context, cfg *config.Config, store *storage.Store, budget *resource.Budget) (report *SchemaReport, err error) {
	if cfg.Input.Type != "parquet" {
		return nil, errors.New("schema requires Parquet input; other formats require reading records")
	}
	d := &Dataset{cfg: cfg, kind: inputParquet, plan: true, store: store, budget: budget,
		log: slog.New(slog.DiscardHandler), recordLimit: positiveOr(cfg.RecordBytes(), defaultRecordBytes),
		batchTarget: positiveOr(cfg.BatchBytes(), defaultBatchBytes), maxDepth: positiveOr(cfg.Execution.MaxDepth, defaultMaxDepth)}
	var owned *SchemaReport
	complete := false
	defer func() {
		err = errors.Join(err, d.Close())
		if !complete || err != nil {
			owned.Release()
			report = nil
		}
	}()
	if err = d.discover(ctx); err != nil {
		return nil, err
	}
	if err = d.loadTransforms(ctx); err != nil {
		return nil, err
	}
	if cfg.Input.Schema != nil {
		if err = d.declareSchema(); err != nil {
			return nil, err
		}
	}
	report = &SchemaReport{ReportVersion: data.ReportVersion, Scope: "schema", MetadataOnly: true,
		Validation: "not_evaluated", Compatible: true, Objects: []SchemaObject{}, DeclaredSchema: []SchemaField{},
		SchemaIssues: []data.Issue{}, Checks: []SchemaCheck{}, NotEvaluated: []SchemaCheck{}, Differences: []SchemaDifference{}, budget: budget}
	owned = report
	// Account retained report fields, check outcomes and diagnostics before growth.
	reserve := report.reserve
	if err = reserve(512); err != nil {
		return nil, err
	}
	addSkipped := func(code, kind, reason string) error {
		if e := reserve(int64(len(code)+len(kind)+len(reason)) + 512); e != nil {
			return e
		}
		report.NotEvaluated = append(report.NotEvaluated, SchemaCheck{Code: code, Type: kind, Status: "not_evaluated", Reason: reason})
		return nil
	}
	for _, c := range d.columns {
		if err = reserve(int64(len(c.name)+len(c.typ.String())) + 512); err != nil {
			return nil, err
		}
		nullable := c.options.Nullable == nil || *c.options.Nullable
		attributes := schemaAttributes(c.typ)
		required := c.options.Required
		attributes.Nullable, attributes.Required = &nullable, &required
		report.DeclaredSchema = append(report.DeclaredSchema, SchemaField{Name: c.name, Type: attributes.Type, ArrowType: attributes.ArrowType, Nullable: nullable, Required: required, Attributes: attributes})
	}
	if err = addSkipped("INGEST", "row_ingestion", "payload decoding, value castability, nullability, trimming, date/time formats, boolean tokens, UTF-8, finite numbers and record/page limits require reading data"); err != nil {
		return nil, err
	}
	if err = addSkipped("INPUT", "content_integrity", "payload integrity and full-file digests are not evaluated; object identity is pinned for metadata access"); err != nil {
		return nil, err
	}
	for _, rule := range cfg.Rules {
		for _, check := range rule.Checks {
			if check.Type == "required_columns" || (check.Type == "schema_matches" && check.Params["mode"] == "strict") {
				continue
			}
			reason := "requires row data or rule reference data; no references are loaded"
			if check.Type == "schema_matches" {
				reason = "castability requires reading all values; only column presence and extra-column metadata gates are inspected"
			}
			if err = addSkipped(rule.Code, check.Type, reason); err != nil {
				return nil, err
			}
		}
	}
	for _, rule := range cfg.DatasetRules {
		if err = addSkipped(rule.Code, rule.Metric, "dataset assertions require row validation; footer row counts are unverified claims"); err != nil {
			return nil, err
		}
	}
	for _, stat := range cfg.Stats {
		if err = addSkipped(stat.Code, stat.Aggregate, "statistics require row data"); err != nil {
			return nil, err
		}
	}
	var baseline *arrow.Schema
	for ordinal, obj := range d.objects {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if err = inspectSchemaObject(ctx, d, report, obj, ordinal, &baseline); err != nil {
			return nil, fmt.Errorf("inspect schema %s: %w", obj.URI, err)
		}
	}
	sort.SliceStable(report.Differences, func(i, j int) bool {
		a, b := &report.Differences[i], &report.Differences[j]
		if a.SourceOrdinal != b.SourceOrdinal {
			return a.SourceOrdinal < b.SourceOrdinal
		}
		if a.Column != b.Column {
			return a.Column < b.Column
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.RuleOrdinal != b.RuleOrdinal {
			return a.RuleOrdinal < b.RuleOrdinal
		}
		return a.CheckOrdinal < b.CheckOrdinal
	})
	complete = true
	return report, nil
}

func inspectSchemaObject(ctx context.Context, d *Dataset, report *SchemaReport, obj storage.Object, ordinal int, baseline **arrow.Schema) (err error) {
	p, err := openParquet(ctx, d, obj, false)
	if err != nil {
		return err
	}
	defer closeInto(&err, p)
	rows := p.file.NumRows()
	if rows < 0 || rows > math.MaxInt64-report.DeclaredRows {
		return errors.New("invalid or overflowing Parquet declared row count")
	}
	var groupRows int64
	for i := range p.file.NumRowGroups() {
		n := p.file.MetaData().RowGroup(i).NumRows()
		if n < 0 || n > math.MaxInt64-groupRows {
			return errors.New("invalid or overflowing Parquet row-group row count")
		}
		groupRows += n
	}
	if groupRows != rows {
		return errors.New("Parquet file and row-group declared row counts disagree")
	}
	if err = report.reserve(int64(len(obj.URI)) + 512 + int64(len(p.header))*1024); err != nil {
		return err
	}
	actual := make([]arrow.Field, len(p.header))
	observed := SchemaObject{SourceOrdinal: uint64(ordinal + 1), URI: obj.URI, DeclaredRows: rows, Fields: make([]SchemaField, 0, len(p.header))}
	seen := make(map[string]bool, len(p.header))
	for i, name := range p.header {
		f := p.schema.Field(i)
		f.Name = d.rename(name)
		if seen[f.Name] {
			return fmt.Errorf("ambiguous renamed column %q", f.Name)
		}
		seen[f.Name] = true
		actual[i] = f
		if err = report.reserve(int64(len(name) + len(f.Name) + len(f.Type.String()))); err != nil {
			return err
		}
		column := p.file.MetaData().Schema.Column(i)
		nullable := column.MaxDefinitionLevel() > 0
		attributes := schemaAttributes(f.Type)
		attributes.PhysicalType, attributes.Nullable = column.PhysicalType().String(), &nullable
		observed.Fields = append(observed.Fields, SchemaField{Name: f.Name, SourceName: name, Type: attributes.Type, ArrowType: attributes.ArrowType, PhysicalType: attributes.PhysicalType, Nullable: nullable, Attributes: attributes})
	}
	schema := arrow.NewSchema(actual, nil)
	expectationSource := "input_schema"
	ruleOrdinal, checkOrdinal := 0, 0
	issue := func(code, level, column, reason, kind string, expected *SchemaAttributes) error {
		if err := report.reserve(int64(len(obj.URI)+len(code)+len(column)+len(reason))*2 + schemaIssueBytes + 1024); err != nil {
			return err
		}
		origin := data.OriginRule
		if code == "INGEST_SCHEMA" {
			origin = data.OriginIngest
		}
		report.SchemaIssues = append(report.SchemaIssues, data.Issue{Location: data.Location{Source: obj.URI, SourceOrdinal: uint64(ordinal + 1)}, Code: code, Severity: level, Origin: origin, Scope: data.ScopeSchema, Column: column, Reason: reason, Message: reason})
		diff := SchemaDifference{SourceOrdinal: uint64(ordinal + 1), Source: obj.URI,
			Column: column, Kind: kind, Expected: expected, Severity: level,
			ExpectationSource: expectationSource, Reason: reason,
			RuleOrdinal: ruleOrdinal, CheckOrdinal: checkOrdinal}
		if expectationSource == "rule" {
			diff.RuleCode = code
		}
		if indices := schema.FieldIndices(column); len(indices) != 0 {
			field := &observed.Fields[indices[0]]
			diff.Actual = field.Attributes
			if field.SourceName != column {
				diff.SourceName = field.SourceName
			}
		} else if expectationSource == "baseline_object" {
			for _, field := range report.Objects[0].Fields {
				if field.Name == column && field.SourceName != column {
					diff.SourceName = field.SourceName
					break
				}
			}
		}
		report.Differences = append(report.Differences, diff)
		if level != "warn" {
			report.Compatible = false
		}
		return nil
	}
	if d.cfg.Input.Schema != nil {
		for columnIndex, c := range d.columns {
			indices := schema.FieldIndices(c.name)
			if len(indices) == 0 {
				if c.options.Required {
					if err = issue("INGEST_SCHEMA", "error", c.name, "required input column is absent", "missing", report.DeclaredSchema[columnIndex].Attributes); err != nil {
						return err
					}
				}
				continue
			}
			if !schemaTypesEqual(schema.Field(indices[0]).Type, c.typ) {
				if err = issue("INGEST_SCHEMA", "error", c.name, "physical schema type differs from declared target: "+schema.Field(indices[0]).Type.String()+" != "+c.typ.String(), "type_mismatch", report.DeclaredSchema[columnIndex].Attributes); err != nil {
					return err
				}
			}
		}
		if !d.ignoreExtras() {
			for _, f := range actual {
				if d.index(f.Name) < 0 {
					if err = issue("INGEST_SCHEMA", "error", f.Name, "undeclared input column", "unexpected", nil); err != nil {
						return err
					}
				}
			}
		}
	} else if *baseline == nil {
		*baseline = schema
		report.BaselineSource, report.BaselineSourceOrdinal = obj.URI, uint64(ordinal+1)
	} else {
		expectationSource = "baseline_object"
		for baselineIndex, f := range (*baseline).Fields() {
			indices := schema.FieldIndices(f.Name)
			if len(indices) == 0 {
				err = issue("INGEST_SCHEMA", "error", f.Name, "input column is absent from this object", "missing", report.Objects[0].Fields[baselineIndex].Attributes)
			} else if !schemaTypesEqual(f.Type, schema.Field(indices[0]).Type) {
				err = issue("INGEST_SCHEMA", "error", f.Name, "incompatible Parquet physical schema", "type_mismatch", report.Objects[0].Fields[baselineIndex].Attributes)
			}
			if err != nil {
				return err
			}
		}
		for _, f := range actual {
			if len((*baseline).FieldIndices(f.Name)) == 0 {
				if err = issue("INGEST_SCHEMA", "error", f.Name, "unexpected input column in this object", "unexpected", nil); err != nil {
					return err
				}
			}
		}
	}
	expectationSource = "rule"
	for ruleIndex, rule := range d.cfg.Rules {
		ruleOrdinal = ruleIndex + 1
		for checkIndex, check := range rule.Checks {
			checkOrdinal = checkIndex + 1
			if check.Type != "required_columns" && check.Type != "schema_matches" {
				continue
			}
			before := len(report.SchemaIssues)
			if check.Type == "required_columns" {
				for _, name := range schemaOperandNames(check.Params, rule.Column) {
					if !seen[name] {
						if err = issue(rule.Code, rule.Level, name, "MISSING_COLUMN", "missing", nil); err != nil {
							return err
						}
					}
				}
			} else {
				types := schemaCheckTypes(check.Params["columns"])
				names := make([]string, 0, len(types))
				for name := range types {
					names = append(names, name)
				}
				sort.Strings(names)
				for _, name := range names {
					indices := schema.FieldIndices(name)
					if len(indices) == 0 {
						if schemaCheckBool(check.Params, "require_columns", true) {
							if err = issue(rule.Code, rule.Level, name, "MISSING_COLUMN", "missing", schemaRuleAttributes(types[name])); err != nil {
								return err
							}
						}
					} else if check.Params["mode"] == "strict" {
						want, _ := config.CanonicalType(types[name])
						if schemaType(schema.Field(indices[0]).Type) != want {
							if err = issue(rule.Code, rule.Level, name, "TYPE_MISMATCH", "type_mismatch", schemaRuleAttributes(types[name])); err != nil {
								return err
							}
						}
					}
				}
				if !schemaCheckBool(check.Params, "allow_extra_columns", true) {
					for _, f := range actual {
						if _, ok := types[f.Name]; !ok {
							if err = issue(rule.Code, rule.Level, f.Name, "UNEXPECTED_COLUMN", "unexpected", nil); err != nil {
								return err
							}
						}
					}
				}
			}
			status := "pass"
			if len(report.SchemaIssues) != before {
				status = "fail"
			} else if check.Type == "schema_matches" && check.Params["mode"] != "strict" {
				status = "not_evaluated"
			}
			if err = report.reserve(int64(len(rule.Code)+len(check.Type)+len(obj.URI)) + 512); err != nil {
				return err
			}
			report.Checks = append(report.Checks, SchemaCheck{Code: rule.Code, Type: check.Type, Source: obj.URI, Status: status})
		}
	}
	report.Objects = append(report.Objects, observed)
	report.ObjectsChecked++
	report.DeclaredRows += rows
	return nil
}

func schemaType(typ arrow.DataType) string {
	if name, ok := config.CanonicalType(typ.Name()); ok {
		return name
	}
	switch typ.ID() {
	case arrow.DATE64:
		return "date"
	case arrow.LARGE_STRING:
		return "string"
	case arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		return "binary"
	default:
		return typ.Name()
	}
}

func schemaCheckNames(value any) []string {
	if names, ok := value.([]string); ok {
		return names
	}
	var names []string
	if values, ok := value.([]any); ok {
		for _, v := range values {
			if name, ok := v.(string); ok {
				names = append(names, name)
			}
		}
	}
	return names
}

func schemaCheckTypes(value any) map[string]string {
	if types, ok := value.(map[string]string); ok {
		return types
	}
	types := make(map[string]string)
	if values, ok := value.(map[string]any); ok {
		for name, v := range values {
			if typ, ok := v.(string); ok {
				types[name] = typ
			}
		}
	}
	return types
}

func schemaCheckBool(params map[string]any, name string, fallback bool) bool {
	if value, ok := params[name].(bool); ok {
		return value
	}
	return fallback
}

// Arrow offset widths do not change the Parquet logical value domain. Decimal
// parameters and timestamp unit/timezone still carry schema constraints.
func schemaTypesEqual(actual, target arrow.DataType) bool {
	if schemaType(actual) != schemaType(target) {
		return false
	}
	switch actual.ID() {
	case arrow.TIMESTAMP, arrow.DECIMAL128:
		return arrow.TypeEqual(actual, target)
	default:
		return true
	}
}

func schemaOperandNames(params map[string]any, ruleColumn string) []string {
	if value, ok := params["columns"]; ok {
		return schemaCheckNames(value)
	}
	name, _ := params["column"].(string)
	if name == "" {
		name = ruleColumn
	}
	if name == "" {
		return nil
	}
	return []string{name}
}

func schemaAttributes(typ arrow.DataType) *SchemaAttributes {
	a := &SchemaAttributes{Type: schemaType(typ), ArrowType: typ.String()}
	switch typ.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		width := typ.(arrow.FixedWidthDataType).BitWidth()
		signed := typ.ID() == arrow.INT8 || typ.ID() == arrow.INT16 || typ.ID() == arrow.INT32 || typ.ID() == arrow.INT64
		a.BitWidth, a.Signed = &width, &signed
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		width := typ.(arrow.FixedWidthDataType).BitWidth()
		a.BitWidth = &width
	}
	switch typ := typ.(type) {
	case *arrow.Decimal128Type:
		precision, scale := typ.Precision, typ.Scale
		a.Precision, a.Scale = &precision, &scale
	case *arrow.Decimal256Type:
		precision, scale := typ.Precision, typ.Scale
		a.Precision, a.Scale = &precision, &scale
	case *arrow.TimestampType:
		unit, timezone := typ.Unit.String(), typ.TimeZone
		a.Unit, a.Timezone = &unit, &timezone
	}
	return a
}

// Rule type gates constrain the canonical type, not unstated decimal or
// timestamp parameters. Those attributes must remain unknown, not defaults.
func schemaRuleAttributes(name string) *SchemaAttributes {
	kind, _ := config.CanonicalType(name)
	if kind == "timestamp" || kind == "decimal" {
		return &SchemaAttributes{Type: kind}
	}
	typ, err := columnType(config.Column{Type: kind})
	if err != nil {
		return nil
	}
	a := schemaAttributes(typ)
	a.ArrowType = ""
	return a
}
