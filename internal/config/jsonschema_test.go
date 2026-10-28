package config

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
)

var updateSchema = flag.Bool("update", false, "rewrite the published pipeline JSON Schema")

const publishedSchema = "../../docs/static/schema/v1/pipeline.schema.json"

// The published schema is generated; editing the config types without
// regenerating it fails here. Regenerate with:
//
//	go test ./internal/config -run TestPublishedSchemaIsCurrent -update
func TestPublishedSchemaIsCurrent(t *testing.T) {
	want, err := JSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	if *updateSchema {
		if err := os.WriteFile(publishedSchema, want, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(publishedSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s is stale; regenerate with: go test ./internal/config -run TestPublishedSchemaIsCurrent -update", publishedSchema)
	}
}

func compiledSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	raw, err := JSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource(SchemaID, doc); err != nil {
		t.Fatal(err)
	}
	s, err := c.Compile(SchemaID)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// documentValue decodes a YAML or JSON pipeline into JSON-model values.
func documentValue(t *testing.T, text string) any {
	t.Helper()
	var v any
	if err := yaml.Unmarshal([]byte(text), &v); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	out, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Every pipeline the project ships must be valid for editors too.
func TestSchemaAcceptsShippedPipelines(t *testing.T) {
	schema := compiledSchema(t)
	var paths []string
	for _, pattern := range []string{"../../examples/*.yml", "../../examples/*.json", "../../examples/cookbook/*.yml", "../../benchmarks/competitors/*.yml"} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, matches...)
	}
	if len(paths) < 5 {
		t.Fatalf("found %d shipped pipelines, want at least 5: %v", len(paths), paths)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			text, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := schema.Validate(documentValue(t, string(text))); err != nil {
				t.Errorf("schema rejects %s: %v", path, err)
			}
			if _, err := Load(context.Background(), path); err != nil {
				t.Errorf("Load(%s) = %v", path, err)
			}
		})
	}
}

// schemaBase is a valid pipeline; each agreement case changes one part of it,
// so a loader error can only come from that change.
const schemaBase = `source: {uris: [data.csv]}
input:
  type: csv
  schema:
    columns:
      id: {type: %s}
      amount: {type: decimal, precision: 18, scale: 2}
      s: {type: string}
rules:
  - code: R
    level: %s
    column: %s
    %s
    checks: [%s]
`

type schemaCase struct {
	name, colType, level, column, when, check string
}

func (c schemaCase) document() string {
	def := func(v, d string) string {
		if v == "" {
			return d
		}
		return v
	}
	return fmt.Sprintf(schemaBase, def(c.colType, "int64"), def(c.level, "error"),
		def(c.column, "id"), c.when, def(c.check, "{type: not_null}"))
}

func loaderError(t *testing.T, doc string) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipeline.yml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(context.Background(), path)
	if err == nil {
		err = cfg.Validate()
	}
	return err
}

// The schema must accept what the loader accepts, or editors flag valid files.
func TestSchemaAcceptsWhatTheLoaderAccepts(t *testing.T) {
	schema := compiledSchema(t)
	for _, tc := range []schemaCase{
		{name: "base"},
		{name: "upper-case type", colType: "INT64"},
		{name: "mixed-case alias", colType: "Integer"},
		{name: "python name", colType: "str", column: "s"},
		{name: "bytes", colType: "bytes", column: "s"},
		{name: "check type any case", check: "{type: type_check, expected_type: Float32}"},
		{name: "decimal text range bound", column: "amount", check: `{type: valid_range, min: "9007199254740993.01"}`},
		{name: "null among valid values", column: "s", check: "{type: valid_values, values: [null, a]}"},
		{name: "condition", when: "when: {column: s, operator: eq, value: x}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := tc.document()
			if err := loaderError(t, doc); err != nil {
				t.Fatalf("loader rejects the case: %v\n%s", err, doc)
			}
			if err := schema.Validate(documentValue(t, doc)); err != nil {
				t.Errorf("schema rejects a pipeline the loader accepts: %v\n%s", err, doc)
			}
		})
	}
	withMalformed := strings.Replace(schemaCase{}.document(), "  type: csv\n", "  type: csv\n  on_malformed: reject\n", 1)
	if err := loaderError(t, withMalformed); err != nil {
		t.Fatalf("loader rejects on_malformed: reject: %v", err)
	}
	if err := schema.Validate(documentValue(t, withMalformed)); err != nil {
		t.Errorf("schema rejects on_malformed: reject: %v", err)
	}
}

// The map form of checks must agree between the schema and the loader.
func TestSchemaAgreesWithLoaderOnCheckMaps(t *testing.T) {
	schema := compiledSchema(t)
	doc := func(checks string) string {
		return strings.Replace(schemaCase{}.document(), "checks: [{type: not_null}]", "checks: "+checks, 1)
	}
	for _, tc := range []struct {
		checks string
		valid  bool
	}{
		{"{not_null: true, valid_range: {min: 0, max: 10}}", true},
		{"{not_null: null, is_unique: {}}", true},
		{"{valid_values: [1, 2], matches_pattern: '^1', min_length: 1, equals: 3}", true},
		{"{is_unique_composite: [id, s]}", true},
		{"{not_nul: true}", false},
		{"{not_null: false}", false},
		{"{valid_range: 5}", false},
		{"{valid_range: {mni: 1}}", false},
		{"{valid_values: null}", false},
	} {
		t.Run(tc.checks, func(t *testing.T) {
			d := doc(tc.checks)
			loaderErr := loaderError(t, d)
			schemaErr := schema.Validate(documentValue(t, d))
			if (loaderErr == nil) != tc.valid || (schemaErr == nil) != tc.valid {
				t.Errorf("valid=%t: loader error = %v; schema error = %v", tc.valid, loaderErr, schemaErr)
			}
		})
	}
}

// Mistakes the loader rejects must also be flagged by the schema.
func TestSchemaRejectsWhatTheLoaderRejects(t *testing.T) {
	schema := compiledSchema(t)
	for _, tc := range []struct {
		name, doc, loaderSays string
	}{
		{"unknown top-level key", schemaCase{}.document() + "rulez: []\n", "unknown field config.rulez"},
		{"misspelled rule field", strings.Replace(schemaCase{}.document(), "checks:", "chekcs:", 1), "chekcs"},
		{"unknown check type", schemaCase{check: "{type: not_nul}"}.document(), "not_nul"},
		{"argument of another check", schemaCase{check: "{type: not_null, min: 1}"}.document(), "min"},
		{"misspelled range bound", schemaCase{check: "{type: valid_range, mni: 1}"}.document(), "mni"},
		{"missing level", schemaCase{level: `""`}.document(), "level must be error or warn"},
		{"unknown level", schemaCase{level: "fatal"}.document(), "level must be error or warn"},
		{"unknown column type", schemaCase{colType: "int128"}.document(), `"int128" is not a supported type`},
		{"c type with _t", schemaCase{colType: "int32_t"}.document(), "write int32"},
		{"unknown condition operator", schemaCase{when: "when: {column: s, operator: equals, value: x}"}.document(), "equals"},
		{"unknown malformed policy", strings.Replace(schemaCase{}.document(), "  type: csv\n", "  type: csv\n  on_malformed: skip\n", 1), "input.on_malformed must be abort or reject"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := loaderError(t, tc.doc)
			if err == nil || !strings.Contains(err.Error(), tc.loaderSays) {
				t.Fatalf("loader error = %v, want it to mention %q\n%s", err, tc.loaderSays, tc.doc)
			}
			if err := schema.Validate(documentValue(t, tc.doc)); err == nil {
				t.Errorf("schema accepts a pipeline the loader rejects (%v)\n%s", tc.loaderSays, tc.doc)
			}
		})
	}
}

func TestLoadIgnoresSchemaReference(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, path := range []string{
		write("pipeline.json", `{"$schema": "`+SchemaID+`", "rules": []}`),
		write("pipeline.yml", "$schema: "+SchemaID+"\nrules: []\n"),
	} {
		if _, err := Load(context.Background(), path); err != nil {
			t.Errorf("Load(%s) = %v, want $schema ignored", filepath.Base(path), err)
		}
	}
	_, err := Load(context.Background(), write("bad.json", `{"$schema": 1, "rules": []}`))
	if err == nil || !strings.Contains(err.Error(), "$schema must be a string") {
		t.Errorf("Load with numeric $schema = %v, want a $schema type error", err)
	}
}
