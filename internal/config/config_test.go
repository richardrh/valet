package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func writeFile(t *testing.T, directory, name, text string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func loadText(t *testing.T, text string) (*Config, error) {
	t.Helper()
	return loadNamedText(t, "pipeline.yml", text)
}

func loadNamedText(t *testing.T, name, text string) (*Config, error) {
	t.Helper()
	return Load(context.Background(), writeFile(t, t.TempDir(), name, text))
}

// loadValid loads a YAML pipeline and validates it, failing the test on either error.
func loadValid(t *testing.T, text string) *Config {
	t.Helper()
	cfg, err := loadText(t, text)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	return cfg
}

// loadAndValidate loads a YAML pipeline and returns the first load or
// validation error.
func loadAndValidate(t *testing.T, text string) (*Config, error) {
	t.Helper()
	cfg, err := loadText(t, text)
	if err != nil {
		return nil, err
	}
	return cfg, cfg.Validate()
}

func baseConfig() *Config { return &Config{Source: Source{URIs: []string{"input.csv"}}} }

// configWithCheck returns a minimal valid configuration with one error rule running check.
func configWithCheck(check Check) *Config {
	cfg := baseConfig()
	cfg.Rules = []Rule{{Code: "RULE", Level: "error", Column: "a", Checks: []Check{check}}}
	return cfg
}

func assertErrorContains(t *testing.T, what string, err error, wants ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: error = nil, want error containing %q", what, wants)
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%s: error = %q, want it to contain %q", what, err, want)
		}
	}
}

func TestOutputModeDefaultsAfterDestinationOverride(t *testing.T) {
	for _, tc := range []struct {
		name, document, destination, mode string
	}{
		{name: "stdout", document: "source: {uris: [rows.csv]}\n", mode: "report"},
		{name: "override destination", document: "source: {uris: [rows.csv]}\n", destination: "out", mode: "all"},
		{name: "configured destination", document: "source: {uris: [rows.csv]}\noutput: {uri: out}\n", destination: "out", mode: "all"},
		{name: "clear destination", document: "source: {uris: [rows.csv]}\noutput: {uri: out}\n", mode: "report"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadText(t, tc.document)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Output.URI = tc.destination
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			if cfg.Output.Mode != tc.mode {
				t.Errorf("effective output mode = %q, want %q", cfg.Output.Mode, tc.mode)
			}
		})
	}
}

func TestLoadDefaultsAndOverrideBoundary(t *testing.T) {
	cfg, err := loadText(t, `name: Example
execution: {memory_budget: invalid, workers: 0}
`)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Input.Type != "" {
		t.Errorf("loaded input type = %q, want empty until validation", cfg.Input.Type)
	}
	if cfg.Input.Mode != "infer_types" {
		t.Errorf("default input mode = %q, want %q", cfg.Input.Mode, "infer_types")
	}
	if cfg.Input.InferSchemaLength != 1000000 {
		t.Errorf("default infer_schema_length = %d, want 1000000", cfg.Input.InferSchemaLength)
	}
	if cfg.Execution.Workers != 0 {
		t.Errorf("explicit workers = %d, want 0", cfg.Execution.Workers)
	}
	wantNulls := []string{"NULL", "null", "N/A", "n/a", ""}
	if !slices.Equal(cfg.Input.NullValues, wantNulls) {
		t.Errorf("default null markers = %q, want %q", cfg.Input.NullValues, wantNulls)
	}

	cfg.Source.URIs = []string{"override.csv"}
	cfg.Execution.MemoryBudget = "32MiB"
	cfg.Execution.Workers = 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() after overrides error = %v", err)
	}
	for _, size := range []struct {
		name      string
		got, want int64
	}{
		{name: "BudgetBytes", got: cfg.BudgetBytes(), want: 32 << 20},
		{name: "BatchBytes", got: cfg.BatchBytes(), want: 8 << 20},
		{name: "RecordBytes", got: cfg.RecordBytes(), want: 8 << 20},
		{name: "PartBytes", got: cfg.PartBytes(), want: 128 << 20},
	} {
		if size.got != size.want {
			t.Errorf("%s() = %d, want %d", size.name, size.got, size.want)
		}
	}
	if cfg.Input.Type != "csv" {
		t.Errorf("input type inferred from override.csv = %q, want %q", cfg.Input.Type, "csv")
	}
	if cfg.Output.URI != "" {
		t.Errorf("output URI = %q, want empty (planning must not fabricate one)", cfg.Output.URI)
	}
}

func TestYAMLAndJSONPipelinesAreSemanticallyIdentical(t *testing.T) {
	yamlDocument := `name: "Equivalent / 😀"
context: {threshold: 9007199254740993.01}
source: {uris: [data.csv]}
input:
  type: csv
  mode: raw_strings
  schema:
    extra_fields: error
    columns:
      id: {type: string}
      amount: {type: decimal, precision: 20, scale: 2}
      observed: {type: datetime, timezone: UTC}
rules:
  - code: EXACT_AMOUNT
    level: error
    column: amount
    when: {column: id, operator: is_present}
    checks: [{type: equals, value: 9007199254740993.01}]
  - code: KNOWN_ID
    level: error
    column: id
    checks: [{type: referential_integrity, reference_file: references/ids.csv, reference_column: id}]
dataset_rules:
  - {code: TOTAL, level: error, metric: sum, column: amount, equals: 9007199254740993.01}
`
	jsonDocument := `{
  "name": "Equivalent \/ \ud83d\ude00",
  "context": {"threshold": 9007199254740993.01},
  "source": {"uris": ["data.csv"]},
  "input": {
    "type": "csv",
    "mode": "raw_strings",
    "schema": {
      "extra_fields": "error",
      "columns": {
        "id": {"type": "string"},
        "amount": {"type": "decimal", "precision": 20, "scale": 2},
        "observed": {"type": "datetime", "timezone": "UTC"}
      }
    }
  },
  "rules": [
    {
      "code": "EXACT_AMOUNT",
      "level": "error",
      "column": "amount",
      "when": {"column": "id", "operator": "is_present"},
      "checks": [{"type": "equals", "value": 9007199254740993.01}]
    },
    {
      "code": "KNOWN_ID",
      "level": "error",
      "column": "id",
      "checks": [{"type": "referential_integrity", "reference_file": "references/ids.csv", "reference_column": "id"}]
    }
  ],
  "dataset_rules": [
    {"code": "TOTAL", "level": "error", "metric": "sum", "column": "amount", "equals": 9007199254740993.01}
  ]
}`
	// Both documents share a directory so relative paths resolve identically.
	directory := t.TempDir()
	loadAndValidate := func(name, text string) *Config {
		t.Helper()
		cfg, err := Load(context.Background(), writeFile(t, directory, name, text))
		if err != nil {
			t.Fatalf("Load(%s) error = %v", name, err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate(%s) error = %v", name, err)
		}
		return cfg
	}
	yamlConfig := loadAndValidate("pipeline.yaml", yamlDocument)
	jsonConfig := loadAndValidate("pipeline.json", jsonDocument)

	if !reflect.DeepEqual(yamlConfig, jsonConfig) {
		t.Fatalf("normalized YAML and JSON differ:\nYAML: %#v\nJSON: %#v", yamlConfig, jsonConfig)
	}
	exact := jsonConfig.Rules[0].Checks[0].Params["value"]
	number, ok := exact.(json.Number)
	if !ok {
		t.Fatalf("JSON equals value = %T(%v), want json.Number", exact, exact)
	}
	if got, want := number.String(), "9007199254740993.01"; got != want {
		t.Errorf("JSON equals value = %s, want %s", got, want)
	}
}

func TestStrictJSONPipelineDocuments(t *testing.T) {
	for _, tc := range []struct{ name, document string }{
		{name: "yaml syntax", document: "name: not-json\n"},
		{name: "trailing comma", document: `{"name":"bad",}`},
		{name: "multiple values", document: `{"name":"one"} {"name":"two"}`},
		{name: "sequence root", document: `[]`},
		{name: "duplicate root", document: `{"name":"one","name":"two"}`},
		{name: "duplicate nested", document: `{"context":{"x":1,"x":2}}`},
		{name: "unknown root", document: `{"unexpected":true}`},
		{
			name:     "unknown parameter",
			document: `{"rules":[{"code":"A","level":"error","checks":[{"type":"not_null","typo":true}]}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadNamedText(t, "pipeline.json", tc.document)
			if err == nil {
				err = cfg.Validate()
			}
			if err == nil {
				t.Fatal("Load+Validate error = nil, want invalid JSON pipeline rejected")
			}
		})
	}
	t.Run("unsupported extension", func(t *testing.T) {
		_, err := loadNamedText(t, "pipeline.toml", `name = "unsupported"`)
		assertErrorContains(t, "Load(pipeline.toml)", err, ".yaml, .yml, or .json")
	})
	t.Run("uppercase JSON extension", func(t *testing.T) {
		if _, err := loadNamedText(t, "PIPELINE.JSON", `{"name":"uppercase extension"}`); err != nil {
			t.Fatalf("Load(PIPELINE.JSON) error = %v, want nil", err)
		}
	})
}

func TestExplicitZeroIsNotDefaulted(t *testing.T) {
	for _, tc := range []struct{ name, field string }{
		{name: "workers", field: "execution: {workers: 0}"},
		{name: "max depth", field: "execution: {max_depth: 0}"},
		{name: "infer schema length", field: "input: {infer_schema_length: 0}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadText(t, "source: {uris: [data.csv]}\n"+tc.field+"\n")
			if err != nil {
				t.Fatal(err)
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want explicit zero rejected rather than defaulted")
			}
		})
	}
}

func TestLoadOrderPathsAndContext(t *testing.T) {
	cfg := loadValid(t, `context: {expected: 7}
source: {uris: [data.csv, "s3://bucket/input.csv"]}
output: {uri: output}
input:
  null_values: []
  schema:
    columns:
      zebra: {type: string}
      alpha: {type: int64}
transforms:
  - {type: rename_columns, mapping_file: mappings/names.csv}
  - {type: schema_rename, schema_file: mappings/schema.json, source_key: vendor}
rules:
  - code: REF
    level: error
    column: zebra
    message: "Expected {expected}; leave {unknown} intact"
    checks: [{type: referential_integrity, reference_file: references/keys.csv, reference_column: id}]
`)
	local := func(rel string) string { return filepath.Join(cfg.BaseDir, rel) }

	t.Run("schema keeps declaration order", func(t *testing.T) {
		want := []string{"zebra", "alpha"}
		if !slices.Equal(cfg.Input.Schema.Order, want) {
			t.Errorf("schema order = %q, want %q", cfg.Input.Schema.Order, want)
		}
	})
	t.Run("explicit empty null markers survive", func(t *testing.T) {
		if cfg.Input.NullValues == nil {
			t.Error("null markers = nil, want explicit empty slice")
		}
		if len(cfg.Input.NullValues) != 0 {
			t.Errorf("null markers = %q, want none", cfg.Input.NullValues)
		}
	})
	t.Run("local sources resolve against config directory", func(t *testing.T) {
		want := []string{local("data.csv"), "s3://bucket/input.csv"}
		if !slices.Equal(cfg.Source.URIs, want) {
			t.Errorf("source URIs = %q, want %q", cfg.Source.URIs, want)
		}
	})
	t.Run("referenced files resolve against config directory", func(t *testing.T) {
		if len(cfg.Transforms) != 2 {
			t.Fatalf("transforms = %d, want 2", len(cfg.Transforms))
		}
		referenceFile, _ := cfg.Rules[0].Checks[0].Params["reference_file"].(string)
		for _, path := range []struct{ name, got, want string }{
			{name: "output URI", got: cfg.Output.URI, want: local("output")},
			{name: "rename mapping file", got: cfg.Transforms[0].MappingFile, want: local("mappings/names.csv")},
			{name: "schema rename file", got: cfg.Transforms[1].SchemaFile, want: local("mappings/schema.json")},
			{name: "reference file", got: referenceFile, want: local("references/keys.csv")},
		} {
			if path.got != path.want {
				t.Errorf("%s = %q, want %q", path.name, path.got, path.want)
			}
		}
	})
	t.Run("context fills known placeholders only", func(t *testing.T) {
		if got, want := cfg.Rules[0].Message, "Expected 7; leave {unknown} intact"; got != want {
			t.Errorf("rule message = %q, want %q", got, want)
		}
	})
}

func TestContextSubstitutionIsBounded(t *testing.T) {
	value := strings.Repeat("x", 1024)
	// One placeholder more than fits in maxExpandedMessageBytes once expanded.
	message := strings.Repeat("{large}", maxExpandedMessageBytes/len(value)+1)
	_, err := loadText(t, fmt.Sprintf("context: {large: %q}\nrules: [{code: EXPAND, message: %q}]\n", value, message))
	assertErrorContains(t, "oversized context expansion", err, "rule EXPAND", "bound")
}

func TestLoadObservesCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Load(ctx, filepath.Join(t.TempDir(), "pipeline.yml")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load(canceled ctx) error = %v, want context.Canceled", err)
	}
}

func TestStrictYAML(t *testing.T) {
	for _, tc := range []struct{ name, text string }{
		{name: "empty", text: ""},
		{name: "sequence", text: "[]"},
		{name: "duplicate root", text: "name: a\nname: b\n"},
		{name: "duplicate nested", text: "context: {x: 1, x: 2}"},
		{name: "unknown root", text: "unexpected: true"},
		{name: "unknown nested", text: "input: {mode: raw_strings, unknown: yes}"},
		{name: "unknown column", text: "input: {schema: {columns: {x: {type: string, optional: true}}}}"},
		{
			name: "unknown condition",
			text: "rules: [{code: A, level: error, when: {column: x, operator: eq, value: 1, typo: 1}}]",
		},
		{name: "multiple documents", text: "name: one\n---\nname: two\n"},
		{name: "alias", text: "context: {x: &a [1], y: *a}"},
		{name: "merge", text: "context: {x: {<<: {a: 1}}}"},
		{name: "custom tag", text: "name: !secret value"},
		{name: "nonstring key", text: "context: {1: value}"},
		{name: "coerced string", text: "input: {null_values: [1]}"},
		{name: "null section", text: "input: null"},
		{name: "noninteger", text: "execution: {workers: 1.5}"},
		{name: "null check", text: "rules: [{code: A, checks: [null]}]"},
		{
			name: "unary null value",
			text: "rules: [{code: A, level: error, when: {column: x, operator: is_present, value: null}}]",
		},
		{
			name: "composition null leaf",
			text: "rules: [{code: A, level: error, when: {all: [{column: x, operator: is_present}], value: null}}]",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := loadText(t, tc.text); err == nil {
				t.Fatal("Load() error = nil, want malformed YAML rejected")
			}
		})
	}
}

func TestBoundedYAML(t *testing.T) {
	tooDeep := maxYAMLDepth + 1
	for _, tc := range []struct{ name, text, wantErr string }{
		{
			name:    "document over size limit",
			text:    "#" + strings.Repeat("x", MaxConfigBytes),
			wantErr: "exceeds",
		},
		{
			name:    "nesting over depth limit",
			text:    "context: {x: " + strings.Repeat("[", tooDeep) + "0" + strings.Repeat("]", tooDeep) + "}",
			wantErr: "depth/node",
		},
		{
			name:    "sequence over node limit",
			text:    "context: {x: [" + strings.Repeat("0,", maxYAMLNodes) + "0]}",
			wantErr: "depth/node",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadText(t, tc.text)
			assertErrorContains(t, "Load()", err, tc.wantErr)
		})
	}
}

func TestParseBytes(t *testing.T) {
	for _, tc := range []struct {
		text string
		want int64
	}{
		{text: "1", want: 1},
		{text: "42B", want: 42},
		{text: "8MiB", want: 8 << 20},
		{text: "512MiB", want: 512 << 20},
		{text: "2GiB", want: 2 << 30},
		{text: "1TiB", want: 1 << 40},
		{text: "1KB", want: 1000},
		{text: "1MB", want: 1000000},
		{text: " 5KiB ", want: 5 << 10},
	} {
		got, err := parseBytes(tc.text)
		if err != nil || got != tc.want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", tc.text, got, err, tc.want)
		}
	}
	invalid := []string{
		"", "0", "-1", "1.5MiB", "MiB", "1 mib", "1PB",
		"9223372036854775808",    // overflows int64
		"9223372036854775807KiB", // overflows after scaling
	}
	for _, text := range invalid {
		if got, err := parseBytes(text); err == nil {
			t.Errorf("parseBytes(%q) = %d, nil; want error", text, got)
		}
	}
}

func TestValidBuiltInArguments(t *testing.T) {
	pair := []any{"a", "b"}
	for _, check := range []Check{
		{Type: "not_null"},
		{Type: "not_blank"},
		{Type: "is_present"},
		{Type: "valid_length", Params: map[string]any{"length": 0}},
		{Type: "min_length", Params: map[string]any{"min_length": 1}},
		{Type: "max_length", Params: map[string]any{"max_length": 10}},
		{Type: "valid_range", Params: map[string]any{"min": 1, "max": 2}},
		{Type: "equals", Params: map[string]any{"value": "x"}},
		{Type: "not_equals", Params: map[string]any{"value": false}},
		{Type: "all_equal", Params: map[string]any{"columns": pair, "value": 0}},
		{Type: "has_null_keys", Params: map[string]any{"columns": pair}},
		{Type: "valid_values", Params: map[string]any{"values": []any{"a", nil, 2}}},
		{Type: "matches_pattern", Params: map[string]any{"pattern": "a+"}},
		{Type: "is_unique"},
		{Type: "is_unique_composite", Params: map[string]any{"columns": pair}},
		{Type: "compare_columns", Params: map[string]any{"other_column": "b", "operator": "<="}},
		{Type: "referential_integrity", Params: map[string]any{"reference_file": "r.csv", "reference_column": "id"}},
		{Type: "referential_integrity_composite", Params: map[string]any{
			"columns":           pair,
			"reference_file":    "r.csv",
			"reference_columns": []any{"x", "y"},
		}},
		{Type: "required_columns", Params: map[string]any{"columns": []any{"a"}}},
		{Type: "schema_matches", Params: map[string]any{
			"columns":         map[string]any{"a": "int64"},
			"mode":            "castable",
			"require_columns": true,
		}},
		{Type: "valid_schema_columns", Params: map[string]any{"reference_file": "r.csv", "reference_column": "id"}},
		{Type: "type_check", Params: map[string]any{"expected_type": "int64"}},
		{Type: "date_format", Params: map[string]any{"formats": []any{"%Y-%m-%d"}}},
		{Type: "timestamp_window", Params: map[string]any{"min": "2024-01-01T00:00:00Z"}},
	} {
		t.Run(check.Type, func(t *testing.T) {
			if err := configWithCheck(check).Validate(); err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestInvalidArguments(t *testing.T) {
	for _, tc := range []struct {
		name  string
		check Check
	}{
		{name: "unknown check type", check: Check{Type: "typo"}},
		{name: "unknown parameter", check: Check{Type: "not_null", Params: map[string]any{"colum": "x"}}},
		{name: "fractional length", check: Check{Type: "valid_length", Params: map[string]any{"length": 1.5}}},
		{name: "negative min length", check: Check{Type: "min_length", Params: map[string]any{"min_length": -1}}},
		{name: "range min above max", check: Check{Type: "valid_range", Params: map[string]any{"min": 3, "max": 2}}},
		{name: "not_equals null value", check: Check{Type: "not_equals", Params: map[string]any{"value": nil}}},
		{
			name:  "all_equal without columns",
			check: Check{Type: "all_equal", Params: map[string]any{"columns": []any{}, "value": 1}},
		},
		{name: "valid_values not a list", check: Check{Type: "valid_values", Params: map[string]any{"values": "a"}}},
		{
			name:  "unsupported regex lookahead",
			check: Check{Type: "matches_pattern", Params: map[string]any{"pattern": "(?=unsupported)"}},
		},
		{
			name:  "unknown comparison operator",
			check: Check{Type: "compare_columns", Params: map[string]any{"other_column": "x", "operator": "eq"}},
		},
		{
			name:  "reference without column",
			check: Check{Type: "referential_integrity", Params: map[string]any{"reference_file": "r.csv"}},
		},
		{
			name: "composite reference column count mismatch",
			check: Check{Type: "referential_integrity_composite", Params: map[string]any{
				"columns":           []any{"a", "b"},
				"reference_file":    "r.csv",
				"reference_columns": []any{"x"},
			}},
		},
		{
			name:  "schema columns not a map",
			check: Check{Type: "schema_matches", Params: map[string]any{"columns": []any{"a"}}},
		},
		{
			name:  "schema column of unknown type",
			check: Check{Type: "schema_matches", Params: map[string]any{"columns": map[string]any{"a": "unknown"}}},
		},
		{
			name: "schema require_columns not a bool",
			check: Check{Type: "schema_matches", Params: map[string]any{
				"columns":         map[string]any{"a": "int64"},
				"require_columns": "true",
			}},
		},
		{
			name:  "type_check unknown type",
			check: Check{Type: "type_check", Params: map[string]any{"expected_type": "object"}},
		},
		{
			name:  "unsupported date directive",
			check: Check{Type: "date_format", Params: map[string]any{"format": "%Q"}},
		},
		{
			name:  "timestamp bound without offset or timezone",
			check: Check{Type: "timestamp_window", Params: map[string]any{"min": "2024-01-01T00:00:00"}},
		},
		{
			name: "timestamp window min after max",
			check: Check{Type: "timestamp_window", Params: map[string]any{
				"min": "2025-01-01T00:00:00Z",
				"max": "2024-01-01T00:00:00Z",
			}},
		},
		{
			name:  "column and columns together",
			check: Check{Type: "not_null", Params: map[string]any{"column": "x", "columns": []any{"y"}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := configWithCheck(tc.check).Validate(); err == nil {
				t.Fatalf("Validate() error = nil, want %+v rejected", tc.check)
			}
		})
	}
}

func TestRuleNamespaceAndEmptyChecks(t *testing.T) {
	t.Run("rule without checks is valid", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Rules = []Rule{{Code: "NOOP", Level: "warn"}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v, want nil", err)
		}
	})
	for _, tc := range []struct {
		name         string
		rule         Rule
		datasetRules []DatasetRule
	}{
		{
			name:         "row and dataset rules share a code",
			rule:         Rule{Code: "NOOP", Level: "warn"},
			datasetRules: []DatasetRule{{Code: "NOOP", Level: "error", Metric: "row_count", Min: 1}},
		},
		{name: "reserved ingestion code", rule: Rule{Code: "INGEST_TYPE", Level: "warn"}},
		{name: "unknown severity", rule: Rule{Code: "A", Level: "warning"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.Rules = []Rule{tc.rule}
			cfg.DatasetRules = tc.datasetRules
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
		})
	}
}

// nestedNot wraps a leaf condition in depth `not` conditions.
func nestedNot(depth int) *Condition {
	condition := &Condition{Column: "x", Operator: "is_present"}
	for range depth {
		condition = &Condition{Not: condition}
	}
	return condition
}

// wideAll returns an `all` condition with width leaf children.
func wideAll(width int) *Condition {
	condition := &Condition{All: make([]Condition, width)}
	for i := range condition.All {
		condition.All[i] = Condition{Column: "x", Operator: "is_present"}
	}
	return condition
}

func TestConditionValidation(t *testing.T) {
	good := &Condition{All: []Condition{
		{Column: "active", Operator: "eq", Value: true},
		{Not: &Condition{Column: "x", Operator: "is_null"}},
	}}
	conditionalRule := func(when *Condition, check Check) *Config {
		cfg := baseConfig()
		cfg.Rules = []Rule{{Code: "R", Level: "error", Column: "x", When: when, Checks: []Check{check}}}
		return cfg
	}
	notBlank := Check{Type: "not_blank"}

	t.Run("nested all/not condition is valid", func(t *testing.T) {
		if err := conditionalRule(good, notBlank).Validate(); err != nil {
			t.Fatalf("Validate() error = %v, want nil", err)
		}
	})
	for _, tc := range []struct {
		name string
		when *Condition
	}{
		{name: "empty condition", when: &Condition{}},
		{name: "empty all", when: &Condition{All: []Condition{}}},
		{name: "unary operator with value", when: &Condition{Column: "x", Operator: "is_null", Value: 1}},
		{name: "in operator with scalar value", when: &Condition{Column: "x", Operator: "in", Value: "a"}},
		{
			name: "leaf and any together",
			when: &Condition{
				Column:   "x",
				Operator: "eq",
				Value:    1,
				Any:      []Condition{{Column: "y", Operator: "is_present"}},
			},
		},
		// Validation caps conditions at depth 16 and 1024 nodes.
		{name: "nesting deeper than limit", when: nestedNot(18)},
		{name: "more nodes than limit", when: wideAll(1025)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := conditionalRule(tc.when, notBlank).Validate(); err == nil {
				t.Error("Validate() error = nil, want condition rejected")
			}
		})
	}
	t.Run("conditional uniqueness is rejected", func(t *testing.T) {
		if err := conditionalRule(good, Check{Type: "is_unique"}).Validate(); err == nil {
			t.Fatal("Validate() error = nil, want conditional is_unique rejected")
		}
	})
}

func TestDatasetAssertions(t *testing.T) {
	datasetConfig := func(rules ...DatasetRule) *Config {
		cfg := baseConfig()
		cfg.Input.Schema = &Schema{Columns: map[string]Column{
			"n": {Type: "decimal", Precision: 18, Scale: 2},
			"f": {Type: "float64"},
		}}
		cfg.Rules = []Rule{{Code: "ROW", Level: "error"}}
		cfg.DatasetRules = rules
		return cfg
	}

	t.Run("valid assertions", func(t *testing.T) {
		cfg := datasetConfig(
			DatasetRule{Code: "N", Level: "error", Metric: "row_count", Min: 1},
			DatasetRule{Code: "F", Level: "warn", Metric: "rule_failure_fraction", RuleCode: "ROW", Max: 0.1},
			DatasetRule{Code: "S", Level: "error", Metric: "sum", Column: "n", Equals: "9007199254740993.12"},
		)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v, want nil", err)
		}
	})
	for _, tc := range []struct {
		name string
		rule DatasetRule
	}{
		{name: "row count without bounds", rule: DatasetRule{Metric: "row_count"}},
		{name: "negative row count", rule: DatasetRule{Metric: "row_count", Min: -1}},
		{name: "fractional row count", rule: DatasetRule{Metric: "row_count", Min: 1.5}},
		{name: "row count with column", rule: DatasetRule{Metric: "row_count", Min: 1, Column: "n"}},
		{name: "null fraction above one", rule: DatasetRule{Metric: "null_fraction", Column: "n", Max: 1.1}},
		{name: "null count without column", rule: DatasetRule{Metric: "null_count", Min: 0}},
		{
			name: "failure count for unknown rule",
			rule: DatasetRule{Metric: "rule_failure_count", RuleCode: "missing", Max: 0},
		},
		{name: "sum min above max", rule: DatasetRule{Metric: "sum", Column: "n", Min: 2, Max: 1}},
		{name: "sum equals with min", rule: DatasetRule{Metric: "sum", Column: "n", Equals: 1, Min: 0}},
		{name: "float avg equality without tolerance", rule: DatasetRule{Metric: "avg", Column: "f", Equals: 1}},
		{name: "unknown metric", rule: DatasetRule{Metric: "unknown", Min: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.rule
			rule.Code, rule.Level = "D", "error"
			if err := datasetConfig(rule).Validate(); err == nil {
				t.Errorf("Validate() error = nil, want %+v rejected", rule)
			}
		})
	}
	t.Run("float avg equality with tolerance", func(t *testing.T) {
		tolerance := 0.001
		cfg := datasetConfig(DatasetRule{
			Code: "D", Level: "error", Metric: "avg", Column: "f", Equals: 1, Tolerance: &tolerance,
		})
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v, want nil", err)
		}
	})
}

func TestSourceAndExecutionValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{name: "neither uris nor prefix", change: func(c *Config) { c.Source.URIs = nil }},
		{name: "uris and prefix together", change: func(c *Config) { c.Source.Prefix = "input" }},
		{name: "prefix without filter", change: func(c *Config) { c.Source.URIs = nil; c.Source.Prefix = "input" }},
		{name: "output inside source prefix", change: func(c *Config) {
			c.Source.URIs = nil
			c.Source.Prefix = "input"
			c.Source.Filter = "csv"
			c.Output.URI = "input/results"
		}},
		{name: "stdin source", change: func(c *Config) { c.Source.URIs = []string{"-"} }},
		{name: "https source", change: func(c *Config) {
			c.Source.URIs = []string{"https://example.invalid/data.csv"}
		}},
		{name: "negative workers", change: func(c *Config) { c.Execution.Workers = -1 }},
		{name: "too many workers", change: func(c *Config) { c.Execution.Workers = 33 }},
		{name: "negative memory budget", change: func(c *Config) { c.Execution.MemoryBudget = "-1MiB" }},
		{name: "newline delimiter", change: func(c *Config) { c.Input.Delimiter = "\n" }},
		{name: "headerless input without columns", change: func(c *Config) { v := false; c.Input.Header = &v }},
		{name: "json output format", change: func(c *Config) { c.Output.Format = "json" }},
		{name: "customer-provided encryption", change: func(c *Config) { c.Output.S3.SSE = "SSE-C" }},
		{name: "kms key without aws:kms", change: func(c *Config) { c.Output.S3.KMSKeyID = "key" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig()
			tc.change(cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("Validate() error = nil, want invalid configuration rejected")
			}
		})
	}
}

func TestWorkerCountBoundaries(t *testing.T) {
	for _, workers := range []int{1, 2, 8, 32} {
		cfg := baseConfig()
		cfg.Execution.Workers = workers
		if err := cfg.Validate(); err != nil {
			t.Errorf("workers=%d: %v", workers, err)
		}
	}
}

func TestSchemaValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		column Column
	}{
		{name: "unknown type", column: Column{Type: "object"}},
		{name: "precision above 38", column: Column{Type: "decimal", Precision: 39}},
		{name: "scale above precision", column: Column{Type: "decimal", Precision: 10, Scale: 11}},
		{name: "unknown timezone", column: Column{Type: "timestamp", Timezone: "Not/A_Zone"}},
		{name: "bad JSON pointer escape", column: Column{Type: "string", Path: "/bad~escape"}},
		{name: "true token spelled false", column: Column{Type: "bool", TrueValues: []string{"false"}}},
		{name: "unsupported date directive", column: Column{Type: "date", Formats: []string{"%Q"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.Input.Schema = &Schema{Columns: map[string]Column{"x": tc.column}}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want invalid schema rejected")
			}
		})
	}
}

func TestTimestampBoundsRejectDSTAmbiguity(t *testing.T) {
	const zone = "America/New_York"
	for _, tc := range []struct{ name, value string }{
		{name: "nonexistent spring-forward time", value: "2024-03-10T02:30:00"},
		{name: "ambiguous fall-back time", value: "2024-11-03T01:30:00"},
	} {
		if _, err := parseTimestampBound(tc.value, zone); err == nil {
			t.Errorf("%s: parseTimestampBound(%q) error = nil, want error", tc.name, tc.value)
		}
	}
	if _, err := parseTimestampBound("2024-11-03T01:30:00-04:00", zone); err != nil {
		t.Fatalf("explicit offset: parseTimestampBound error = %v, want nil", err)
	}
}

func TestExactYAMLNumbers(t *testing.T) {
	cfg := loadValid(t, `source: {uris: [input.csv]}
input:
  schema:
    columns:
      n: {type: decimal, precision: 38, scale: 2}
rules:
  - code: EXACT
    level: error
    column: n
    when: {column: n, operator: ge, value: 9007199254740993.12}
    checks: [{type: valid_range, min: 9007199254740993.11, max: 9007199254740993.13}]
  - code: VALUES
    level: warn
    column: n
    checks: [{type: valid_values, values: [+.5, 01.25, 1_000.5, 3.]}]
dataset_rules:
  - {code: SUM, level: error, metric: sum, column: n, equals: 9007199254740993.12}
`)
	minimum := cfg.Rules[0].Checks[0].Params["min"]
	if got, ok := minimum.(json.Number); !ok || string(got) != "9007199254740993.11" {
		t.Errorf("valid_range min = %#v, want json.Number(9007199254740993.11)", minimum)
	}
	exact := json.Number("9007199254740993.12")
	if got := cfg.Rules[0].When.Value; got != exact {
		t.Errorf("condition value = %#v, want %#v", got, exact)
	}
	if got := cfg.DatasetRules[0].Equals; got != exact {
		t.Errorf("dataset equals = %#v, want %#v", got, exact)
	}
	values, ok := cfg.Rules[1].Checks[0].Params["values"].([]any)
	if !ok {
		t.Fatalf("valid_values values = %T, want []any", cfg.Rules[1].Checks[0].Params["values"])
	}
	want := []any{json.Number("0.5"), json.Number("1.25"), json.Number("1000.5"), json.Number("3.0")}
	if !reflect.DeepEqual(values, want) {
		t.Errorf("normalized valid_values = %#v, want %#v", values, want)
	}
	if _, err := json.Marshal(cfg.Rules); err != nil {
		t.Errorf("json.Marshal(rules) error = %v, want numbers to round-trip", err)
	}
}

func TestNumericLiteralWorkIsBounded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{name: "huge exponent", value: "1e999999999"},
		{name: "exponent just over limit", value: "1e1001"},
		{name: "literal over 1024 digits", value: strings.Repeat("1", 1025)},
		{name: "hex float", value: "0x1p100"},
		{name: "fraction", value: "1/2"},
		{name: "NaN json number", value: json.Number("NaN")},
	} {
		if got, ok := decimalNumber(tc.value); ok {
			t.Errorf("%s: decimalNumber(%v) = %v, true; want rejected", tc.name, tc.value, got)
		}
	}
}

// TestExistingRuleDocuments loads rule documents copied from the Python
// validnator repository, so the Go loader keeps accepting them unchanged.
func TestExistingRuleDocuments(t *testing.T) {
	for _, name := range []string{"csv-validator-pipeline.yaml", "shared_oed_rules.yaml"} {
		t.Run(name, func(t *testing.T) {
			cfg, err := Load(context.Background(), filepath.Join("testdata", "legacy", name))
			if err != nil {
				t.Fatal(err)
			}
			cfg.Source.URIs = []string{"input.csv"}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
			if len(cfg.Rules) == 0 {
				t.Fatal("rules = 0, want the document's rules loaded")
			}
		})
	}
}

func FuzzParseBytes(f *testing.F) {
	for _, seed := range []string{"512MiB", "0", "9223372036854775807GiB", "1B", "bad"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		n, err := parseBytes(s)
		if err == nil && n <= 0 {
			t.Fatalf("parseBytes(%q) = %d, nil; want positive size or error", s, n)
		}
	})
}

func TestUnquotedTimestampBoundsRemainLexical(t *testing.T) {
	cfg := loadValid(t, `source: {uris: [data.csv]}
rules:
  - code: TIME
    level: error
    column: observed
    checks: [{type: timestamp_window, min: 2024-01-01T12:30:00-05:00}]
`)
	const want = "2024-01-01T12:30:00-05:00"
	if got := cfg.Rules[0].Checks[0].Params["min"]; got != want {
		t.Fatalf("timestamp_window min = %#v, want lexeme %q", got, want)
	}
}

func TestTransformArguments(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transform Transform
	}{
		{name: "unknown type", transform: Transform{Type: "unknown"}},
		{name: "rename without mapping", transform: Transform{Type: "rename_columns"}},
		{
			name: "inline mapping and mapping file",
			transform: Transform{
				Type: "rename_columns", Mapping: map[string]string{"a": "b"}, MappingFile: "names.csv",
			},
		},
		{
			name:      "negative source column",
			transform: Transform{Type: "rename_columns", MappingFile: "names.csv", SourceCol: -1},
		},
		{
			name:      "source column equals default target column",
			transform: Transform{Type: "rename_columns", MappingFile: "names.csv", SourceCol: 1},
		},
		{
			name:      "schema rename without source key",
			transform: Transform{Type: "schema_rename", SchemaFile: "schema.json"},
		},
		{
			name: "schema rename with mapping file",
			transform: Transform{
				Type: "schema_rename", SchemaFile: "schema.json", SourceKey: "vendor", MappingFile: "names.csv",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateTransform(tc.transform); err == nil {
				t.Errorf("validateTransform(%+v) error = nil, want error", tc.transform)
			}
		})
	}
	t.Run("aliases for excluded keys may share a target", func(t *testing.T) {
		good := Transform{
			Type:        "rename_columns",
			Mapping:     map[string]string{"a": "target", "unused": "target"},
			ExcludeKeys: []string{"unused"},
		}
		if err := validateTransform(good); err != nil {
			t.Fatalf("validateTransform error = %v, want nil", err)
		}
	})
}

func TestPortableDateDirectives(t *testing.T) {
	for _, format := range []string{"%Y-%m-%d", "%d/%b/%Y", "%Y-%m-%dT%H:%M:%S%:z", "%F %T", "%%"} {
		if err := validateDateFormat(format); err != nil {
			t.Errorf("validateDateFormat(%q) error = %v, want nil", format, err)
		}
	}
	for _, format := range []string{"%Q", "%x", "%G-%V", "%", "%:Q"} {
		if err := validateDateFormat(format); err == nil {
			t.Errorf("validateDateFormat(%q) error = nil, want unsupported format rejected", format)
		}
	}
}

// Bare names follow Python (int and float are 64-bit); sized names follow C
// without _t and Arrow/Parquet. Matching ignores case.
func TestCanonicalTypeAliases(t *testing.T) {
	for _, tc := range []struct{ alias, want string }{
		{alias: "boolean", want: "bool"},
		{alias: "int", want: "int64"},
		{alias: "integer", want: "int64"},
		{alias: "Int64", want: "int64"},
		{alias: "float", want: "float64"},
		{alias: "double", want: "float64"},
		{alias: "FLOAT32", want: "float32"},
		{alias: "str", want: "string"},
		{alias: "utf8", want: "string"},
		{alias: "large_string", want: "string"},
		{alias: "bytes", want: "binary"},
		{alias: "large_binary", want: "binary"},
		{alias: "date32", want: "date"},
		{alias: "date64", want: "date"},
		{alias: "datetime", want: "timestamp"},
		{alias: "Decimal", want: "decimal"},
		{alias: "decimal128", want: "decimal"},
	} {
		got, ok := CanonicalType(tc.alias)
		if !ok || got != tc.want {
			t.Errorf("CanonicalType(%q) = %q, %v; want %q, true", tc.alias, got, ok, tc.want)
		}
	}
}

// Names whose size Valet would have to guess are rejected with the spelling to
// write instead, wherever a type is declared.
func TestUnsupportedTypeNamesSuggestASpelling(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{name: "long", want: `"long" is not a supported type: long is 32-bit on Windows and 64-bit on Linux and macOS; write int32 or int64`},
		{name: "unsigned  int", want: "write uint32"},
		{name: "char", want: "write int8 or uint8, or str for text"},
		{name: "int32_t", want: `"int32_t" is not a supported type: write int32`},
		{name: "UINT8_T", want: "write uint8"},
		{name: "object", want: `"object" is not a supported type`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, doc := range []string{
				"input: {schema: {columns: {x: {type: '" + tc.name + "'}}}}\nrules: []\n",
				"rules:\n  - {code: R, level: error, column: x, checks: [{type: type_check, expected_type: '" + tc.name + "'}]}\n",
				"rules:\n  - {code: R, level: error, checks: [{type: schema_matches, columns: {x: '" + tc.name + "'}}]}\n",
			} {
				_, err := loadAndValidate(t, "source: {uris: [data.csv]}\n"+doc)
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Errorf("%q: error = %v, want it to contain %q", doc, err, tc.want)
				}
			}
		})
	}
}

// TestPortableDateDirectivesParse checks the composite and alias directives
// translate to Go layouts. The consumers (source, checks) import this package,
// so a consumer round-trip cannot live here.
func TestPortableDateDirectivesParse(t *testing.T) {
	for _, tc := range []struct{ format, want string }{
		{format: "%D", want: "01/02/06"},
		{format: "%F", want: "2006-01-02"},
		{format: "%R", want: "15:04"},
		{format: "%T", want: "15:04:05"},
		{format: "%h", want: "Jan"},
		{format: "%:z", want: "-07:00"},
	} {
		got, err := DateLayout(tc.format)
		if err != nil {
			t.Errorf("DateLayout(%q) error = %v, want nil", tc.format, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DateLayout(%q) = %q, want %q", tc.format, got, tc.want)
		}
	}
}

func TestCanonicalLocationOverlap(t *testing.T) {
	for _, tc := range []struct{ name, a, b string }{
		{name: "s3 key prefix", a: "s3://bucket/data", b: "s3://bucket/database/results"},
		{name: "percent-encoded s3 separator", a: "s3://bucket/data%2Finput", b: "s3://bucket/data/input/results"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			overlap, err := pathsOverlap(tc.a, tc.b)
			if err != nil || !overlap {
				t.Errorf("pathsOverlap(%q, %q) = %v, %v; want true, nil", tc.a, tc.b, overlap, err)
			}
		})
	}
	t.Run("output through symlink into source", func(t *testing.T) {
		source := filepath.Join(t.TempDir(), "source")
		if err := os.Mkdir(source, 0700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(t.TempDir(), "output-link")
		if err := os.Symlink(source, link); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		overlap, err := pathsOverlap(source, filepath.Join(link, "results"))
		if err != nil || !overlap {
			t.Errorf("pathsOverlap(source, symlinked output) = %v, %v; want true, nil", overlap, err)
		}
	})
}

func TestInferInputTypeUsesDecodedS3Key(t *testing.T) {
	if got := InferInputType("s3://bucket/data%2Eparquet"); got != "parquet" {
		t.Fatalf("InferInputType(data%%2Eparquet) = %q, want %q", got, "parquet")
	}
}

func TestDatasetToleranceApplicability(t *testing.T) {
	tolerance := 1.0
	schema := &Schema{Columns: map[string]Column{
		"floating": {Type: "float64"},
		"integer":  {Type: "int64"},
		"decimal":  {Type: "decimal", Precision: 10, Scale: 2},
	}}
	t.Run("float sum accepts tolerance", func(t *testing.T) {
		valid := DatasetRule{Code: "FLOAT", Metric: "sum", Column: "floating", Equals: 10, Tolerance: &tolerance}
		if err := validateDatasetRule(valid, nil, schema); err != nil {
			t.Fatalf("validateDatasetRule error = %v, want nil", err)
		}
	})
	for _, rule := range []DatasetRule{
		{Code: "ROWS", Metric: "row_count", Equals: 10, Tolerance: &tolerance},
		{Code: "FRACTION", Metric: "null_fraction", Column: "integer", Equals: 1, Tolerance: &tolerance},
		{Code: "INTEGER", Metric: "sum", Column: "integer", Equals: 10, Tolerance: &tolerance},
		{Code: "DECIMAL", Metric: "sum", Column: "decimal", Equals: 10, Tolerance: &tolerance},
	} {
		t.Run(rule.Code+" rejects tolerance", func(t *testing.T) {
			if err := validateDatasetRule(rule, nil, schema); err == nil {
				t.Errorf("validateDatasetRule(%s) error = nil, want tolerance rejected", rule.Code)
			}
		})
	}
}

func TestReservedColumnsAndPortableTimezones(t *testing.T) {
	t.Run("reserved column name suggests rename", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Input.Schema = &Schema{Columns: map[string]Column{"__valet_source": {Type: "string"}}}
		assertErrorContains(t, "Validate()", cfg.Validate(), "rename")
	})
	t.Run("local schema timezone", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Input.Schema = &Schema{Columns: map[string]Column{"time": {Type: "timestamp", Timezone: "Local"}}}
		if err := cfg.Validate(); err == nil {
			t.Fatal("Validate() error = nil, want machine-dependent timezone rejected")
		}
	})
	t.Run("local timestamp bound timezone", func(t *testing.T) {
		window := Check{Type: "timestamp_window", Params: map[string]any{
			"min":      "2024-01-01T00:00:00",
			"timezone": "Local",
		}}
		cfg := baseConfig()
		cfg.Rules = []Rule{{Code: "TIME", Level: "error", Column: "time", Checks: []Check{window}}}
		if err := cfg.Validate(); err == nil {
			t.Fatal("Validate() error = nil, want machine-dependent timestamp bound rejected")
		}
	})
}

func TestInputTypeInferredFromEffectiveSource(t *testing.T) {
	for _, tc := range []struct{ uri, want string }{
		{uri: "data.parquet", want: "parquet"},
		{uri: "DATA.PARQUET.GZ", want: "parquet"},
		{uri: "rows.ndjson", want: "ndjson"},
		{uri: "rows.jsonl.gz", want: "ndjson"},
		{uri: "rows.json", want: "json"},
		{uri: "rows.csv", want: "csv"},
		{uri: "rows.txt", want: "csv"},
		{uri: "s3://bucket/key.json.gz", want: "json"},
	} {
		t.Run(tc.uri, func(t *testing.T) {
			cfg := &Config{Source: Source{URIs: []string{tc.uri}}}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
			if cfg.Input.Type != tc.want {
				t.Errorf("inferred input type = %q, want %q", cfg.Input.Type, tc.want)
			}
		})
	}
	t.Run("prefix filter", func(t *testing.T) {
		cfg := &Config{Source: Source{Prefix: "s3://bucket/in/", Filter: "ndjson"}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v, want nil", err)
		}
		if cfg.Input.Type != "ndjson" {
			t.Errorf("inferred input type = %q, want %q", cfg.Input.Type, "ndjson")
		}
	})
	t.Run("explicit type wins over suffix", func(t *testing.T) {
		cfg := loadValid(t, `input: {type: csv}
source: {uris: [data.parquet]}
`)
		if cfg.Input.Type != "csv" {
			t.Errorf("input type = %q, want explicit %q", cfg.Input.Type, "csv")
		}
	})
}

func TestValidateIsIdempotent(t *testing.T) {
	cfg := loadValid(t, `source: {uris: [data.ndjson]}
execution: {memory_budget: 64MiB}
input: {schema: {columns: {b: {type: string}, a: {type: int64}}}}
rules: [{code: R, level: error, column: a, checks: [{type: not_null}]}]
stats: [{code: S, column: a, aggregate: sum}]
`)
	first := *cfg
	firstSchema := *cfg.Input.Schema
	if err := cfg.Validate(); err != nil {
		t.Fatalf("second Validate() error = %v, want nil", err)
	}
	if !reflect.DeepEqual(&first, cfg) {
		t.Errorf("second Validate changed configuration:\nbefore %+v\nafter  %+v", first, *cfg)
	}
	if !reflect.DeepEqual(&firstSchema, cfg.Input.Schema) {
		t.Errorf("second Validate changed schema:\nbefore %+v\nafter  %+v", firstSchema, *cfg.Input.Schema)
	}
	if got := cfg.BudgetBytes(); got != 64<<20 {
		t.Errorf("BudgetBytes() = %d, want %d", got, 64<<20)
	}
	if cfg.Input.Type != "ndjson" {
		t.Errorf("input type = %q, want %q", cfg.Input.Type, "ndjson")
	}
	if cfg.Rules[0].Name != "R" {
		t.Errorf("rule name = %q, want code %q", cfg.Rules[0].Name, "R")
	}
	if cfg.Stats[0].Name != "S" {
		t.Errorf("stat name = %q, want code %q", cfg.Stats[0].Name, "S")
	}
}

func TestForReferenceIsIndependent(t *testing.T) {
	cfg := loadValid(t, `source: {uris: [data.csv], s3: {region: eu-west-2}}
input: {null_values: ["NULL"]}
execution: {batch_target: 1MiB}
rules: [{code: R, level: error, column: a, checks: [{type: not_null}]}]
`)
	ref := cfg.ForReference("s3://bucket/ref.parquet", []string{"k", "v", "k"})

	if ref.Input.Type != "parquet" {
		t.Errorf("reference input type = %q, want %q", ref.Input.Type, "parquet")
	}
	if ref.Input.Mode != "raw_strings" {
		t.Errorf("reference input mode = %q, want %q", ref.Input.Mode, "raw_strings")
	}
	if ref.Source.S3.Region != "eu-west-2" {
		t.Errorf("reference S3 region = %q, want inherited %q", ref.Source.S3.Region, "eu-west-2")
	}
	if len(ref.Rules) != 0 {
		t.Errorf("reference rules = %d, want 0", len(ref.Rules))
	}
	if want := []string{"k", "v"}; !slices.Equal(ref.Input.Schema.Order, want) {
		t.Errorf("reference schema order = %q, want deduplicated %q", ref.Input.Schema.Order, want)
	}
	if ref.Input.Schema.ExtraFields != "ignore" {
		t.Errorf("reference extra_fields = %q, want %q", ref.Input.Schema.ExtraFields, "ignore")
	}
	if !ref.Input.Schema.Columns["v"].Required {
		t.Error("reference column v required = false, want true")
	}
	if got := ref.BatchBytes(); got != 1<<20 {
		t.Errorf("reference BatchBytes() = %d, want %d", got, 1<<20)
	}
	if got, want := ref.BudgetBytes(), cfg.BudgetBytes(); got != want {
		t.Errorf("reference BudgetBytes() = %d, want run's %d", got, want)
	}
	ref.Input.NullValues[0] = "changed"
	if got := cfg.Input.NullValues[0]; got != "NULL" {
		t.Errorf("run null marker after mutating reference = %q, want %q (must not share storage)", got, "NULL")
	}
}
