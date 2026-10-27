package config

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"unicode"
)

// SchemaID is the published URL of the JSON Schema for config_version 1
// pipeline documents.
const SchemaID = "https://richardrh.github.io/valet/schema/v1/pipeline.schema.json"

// JSONSchema describes pipeline documents as a JSON Schema (draft 2020-12) for
// editors and other clients. It is derived from Config's fields and the Checks
// table, the same sources the loader validates against, plus the allowed
// values Validate enforces. Checks that need several fields or the data itself
// (min <= max, column existence) stay with Validate and `valet plan`.
func JSONSchema() ([]byte, error) {
	b := schemaBuilder{defs: map[string]any{
		"check":     checkSchema(),
		"check_map": checkMapSchema(),
		"condition": conditionSchema(),
		"scalar":    scalarSchema,
		"type_name": typeEnum(),
	}}
	root := b.object(reflect.TypeFor[Config]())
	root["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	root["$id"] = SchemaID
	root["title"] = "Valet pipeline"
	root["description"] = "A Valet validation pipeline (YAML or JSON). See https://richardrh.github.io/valet/guides/configuration/."
	root["properties"].(map[string]any)["$schema"] = map[string]any{
		"type":        "string",
		"description": "URL of this JSON Schema, for editors; Valet ignores it.",
	}
	root["$defs"] = b.defs
	out, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

var scalarSchema = map[string]any{"type": []string{"string", "number", "boolean", "null"}}

func enum(values ...string) map[string]any { return map[string]any{"type": "string", "enum": values} }

var numberOrString = map[string]any{"type": []string{"number", "string"}}

// fieldSchemas pins fields whose allowed values Validate restricts, keyed by
// "Struct.yaml_name".
var fieldSchemas = map[string]any{
	"Config.config_version":    map[string]any{"type": "integer", "enum": []int{0, 1}},
	"Config.context":           map[string]any{"type": "object"},
	"Input.type":               enum("csv", "parquet", "ndjson", "json"),
	"Input.mode":               enum("raw_strings", "infer_types"),
	"Input.on_type_error":      enum("row_error", "abort"),
	"Input.on_malformed":       enum("abort", "reject"),
	"InputParquet.statistics":  enum(ParquetStatisticsPolicies...),
	"Output.mode":              enum(OutputModes...),
	"Output.format":            enum("csv", "parquet"),
	"Output.issues":            enum("ndjson"),
	"OutputParquet.statistics": enum("none", "bounded"),
	"Source.filter":            enum("csv", "parquet", "json", "ndjson"),
	"S3.sse":                   enum("AES256", "aws:kms"),
	"Schema.extra_fields":      enum("error", "ignore"),
	"Column.type":              ref("type_name"),
	"Rule.level":               enum("error", "warn"),
	"Rule.checks": map[string]any{
		"description": "Checks, as a mapping keyed by check name or a list of {type: ...} entries.",
		"anyOf":       []any{ref("check_map"), map[string]any{"type": "array", "items": ref("check")}},
	},
	"DatasetRule.level": enum("error", "warn"),
	"DatasetRule.metric": enum("row_count",
		"missing_count", "null_count", "blank_count", "conversion_error_count",
		"missing_fraction", "null_fraction", "blank_fraction", "conversion_error_fraction",
		"rule_failure_count", "rule_failure_fraction", "sum", "min", "max", "avg"),
	"DatasetRule.min":    numberOrString,
	"DatasetRule.max":    numberOrString,
	"DatasetRule.equals": numberOrString,
	"Stat.aggregate":     enum("sum", "count", "avg"),
	"Transform.type":     enum("rename_columns", "schema_rename"),
}

// requiredFields lists keys Validate always demands.
var requiredFields = map[string][]string{
	"Rule":        {"code", "level", "checks"},
	"DatasetRule": {"code", "level", "metric"},
	"Stat":        {"code", "column", "aggregate"},
	"Transform":   {"type"},
	"Column":      {"type"},
}

var descriptions = map[string]string{
	"Config.input":         "How input files are read: format, mode and optional declared schema.",
	"Config.source":        "Where input comes from when -i is not given: URIs or a prefix.",
	"Config.output":        "Optional artifact publication; without uri, only the stdout summary is produced.",
	"Config.execution":     "Managed memory budget, batch size, record limit and workers.",
	"Config.transforms":    "Column renames applied before rules bind.",
	"Config.rules":         "Row and schema rules, evaluated in order.",
	"Config.stats":         "Aggregates reported in the summary.",
	"Config.dataset_rules": "Assertions over whole-dataset metrics.",
	"Config.context":       "Values substituted into rule messages.",
}

const typeDescription = "Type name, any letter case. Sized names: int8-int64, uint8-uint64, float32, " +
	"float64 or double. Bare Python names: int = int64 and float = float64; in C, Arrow and " +
	"Parquet float is 32-bit, so write float32 for those columns. Also str/string, bytes/binary, " +
	"bool, decimal (with precision and scale), date and datetime/timestamp."

// typeEnum offers every accepted spelling for completion and, because the
// loader ignores case, also accepts any casing of them.
func typeEnum() map[string]any {
	names := make([]string, 0, len(typeSpellings))
	for name := range typeSpellings {
		names = append(names, name)
	}
	slices.Sort(names)
	var pattern strings.Builder
	pattern.WriteString("^(?:")
	for i, name := range names {
		if i > 0 {
			pattern.WriteByte('|')
		}
		for _, r := range name {
			if upper := unicode.ToUpper(r); upper != r {
				pattern.WriteString("[" + string(r) + string(upper) + "]")
			} else {
				pattern.WriteRune(r)
			}
		}
	}
	pattern.WriteString(")$")
	return map[string]any{
		"type":        "string",
		"description": typeDescription,
		"anyOf":       []any{map[string]any{"enum": names}, map[string]any{"pattern": pattern.String()}},
	}
}

type schemaBuilder struct {
	defs map[string]any
}

func (b *schemaBuilder) object(t reflect.Type) map[string]any {
	props := map[string]any{}
	for f := range t.Fields() {
		name, ok := yamlName(f)
		if !ok {
			continue
		}
		key := t.Name() + "." + name
		var s any
		if pinned, ok := fieldSchemas[key]; ok {
			s = pinned
		} else {
			s = b.of(f.Type)
		}
		if d, ok := descriptions[key]; ok {
			s = withDescription(s, d)
		}
		props[name] = s
	}
	out := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if req := requiredFields[t.Name()]; req != nil {
		out["required"] = req
	}
	return out
}

func (b *schemaBuilder) of(t reflect.Type) any {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t {
	case reflect.TypeFor[Check]():
		return ref("check")
	case reflect.TypeFor[Condition]():
		return ref("condition")
	}
	switch t.Kind() {
	case reflect.Struct:
		name := strings.ToLower(t.Name())
		if _, ok := b.defs[name]; !ok {
			b.defs[name] = nil // reserve before recursing
			b.defs[name] = b.object(t)
		}
		return ref(name)
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": b.of(t.Elem())}
	case reflect.Slice:
		return map[string]any{"type": "array", "items": b.of(t.Elem())}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int32, reflect.Int64:
		return map[string]any{"type": "integer"}
	case reflect.Float64:
		return map[string]any{"type": "number"}
	}
	return map[string]any{}
}

// yamlName mirrors validateShape: skipped and inline fields are not keys.
func yamlName(f reflect.StructField) (string, bool) {
	tag := f.Tag.Get("yaml")
	if tag == "-" {
		return "", false
	}
	parts := strings.Split(tag, ",")
	if len(parts) > 1 && parts[1] == "inline" {
		return "", false
	}
	if parts[0] == "" {
		return strings.ToLower(f.Name), true
	}
	return parts[0], true
}

func ref(name string) map[string]any { return map[string]any{"$ref": "#/$defs/" + name} }

func withDescription(s any, d string) any {
	m, ok := s.(map[string]any)
	if !ok {
		return s
	}
	out := make(map[string]any, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	out["description"] = d
	return out
}

func nonEmptyStrings() map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "minItems": 1}
}

// checkParamSchemas types each check argument; checkParamOverrides covers
// arguments whose meaning depends on the check.
var checkParamSchemas = map[string]any{
	"column":              map[string]any{"type": "string"},
	"columns":             nonEmptyStrings(),
	"other_column":        map[string]any{"type": "string"},
	"operator":            enum("==", "!=", "<", "<=", ">", ">="),
	"length":              map[string]any{"type": "integer", "minimum": 0},
	"min_length":          map[string]any{"type": "integer", "minimum": 0},
	"max_length":          map[string]any{"type": "integer", "minimum": 0},
	"min":                 numberOrString,
	"max":                 numberOrString,
	"exclusive_min":       map[string]any{"type": "boolean"},
	"exclusive_max":       map[string]any{"type": "boolean"},
	"value":               ref("scalar"),
	"values":              map[string]any{"type": "array", "items": ref("scalar")},
	"pattern":             map[string]any{"type": "string"},
	"reference_file":      map[string]any{"type": "string"},
	"reference_column":    map[string]any{"type": "string"},
	"reference_columns":   nonEmptyStrings(),
	"mode":                enum("strict", "castable"),
	"allow_extra_columns": map[string]any{"type": "boolean"},
	"require_columns":     map[string]any{"type": "boolean"},
	"expected_type":       ref("type_name"),
	"format":              map[string]any{"type": "string"},
	"formats":             nonEmptyStrings(),
	"timezone":            map[string]any{"type": "string"},
}

var checkParamOverrides = map[string]map[string]any{
	"schema_matches":   {"columns": map[string]any{"type": "object", "minProperties": 1, "additionalProperties": ref("type_name")}},
	"timestamp_window": {"min": map[string]any{"type": "string"}, "max": map[string]any{"type": "string"}},
}

func checkNames() []string {
	names := make([]string, 0, len(Checks))
	for name := range Checks {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// checkArguments is the schema of each argument a check accepts.
func checkArguments(name string) map[string]any {
	props := map[string]any{}
	for _, p := range Checks[name].Params {
		if s, ok := checkParamOverrides[name][p]; ok {
			props[p] = s
		} else {
			props[p] = checkParamSchemas[p]
		}
	}
	return props
}

// checkSchema describes one entry of the list form: {type: name, ...arguments}.
func checkSchema() map[string]any {
	names := checkNames()
	variants := make([]any, 0, len(names))
	for _, name := range names {
		spec := Checks[name]
		props := checkArguments(name)
		props["type"] = map[string]any{"const": name}
		variant := map[string]any{
			"title":                name,
			"description":          "A " + spec.Scope + "-scope check.",
			"type":                 "object",
			"properties":           props,
			"required":             []string{"type"},
			"additionalProperties": false,
		}
		if spec.Operands == OperandsColumns {
			variant["required"] = []string{"type", "columns"}
		}
		variants = append(variants, variant)
	}
	return map[string]any{
		"type":     "object",
		"required": []string{"type"},
		"properties": map[string]any{
			"type": enum(names...),
		},
		"oneOf": variants,
	}
}

// checkMapSchema describes the map form: each key is a check name; its value
// is the arguments mapping, the check's shorthand argument, or true/null for a
// check without arguments.
func checkMapSchema() map[string]any {
	props := map[string]any{}
	for _, name := range checkNames() {
		spec := Checks[name]
		args := map[string]any{"type": "object", "properties": checkArguments(name), "additionalProperties": false}
		if spec.Operands == OperandsColumns {
			args["required"] = []string{"columns"}
		}
		forms := []any{args}
		description := "A " + spec.Scope + "-scope check: its arguments as a mapping"
		if spec.Shorthand != "" {
			forms = append(forms, checkArguments(name)[spec.Shorthand])
			description += ", or just its " + spec.Shorthand
		} else {
			forms = append(forms, map[string]any{"const": true}, map[string]any{"type": "null"})
			description += ", or true when it needs none"
		}
		props[name] = map[string]any{"description": description + ".", "anyOf": forms}
	}
	return map[string]any{"type": "object", "properties": props, "additionalProperties": false}
}

func conditionSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"column":   map[string]any{"type": "string"},
			"operator": enum("eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "is_present"),
			"value":    map[string]any{"anyOf": []any{ref("scalar"), map[string]any{"type": "array", "items": ref("scalar")}}},
			"all":      map[string]any{"type": "array", "items": ref("condition")},
			"any":      map[string]any{"type": "array", "items": ref("condition")},
			"not":      ref("condition"),
		},
		"additionalProperties": false,
	}
}
