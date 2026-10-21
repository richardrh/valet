package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"
)

const (
	MaxConfigBytes          = 4 << 20
	maxExpandedMessageBytes = 1 << 20
	maxYAMLDepth            = 64
	maxYAMLNodes            = 100000
)

// Load decodes a local YAML or JSON pipeline document and applies defaults.
// Semantic validation is deliberately separate so callers can apply CLI
// overrides before Validate.
func Load(ctx context.Context, path string) (*Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.Contains(path, "://") {
		return nil, fmt.Errorf("pipeline document must be a local file")
	}
	format, err := pipelineDocumentFormat(path)
	if err != nil {
		return nil, err
	}
	f, err := openPipelineFile(path)
	if err != nil {
		return nil, fmt.Errorf("open configuration: %w", err)
	}
	defer f.Close()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxConfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read configuration: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if len(b) > MaxConfigBytes {
		return nil, fmt.Errorf("configuration exceeds %d bytes", MaxConfigBytes)
	}
	if !utf8.Valid(b) {
		return nil, fmt.Errorf("configuration is not valid UTF-8")
	}
	var doc yaml.Node
	if format == "json" {
		doc, err = decodeJSONDocument(b)
		if err != nil {
			return nil, fmt.Errorf("decode JSON configuration: %w", err)
		}
	} else {
		dec := yaml.NewDecoder(bytes.NewReader(b))
		if err = dec.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode configuration: %w", err)
		}
		var trailing yaml.Node
		if err = dec.Decode(&trailing); err != io.EOF {
			if err != nil {
				return nil, fmt.Errorf("decode trailing configuration: %w", err)
			}
			return nil, fmt.Errorf("configuration must contain exactly one document")
		}
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("configuration must be a mapping")
	}
	count := 0
	if err := validateNode(doc.Content[0], 0, &count); err != nil {
		return nil, err
	}
	if err := validateShape(doc.Content[0], reflect.TypeOf(Config{}), "config"); err != nil {
		return nil, err
	}
	var cfg Config
	// Scalar defaults precede decoding so explicit document zeros are preserved.
	cfg.applyDefaults()
	if err := doc.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode configuration: %w", err)
	}
	if err := preserveNumbers(doc.Content[0], &cfg); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve configuration path: %w", err)
	}
	cfg.BaseDir = filepath.Dir(abs)
	if cfg.Input.Schema != nil {
		node := mappingValue(mappingValue(mappingValue(doc.Content[0], "input"), "schema"), "columns")
		if node != nil {
			for i := 0; i < len(node.Content); i += 2 {
				cfg.Input.Schema.Order = append(cfg.Input.Schema.Order, node.Content[i].Value)
			}
		}
	}
	cfg.deriveDefaults()
	cfg.resolvePaths()
	if err = substituteMessages(cfg.Rules, cfg.Context); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func pipelineDocumentFormat(path string) (string, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return "yaml", nil
	case ".json":
		return "json", nil
	default:
		return "", fmt.Errorf("pipeline document must use .yaml, .yml, or .json")
	}
}

func mappingValue(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

func validateNode(n *yaml.Node, depth int, count *int) error {
	*count++
	if depth > maxYAMLDepth || *count > maxYAMLNodes {
		return fmt.Errorf("configuration exceeds YAML depth/node limit at line %d", n.Line)
	}
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return fmt.Errorf("YAML anchors and aliases are not supported (line %d)", n.Line)
	}
	switch n.Tag {
	case "!!timestamp":
		// Timestamp bounds are lexical configuration values, not machine-time values.
		n.Tag = "!!str"
	case "!!map", "!!seq", "!!str", "!!bool", "!!int", "!!float", "!!null":
	default:
		return fmt.Errorf("unsupported YAML tag %q at line %d", n.Tag, n.Line)
	}
	if n.Kind == yaml.MappingNode {
		seen := make(map[string]bool, len(n.Content)/2)
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			if k.Kind != yaml.ScalarNode || k.Tag != "!!str" {
				return fmt.Errorf("mapping keys must be strings at line %d", k.Line)
			}
			if seen[k.Value] {
				return fmt.Errorf("duplicate YAML key %q at line %d", k.Value, k.Line)
			}
			seen[k.Value] = true
		}
	}
	for _, child := range n.Content {
		if err := validateNode(child, depth+1, count); err != nil {
			return err
		}
	}
	return nil
}

// Validate structural field names before decoding, including maps nested under
// schema declarations. yaml.Node.Decode alone does not enforce KnownFields.
func validateShape(n *yaml.Node, t reflect.Type, path string) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Interface {
		return nil
	}
	if n.Tag == "!!null" {
		return fmt.Errorf("%s must not be null (line %d)", path, n.Line)
	}
	switch t.Kind() {
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			return fmt.Errorf("%s must be a mapping", path)
		}
		if t == reflect.TypeOf(Condition{}) {
			value := mappingValue(n, "value")
			operator := mappingValue(n, "operator")
			if value != nil && operator != nil && oneOf(operator.Value, "is_null", "is_present") {
				return fmt.Errorf("%s: unary condition must omit value", path)
			}
			if mappingValue(n, "all") != nil || mappingValue(n, "any") != nil || mappingValue(n, "not") != nil {
				if value != nil || operator != nil || mappingValue(n, "column") != nil {
					return fmt.Errorf("%s: condition composition cannot contain leaf fields", path)
				}
			}
		}
		fields := make(map[string]reflect.Type)
		inline := false
		for i := range t.NumField() {
			f := t.Field(i)
			tag := f.Tag.Get("yaml")
			if tag == "-" {
				continue
			}
			parts := strings.Split(tag, ",")
			if len(parts) > 1 && parts[1] == "inline" {
				inline = true
				continue
			}
			name := parts[0]
			if name == "" {
				name = strings.ToLower(f.Name)
			}
			fields[name] = f.Type
		}
		for i := 0; i < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			ft, ok := fields[k.Value]
			if !ok {
				if inline {
					continue
				}
				return fmt.Errorf("unknown field %s.%s (line %d)", path, k.Value, k.Line)
			}
			if err := validateShape(v, ft, path+"."+k.Value); err != nil {
				return err
			}
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode {
			return fmt.Errorf("%s must be a mapping", path)
		}
		for i := 0; i < len(n.Content); i += 2 {
			if err := validateShape(n.Content[i+1], t.Elem(), path+"."+n.Content[i].Value); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if n.Kind != yaml.SequenceNode {
			return fmt.Errorf("%s must be a sequence", path)
		}
		for i, child := range n.Content {
			if err := validateShape(child, t.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case reflect.String:
		if n.Tag != "!!str" {
			return fmt.Errorf("%s must be a string", path)
		}
	case reflect.Bool:
		if n.Tag != "!!bool" {
			return fmt.Errorf("%s must be a boolean", path)
		}
	case reflect.Int, reflect.Int32, reflect.Int64:
		if n.Tag != "!!int" {
			return fmt.Errorf("%s must be an integer", path)
		}
	case reflect.Float64:
		if n.Tag != "!!int" && n.Tag != "!!float" {
			return fmt.Errorf("%s must be numeric", path)
		}
	}
	return nil
}

// applyDefaults fills scalar defaults once. input.type is deliberately left
// empty: it is inferred from the effective source by Validate after overrides.
func (c *Config) applyDefaults() {
	if c.defaulted {
		return
	}
	c.defaulted = true
	if c.Name == "" {
		c.Name = "Unnamed Pipeline"
	}
	if c.Version == "" {
		c.Version = "1.0"
	}
	if c.Input.Parquet.Statistics == "" {
		c.Input.Parquet.Statistics = "off"
	}
	if c.Output.Parquet.Statistics == "" {
		c.Output.Parquet.Statistics = "none"
	}
	if c.Input.Mode == "" {
		c.Input.Mode = "infer_types"
	}
	if c.Input.NullValues == nil {
		c.Input.NullValues = []string{"NULL", "null", "N/A", "n/a", ""}
	}
	if c.Input.InferSchemaLength == 0 {
		c.Input.InferSchemaLength = 1000000
	}
	if c.Input.OnTypeError == "" {
		c.Input.OnTypeError = "row_error"
	}
	if c.Input.Delimiter == "" {
		c.Input.Delimiter = ","
	}
	if c.Execution.MemoryBudget == "" {
		c.Execution.MemoryBudget = "512MiB"
	}
	if c.Execution.BatchTarget == "" {
		c.Execution.BatchTarget = "8MiB"
	}
	if c.Execution.MaxRecordBytes == "" {
		c.Execution.MaxRecordBytes = "8MiB"
	}
	if c.Execution.Workers == 0 {
		c.Execution.Workers = 1
	}
	if c.Execution.MaxDepth == 0 {
		c.Execution.MaxDepth = 64
	}
	if c.Output.Format == "" {
		c.Output.Format = "parquet"
	}
	if c.Output.Issues == "" {
		c.Output.Issues = "ndjson"
	}
	if c.Output.PartSize == "" {
		c.Output.PartSize = "128MiB"
	}
}

// deriveDefaults fills values derived from decoded sections. Every step only
// fills empty fields, so repeated calls do not change the configuration.
func (c *Config) deriveDefaults() {
	if s := c.Input.Schema; s != nil {
		if s.ExtraFields == "" {
			s.ExtraFields = "error"
		}
		if len(s.Order) == 0 {
			for name := range s.Columns {
				s.Order = append(s.Order, name)
			}
			sort.Strings(s.Order)
		}
	}
	for i := range c.Rules {
		r := &c.Rules[i]
		if r.Message == "" {
			r.Message = r.Code
		}
		if r.Name == "" {
			r.Name = r.Code
		}
	}
	for i := range c.Stats {
		if c.Stats[i].Name == "" {
			c.Stats[i].Name = c.Stats[i].Code
		}
	}
}

func (c *Config) resolvePaths() {
	resolve := func(path string) string {
		if path == "" || path == "-" || filepath.IsAbs(path) || strings.Contains(path, "://") {
			return path
		}
		return filepath.Join(c.BaseDir, path)
	}
	for i := range c.Source.URIs {
		c.Source.URIs[i] = resolve(c.Source.URIs[i])
	}
	c.Source.Prefix = resolve(c.Source.Prefix)
	c.Output.URI = resolve(c.Output.URI)
	for i := range c.Transforms {
		t := &c.Transforms[i]
		t.MappingFile = resolve(t.MappingFile)
		t.SchemaFile = resolve(t.SchemaFile)
	}
	for i := range c.Rules {
		for j := range c.Rules[i].Checks {
			p := c.Rules[i].Checks[j].Params
			for _, k := range []string{"reference_file", "mapping_file", "schema_file"} {
				if s, ok := p[k].(string); ok {
					p[k] = resolve(s)
				}
			}
		}
	}
}

var errExpandedMessageLimit = errors.New("expanded rule message exceeds configured bound")

type boundedStringBuilder struct {
	builder strings.Builder
	limit   int
}

func (b *boundedStringBuilder) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.builder.Len() {
		return 0, errExpandedMessageLimit
	}
	return b.builder.Write(p)
}

func (b *boundedStringBuilder) WriteString(s string) (int, error) {
	if len(s) > b.limit-b.builder.Len() {
		return 0, errExpandedMessageLimit
	}
	return b.builder.WriteString(s)
}

func substituteMessages(rules []Rule, contextValues map[string]any) error {
	if len(contextValues) == 0 {
		return nil
	}
	keys := make([]string, 0, len(contextValues))
	for key := range contextValues {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, 2*len(keys))
	tableBytes := 0
	for _, key := range keys {
		placeholder := "{" + key + "}"
		value := fmt.Sprint(contextValues[key])
		if len(placeholder) > MaxConfigBytes-tableBytes {
			return errors.New("context replacement table exceeds configuration bound")
		}
		tableBytes += len(placeholder)
		if len(value) > MaxConfigBytes-tableBytes {
			return errors.New("context replacement table exceeds configuration bound")
		}
		tableBytes += len(value)
		pairs = append(pairs, placeholder, value)
	}
	replacer := strings.NewReplacer(pairs...)
	total := 0
	for i := range rules {
		remaining := MaxConfigBytes - total
		limit := min(maxExpandedMessageBytes, remaining)
		out := boundedStringBuilder{limit: limit}
		out.builder.Grow(min(len(rules[i].Message), limit))
		if _, err := replacer.WriteString(&out, rules[i].Message); err != nil {
			return fmt.Errorf("rule %s: %w", rules[i].Code, errExpandedMessageLimit)
		}
		rules[i].Message = out.builder.String()
		total += len(rules[i].Message)
	}
	return nil
}
