package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
	"unicode/utf8"
)

// byteUnits is ordered so that multi-letter suffixes are tried before "B".
var byteUnits = [...]struct {
	suffix string
	mul    int64
}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}, {"KB", 1000}, {"MB", 1000000}, {"GB", 1000000000}, {"TB", 1000000000000}, {"B", 1}}

// parseBytes accepts positive integral byte quantities in decimal or IEC units.
func parseBytes(s string) (int64, error) {
	digits, mul := strings.TrimSpace(s), int64(1)
	for _, unit := range byteUnits {
		if rest, ok := strings.CutSuffix(digits, unit.suffix); ok {
			digits, mul = rest, unit.mul
			break
		}
	}
	if digits == "" || strings.TrimLeft(digits, "0123456789") != "" {
		return 0, fmt.Errorf("invalid byte size %q (use bytes, KiB, MiB, GiB, or TiB)", s)
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("byte size %q must be positive and fit int64", s)
	}
	if n > math.MaxInt64/mul {
		return 0, fmt.Errorf("byte size %q overflows int64", s)
	}
	return n * mul, nil
}

// Size accessors return the values resolved by Validate. Configurations built
// in code without Validate fall back to parsing (0 when invalid).
func (c *Config) BudgetBytes() int64 { return c.size(c.sizes.budget, c.Execution.MemoryBudget) }
func (c *Config) BatchBytes() int64  { return c.size(c.sizes.batch, c.Execution.BatchTarget) }
func (c *Config) RecordBytes() int64 { return c.size(c.sizes.record, c.Execution.MaxRecordBytes) }
func (c *Config) PartBytes() int64   { return c.size(c.sizes.part, c.Output.PartSize) }
func (c *Config) size(resolved int64, text string) int64 {
	if c.resolved {
		return resolved
	}
	n, _ := parseBytes(text)
	return n
}

// InferInputType maps a source URI suffix to an input type, ignoring a
// trailing .gz: .parquet, .ndjson/.jsonl, .json, otherwise csv.
func InferInputType(uri string) string {
	name := uri
	ext := filepath.Ext
	if parsed, err := url.Parse(uri); err == nil && parsed.Scheme == "s3" {
		name = strings.TrimPrefix(parsed.Path, "/")
		ext = path.Ext
	}
	name = strings.TrimSuffix(strings.ToLower(name), ".gz")
	switch ext(name) {
	case ".parquet":
		return "parquet"
	case ".ndjson", ".jsonl":
		return "ndjson"
	case ".json":
		return "json"
	}
	return "csv"
}

// inferInputType resolves an omitted input.type from the first source URI, or
// from the format filter of a prefix source. An explicit type always wins.
func (c *Config) inferInputType() string {
	if len(c.Source.URIs) > 0 {
		return InferInputType(c.Source.URIs[0])
	}
	if oneOf(c.Source.Filter, "parquet", "ndjson", "json") {
		return c.Source.Filter
	}
	return "csv"
}

// ForReference returns an independent configuration that reads columns from a
// reference dataset as required raw strings, sharing this run's storage
// settings, null markers and resolved limits but none of its rules.
func (c *Config) ForReference(uri string, columns []string) *Config {
	schema := &Schema{ExtraFields: "ignore", Columns: make(map[string]Column, len(columns))}
	for _, name := range columns {
		if _, exists := schema.Columns[name]; !exists {
			schema.Order = append(schema.Order, name)
		}
		schema.Columns[name] = Column{Type: "string", Required: true}
	}
	return &Config{
		ConfigVersion: c.ConfigVersion, Name: c.Name, Version: c.Version, BaseDir: c.BaseDir,
		Source:    Source{URIs: []string{uri}, S3: c.Source.S3},
		Input:     Input{Type: InferInputType(uri), Mode: "raw_strings", NullValues: slices.Clone(c.Input.NullValues), Schema: schema},
		Execution: c.Execution,
		defaulted: c.defaulted, resolved: c.resolved, sizes: c.sizes,
	}
}

// Validate checks the effective configuration after overrides. Output.URI may be
// absent for reporting and planning. The first call fills defaults for code-built
// configurations and infers input.type from the effective source; later calls
// only re-check and never change values.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("configuration is nil")
	}
	c.applyDefaults()
	c.deriveDefaults()
	if c.Output.Mode == "" {
		c.Output.Mode = "report"
		if c.Output.URI != "" {
			c.Output.Mode = "all"
		}
	}
	if c.Input.Type == "" {
		c.Input.Type = c.inferInputType()
	}
	if c.ConfigVersion != 0 && c.ConfigVersion != 1 {
		return fmt.Errorf("unsupported config_version %d", c.ConfigVersion)
	}
	if !oneOf(c.Input.Type, "csv", "parquet", "ndjson", "json") {
		return fmt.Errorf("unsupported input.type %q", c.Input.Type)
	}
	if !oneOf(c.Input.Parquet.Statistics, "off", "trusted") {
		return fmt.Errorf("input.parquet.statistics must be off or trusted")
	}
	if c.Input.Parquet.Statistics == "trusted" && c.Input.Type != "parquet" {
		return fmt.Errorf("trusted input.parquet.statistics requires parquet input")
	}
	if !oneOf(c.Output.Mode, "all", "failures", "report") {
		return fmt.Errorf("output.mode must be all, failures, or report")
	}
	if !oneOf(c.Output.Parquet.Statistics, "none", "bounded") {
		return fmt.Errorf("output.parquet.statistics must be none or bounded")
	}
	if c.Output.Parquet.Statistics == "bounded" && c.Output.Format != "parquet" {
		return fmt.Errorf("bounded output.parquet.statistics requires parquet output")
	}
	if !oneOf(c.Input.Mode, "raw_strings", "infer_types") {
		return fmt.Errorf("unsupported input.mode %q", c.Input.Mode)
	}
	if !oneOf(c.Input.OnTypeError, "row_error", "abort") {
		return fmt.Errorf("input.on_type_error must be row_error or abort")
	}
	if c.Input.InferSchemaLength <= 0 {
		return fmt.Errorf("input.infer_schema_length must be positive")
	}
	if utf8.RuneCountInString(c.Input.Delimiter) != 1 || strings.ContainsAny(c.Input.Delimiter, "\r\n\x00\"\ufffd") {
		return fmt.Errorf("input.delimiter must be one valid non-quote, non-newline rune")
	}
	if c.Input.Header != nil && !*c.Input.Header && len(c.Input.Columns) == 0 {
		return fmt.Errorf("input.columns is required when input.header is false")
	}
	if err := uniqueNames(c.Input.Columns, "input.columns"); err != nil {
		return err
	}
	if (len(c.Source.URIs) == 0) == (c.Source.Prefix == "") {
		return fmt.Errorf("source requires exactly one of uris or prefix")
	}
	for _, uri := range c.Source.URIs {
		if err := validateURI(uri, "source.uris"); err != nil {
			return err
		}
	}
	if c.Source.Prefix != "" {
		if err := validateURI(c.Source.Prefix, "source.prefix"); err != nil {
			return err
		}
		if !oneOf(c.Source.Filter, "csv", "parquet", "json", "ndjson") {
			return fmt.Errorf("source.prefix requires a format filter: csv, parquet, json, or ndjson")
		}
	} else if c.Source.Filter != "" || c.Source.Recursive {
		return fmt.Errorf("source.filter and source.recursive require source.prefix")
	}
	if c.Output.URI != "" {
		if err := validateURI(c.Output.URI, "output.uri"); err != nil {
			return err
		}
		if c.Source.Prefix != "" {
			overlap, overlapErr := pathsOverlap(c.Source.Prefix, c.Output.URI)
			if overlapErr != nil {
				return fmt.Errorf("compare source/output locations: %w", overlapErr)
			}
			if overlap {
				return fmt.Errorf("source.prefix and output.uri overlap; use a separate output location")
			}
		}
	}
	if !oneOf(c.Output.Format, "csv", "parquet") {
		return fmt.Errorf("output.format must be csv or parquet")
	}
	if c.Output.Issues != "ndjson" {
		return fmt.Errorf("output.issues must be ndjson")
	}
	var sizes byteSizes
	for _, item := range []struct {
		name, value string
		into        *int64
	}{{"execution.memory_budget", c.Execution.MemoryBudget, &sizes.budget}, {"execution.batch_target", c.Execution.BatchTarget, &sizes.batch}, {"execution.max_record_bytes", c.Execution.MaxRecordBytes, &sizes.record}, {"output.part_size", c.Output.PartSize, &sizes.part}} {
		n, err := parseBytes(item.value)
		if err != nil {
			return fmt.Errorf("%s: %w", item.name, err)
		}
		*item.into = n
	}
	if c.Execution.Workers < 1 || c.Execution.Workers > 32 {
		return fmt.Errorf("execution.workers must be between 1 and 32")
	}
	if c.Execution.MaxDepth <= 0 || c.Execution.MaxDepth > 1024 {
		return fmt.Errorf("execution.max_depth must be between 1 and 1024")
	}
	if err := validateS3(c.Source.S3, "source.s3"); err != nil {
		return err
	}
	if err := validateS3(c.Output.S3, "output.s3"); err != nil {
		return err
	}
	if c.Input.Schema != nil {
		if err := validateSchema(c.Input.Schema); err != nil {
			return err
		}
	}
	for i, t := range c.Transforms {
		if err := validateTransform(t); err != nil {
			return fmt.Errorf("transforms[%d]: %w", i, err)
		}
	}
	codes := make(map[string]bool)
	for i := range c.Rules {
		r := &c.Rules[i]
		if err := validateCode(r.Code, r.Level, codes); err != nil {
			return err
		}
		gate, row := false, false
		for _, check := range r.Checks {
			if err := validateCheck(*r, check); err != nil {
				return fmt.Errorf("rule %s: %w", r.Code, err)
			}
			spec := Checks[check.Type]
			if r.Level == "error" && spec.Gate {
				gate = true
			} else {
				row = true
			}
			if r.When != nil && !spec.AllowsWhen() {
				return fmt.Errorf("rule %s: when is unsupported for global/schema checks", r.Code)
			}
		}
		if gate && row {
			return fmt.Errorf("rule %s mixes error schema gates and row checks; split into separate rules", r.Code)
		}
		if r.When != nil {
			nodes := 0
			if err := validateCondition(r.When, 0, &nodes); err != nil {
				return fmt.Errorf("rule %s when: %w", r.Code, err)
			}
		}
	}
	rowCodes := make(map[string]bool, len(codes))
	for code := range codes {
		rowCodes[code] = true
	}
	for _, r := range c.DatasetRules {
		if err := validateCode(r.Code, r.Level, codes); err != nil {
			return err
		}
		if err := validateDatasetRule(r, rowCodes, c.Input.Schema); err != nil {
			return fmt.Errorf("dataset rule %s: %w", r.Code, err)
		}
	}
	statCodes := make(map[string]bool)
	for _, s := range c.Stats {
		if strings.TrimSpace(s.Code) == "" || statCodes[s.Code] {
			return fmt.Errorf("stats requires unique nonempty codes")
		}
		statCodes[s.Code] = true
		if s.Column == "" || !oneOf(s.Aggregate, "sum", "count", "avg") {
			return fmt.Errorf("stat %s requires column and aggregate sum, count, or avg", s.Code)
		}
	}
	c.sizes, c.resolved = sizes, true
	return nil
}

func oneOf(s string, choices ...string) bool {
	for _, choice := range choices {
		if s == choice {
			return true
		}
	}
	return false
}
func uniqueNames(names []string, label string) error {
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		if n == "" || seen[n] {
			return fmt.Errorf("%s requires nonempty, unique names", label)
		}
		seen[n] = true
	}
	return nil
}
func validateURI(s, label string) error {
	if strings.TrimSpace(s) == "" || s == "-" || strings.ContainsRune(s, 0) {
		return fmt.Errorf("%s requires a replayable local path or s3:// URI", label)
	}
	if strings.Contains(s, "://") {
		u, err := url.Parse(s)
		if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("%s requires a local path or s3://bucket/key URI", label)
		}
	}
	return nil
}

type canonicalLocation struct {
	s3          bool
	bucket, key string
	local       string
}

func parseCanonicalLocation(raw string) (canonicalLocation, error) {
	if strings.HasPrefix(raw, "s3://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return canonicalLocation{}, err
		}
		return canonicalLocation{s3: true, bucket: strings.ToLower(parsed.Host), key: strings.TrimPrefix(parsed.Path, "/")}, nil
	}
	absolute, err := filepath.Abs(raw)
	if err != nil {
		return canonicalLocation{}, err
	}
	probe := filepath.Clean(absolute)
	var suffix []string
	for {
		resolved, resolveErr := filepath.EvalSymlinks(probe)
		if resolveErr == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			resolved = filepath.Clean(resolved)
			if runtime.GOOS == "windows" {
				resolved = strings.ToLower(resolved)
			}
			return canonicalLocation{local: resolved}, nil
		}
		if !errors.Is(resolveErr, os.ErrNotExist) {
			return canonicalLocation{}, resolveErr
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return canonicalLocation{}, resolveErr
		}
		suffix = append(suffix, filepath.Base(probe))
		probe = parent
	}
}

func localContains(base, candidate string) bool {
	relative, err := filepath.Rel(base, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func pathsOverlap(a, b string) (bool, error) {
	left, err := parseCanonicalLocation(a)
	if err != nil {
		return false, err
	}
	right, err := parseCanonicalLocation(b)
	if err != nil {
		return false, err
	}
	if left.s3 != right.s3 {
		return false, nil
	}
	if left.s3 {
		if left.bucket != right.bucket {
			return false, nil
		}
		return strings.HasPrefix(left.key, right.key) || strings.HasPrefix(right.key, left.key), nil
	}
	return localContains(left.local, right.local) || localContains(right.local, left.local), nil
}
func validateS3(s S3, label string) error {
	if s.Endpoint != "" {
		u, err := url.Parse(s.Endpoint)
		if err != nil || !oneOf(u.Scheme, "http", "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("%s.endpoint must be an HTTP(S) endpoint without credentials/query/fragment", label)
		}
	}
	if s.SSE != "" && !oneOf(s.SSE, "AES256", "aws:kms") {
		return fmt.Errorf("%s.sse must be AES256 or aws:kms", label)
	}
	if s.KMSKeyID != "" && s.SSE != "aws:kms" {
		return fmt.Errorf("%s.kms_key_id requires sse: aws:kms", label)
	}
	return nil
}
func validateCode(code, level string, seen map[string]bool) error {
	if strings.TrimSpace(code) == "" {
		return fmt.Errorf("rule code must not be empty")
	}
	if strings.HasPrefix(code, "INGEST_") {
		return fmt.Errorf("rule code %q uses reserved INGEST_ namespace", code)
	}
	if seen[code] {
		return fmt.Errorf("duplicate rule code %q", code)
	}
	seen[code] = true
	if !oneOf(level, "error", "warn") {
		return fmt.Errorf("rule %s: level must be error or warn", code)
	}
	return nil
}

func supportedType(s string) bool {
	_, ok := CanonicalType(s)
	return ok
}
func numericType(s string) bool {
	canonical, ok := CanonicalType(s)
	return ok && (strings.HasPrefix(canonical, "int") || strings.HasPrefix(canonical, "uint") || strings.HasPrefix(canonical, "float") || canonical == "decimal")
}
func validateSchema(s *Schema) error {
	if !oneOf(s.ExtraFields, "error", "ignore") {
		return fmt.Errorf("input.schema.extra_fields must be error or ignore")
	}
	if len(s.Columns) == 0 {
		return fmt.Errorf("input.schema.columns must not be empty")
	}
	if err := uniqueNames(s.Order, "input.schema column order"); err != nil {
		return err
	}
	if len(s.Order) != len(s.Columns) {
		return fmt.Errorf("input.schema column order must contain every declared column")
	}
	for _, name := range s.Order {
		col, ok := s.Columns[name]
		if !ok {
			return fmt.Errorf("input.schema order references undeclared column %q", name)
		}
		if name == "" || !supportedType(col.Type) {
			return fmt.Errorf("input.schema column %q has unsupported type %q", name, col.Type)
		}
		if strings.HasPrefix(name, "__valet_") {
			return fmt.Errorf("column %q uses reserved output prefix __valet_; rename it before declaring the schema", name)
		}
		kind, _ := CanonicalType(col.Type)
		if kind == "decimal" {
			if col.Precision < 1 || col.Precision > 38 || col.Scale < 0 || col.Scale > col.Precision {
				return fmt.Errorf("column %s: decimal requires precision 1..38 and scale 0..precision", name)
			}
		} else if col.Precision != 0 || col.Scale != 0 {
			return fmt.Errorf("column %s: precision and scale require decimal type", name)
		}
		if col.Timezone == "Local" {
			return fmt.Errorf("column %s: timezone Local is machine-dependent; use UTC or an IANA timezone", name)
		}
		if col.Timezone != "" {
			if kind != "timestamp" {
				return fmt.Errorf("column %s: timezone requires timestamp type", name)
			}
			if _, err := time.LoadLocation(col.Timezone); err != nil {
				return fmt.Errorf("column %s timezone: %w", name, err)
			}
		}
		if len(col.Formats) > 0 && kind != "date" && kind != "timestamp" {
			return fmt.Errorf("column %s: formats require date/timestamp type", name)
		}
		for _, f := range col.Formats {
			if err := validateDateFormat(f); err != nil {
				return fmt.Errorf("column %s: %w", name, err)
			}
		}
		if len(col.TrueValues)+len(col.FalseValues) > 0 && kind != "bool" {
			return fmt.Errorf("column %s: boolean tokens require bool type", name)
		}
		trueTokens, falseTokens := col.TrueValues, col.FalseValues
		if trueTokens == nil {
			trueTokens = []string{"true"}
		}
		if falseTokens == nil {
			falseTokens = []string{"false"}
		}
		tokens := make(map[string]bool, len(trueTokens)+len(falseTokens))
		for _, set := range [][]string{trueTokens, falseTokens} {
			for _, token := range set {
				if tokens[token] {
					return fmt.Errorf("column %s: boolean tokens must be unique and nonoverlapping", name)
				}
				tokens[token] = true
			}
		}
		if col.Path != "" {
			if !strings.HasPrefix(col.Path, "/") {
				return fmt.Errorf("column %s: path must be a JSON Pointer", name)
			}
			for i := range len(col.Path) {
				if col.Path[i] == '~' && (i+1 == len(col.Path) || !strings.ContainsRune("01", rune(col.Path[i+1]))) {
					return fmt.Errorf("column %s: invalid JSON Pointer escape", name)
				}
			}
		}
	}
	return nil
}
func validateTransform(t Transform) error {
	switch t.Type {
	case "rename_columns":
		if (len(t.Mapping) == 0) == (t.MappingFile == "") {
			return fmt.Errorf("rename_columns requires exactly one of mapping or mapping_file")
		}
		if t.SchemaFile != "" || t.SourceKey != "" {
			return fmt.Errorf("rename_columns does not accept schema_file/source_key")
		}
		if t.SourceCol < 0 || (t.TargetCol != nil && (*t.TargetCol < 0 || *t.TargetCol == t.SourceCol)) || (t.TargetCol == nil && t.SourceCol == 1) {
			return fmt.Errorf("mapping source_col/target_col must be distinct nonnegative indexes")
		}
		if t.MappingFile != "" {
			if err := validateURI(t.MappingFile, "mapping_file"); err != nil {
				return err
			}
		}
		// Only effective source columns are renamed; aliases for absent/excluded
		// columns are legal. The source binder detects actual output collisions.
		for from, to := range t.Mapping {
			if from == "" || to == "" {
				return fmt.Errorf("mapping names must be nonempty")
			}
		}
	case "schema_rename":
		if t.SchemaFile == "" || t.SourceKey == "" {
			return fmt.Errorf("schema_rename requires schema_file and source_key")
		}
		if t.MappingFile != "" || len(t.Mapping) > 0 || t.SourceCol != 0 || t.TargetCol != nil || len(t.ExcludeKeys) > 0 {
			return fmt.Errorf("schema_rename only accepts schema_file and source_key")
		}
		if err := validateURI(t.SchemaFile, "schema_file"); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown transform %q", t.Type)
	}
	return nil
}

// decimalNumber preserves decimal/integer thresholds instead of comparing via a
// lossy float64 conversion. Strings permit precision beyond YAML float64 values.
var decimalLiteral = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE]([+-]?[0-9]{1,4}))?$`)

func decimalNumber(v any) (*big.Rat, bool) {
	var s string
	switch n := v.(type) {
	case int:
		s = strconv.Itoa(n)
	case int64:
		s = strconv.FormatInt(n, 10)
	case uint64:
		s = strconv.FormatUint(n, 10)
	case float64:
		if math.IsInf(n, 0) || math.IsNaN(n) {
			return nil, false
		}
		s = strconv.FormatFloat(n, 'g', -1, 64)
	case float32:
		if math.IsInf(float64(n), 0) || math.IsNaN(float64(n)) {
			return nil, false
		}
		s = strconv.FormatFloat(float64(n), 'g', -1, 32)
	case string:
		s = n
	case json.Number:
		s = string(n)
	default:
		return nil, false
	}
	// Limit exponent work before constructing big integers; a tiny YAML scalar
	// such as 1e999999999 must not cause an unbounded allocation.
	if len(s) > 1024 {
		return nil, false
	}
	match := decimalLiteral.FindStringSubmatch(s)
	if match == nil {
		return nil, false
	}
	if match[1] != "" {
		exponent, err := strconv.Atoi(match[1])
		if err != nil || exponent < -1000 || exponent > 1000 {
			return nil, false
		}
	}
	n, ok := new(big.Rat).SetString(s)
	return n, ok
}
