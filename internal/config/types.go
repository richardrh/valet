package config

// Config describes a complete run. Load decodes it; Validate follows overrides.
type Config struct {
	ConfigVersion int            `yaml:"config_version"`
	Name          string         `yaml:"name"`
	Version       string         `yaml:"version"`
	Context       map[string]any `yaml:"context"`
	Input         Input          `yaml:"input"`
	Source        Source         `yaml:"source"`
	Output        Output         `yaml:"output"`
	Execution     Execution      `yaml:"execution"`
	Transforms    []Transform    `yaml:"transforms"`
	Rules         []Rule         `yaml:"rules"`
	Stats         []Stat         `yaml:"stats"`
	DatasetRules  []DatasetRule  `yaml:"dataset_rules"`
	BaseDir       string         `yaml:"-"`
	defaulted     bool           `yaml:"-"`
	resolved      bool           `yaml:"-"`
	sizes         byteSizes      `yaml:"-"`
}

// byteSizes caches execution/output sizes parsed by Validate.
type byteSizes struct {
	budget, batch, record, part int64
}
type S3 struct {
	Region    string `yaml:"region"`
	Endpoint  string `yaml:"endpoint"`
	PathStyle bool   `yaml:"path_style"`
	Profile   string `yaml:"profile"`
	SSE       string `yaml:"sse"`
	KMSKeyID  string `yaml:"kms_key_id"`
}
type Source struct {
	URIs      []string `yaml:"uris"`
	Prefix    string   `yaml:"prefix"`
	Recursive bool     `yaml:"recursive"`
	Filter    string   `yaml:"filter"`
	S3        S3       `yaml:"s3"`
}
type Output struct {
	Mode     string        `yaml:"mode"`
	Parquet  OutputParquet `yaml:"parquet"`
	URI      string        `yaml:"uri"`
	Format   string        `yaml:"format"`
	Issues   string        `yaml:"issues"`
	S3       S3            `yaml:"s3"`
	PartSize string        `yaml:"part_size"`
}
type OutputParquet struct {
	Statistics string `yaml:"statistics"`
}
type InputParquet struct {
	Statistics string `yaml:"statistics"`
}
type Execution struct {
	MemoryBudget   string `yaml:"memory_budget"`
	BatchTarget    string `yaml:"batch_target"`
	MaxRecordBytes string `yaml:"max_record_bytes"`
	Workers        int    `yaml:"workers"`
	MaxDepth       int    `yaml:"max_depth"`
}
type Input struct {
	Parquet           InputParquet `yaml:"parquet"`
	Type              string       `yaml:"type"`
	Mode              string       `yaml:"mode"`
	NullValues        []string     `yaml:"null_values"`
	InferSchemaLength int          `yaml:"infer_schema_length"`
	Schema            *Schema      `yaml:"schema"`
	OnTypeError       string       `yaml:"on_type_error"`
	Delimiter         string       `yaml:"delimiter"`
	Header            *bool        `yaml:"header"`
	Columns           []string     `yaml:"columns"`
}
type Schema struct {
	ExtraFields string            `yaml:"extra_fields"`
	Columns     map[string]Column `yaml:"columns"`
	Order       []string          `yaml:"-"`
}
type Column struct {
	Type        string   `yaml:"type"`
	Required    bool     `yaml:"required"`
	Nullable    *bool    `yaml:"nullable"`
	Precision   int32    `yaml:"precision"`
	Scale       int32    `yaml:"scale"`
	Formats     []string `yaml:"formats"`
	Timezone    string   `yaml:"timezone"`
	Trim        bool     `yaml:"trim"`
	TrueValues  []string `yaml:"true_values"`
	FalseValues []string `yaml:"false_values"`
	Path        string   `yaml:"path"`
}
type Transform struct {
	Type        string            `yaml:"type"`
	Mapping     map[string]string `yaml:"mapping"`
	MappingFile string            `yaml:"mapping_file"`
	SchemaFile  string            `yaml:"schema_file"`
	SourceKey   string            `yaml:"source_key"`
	SourceCol   int               `yaml:"source_col"`
	TargetCol   *int              `yaml:"target_col"`
	ExcludeKeys []string          `yaml:"exclude_keys"`
}
type Rule struct {
	Code    string     `yaml:"code"`
	Name    string     `yaml:"name"`
	Message string     `yaml:"message"`
	Level   string     `yaml:"level"`
	Column  string     `yaml:"column"`
	Checks  []Check    `yaml:"checks"`
	When    *Condition `yaml:"when"`
}
type Check struct {
	Type   string         `yaml:"type"`
	Params map[string]any `yaml:",inline"`
}
type Condition struct {
	Column   string      `yaml:"column"`
	Operator string      `yaml:"operator"`
	Value    any         `yaml:"value"`
	All      []Condition `yaml:"all"`
	Any      []Condition `yaml:"any"`
	Not      *Condition  `yaml:"not"`
}
type Stat struct {
	Code      string `yaml:"code"`
	Name      string `yaml:"name"`
	Column    string `yaml:"column"`
	Aggregate string `yaml:"aggregate"`
}
type DatasetRule struct {
	Code      string   `yaml:"code"`
	Level     string   `yaml:"level"`
	Metric    string   `yaml:"metric"`
	Column    string   `yaml:"column"`
	RuleCode  string   `yaml:"rule_code"`
	Min       any      `yaml:"min"`
	Max       any      `yaml:"max"`
	Equals    any      `yaml:"equals"`
	Tolerance *float64 `yaml:"tolerance"`
}
