// Package data defines the run-scoped Arrow ownership and result contracts.
package data

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// State is the ingestion state of one cell.
type State uint8

const (
	Valid State = iota
	Missing
	Null
	Invalid
)

var stateNames = [...]string{Valid: "valid", Missing: "missing", Null: "null", Invalid: "invalid"}

// String returns the report spelling of s, or "state(N)" when s is unknown.
func (s State) String() string {
	if int(s) < len(stateNames) {
		return stateNames[s]
	}
	return fmt.Sprintf("state(%d)", uint8(s))
}

// MarshalText encodes s with its report spelling and rejects unknown states.
func (s State) MarshalText() ([]byte, error) {
	if int(s) >= len(stateNames) {
		return nil, fmt.Errorf("unknown field state %d", uint8(s))
	}
	return []byte(stateNames[s]), nil
}

// UnmarshalText decodes the report spelling written by MarshalText.
func (s *State) UnmarshalText(text []byte) error {
	i, err := lookupName(stateNames[:], text, "field state")
	if err == nil {
		*s = State(i)
	}
	return err
}

// Bucket is the output partition chosen for one evaluated row.
type Bucket uint8

const (
	BucketPass Bucket = iota
	BucketWarn
	BucketError
)

var bucketNames = [...]string{BucketPass: "pass", BucketWarn: "warn", BucketError: "error"}

// String returns the report spelling of b, or "bucket(N)" when b is unknown.
func (b Bucket) String() string {
	if int(b) < len(bucketNames) {
		return bucketNames[b]
	}
	return fmt.Sprintf("bucket(%d)", uint8(b))
}

// Valid reports whether b is one of the defined buckets.
func (b Bucket) Valid() bool { return int(b) < len(bucketNames) }

// MarshalText encodes b with its report spelling and rejects unknown buckets.
func (b Bucket) MarshalText() ([]byte, error) {
	if !b.Valid() {
		return nil, fmt.Errorf("unknown row bucket %d", uint8(b))
	}
	return []byte(bucketNames[b]), nil
}

// UnmarshalText decodes the report spelling written by MarshalText.
func (b *Bucket) UnmarshalText(text []byte) error {
	i, err := lookupName(bucketNames[:], text, "row bucket")
	if err == nil {
		*b = Bucket(i)
	}
	return err
}

func lookupName(names []string, text []byte, kind string) (int, error) {
	for i, name := range names {
		if string(text) == name {
			return i, nil
		}
	}
	return 0, fmt.Errorf("unknown %s %q", kind, text)
}

// Report vocabulary shared by producers and the sink. The JSON spelling is the
// constant value.
const (
	ReportVersion        = 2
	ScopeData            = "data"
	ScopePlan            = "plan"
	StatusCompleted      = "completed"
	StatusSchemaRejected = "schema_rejected"

	ScopeRow     = "row"
	ScopeSchema  = "schema"
	ScopeDataset = "dataset"

	OriginRule    = "rule"
	OriginIngest  = "ingest"
	OriginDataset = "dataset"

	ValidationPass = "pass"
	ValidationFail = "fail"
)

type Location struct {
	Source        string  `json:"source"`
	SourceOrdinal uint64  `json:"source_ordinal"`
	Row           uint64  `json:"row"`
	GlobalRow     uint64  `json:"global_row"`
	Line          uint64  `json:"line,omitempty"`
	RowGroup      *uint64 `json:"row_group,omitempty"`
	RowInGroup    uint64  `json:"row_in_group,omitempty"`
	PositionBasis string  `json:"position_basis,omitempty"`
}

// Batch owns Record and bounded metadata. Release must be called on every path.
// States omitted for a column means use the Arrow validity bitmap (valid/null).
type Batch struct {
	Record    arrow.RecordBatch
	States    map[string][]State
	Locations []Location
	Raw       []json.RawMessage
	Ingest    []Issue
	Evidence  *GroupEvidence
	Cleanup   func()
	once      sync.Once
}

func (b *Batch) Release() {
	if b != nil {
		b.once.Do(func() {
			if b.Record != nil {
				b.Record.Release()
			}
			if b.Cleanup != nil {
				b.Cleanup()
			}
		})
	}
}
func (b *Batch) State(column string, row int) State {
	if s, ok := b.States[column]; ok {
		return s[row]
	}
	indices := b.Record.Schema().FieldIndices(column)
	if len(indices) == 0 {
		return Missing
	}
	if b.Record.Column(indices[0]).IsNull(row) {
		return Null
	}
	return Valid
}

// ColumnState binds ingestion overrides and Arrow validity once per batch.
// It borrows the batch's arrays and is valid only until Batch.Release.
type ColumnState struct {
	values     arrow.Array
	states     []State
	overridden bool
}

// BindState binds a previously resolved schema index without resolving a name
// for each row. A negative index represents an absent column.
func (b *Batch) BindState(column string, index int) ColumnState {
	states, overridden := b.States[column]
	bound := ColumnState{states: states, overridden: overridden}
	if index >= 0 {
		bound.values = b.Record.Column(index)
	}
	return bound
}

// State returns the ingestion state of the bound row.
func (c ColumnState) State(row int) State {
	if c.overridden {
		return c.states[row]
	}
	if c.values == nil {
		return Missing
	}
	if c.values.IsNull(row) {
		return Null
	}
	return Valid
}

type Issue struct {
	Location
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Origin   string `json:"origin"`
	Scope    string `json:"scope"`
	Column   string `json:"column,omitempty"`
	Message  string `json:"message,omitempty"`
	Reason   string `json:"reason,omitempty"`
	Count    uint64 `json:"count,omitempty"`
}

// Result owns evaluation storage independently of its Program and Batch.
// Call Release after its final consumer, including on unsuccessful writes.
type Result struct {
	Buckets []Bucket
	Issues  []Issue
	Cleanup func()
	once    sync.Once
}

// Release returns this result's storage exactly once. It is safe on nil and
// may run concurrently with evaluation of a different batch.
func (r *Result) Release() {
	if r != nil {
		r.once.Do(func() {
			if r.Cleanup != nil {
				r.Cleanup()
			}
			r.Buckets, r.Issues, r.Cleanup = nil, nil, nil
		})
	}
}

type DatasetResult struct {
	Code          string `json:"code"`
	Metric        string `json:"metric"`
	Value         any    `json:"value"`
	Status        string `json:"status"`
	Reason        string `json:"reason,omitempty"`
	Numerator     uint64 `json:"numerator,omitempty"`
	Denominator   uint64 `json:"denominator,omitempty"`
	Contributing  uint64 `json:"contributing,omitempty"`
	Excluded      uint64 `json:"excluded,omitempty"`
	NotApplicable uint64 `json:"not_applicable"`
	Unevaluable   uint64 `json:"unevaluable"`
}
type Summary struct {
	ReportVersion           int                           `json:"report_version"`
	RunID                   string                        `json:"run_id"`
	Status                  string                        `json:"status"`
	Validation              string                        `json:"validation"`
	Scope                   string                        `json:"scope"`
	OutputMode              string                        `json:"output_mode"`
	Verification            string                        `json:"verification"`
	TrustPolicy             string                        `json:"trust_policy"`
	IngestionVerification   string                        `json:"ingestion_verification"`
	ProvenancePositionBasis string                        `json:"provenance_position_basis"`
	Coverage                Coverage                      `json:"coverage"`
	RuleCoverage            map[string]RuleCoverage       `json:"rule_coverage"`
	RowOutput               map[string]RowMaterialization `json:"row_output"`
	Total                   uint64                        `json:"total"`
	Passed                  uint64                        `json:"pass"`
	Warned                  uint64                        `json:"warn"`
	Errored                 uint64                        `json:"error"`
	RowsScanned             uint64                        `json:"rows_scanned"`
	RuleCounts              map[string]uint64             `json:"rule_counts"`
	IngestCounts            map[string]uint64             `json:"ingest_counts"`
	Unevaluable             map[string]uint64             `json:"unevaluable"`
	NotApplicable           map[string]uint64             `json:"not_applicable"`
	Stats                   map[string]any                `json:"stats"`
	DatasetResults          []DatasetResult               `json:"dataset_results"`
	DatasetIssues           []Issue                       `json:"dataset_issues"`
	SchemaIssues            []Issue                       `json:"schema_issues"`
	DurationSeconds         float64                       `json:"duration_seconds"`
	Passes                  int                           `json:"passes"`
	ManagedPeak             int64                         `json:"managed_peak_bytes"`
	ManifestURI             string                        `json:"manifest_uri,omitempty"`
}

func NewSummary() *Summary {
	return &Summary{ReportVersion: ReportVersion, Status: StatusCompleted, Scope: ScopeData, OutputMode: "all", Verification: "values", TrustPolicy: "none", IngestionVerification: "full", ProvenancePositionBasis: "values", RuleCoverage: map[string]RuleCoverage{}, RowOutput: map[string]RowMaterialization{}, Validation: ValidationPass, RuleCounts: map[string]uint64{}, IngestCounts: map[string]uint64{}, Unevaluable: map[string]uint64{}, NotApplicable: map[string]uint64{}, Stats: map[string]any{}, DatasetResults: []DatasetResult{}, DatasetIssues: []Issue{}, SchemaIssues: []Issue{}}
}
