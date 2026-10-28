package sink

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/richardrh/valet/internal/data"
	"github.com/richardrh/valet/internal/diagnostics"
)

type summaryDocument struct {
	*data.Summary
	Inputs    Reference `json:"inputs"`
	Rules     Reference `json:"rules"`
	Artifacts Reference `json:"artifacts"`
}

// textBytes bounds the encoded size of n bytes of string content: JSON escapes
// expand a byte to at most six (\u00XX).
func textBytes(n int) int64 { return 6*int64(n) + 64 }

// recordOverhead bounds keys, punctuation and numeric values of a flat record.
func recordOverhead(fields int) int64 { return 128 + int64(fields)*96 }

// issueBytes bounds one encoded data.Issue from its string lengths.
func issueBytes(issue *data.Issue) int64 {
	return textBytes(len(issue.Source)+len(issue.Code)+len(issue.Severity)+len(issue.Origin)+len(issue.Scope)+len(issue.Column)+len(issue.Message)+len(issue.Reason)) + recordOverhead(13)
}

// lineEncoder reuses one buffer and json.Encoder per NDJSON stream. The stream
// charge covers lineBufferRetain of retained capacity; a larger record grows the
// buffer under its own transient reservation and the buffer is dropped after.
type lineEncoder struct {
	buf bytes.Buffer
	enc *json.Encoder
}

// writeRecord encodes value as one NDJSON line. estimate bounds the encoded
// record; it is reserved twice over for buffer growth copies.
func (s *stream) writeRecord(value any, estimate int64) error {
	charge := 2*estimate + jsonRecordGrowthReserve
	if err := s.budget.Reserve(charge, "JSON output record"); err != nil {
		return err
	}
	defer s.budget.Release(charge)
	e := &s.line
	if e.enc == nil {
		e.enc = json.NewEncoder(&e.buf)
	}
	e.buf.Reset()
	err := e.enc.Encode(value)
	if err == nil {
		_, err = s.Write(e.buf.Bytes())
	} else {
		err = fmt.Errorf("encode report: %w", err)
	}
	if e.buf.Cap() > lineBufferRetain {
		e.buf = bytes.Buffer{}
		e.enc = nil
	}
	return err
}

// JSON is bounded by the current metadata record, never the stream length.
// Reserve for the JSON encoder's escaping and simultaneous growth buffers before
// encoding; source records, rule configuration and summary state are already
// bounded and accounted for by their owners. Only the once-per-run metadata
// documents (rules, summary, manifest) are sized this way; per-line records use
// estimates computed from their string lengths.
func jsonCapacity(v reflect.Value) int64 {
	if !v.IsValid() {
		return 16
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return 16
		}
		return jsonCapacity(v.Elem())
	case reflect.String:
		return 6*int64(v.Len()) + 64
	case reflect.Slice, reflect.Array:
		n := int64(64)
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return 6*int64(v.Len()) + 64
		}
		for i := range v.Len() {
			n += jsonCapacity(v.Index(i))
		}
		return n
	case reflect.Map:
		n := int64(64)
		iter := v.MapRange()
		for iter.Next() {
			n += jsonCapacity(iter.Key()) + jsonCapacity(iter.Value())
		}
		return n
	case reflect.Struct:
		n := int64(128)
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				n += int64(len(v.Type().Field(i).Name)*6+64) + jsonCapacity(v.Field(i))
			}
		}
		return n
	default:
		return 128
	}
}
func metadataJSONCharge(value any) int64 {
	return 2*jsonCapacity(reflect.ValueOf(value)) + jsonRecordGrowthReserve
}

func (w *Writer) publishManifest(value any) (err error) {
	charge := metadataJSONCharge(value)
	if err := w.budget.Reserve(charge, "JSON output record"); err != nil {
		return err
	}
	defer w.budget.Release(charge)
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	encoded = append(encoded, '\n')
	committedBytes := w.publishedBytes
	if err = addCount(&committedBytes, uint64(len(encoded)), "published bytes"); err != nil {
		return err
	}
	s, err := w.open("manifest.json")
	if err != nil {
		return err
	}
	defer s.abort()
	if _, err = s.Write(encoded); err != nil {
		return err
	}
	if err = s.close(); err != nil {
		return err
	}
	if recorder := diagnostics.FromContext(w.ctx); recorder != nil {
		recorder.SetResultBytesCommitted(w.publishedBytes)
	}
	return nil
}
func (w *Writer) writeMetadata(path string, value any) (Reference, error) {
	s, err := w.open(path)
	if err != nil {
		return Reference{}, err
	}
	defer s.abort()
	if err = s.writeRecord(value, jsonCapacity(reflect.ValueOf(value))); err != nil {
		return Reference{}, err
	}
	s.count = 1
	if err = s.close(); err != nil {
		return Reference{}, err
	}
	return s.ref(), nil
}

type ruleMetadata struct {
	Code       string `json:"code"`
	Message    string `json:"message,omitempty"`
	Severity   string `json:"severity"`
	Definition any    `json:"definition"`
}
type fieldMetadata struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}
type rulesMetadata struct {
	ReportVersion        int             `json:"report_version"`
	ConfigVersion        int             `json:"config_version"`
	Name                 string          `json:"name"`
	Version              string          `json:"version"`
	Rules                []ruleMetadata  `json:"rules"`
	DatasetRules         []ruleMetadata  `json:"dataset_rules"`
	Fields               []fieldMetadata `json:"fields"`
	CSVNull              string          `json:"csv_null"`
	CSVStringEscape      string          `json:"csv_string_escape"`
	CSVBinary            string          `json:"csv_binary_encoding"`
	JSONIntegerPrecision string          `json:"json_integer_precision"`
}

// issueRecord is one issues/*.ndjson line. A schema/dataset issue does not
// invent a source ordinal or row zero.
type issueRecord struct {
	ReportVersion int     `json:"report_version"`
	RunID         string  `json:"run_id"`
	Source        string  `json:"source,omitempty"`
	SourceOrdinal uint64  `json:"source_ordinal,omitempty"`
	Row           uint64  `json:"row,omitempty"`
	GlobalRow     uint64  `json:"global_row,omitempty"`
	Line          uint64  `json:"line,omitempty"`
	RowGroup      *uint64 `json:"row_group,omitempty"`
	RowInGroup    uint64  `json:"row_in_group,omitempty"`
	PositionBasis string  `json:"position_basis,omitempty"`
	Code          string  `json:"code"`
	Severity      string  `json:"severity"`
	Origin        string  `json:"origin"`
	Scope         string  `json:"scope"`
	Column        string  `json:"column,omitempty"`
	Reason        string  `json:"reason,omitempty"`
	Count         uint64  `json:"count,omitempty"`
}

func (w *Writer) writeIssue(issue data.Issue) error {
	record := issueRecord{ReportVersion: data.ReportVersion, RunID: w.runID, Code: issue.Code, Severity: issue.Severity, Origin: issue.Origin, Scope: issue.Scope, Column: issue.Column, Reason: issue.Reason, Count: issue.Count}
	if issue.Scope != data.ScopeDataset {
		record.Source = issue.Source
		record.SourceOrdinal = issue.SourceOrdinal
		record.Row = issue.Row
		record.GlobalRow = issue.GlobalRow
		record.Line = issue.Line
		record.RowGroup = issue.RowGroup
		record.RowInGroup = issue.RowInGroup
		record.PositionBasis = issue.PositionBasis
	}
	return w.writeLine(kindIssues, record, issueBytes(&issue)+textBytes(len(w.runID)))
}
func (w *Writer) writeLine(kind partKind, value any, estimate int64) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if p := w.parts[kind]; p != nil && p.stream.size >= w.partBytes {
		if err := w.closePart(kind); err != nil {
			return err
		}
	}
	p, err := w.ensurePart(kind)
	if err != nil {
		return err
	}
	if err = p.stream.writeRecord(value, estimate); err != nil {
		return err
	}
	if err = addCount(&p.stream.count, 1, "ledger part records"); err != nil {
		return err
	}
	if err = addCount(&w.ledgerCounts[kind], 1, "ledger records"); err != nil {
		return err
	}
	return nil
}
