//go:build integration

package integration_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/metadata"
)

// All expected populations below are literal fixture oracles, not values derived
// from the implementation's reports. Only public Arrow APIs produce fixtures.
func vmStatFile(t *testing.T, fields []arrow.Field, groups ...[][]string) string {
	t.Helper()
	schema := arrow.NewSchema(fields, nil)
	var records []arrow.RecordBatch
	for _, rows := range groups {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		for _, row := range rows {
			for c, value := range row {
				if value == "NULL" {
					b.Field(c).AppendNull()
				} else if err := b.Field(c).AppendValueFromString(value); err != nil {
					t.Fatal(err)
				}
			}
		}
		records = append(records, b.NewRecordBatch())
		b.Release()
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()
	path := filepath.Join(t.TempDir(), "input.parquet")
	vmWriteParquet(t, path, schema, records)
	return path
}
func vmStatInt(t *testing.T, values ...string) string {
	rows := make([][]string, len(values))
	for i, v := range values {
		rows[i] = []string{v}
	}
	return vmStatFile(t, []arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, rows)
}
func vmStatRange(min string) string {
	return "rules: [{code: POSITIVE, column: amount, level: error, checks: [{type: valid_range, min: " + min + ", exclusive_min: true}]}]"
}

const vmStatSchema = "input:\n  schema:\n    columns:\n      amount: {type: int64}\n"

func vmStatRun(t *testing.T, path, config, mode string, flags ...string) *runResult {
	t.Helper()
	root := t.TempDir()
	p := defaultPipeline(path, "parquet", filepath.Join(root, "output"), "csv").with(t, config)
	args := []string{"--output-mode", mode, "--parquet-stats", "trusted", "--log-level", "error", "--json-output", "-"}
	args = append(args, flags...)
	return runPipeline(t, root, p, args...)
}

type vmStatReport struct {
	Verification string `json:"verification"`
	Trust        string `json:"trust_policy"`
	Ingestion    string `json:"ingestion_verification"`
	Basis        string `json:"provenance_position_basis"`
	Coverage     struct {
		Values    uint64 `json:"value_rows"`
		Claimed   uint64 `json:"metadata_claimed_rows"`
		Accounted uint64 `json:"accounted_rows"`
		Scanned   uint64 `json:"row_groups_scanned"`
		Skipped   uint64 `json:"row_groups_statistics_only"`
		Groups    uint64 `json:"row_groups_total"`
	} `json:"coverage"`
	Rules map[string]struct {
		Values  uint64 `json:"value_rows"`
		Claimed uint64 `json:"metadata_claimed_rows"`
	} `json:"rule_coverage"`
}

func vmStatAssert(t *testing.T, r *runResult, code int, total, pass, warn, errors, claimed, scanned, skipped uint64, rules ...string) vmStatReport {
	t.Helper()
	if r.code != code {
		t.Fatalf("exit=%d want=%d stderr=%s stdout=%s", r.code, code, r.stderr, r.stdout)
	}
	if countsOf(r) != (counts{total: total, pass: pass, warn: warn, error: errors}) {
		t.Fatalf("counts=%+v want=%d/%d/%d/%d", countsOf(r), total, pass, warn, errors)
	}
	vmAssertArtifacts(t, r)
	var presented vmStatReport
	vmJSON(t, r.stdout, &presented)
	var s vmStatReport
	decodeFile(t, filepath.Join(r.dir(t), "summary.json"), &s)
	c := s.Coverage
	if c.Values != total || c.Claimed != claimed || c.Accounted != total+claimed || c.Scanned != scanned || c.Skipped != skipped || c.Groups != scanned+skipped {
		t.Fatalf("coverage=%+v want values=%d claimed=%d scan=%d skip=%d", c, total, claimed, scanned, skipped)
	}
	wantVerification := "values"
	if claimed > 0 {
		wantVerification = "metadata_assisted"
	}
	if s.Verification != wantVerification {
		t.Errorf("verification=%q want=%q", s.Verification, wantVerification)
	}
	if claimed > 0 {
		wantIngestion := "not_evaluated"
		if total > 0 {
			wantIngestion = "partial"
		}
		if s.Ingestion != wantIngestion {
			t.Errorf("ingestion_verification=%q want=%q", s.Ingestion, wantIngestion)
		}
	}
	for _, rule := range rules {
		c, ok := s.Rules[rule]
		if !ok || c.Values != total || c.Claimed != claimed {
			t.Errorf("rule %s coverage=%+v present=%v", rule, c, ok)
		}
	}
	if code == 0 && warn > 0 {
		for _, i := range r.issues {
			if i.Severity != "warn" {
				t.Errorf("warning-only issue severity=%q", i.Severity)
			}
		}
	}
	if presented.Coverage != s.Coverage || presented.Trust != s.Trust || presented.Verification != s.Verification {
		t.Errorf("stdout/committed coverage mismatch")
	}
	if s.Trust == "producer_statistics" {
		ev := vmEvidence(t, r)
		if uint64(len(ev)) != scanned+skipped {
			t.Errorf("evidence groups=%d want=%d", len(ev), scanned+skipped)
		}
		for _, e := range ev {
			if e["reason"] == "" || e["footer_sha256"] == "" || e["identity_method"] == "" {
				t.Errorf("incomplete audit basis=%v", e)
			}
			if e["action"] == "skip" {
				if e["payload_verification"] != "not_read" {
					t.Errorf("skipped payload verification=%v", e["payload_verification"])
				}
				checks, ok := e["checks"].([]any)
				if !ok {
					t.Errorf("missing check proofs=%v", e)
					continue
				}
				proved := map[string]bool{}
				for _, raw := range checks {
					check, ok := raw.(map[string]any)
					if ok && check["claim"] == "pass" {
						code, _ := check["code"].(string)
						proved[code] = true
					}
				}
				for _, rule := range rules {
					if !proved[rule] {
						t.Errorf("skip lacks %s proof: %v", rule, e)
					}
				}
			}
		}
	}
	return s
}
func vmStatDenied(t *testing.T, path, config, mode string, groups []int, flags ...string) (*runResult, *vmProxy) {
	t.Helper()
	return vmStatDeniedPresentation(t, path, config, mode, groups, true, flags...)
}
func vmStatDeniedPresentation(t *testing.T, path, config, mode string, groups []int, jsonOutput bool, flags ...string) (*runResult, *vmProxy) {
	t.Helper()
	e := minioEnvironment(t)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	e.put(t, "input.parquet", string(body))
	proxy := vmNewProxy(t, e)
	proxy.DenyPayload("/"+e.bucket+"/"+e.prefix+"input.parquet", vmInspectParquet(t, path), groups...)
	root := t.TempDir()
	p := defaultPipeline(e.uri("input.parquet"), "parquet", filepath.Join(root, "output"), "csv").with(t, config)
	p.Source.S3 = e.s3(proxy.URL)
	args := []string{"--output-mode", mode, "--parquet-stats", "trusted", "--log-level", "error"}
	if jsonOutput {
		args = append(args, "--json-output", "-")
	}
	args = append(args, flags...)
	r := runPipeline(t, root, p, args...)
	if len(r.manifests) == 1 {
		vmAssertEvidence(t, r, vmInspectParquet(t, path))
	}
	return r, proxy
}
func vmStatNoDenied(t *testing.T, p *vmProxy) {
	t.Helper()
	for _, r := range p.Requests() {
		if r.Status == 403 {
			t.Fatalf("forbidden payload read attempted: %+v", r)
		}
	}
}
func vmStatHuman(t *testing.T, path, config string, values, claimed int) {
	t.Helper()
	for _, plain := range []bool{false, true} {
		var args []string
		if plain {
			args = append(args, "--plain")
		}
		var groups []int
		if values > 0 {
			groups = []int{0}
		}
		r, proxy := vmStatDeniedPresentation(t, path, config, "report", groups, false, args...)
		vmStatNoDenied(t, proxy)
		if r.code > 1 {
			t.Fatalf("presentation exit=%d stderr=%s", r.code, r.stderr)
		}
		text := strings.ToLower(string(r.stdout))
		for _, pair := range []struct {
			label string
			n     int
		}{{"value", values}, {"metadata", claimed}} {
			re := regexp.MustCompile(fmt.Sprintf(`(?m)(%s[^\n]*\b%d\b|\b%d\b[^\n]*%s)`, pair.label, pair.n, pair.n, pair.label))
			if !re.MatchString(text) {
				t.Errorf("missing labeled %s count %d: %s", pair.label, pair.n, text)
			}
		}
		if !strings.Contains(text, "producer") || !strings.Contains(text, "trust") {
			t.Errorf("trust policy absent: %s", text)
		}
	}
}
func TestVMStatistics01(t *testing.T) {
	path := vmFixtureFAST(t, t.TempDir())
	for _, mode := range []string{"report", "failures"} {
		t.Run("VM-STAT-01/"+mode, func(t *testing.T) {
			r, p := vmStatDenied(t, path, vmStatConfig, mode, nil)
			s := vmStatAssert(t, r, 0, 0, 0, 0, 0, 8, 0, 2, "POSITIVE", "NOT_NULL")
			vmStatNoDenied(t, p)
			if s.Trust != "producer_statistics" || s.Ingestion != "not_evaluated" {
				t.Errorf("undisclosed trust: %+v", s)
			}
			for _, bucket := range []string{"pass", "warn", "error"} {
				if len(vmRows(t, r, bucket)) != 0 {
					t.Errorf("fabricated %s rows", bucket)
				}
			}
			ev := vmEvidence(t, r)
			if len(ev) != 2 {
				t.Fatalf("evidence=%d want 2", len(ev))
			}
			for i, e := range ev {
				if e["action"] != "skip" || fmt.Sprint(e["row_group"]) != strconv.Itoa(i) {
					t.Errorf("evidence %d=%v", i, e)
				}
			}
		})
	}
	t.Run("human-and-plain", func(t *testing.T) { vmStatHuman(t, path, vmStatConfig, 0, 8) })
}
func TestVMStatistics02(t *testing.T) {
	t.Run("VM-STAT-02", func(t *testing.T) {
		path := vmFixtureSTAT(t, t.TempDir())
		r, p := vmStatDenied(t, path, vmStatConfig, "failures", []int{0})
		s := vmStatAssert(t, r, 1, 12, 9, 0, 3, 4, 3, 1, "POSITIVE", "NOT_NULL")
		vmStatNoDenied(t, p)
		if s.Basis != "metadata_prefix" {
			t.Errorf("position basis=%q", s.Basis)
		}
		if !slices.Equal(issuesOf(r), []issueAt{{6, "NOT_NULL"}, {9, "POSITIVE"}, {10, "POSITIVE"}}) {
			t.Errorf("issues=%v", issuesOf(r))
		}
		var raw []struct {
			Row     uint64  `json:"row"`
			Global  uint64  `json:"global_row"`
			Group   *uint64 `json:"row_group"`
			InGroup uint64  `json:"row_in_group"`
			Basis   string  `json:"position_basis"`
		}
		for _, path := range files(t, filepath.Join(r.dir(t), "issues", "*.ndjson")) {
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			d := json.NewDecoder(f)
			for d.More() {
				var v struct {
					Row     uint64  `json:"row"`
					Global  uint64  `json:"global_row"`
					Group   *uint64 `json:"row_group"`
					InGroup uint64  `json:"row_in_group"`
					Basis   string  `json:"position_basis"`
				}
				if err := d.Decode(&v); err != nil {
					t.Fatal(err)
				}
				raw = append(raw, v)
			}
			f.Close()
		}
		for i, v := range raw {
			wantGroup := uint64(2)
			wantLocal := uint64(i)
			if i == 0 {
				wantGroup = 1
				wantLocal = 2
			}
			if v.Group == nil || *v.Group != wantGroup || v.InGroup != wantLocal || v.Basis != "metadata_prefix" || v.Row != v.Global {
				t.Errorf("issue coordinate=%+v", v)
			}
		}
		if len(vmEvidence(t, r)) != 4 {
			t.Error("missing group decisions")
		}
		vmStatHuman(t, path, vmStatConfig, 12, 4)
	})
}
func TestVMStatistics03(t *testing.T) {
	t.Run("VM-STAT-03/missing-null-count", func(t *testing.T) {
		path := vmStatInt(t, "1", "NULL", "2", "3")
		vmRewriteFooter(t, path, func(m *metadata.FileMetaData) { m.RowGroups[0].Columns[0].MetaData.Statistics.NullCount = nil })
		cfg := vmStatSchema + "rules: [{code: NOT_NULL, column: amount, level: error, checks: [{type: not_null}]}]"
		r := vmStatRun(t, path, cfg, "report")
		vmStatAssert(t, r, 1, 4, 3, 0, 1, 0, 1, 0, "NOT_NULL")
		if !slices.Equal(issuesOf(r), []issueAt{{2, "NOT_NULL"}}) {
			t.Errorf("issues=%v", issuesOf(r))
		}
		denied, _ := vmStatDenied(t, path, cfg, "report", nil)
		if denied.code != 2 || len(denied.manifests) != 0 {
			t.Fatalf("denied fallback exit=%d manifests=%v", denied.code, denied.manifests)
		}
	})
	t.Run("VM-STAT-03/multicol-missing", func(t *testing.T) { vmStatMulti(t, "missing") })
}
func vmStatMulti(t *testing.T, kind string) {
	t.Helper()
	rows := [][]string{{"1", "1"}, {"2", "NULL"}, {"3", "-1"}, {"4", "-2"}}
	if kind == "both" {
		rows[3][0] = "-1"
	}
	if kind == "missing" || kind == "positive-null" {
		rows = [][]string{{"1", "1"}, {"2", "NULL"}, {"3", "2"}}
	}
	path := vmStatFile(t, []arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, rows)
	if kind == "missing" {
		vmRewriteFooter(t, path, func(m *metadata.FileMetaData) { m.RowGroups[0].Columns[1].MetaData.Statistics.NullCount = nil })
	}
	cfg := "input: {schema: {columns: {a: {type: int64}, b: {type: int64}}}}\nrules:\n - {code: NN, level: error, checks: [{type: not_null, columns: [a,b]}]}\n - {code: RANGE, level: error, checks: [{type: valid_range, columns: [a,b], min: 0, exclusive_min: true}]}\n"
	r := vmStatRun(t, path, cfg, "failures")
	n := uint64(len(rows))
	bad := uint64(3)
	if kind == "missing" || kind == "positive-null" {
		bad = 1
	}
	vmStatAssert(t, r, 1, n, n-bad, 0, bad, 0, 1, 0, "NN", "RANGE")
	want := []issueAt{{2, "NN"}, {3, "RANGE"}, {4, "RANGE"}}
	if kind == "missing" || kind == "positive-null" {
		want = []issueAt{{2, "NN"}}
	}
	if !slices.Equal(issuesOf(r), want) {
		t.Errorf("multi-column issues=%v want=%v", issuesOf(r), want)
	}
}
func TestVMStatistics04(t *testing.T) {
	for _, constraint := range []string{"allowed", "not-null", "schema"} {
		t.Run("VM-STAT-04/"+constraint, func(t *testing.T) {
			path := vmStatInt(t, "NULL", "NULL", "NULL", "NULL")
			vmRewriteFooter(t, path, func(m *metadata.FileMetaData) {
				s := m.RowGroups[0].Columns[0].MetaData.Statistics
				s.MinValue = nil
				s.MaxValue = nil
				s.Min = nil
				s.Max = nil
			})
			cfg := vmStatSchema + vmStatRange("0")
			if constraint == "not-null" {
				cfg = vmStatSchema + "rules: [{code: NOT_NULL, column: amount, level: error, checks: [{type: not_null}]}]"
			}
			if constraint == "schema" {
				cfg = strings.Replace(cfg, "{type: int64}", "{type: int64, nullable: false}", 1)
			}
			if constraint == "allowed" {
				r, p := vmStatDenied(t, path, cfg, "report", nil)
				vmStatAssert(t, r, 0, 0, 0, 0, 0, 4, 0, 1, "POSITIVE")
				vmStatNoDenied(t, p)
			} else {
				r := vmStatRun(t, path, cfg, "report")
				rule := "NOT_NULL"
				if constraint == "schema" {
					rule = "INGEST_NULL"
				}
				vmStatAssert(t, r, 1, 4, 0, 0, 4, 0, 1, 0)
				if len(r.issues) != 4 {
					t.Errorf("null issues=%v", r.issues)
				}
				for i, v := range r.issues {
					if v.Row != uint64(i+1) || v.Code != rule || v.Column != "amount" {
						t.Errorf("null issue=%+v want row=%d code=%s", v, i+1, rule)
					}
				}
			}
		})
	}
}
func TestVMStatistics05(t *testing.T) {
	for _, c := range []struct {
		name, check  string
		pass, errors uint64
	}{{"inclusive", "min: -1, max: 2", 4, 0}, {"exclusive-lower", "min: -1, exclusive_min: true", 3, 1}, {"exclusive-upper", "max: 2, exclusive_max: true", 3, 1}, {"positive", "min: 0, exclusive_min: true", 2, 2}} {
		t.Run("VM-STAT-05/"+c.name, func(t *testing.T) {
			path := vmStatInt(t, "-1", "0", "1", "2")
			cfg := vmStatSchema + "rules: [{code: R, column: amount, level: error, checks: [{type: valid_range, " + c.check + "}]}]"
			r := vmStatRun(t, path, cfg, "all")
			code := 0
			if c.errors > 0 {
				code = 1
			}
			vmStatAssert(t, r, code, 4, c.pass, 0, c.errors, 0, 1, 0, "R")
		})
	}
	t.Run("VM-STAT-05/multicol", func(t *testing.T) { vmStatMulti(t, "contrary") })
	t.Run("VM-STAT-05/both-operands-fail-one-issue", func(t *testing.T) { vmStatMulti(t, "both") })
	t.Run("VM-STAT-05/second-column-null", func(t *testing.T) { vmStatMulti(t, "positive-null") })
	for _, min := range []string{"0", "5"} {
		t.Run("VM-STAT-05/inexact-"+min, func(t *testing.T) {
			path := vmStatInt(t, "10", "11")
			vmRewriteFooter(t, path, func(m *metadata.FileMetaData) {
				s := m.RowGroups[0].Columns[0].MetaData.Statistics
				s.MinValue = vmStatLE(1)
				s.MaxValue = vmStatLE(20)
				s.IsMinValueExact = new(false)
				s.IsMaxValueExact = new(false)
			})
			cfg := vmStatSchema + vmStatRange(min)
			if min == "0" {
				r, p := vmStatDenied(t, path, cfg, "report", nil)
				vmStatAssert(t, r, 0, 0, 0, 0, 0, 2, 0, 1, "POSITIVE")
				vmStatNoDenied(t, p)
			} else {
				r := vmStatRun(t, path, cfg, "report")
				vmStatAssert(t, r, 0, 2, 2, 0, 0, 0, 1, 0, "POSITIVE")
				for _, ev := range vmEvidence(t, r) {
					if ev["action"] != "scan" {
						t.Errorf("inconclusive decision=%v", ev)
					}
					checks, ok := ev["checks"].([]any)
					if !ok {
						t.Fatal("missing inexact proof evidence")
					}
					found := false
					for _, raw := range checks {
						check, ok := raw.(map[string]any)
						if ok && check["code"] == "POSITIVE" {
							found = true
							if check["claim"] != "unknown" {
								t.Errorf("inexact crossing bound fabricated verdict: %v", check)
							}
						}
					}
					if !found {
						t.Error("missing POSITIVE decision")
					}
				}
			}
		})
	}
	for _, mode := range []string{"failures", "report"} {
		t.Run("VM-STAT-05/warnings-"+mode, func(t *testing.T) {
			path := vmStatInt(t, "1", "NULL", "-1", "2")
			cfg := strings.ReplaceAll(vmStatConfig, "level: error", "level: warn")
			r := vmStatRun(t, path, cfg, mode)
			vmStatAssert(t, r, 0, 4, 2, 2, 0, 0, 1, 0, "POSITIVE", "NOT_NULL")
			if !slices.Equal(issuesOf(r), []issueAt{{2, "NOT_NULL"}, {3, "POSITIVE"}}) {
				t.Errorf("warning issues=%v", issuesOf(r))
			}
			want := 0
			if mode == "failures" {
				want = 2
			}
			rows := vmRows(t, r, "warn")
			if len(rows) != want {
				t.Errorf("warning rows=%v", rows)
			}
			if want > 0 && (!slices.Equal(column(rows, "amount"), []string{`\N`, "-1"})) {
				t.Errorf("warning values=%v", rows)
			}
		})
	}
}
func vmStatLE(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}
func TestVMStatistics06(t *testing.T) {
	cases := []struct {
		name, typ, schema, min string
		dt                     arrow.DataType
		values                 []string
		bad                    bool
	}{
		{"int64-pass", "int64", "", "9007199254740992", arrow.PrimitiveTypes.Int64, []string{"9007199254740993", "9007199254740994"}, false},
		{"int64-equality", "int64", "", "9007199254740993", arrow.PrimitiveTypes.Int64, []string{"9007199254740993", "9007199254740994"}, true},
		{"uint64-pass", "uint64", "", "9223372036854775808", arrow.PrimitiveTypes.Uint64, []string{"9223372036854775809", "18446744073709551615"}, false},
		{"uint64-equality", "uint64", "", "9223372036854775809", arrow.PrimitiveTypes.Uint64, []string{"9223372036854775809", "18446744073709551615"}, true},
		{"decimal-pass", "decimal", ", precision: 20, scale: 2", "0.00", &arrow.Decimal128Type{Precision: 20, Scale: 2}, []string{"0.01", "9007199254740993.01"}, false},
		{"decimal-negative", "decimal", ", precision: 20, scale: 2", "0.00", &arrow.Decimal128Type{Precision: 20, Scale: 2}, []string{"-0.01", "0.01"}, true},
		{"decimal-equality", "decimal", ", precision: 20, scale: 2", "0.01", &arrow.Decimal128Type{Precision: 20, Scale: 2}, []string{"0.01", "0.02"}, true},
	}
	for _, c := range cases {
		t.Run("VM-STAT-06/"+c.name, func(t *testing.T) {
			rows := [][]string{{c.values[0]}, {c.values[1]}}
			path := vmStatFile(t, []arrow.Field{{Name: "amount", Type: c.dt, Nullable: true}}, rows)
			cfg := fmt.Sprintf("input: {schema: {columns: {amount: {type: %s%s}}}}\n", c.typ, c.schema) + vmStatRange(c.min)
			if !c.bad {
				r, p := vmStatDenied(t, path, cfg, "report", nil)
				vmStatAssert(t, r, 0, 0, 0, 0, 0, 2, 0, 1, "POSITIVE")
				vmStatNoDenied(t, p)
			} else {
				r := vmStatRun(t, path, cfg, "failures")
				vmStatAssert(t, r, 1, 2, 1, 0, 1, 0, 1, 0, "POSITIVE")
				if !slices.Equal(issuesOf(r), []issueAt{{1, "POSITIVE"}}) {
					t.Errorf("exact boundary=%v", issuesOf(r))
				}
				if got := column(vmRows(t, r, "error"), "amount"); !slices.Equal(got, []string{c.values[0]}) {
					t.Errorf("exact value=%v", got)
				}
			}
		})
	}
}
func TestVMStatistics07(t *testing.T) {
	for _, c := range []struct {
		name   string
		values []string
		nan    *int64
		scan   bool
		bad    uint64
	}{{"absent-nan", []string{"1", "2"}, nil, true, 0}, {"positive-nan", []string{"NaN", "2"}, new(int64(1)), true, 1}, {"zero-nan", []string{"1", "2"}, new(int64(0)), false, 0}, {"infinity", []string{"+Inf", "2"}, new(int64(0)), true, 1}, {"negative-infinity", []string{"-Inf", "2"}, new(int64(0)), true, 1}, {"signed-zero", []string{"-0", "0"}, new(int64(0)), false, 0}} {
		t.Run("VM-STAT-07/"+c.name, func(t *testing.T) {
			rows := [][]string{{c.values[0]}, {c.values[1]}}
			path := vmStatFile(t, []arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, rows)
			vmRewriteFooter(t, path, func(m *metadata.FileMetaData) { m.RowGroups[0].Columns[0].MetaData.Statistics.NanCount = c.nan })
			cfg := "input: {on_type_error: row_error, schema: {columns: {amount: {type: float64}}}}\nrules: [{code: R, column: amount, level: error, checks: [{type: valid_range, min: 0}]}]"
			if !c.scan {
				r, p := vmStatDenied(t, path, cfg, "report", nil)
				vmStatAssert(t, r, 0, 0, 0, 0, 0, 2, 0, 1, "R")
				vmStatNoDenied(t, p)
			} else {
				r := vmStatRun(t, path, cfg, "report")
				code := 0
				if c.bad > 0 {
					code = 1
				}
				vmStatAssert(t, r, code, 2, 2-c.bad, 0, c.bad, 0, 1, 0, "R")
				if c.bad > 0 && (r.summary.IngestCounts["INGEST_TYPE"] != 1 || r.summary.Unevaluable["R"] != 1) {
					t.Errorf("nonfinite ingestion=%v unevaluable=%v", r.summary.IngestCounts, r.summary.Unevaluable)
				}
			}
		})
	}
	t.Run("VM-STAT-07/exclusive-zero", func(t *testing.T) {
		path := vmStatFile(t, []arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, [][]string{{"-0"}, {"0"}})
		vmRewriteFooter(t, path, func(m *metadata.FileMetaData) { m.RowGroups[0].Columns[0].MetaData.Statistics.NanCount = new(int64(0)) })
		r := vmStatRun(t, path, strings.Replace(vmStatSchema, "int64", "float64", 1)+vmStatRange("0"), "failures")
		vmStatAssert(t, r, 1, 2, 0, 0, 2, 0, 1, 0, "POSITIVE")
		rows := vmRows(t, r, "error")
		if len(rows) != 2 {
			t.Fatal(rows)
		}
		v, err := strconv.ParseFloat(rows[0]["amount"], 64)
		if err != nil || !math.Signbit(v) {
			t.Errorf("negative zero lost: %v", rows)
		}
	})
}
func TestVMStatistics08(t *testing.T) {
	for _, kind := range []string{"missing-modern", "legacy-only", "unknown-order", "invalid-width", "reversed", "negative-null", "too-many-null", "negative-nan", "too-many-nan"} {
		t.Run("VM-STAT-08/"+kind, func(t *testing.T) {
			path := vmStatInt(t, "1", "2")
			vmRewriteFooter(t, path, func(m *metadata.FileMetaData) {
				s := m.RowGroups[0].Columns[0].MetaData.Statistics
				switch kind {
				case "missing-modern":
					s.MinValue = nil
					s.MaxValue = nil
					s.Min = nil
					s.Max = nil
				case "legacy-only":
					s.Min = s.MinValue
					s.Max = s.MaxValue
					s.MinValue = nil
					s.MaxValue = nil
				case "unknown-order":
					m.ColumnOrders = nil
				case "invalid-width":
					s.MinValue = []byte{1}
				case "reversed":
					s.MinValue = vmStatLE(9)
					s.MaxValue = vmStatLE(1)
				case "negative-null":
					s.NullCount = new(int64(-1))
				case "too-many-null":
					s.NullCount = new(int64(3))
				case "negative-nan":
					s.NanCount = new(int64(-1))
				case "too-many-nan":
					s.NanCount = new(int64(3))
				}
			})
			r := vmStatRun(t, path, vmStatSchema+vmStatRange("0"), "report")
			vmStatAssert(t, r, 0, 2, 2, 0, 0, 0, 1, 0, "POSITIVE")
		})
	}
}
func TestVMStatistics09(t *testing.T) {
	t.Run("VM-STAT-09/conditional-conversion", func(t *testing.T) {
		path := vmStatFile(t, []arrow.Field{{Name: "amount", Type: arrow.BinaryTypes.String, Nullable: true}}, [][]string{{"0"}, {"2"}, {"oops"}, {"3"}})
		cfg := strings.Replace(vmStatSchema, "input:\n", "input:\n  on_type_error: row_error\n", 1) + "rules: [{code: C, column: amount, level: error, when: {column: amount, operator: gt, value: 0}, checks: [{type: not_null}]}]"
		r := vmStatRun(t, path, cfg, "report")
		vmStatAssert(t, r, 1, 4, 3, 0, 1, 0, 1, 0, "C")
		if r.summary.NotApplicable["C"] != 1 || r.summary.Unevaluable["C"] != 1 || !slices.Equal(issuesOf(r), []issueAt{{3, "INGEST_TYPE"}}) {
			t.Errorf("conditional counters=%+v issues=%v", r.summary, issuesOf(r))
		}
	})
	t.Run("VM-STAT-09/legacy-float-coercion", func(t *testing.T) {
		path := vmStatInt(t, "9007199254740993")
		r := vmStatRun(t, path, vmStatRange("9007199254740992"), "report")
		vmStatAssert(t, r, 1, 1, 0, 0, 1, 0, 1, 0, "POSITIVE")
		if !slices.Equal(issuesOf(r), []issueAt{{1, "POSITIVE"}}) {
			t.Errorf("legacy coercion changed: %v", issuesOf(r))
		}
	})
	t.Run("VM-STAT-09/duplicate-prepass", func(t *testing.T) {
		path := vmStatInt(t, "1", "2", "1")
		cfg := vmStatSchema + "rules: [{code: U, column: amount, level: error, checks: [{type: is_unique}]}]"
		r := vmStatRun(t, path, cfg, "report")
		vmStatAssert(t, r, 1, 3, 1, 0, 2, 0, 1, 0, "U")
		if !slices.Equal(issuesOf(r), []issueAt{{1, "U"}, {3, "U"}}) || r.summary.RowsScanned <= 3 {
			t.Errorf("duplicate prepass issues=%v scanned=%d", issuesOf(r), r.summary.RowsScanned)
		}
	})
	for _, c := range []struct {
		name, config string
		bad          uint64
	}{{"condition", vmStatSchema + "rules: [{code: R, column: amount, level: error, when: {column: amount, operator: gt, value: 0}, checks: [{type: not_null}]}]", 0}, {"no-explicit-schema", vmStatRange("0"), 0}, {"uniqueness", vmStatSchema + "rules: [{code: R, column: amount, level: error, checks: [{type: is_unique}]}]", 0}, {"custom-string", vmStatSchema + "rules: [{code: R, column: amount, level: error, checks: [{type: not_equals, value: -1}]}]", 0}, {"dataset", vmStatSchema + "dataset_rules: [{code: COUNT, level: error, metric: row_count, equals: 2}]", 0}} {
		t.Run("VM-STAT-09/"+c.name, func(t *testing.T) {
			r := vmStatRun(t, vmStatInt(t, "1", "2"), c.config, "report")
			vmStatAssert(t, r, 0, 2, 2, 0, 0, 0, 1, 0)
		})
	}
	t.Run("VM-STAT-09/lexical-null", func(t *testing.T) {
		path := vmStatFile(t, []arrow.Field{{Name: "amount", Type: arrow.BinaryTypes.String, Nullable: true}}, [][]string{{"1"}, {"MISSING"}})
		cfg := strings.Replace(vmStatSchema, "input:\n", "input:\n  null_values: [MISSING]\n", 1) + "rules: [{code: R, column: amount, level: error, checks: [{type: not_null}]}]"
		r := vmStatRun(t, path, cfg, "report")
		vmStatAssert(t, r, 1, 2, 1, 0, 1, 0, 1, 0, "R")
		if !slices.Equal(issuesOf(r), []issueAt{{2, "INGEST_TYPE"}}) || r.summary.Unevaluable["R"] != 0 {
			t.Errorf("native string null marker must remain a conversion failure: issues=%v unevaluable=%v", issuesOf(r), r.summary.Unevaluable)
		}
	})
	t.Run("VM-STAT-09/native-string-statistics", func(t *testing.T) {
		path := vmStatFile(t, []arrow.Field{
			{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "status", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
		}, [][]string{{"1", "MISSING", "unrelated-a"}, {"2", "ok", "unrelated-b"}})
		cfg := `input:
  null_values: [MISSING]
  schema:
    columns:
      amount: {type: int64}
      status: {type: string}
      note: {type: string}
rules:
  - {code: POSITIVE, column: amount, level: error, checks: [{type: valid_range, min: 0, exclusive_min: true}]}
  - {code: STATUS_NOT_NULL, column: status, level: error, checks: [{type: not_null}]}
`
		r, proxy := vmStatDenied(t, path, cfg, "report", nil)
		vmStatAssert(t, r, 0, 0, 0, 0, 0, 2, 0, 1, "POSITIVE", "STATUS_NOT_NULL")
		vmStatNoDenied(t, proxy)
	})
	t.Run("VM-STAT-09/reference", func(t *testing.T) {
		path := vmStatInt(t, "1", "2")
		ref := put(t, filepath.Join(t.TempDir(), "ref.csv"), "key\n1\n")
		cfg := vmStatSchema + fmt.Sprintf("rules: [{code: R, column: amount, level: error, checks: [{type: referential_integrity, reference_file: %q, reference_column: key}]}]", ref)
		r := vmStatRun(t, path, cfg, "report")
		vmStatAssert(t, r, 1, 2, 1, 0, 1, 0, 1, 0, "R")
		if !slices.Equal(issuesOf(r), []issueAt{{2, "R"}}) {
			t.Errorf("reference issues=%v", issuesOf(r))
		}
	})
	t.Run("VM-STAT-09/pure-rename", func(t *testing.T) {
		cfg := strings.ReplaceAll(vmStatSchema+vmStatRange("0"), "amount", "renamed") + "\ntransforms: [{type: rename_columns, mapping: {amount: renamed}}]"
		r, p := vmStatDenied(t, vmStatInt(t, "1", "2"), cfg, "report", nil)
		vmStatAssert(t, r, 0, 0, 0, 0, 0, 2, 0, 1, "POSITIVE")
		vmStatNoDenied(t, p)
	})
}
func TestVMStatistics10(t *testing.T) {
	for _, name := range []string{"FAST", "LIE"} {
		t.Run("VM-STAT-10/"+name, func(t *testing.T) {
			path := vmFixtureFAST(t, t.TempDir())
			total, pass, errors, groups := uint64(8), uint64(8), uint64(0), uint64(2)
			want := []string{"1", "2", "3", "4", "5", "6", "7", "8"}
			if name == "LIE" {
				path = vmFixtureLIE(t, t.TempDir())
				total = 4
				pass = 2
				errors = 2
				groups = 1
				want = []string{"11", "12"}
			}
			r := vmStatRun(t, path, vmStatConfig, "all")
			code := 0
			if errors > 0 {
				code = 1
			}
			vmStatAssert(t, r, code, total, pass, 0, errors, 0, groups, 0, "POSITIVE", "NOT_NULL")
			if !slices.Equal(column(vmRows(t, r, "pass"), "amount"), want) {
				t.Errorf("all actual rows=%v", vmRows(t, r, "pass"))
			}
			if name == "LIE" && !slices.Equal(column(vmRows(t, r, "error"), "amount"), []string{`\N`, "-1"}) {
				t.Errorf("lie actual errors=%v", vmRows(t, r, "error"))
			}
		})
	}
}
func TestVMStatistics11(t *testing.T) {
	t.Run("VM-STAT-11", func(t *testing.T) {
		path := vmStatFile(t, []arrow.Field{{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "status", Type: arrow.BinaryTypes.String, Nullable: true}}, [][]string{{"-1", "ok"}, {"2", "ok"}})
		vmRewriteFooter(t, path, func(m *metadata.FileMetaData) {
			s := m.RowGroups[0].Columns[0].MetaData.Statistics
			s.MinValue = vmStatLE(1)
			s.MaxValue = vmStatLE(2)
		})
		cfg := strings.Replace(vmStatSchema, "      amount: {type: int64}\n", "      amount: {type: int64}\n      status: {type: string}\n", 1) + "rules:\n - {code: POSITIVE, column: amount, level: error, checks: [{type: valid_range, min: 0, exclusive_min: true}]}\n - {code: STATUS, column: status, level: error, checks: [{type: equals, value: ok}]}\n"
		r := vmStatRun(t, path, cfg, "report")
		vmStatAssert(t, r, 1, 2, 1, 0, 1, 0, 1, 0, "POSITIVE", "STATUS")
		if !slices.Equal(issuesOf(r), []issueAt{{1, "POSITIVE"}}) {
			t.Errorf("lying candidate hid issue: %v", issuesOf(r))
		}
	})
}
func TestVMStatistics12(t *testing.T) {
	t.Run("VM-STAT-12/config-trusted-cli-off", func(t *testing.T) {
		r := vmStatRun(t, vmFixtureLIE(t, t.TempDir()), strings.Replace(vmStatConfig, "input:\n", "input:\n  parquet: {statistics: trusted}\n", 1), "report", "--parquet-stats", "off")
		s := vmStatAssert(t, r, 1, 4, 2, 0, 2, 0, 1, 0, "POSITIVE", "NOT_NULL")
		if s.Trust != "none" {
			t.Errorf("policy=%q", s.Trust)
		}
	})
	t.Run("VM-STAT-12/config-off-cli-trusted", func(t *testing.T) {
		r, p := vmStatDenied(t, vmFixtureFAST(t, t.TempDir()), strings.Replace(vmStatConfig, "input:\n", "input:\n  parquet: {statistics: off}\n", 1), "report", nil)
		s := vmStatAssert(t, r, 0, 0, 0, 0, 0, 8, 0, 2, "POSITIVE", "NOT_NULL")
		if s.Trust != "producer_statistics" {
			t.Errorf("policy=%q", s.Trust)
		}
		vmStatNoDenied(t, p)
	})
	for _, flag := range []string{"", "TRUSTED", "unknown"} {
		t.Run("VM-STAT-12/invalid-"+flag, func(t *testing.T) {
			root := t.TempDir()
			p := defaultPipeline("/definitely/not/an/input.parquet", "parquet", filepath.Join(root, "out"), "csv").with(t, strings.Replace(vmStatConfig, "input:\n", "input:\n  parquet: {statistics: trusted}\n", 1))
			r := runPipeline(t, root, p, "--parquet-stats="+flag)
			if r.code != 2 || len(r.manifests) != 0 {
				t.Fatalf("invalid trust exit=%d manifests=%v", r.code, r.manifests)
			}
			if strings.Contains(strings.ToLower(string(r.stderr)), "no such file") {
				t.Errorf("input accessed before enum validation: %s", r.stderr)
			}
		})
	}
	for _, mode := range []string{"report", "failures"} {
		t.Run("VM-STAT-12/default-"+mode, func(t *testing.T) {
			path := vmFixtureLIE(t, t.TempDir())
			root := t.TempDir()
			p := defaultPipeline(path, "parquet", filepath.Join(root, "out"), "csv").with(t, vmStatConfig)
			r := runPipeline(t, root, p, "--output-mode", mode, "--json-output", "-")
			s := vmStatAssert(t, r, 1, 4, 2, 0, 2, 0, 1, 0, "POSITIVE", "NOT_NULL")
			if s.Trust != "none" {
				t.Errorf("implicit trust=%q", s.Trust)
			}
		})
	}
}
func TestVMStatistics13(t *testing.T) {
	t.Run("VM-STAT-13", func(t *testing.T) {
		path := vmFixtureLIE(t, t.TempDir())
		var values []string
		readParquet(t, path, func(r arrow.RecordBatch, i int) {
			a := arrowColumn(t, r, "amount")
			if a.IsNull(i) {
				values = append(values, "null")
			} else {
				values = append(values, a.ValueStr(i))
			}
		})
		if !slices.Equal(values, []string{"null", "-1", "11", "12"}) {
			t.Fatalf("independent lie payload=%v", values)
		}
		off := vmStatRun(t, path, vmStatConfig, "report", "--parquet-stats", "off")
		vmStatAssert(t, off, 1, 4, 2, 0, 2, 0, 1, 0, "POSITIVE", "NOT_NULL")
		r, p := vmStatDenied(t, path, vmStatConfig, "report", nil)
		s := vmStatAssert(t, r, 0, 0, 0, 0, 0, 4, 0, 1, "POSITIVE", "NOT_NULL")
		vmStatNoDenied(t, p)
		if s.Trust != "producer_statistics" || s.Ingestion != "not_evaluated" {
			t.Errorf("lying metadata not disclosed: %+v", s)
		}
		ev := vmEvidence(t, r)
		if len(ev) != 1 || ev[0]["action"] != "skip" {
			t.Errorf("lying evidence=%v", ev)
		}
	})
}
func TestVMStatistics14(t *testing.T) {
	t.Run("VM-STAT-14", func(t *testing.T) {
		path := vmFixtureLIE(t, t.TempDir())
		desc := vmInspectParquet(t, path)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		originalFooter := append([]byte(nil), body[desc.Footer.Start:desc.Footer.End]...)
		for _, chunk := range desc.Groups[0] {
			for i := chunk.Start; i < chunk.End; i++ {
				body[i] = 0xff
			}
		}
		if !bytes.Equal(originalFooter, body[desc.Footer.Start:desc.Footer.End]) {
			t.Fatal("fixture corrupted footer")
		}
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		r := vmStatRun(t, path, vmStatConfig, "report", "--parquet-stats", "off")
		if r.code != 2 || len(r.manifests) != 0 {
			t.Fatalf("ordinary corrupt payload exit=%d manifests=%v", r.code, r.manifests)
		}
		trusted, p := vmStatDenied(t, path, vmStatConfig, "report", nil)
		s := vmStatAssert(t, trusted, 0, 0, 0, 0, 0, 4, 0, 1, "POSITIVE", "NOT_NULL")
		vmStatNoDenied(t, p)
		if s.Ingestion != "not_evaluated" {
			t.Errorf("corruption falsely verified: %+v", s)
		}
	})
}
func TestVMStatistics15(t *testing.T) {
	for _, kind := range []string{"negative-file-rows", "negative-group-rows", "inconsistent-count", "offset-overflow", "length-overflow", "malformed-footer", "oversized-footer", "aggregate-overflow"} {
		t.Run("VM-STAT-15/"+kind, func(t *testing.T) {
			path := vmStatInt(t, "1", "2")
			if kind == "malformed-footer" || kind == "oversized-footer" {
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "oversized-footer" {
					binary.LittleEndian.PutUint32(body[len(body)-8:], math.MaxUint32)
				} else {
					n := int(binary.LittleEndian.Uint32(body[len(body)-8:]))
					for i := len(body) - 8 - n; i < len(body)-8; i++ {
						body[i] = 0xff
					}
				}
				if err := os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				vmRewriteFooter(t, path, func(m *metadata.FileMetaData) {
					switch kind {
					case "negative-file-rows":
						m.NumRows = -1
					case "negative-group-rows":
						m.RowGroups[0].NumRows = -1
					case "inconsistent-count":
						m.NumRows = 3
					case "offset-overflow":
						m.RowGroups[0].Columns[0].MetaData.DataPageOffset = math.MaxInt64
					case "length-overflow":
						m.RowGroups[0].Columns[0].MetaData.TotalCompressedSize = math.MaxInt64
					case "aggregate-overflow":
						m.NumRows = math.MaxInt64
						m.RowGroups[0].NumRows = math.MaxInt64
						m.RowGroups[0].Columns[0].MetaData.NumValues = math.MaxInt64
					}
				})
			}
			root := t.TempDir()
			p := defaultPipeline(path, "parquet", filepath.Join(root, "out"), "csv").with(t, vmStatConfig)
			if kind == "aggregate-overflow" {
				for i := range 2 {
					b, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					copy := filepath.Join(root, fmt.Sprintf("copy%d.parquet", i))
					if err := os.WriteFile(copy, b, 0600); err != nil {
						t.Fatal(err)
					}
					p.Source.URIs = append(p.Source.URIs, copy)
				}
			}
			r := runPipeline(t, root, p, "--output-mode", "report", "--parquet-stats", "trusted", "--json-output", "-")
			if r.code != 2 || len(r.manifests) != 0 {
				t.Fatalf("unsafe structural input exit=%d manifests=%v stdout=%s stderr=%s", r.code, r.manifests, r.stdout, r.stderr)
			}
		})
	}
}
