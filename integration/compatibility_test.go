//go:build integration

package integration_test

import (
	"path/filepath"
	"slices"
	"testing"
)

// These failure ordinals are hand-authored from the documented violation
// predicates (docs/content/spec.md section 9). Every check is an assertion, so equals
// flags rows that differ from the value or are null.
func TestE2E22BuiltinCompatibility(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		rule     string // rule body; the test adds code CHECK and level error
		failed   []uint64
		wantExit int
	}{
		{
			name:     "not_null",
			input:    joinLines("x", "A", "NULL"),
			rule:     "{column: x, checks: [{type: not_null}]}",
			failed:   []uint64{2},
			wantExit: exitValidationFailed,
		},
		{
			name:     "valid_length",
			input:    joinLines("x", "界λ", "a", "NULL"),
			rule:     "{column: x, checks: [{type: valid_length, length: 2}]}",
			failed:   []uint64{2, 3},
			wantExit: exitValidationFailed,
		},
		{
			name:     "min_length",
			input:    joinLines("x", "界λ", "a", "NULL"),
			rule:     "{column: x, checks: [{type: min_length, min_length: 2}]}",
			failed:   []uint64{2, 3},
			wantExit: exitValidationFailed,
		},
		{
			name:     "max_length",
			input:    joinLines("x", "界λ", "a", "NULL"),
			rule:     "{column: x, checks: [{type: max_length, max_length: 1}]}",
			failed:   []uint64{1, 3},
			wantExit: exitValidationFailed,
		},
		{
			name:     "valid_range",
			input:    joinLines("x", "0", "10", "-1", "11", "NaN", "Inf", "NULL"),
			rule:     "{column: x, checks: [{type: valid_range, min: 0, max: 10}]}",
			failed:   []uint64{3, 4, 5, 6},
			wantExit: exitValidationFailed,
		},
		{
			name:     "equals",
			input:    joinLines("x", "a", "b", "NULL"),
			rule:     "{column: x, checks: [{type: equals, value: a}]}",
			failed:   []uint64{2, 3},
			wantExit: exitValidationFailed,
		},
		{
			name:     "not_equals",
			input:    joinLines("x", "a", "b", "NULL"),
			rule:     "{column: x, checks: [{type: not_equals, value: a}]}",
			failed:   []uint64{1},
			wantExit: exitValidationFailed,
		},
		{
			name:     "all_equal",
			input:    joinLines("x,y", "a,a", "a,b", "a,NULL"),
			rule:     "{checks: [{type: all_equal, columns: [x,y], value: a}]}",
			failed:   []uint64{2, 3},
			wantExit: exitValidationFailed,
		},
		{
			name:     "has_null_keys",
			input:    joinLines("x,y", "a,b", "NULL,b", "a,NULL"),
			rule:     "{checks: [{type: has_null_keys, columns: [x,y]}]}",
			failed:   []uint64{2, 3},
			wantExit: exitValidationFailed,
		},
		{
			name:     "valid_values",
			input:    joinLines("x", "a", "b", "NULL"),
			rule:     "{column: x, checks: [{type: valid_values, values: [a]}]}",
			failed:   []uint64{2},
			wantExit: exitValidationFailed,
		},
		{
			name:     "matches_pattern",
			input:    joinLines("x", "xxabcxx", "ABC", "NULL"),
			rule:     "{column: x, checks: [{type: matches_pattern, pattern: abc}]}",
			failed:   []uint64{2, 3},
			wantExit: exitValidationFailed,
		},
		{
			name:     "is_unique",
			input:    joinLines("x", "a", "b", "a", "NULL"),
			rule:     "{column: x, checks: [{type: is_unique}]}",
			failed:   []uint64{1, 3, 4},
			wantExit: exitValidationFailed,
		},
		{
			name:     "is_unique_composite",
			input:    joinLines("x,y", "a,b", "b,a", "a,b", "NULL,b"),
			rule:     "{checks: [{type: is_unique_composite, columns: [x,y]}]}",
			failed:   []uint64{1, 3, 4},
			wantExit: exitValidationFailed,
		},
		{
			name:     "compare_columns",
			input:    joinLines("x,y", "1,2", "2,2", "3,2", "NULL,2"),
			rule:     "{column: x, checks: [{type: compare_columns, other_column: y, operator: '<='}]}",
			failed:   []uint64{3, 4},
			wantExit: exitValidationFailed,
		},
		{
			name:     "type_check",
			input:    joinLines("x", "127", "128", "1.5", "NULL"),
			rule:     "{column: x, checks: [{type: type_check, expected_type: int8}]}",
			failed:   []uint64{2, 3},
			wantExit: exitValidationFailed,
		},
		{
			name:     "date_format",
			input:    joinLines("x", "2024-02-29", "2023-02-29", "NULL"),
			rule:     "{column: x, checks: [{type: date_format, format: '%Y-%m-%d'}]}",
			failed:   []uint64{2},
			wantExit: exitValidationFailed,
		},
		{
			name:     "not_blank",
			input:    joinLines("x,y", "text,z", ",z", "\u2003,z", "NULL,z"),
			rule:     "{column: x, checks: [{type: not_blank}]}",
			failed:   []uint64{2, 3, 4},
			wantExit: exitValidationFailed,
		},
		{
			name:     "is_present",
			input:    joinLines("x", "a", "NULL"),
			rule:     "{column: x, checks: [{type: is_present}]}",
			wantExit: exitOK,
		},
		{
			name:  "timestamp_window",
			input: joinLines("x", "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "NULL"),
			rule: "{column: x, checks: [{type: timestamp_window, " +
				"min: '2024-01-01T00:00:00Z', max: '2024-01-01T00:00:00Z'}]}",
			failed:   []uint64{2},
			wantExit: exitValidationFailed,
		},
		{
			name:     "required_columns",
			input:    joinLines("x", "a"),
			rule:     "{checks: [{type: required_columns, columns: [x]}]}",
			wantExit: exitOK,
		},
		{
			name:     "schema_matches",
			input:    joinLines("x", "1", "2"),
			rule:     "{checks: [{type: schema_matches, columns: {x: int64}, mode: castable}]}",
			wantExit: exitOK,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rule, ok := yamlValue(t, c.rule).(obj)
			if !ok {
				t.Fatalf("rule %q is not a mapping", c.rule)
			}
			rule["code"], rule["level"] = "CHECK", "error"
			r := run(t, scenario{
				input:  c.input,
				config: []string{"input: {null_values: ['NULL']}", fragment(t, obj{"rules": []obj{rule}})},
			})
			if r.code != c.wantExit {
				t.Fatalf("validate %s exit = %d, want %d; stderr=%s", c.name, r.code, c.wantExit, r.stderr)
			}
			if got, want := issuesOf(r), checkIssues(c.failed); !slices.Equal(got, want) {
				t.Errorf("validate %s issues = %v, want %v", c.name, got, want)
			}
		})
	}

	referenceCases := []struct {
		name   string
		check  string
		failed []uint64
	}{
		{
			name:   "referential_integrity",
			check:  "{type: referential_integrity, column: x, reference_file: ref.csv, reference_column: key}",
			failed: []uint64{2},
		},
		{
			name: "referential_integrity_composite",
			check: "{type: referential_integrity_composite, columns: [x,y], " +
				"reference_file: ref.csv, reference_columns: [key,second]}",
			failed: []uint64{2},
		},
		{
			name:   "valid_schema_columns",
			check:  "{type: valid_schema_columns, reference_file: ref.csv, reference_column: key}",
			failed: []uint64{1, 2},
		},
	}
	for _, c := range referenceCases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			put(t, filepath.Join(root, "ref.csv"), joinLines("key,second", "a,1", "x,2"))
			input := put(t, filepath.Join(root, "input.csv"), joinLines("x,y", "a,1", "b,2"))
			rule := obj{"code": "CHECK", "level": "warn", "checks": []any{yamlValue(t, c.check)}}
			rules := fragment(t, obj{"rules": []obj{rule}})
			r := run(t, scenario{inputFile: input, config: []string{rules}})
			if r.code != exitOK {
				t.Fatalf("validate %s reference exit = %d, want %d; stderr=%s", c.name, r.code, exitOK, r.stderr)
			}
			if got, want := issuesOf(r), checkIssues(c.failed); !slices.Equal(got, want) {
				t.Errorf("validate %s reference issues = %v, want %v", c.name, got, want)
			}
		})
	}

	t.Run("OR produces one issue", func(t *testing.T) {
		r := run(t, scenario{
			input: joinLines("x", "NULL"),
			config: []string{
				"input: {null_values: ['NULL']}",
				"rules: [{code: CHECK, level: error, column: x, checks: [{type: not_null}, {type: min_length, min_length: 1}]}]",
			},
		})
		if r.code != exitValidationFailed {
			t.Fatalf("validate OR rule exit = %d, want %d; stderr=%s", r.code, exitValidationFailed, r.stderr)
		}
		if got, want := issuesOf(r), checkIssues([]uint64{1}); !slices.Equal(got, want) {
			t.Errorf("validate OR rule issues = %v, want %v", got, want)
		}
	})
}

// validnator's NULL_ISLAND rule (internal/config/testdata/legacy/
// shared_oed_rules.yaml) used equals to flag coordinates that are 0. Under
// valet's assertion semantics equals flags rows that differ, so the rule must
// be rewritten with not_equals (docs/content/spec.md, deliberate migration changes).
func TestE2E22MigrationNullIsland(t *testing.T) {
	const coordinates = `Latitude,Longitude
0,0
51.5,-0.12
,
`
	nullIsland := func(check string) string {
		return `rules:
  - code: NULL_ISLAND
    name: Null Island Coordinates
    checks:
      - type: ` + check + `
        columns:
          - Latitude
          - Longitude
        value: 0
    level: warn
    message: "Lat or Lon is 0 — likely a geocoding failure (null island)"
`
	}
	cases := []struct {
		name  string
		check string
		want  []issueAt
		count counts
	}{
		{
			name:  "rule copied verbatim flags every row except null island",
			check: "equals",
			want:  []issueAt{{2, "NULL_ISLAND"}, {3, "NULL_ISLAND"}},
			count: counts{total: 3, pass: 1, warn: 2},
		},
		{
			name:  "rule rewritten with not_equals flags only null island",
			check: "not_equals",
			want:  []issueAt{{1, "NULL_ISLAND"}},
			count: counts{total: 3, pass: 2, warn: 1},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := run(t, scenario{input: coordinates, config: []string{nullIsland(c.check)}})
			if r.code != exitOK {
				t.Fatalf("validate null-island %s exit = %d, want %d; stderr=%s", c.name, r.code, exitOK, r.stderr)
			}
			if got := issuesOf(r); !slices.Equal(got, c.want) {
				t.Errorf("validate null-island %s issues = %v, want %v", c.name, got, c.want)
			}
			if got := countsOf(r); got != c.count {
				t.Errorf("validate null-island %s counts = %+v, want %+v", c.name, got, c.count)
			}
		})
	}
}
