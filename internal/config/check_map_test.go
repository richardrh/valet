package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const mapFormBase = "source: {uris: [data.csv]}\ninput: {type: csv}\nrules:\n  - code: R\n    level: error\n    column: c\n    checks:\n"

// The map form is another spelling of the list form: the same document must
// load into the same checks, in document order, with exact numbers kept.
func TestCheckMapFormMatchesListForm(t *testing.T) {
	for _, tc := range []struct{ name, mapForm, listForm string }{
		{"no-argument check", "      not_null: true\n", "      - type: not_null\n"},
		{"empty mapping", "      is_unique: {}\n", "      - type: is_unique\n"},
		{"null value", "      not_blank:\n", "      - type: not_blank\n"},
		{"arguments", "      valid_range: {min: 0, max: 10000, exclusive_max: true}\n", "      - {type: valid_range, min: 0, max: 10000, exclusive_max: true}\n"},
		{"exact numbers", "      valid_range: {min: 9007199254740993.01}\n", "      - {type: valid_range, min: 9007199254740993.01}\n"},
		{"list shorthand", "      valid_values: [OPEN, PAID]\n", "      - {type: valid_values, values: [OPEN, PAID]}\n"},
		{"scalar shorthand", "      matches_pattern: '^\\d+$'\n", "      - {type: matches_pattern, pattern: '^\\d+$'}\n"},
		{"boolean shorthand", "      equals: true\n", "      - {type: equals, value: true}\n"},
		{"numeric shorthand", "      min_length: 3\n", "      - {type: min_length, min_length: 3}\n"},
		{
			"several checks keep document order",
			"      not_null: true\n      valid_values: [A, B]\n      max_length: 5\n",
			"      - type: not_null\n      - {type: valid_values, values: [A, B]}\n      - {type: max_length, max_length: 5}\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fromMap := loadValid(t, mapFormBase+tc.mapForm)
			fromList := loadValid(t, mapFormBase+tc.listForm)
			if !reflect.DeepEqual(fromMap.Rules, fromList.Rules) {
				t.Errorf("map form rules = %#v\nlist form rules = %#v", fromMap.Rules[0].Checks, fromList.Rules[0].Checks)
			}
		})
	}
}

func TestCheckMapFormInJSON(t *testing.T) {
	const head = `{"source": {"uris": ["data.csv"]}, "input": {"type": "csv"}, "rules": [{"code": "R", "level": "error", "column": "c", "checks": `
	load := func(checks string) *Config {
		cfg, err := loadNamedText(t, "pipeline.json", head+checks+`}]}`)
		if err == nil {
			err = cfg.Validate()
		}
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	fromMap := load(`{"not_null": true, "valid_values": ["A"], "valid_range": {"min": 9007199254740993.5}}`)
	fromList := load(`[{"type": "not_null"}, {"type": "valid_values", "values": ["A"]}, {"type": "valid_range", "min": 9007199254740993.5}]`)
	if !reflect.DeepEqual(fromMap.Rules, fromList.Rules) {
		t.Errorf("map form checks = %#v\nlist form checks = %#v", fromMap.Rules[0].Checks, fromList.Rules[0].Checks)
	}
	if got := fromMap.Rules[0].Checks[2].Params["min"]; got != json.Number("9007199254740993.5") {
		t.Errorf("min = %#v, want the exact number 9007199254740993.5", got)
	}
}

func TestCheckMapFormErrors(t *testing.T) {
	for _, tc := range []struct{ name, checks, want string }{
		{"false", "      not_null: false\n", "checks.not_null (line 8): remove the check instead of setting it to false"},
		{"no shorthand", "      valid_range: 5\n", "checks.valid_range (line 8): give the arguments as a mapping, for example valid_range: {min: ..., max: ...}"},
		{"missing shorthand value", "      valid_values:\n", "checks.valid_values (line 8): give values, for example valid_values: [...]"},
		{"type inside", "      valid_values: {type: not_null, values: [A]}\n", "checks.valid_values (line 8): the check name is the key; remove type"},
		{"unknown check", "      not_nul: true\n", `unknown check "not_nul"`},
		{"unknown argument", "      valid_range: {mni: 1}\n", `valid_range: unknown argument "mni"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadAndValidate(t, mapFormBase+tc.checks)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}
