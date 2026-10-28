package config

import (
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"
)

// shorthandExamples are the example values used in shorthand error messages.
var shorthandExamples = map[string]string{
	"values":        "[...]",
	"columns":       "[...]",
	"pattern":       "'^[A-Z]+$'",
	"length":        "10",
	"min_length":    "1",
	"max_length":    "100",
	"value":         "'X'",
	"expected_type": "int64",
	"format":        "'%Y-%m-%d'",
}

// normalizeCheckMaps rewrites the map form of a rule's checks,
//
//	checks:
//	  not_null: true
//	  valid_values: [OPEN, PAID]
//	  valid_range: {min: 0, max: 10}
//
// into the list form every later step reads, in document order. Each value is
// the check's arguments as a mapping, the check's shorthand argument (a
// non-mapping value), or true / null / {} for a check without arguments. Line
// numbers are kept, so later errors still point at the document.
func normalizeCheckMaps(root *yaml.Node) error {
	rules := mappingValue(root, "rules")
	if rules == nil || rules.Kind != yaml.SequenceNode {
		return nil
	}
	for i, rule := range rules.Content {
		if rule.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j+1 < len(rule.Content); j += 2 {
			if rule.Content[j].Value != "checks" || rule.Content[j+1].Kind != yaml.MappingNode {
				continue
			}
			list, err := checkMapToList(i, rule.Content[j+1])
			if err != nil {
				return err
			}
			rule.Content[j+1] = list
		}
	}
	return nil
}

func checkMapToList(rule int, checks *yaml.Node) (*yaml.Node, error) {
	list := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Line: checks.Line, Column: checks.Column}
	for k := 0; k+1 < len(checks.Content); k += 2 {
		key, value := checks.Content[k], checks.Content[k+1]
		fail := func(format string, args ...any) error {
			return fmt.Errorf("rules[%d].checks.%s (line %d): %s", rule, key.Value, key.Line, fmt.Sprintf(format, args...))
		}
		check := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Line: key.Line, Column: key.Column}
		check.Content = append(check.Content, scalarAt(key, "type"), scalarAt(key, key.Value))
		spec, known := Checks[key.Value]
		switch {
		case value.Kind == yaml.MappingNode:
			if mappingValue(value, "type") != nil {
				return nil, fail("the check name is the key; remove type")
			}
			check.Content = append(check.Content, value.Content...)
		case !known:
			// Leave the arguments empty; validation reports the unknown check.
		case spec.Shorthand != "":
			if value.Tag == "!!null" {
				return nil, fail("give %s, for example %s: %s", spec.Shorthand, key.Value, shorthandExamples[spec.Shorthand])
			}
			check.Content = append(check.Content, scalarAt(key, spec.Shorthand), value)
		case value.Tag == "!!null" || (value.Tag == "!!bool" && value.Value == "true"):
			// A check without arguments.
		case value.Tag == "!!bool":
			return nil, fail("remove the check instead of setting it to false")
		default:
			return nil, fail("give the arguments as a mapping, for example %s: %s", key.Value, argumentExample(spec))
		}
		list.Content = append(list.Content, check)
	}
	return list, nil
}

// argumentExample shows the mapping form of a check's own arguments.
func argumentExample(spec CheckSpec) string {
	var names []string
	for _, p := range spec.Params {
		if p != "column" && p != "columns" {
			names = append(names, p+": ...")
		}
		if len(names) == 2 {
			break
		}
	}
	return "{" + strings.Join(names, ", ") + "}"
}

func scalarAt(at *yaml.Node, value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value, Line: at.Line, Column: at.Column}
}
