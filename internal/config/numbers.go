package config

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"
)

// The generic node decoder uses float64. Restore floating numeric literals from
// the syntax tree before any comparison can lose precision. json.Number keeps
// numbers distinct from strings and round-trips through rules metadata exactly.
func preserveNumbers(root *yaml.Node, c *Config) error {
	if n := mappingValue(root, "context"); n != nil {
		if _, err := exactValue(n, c.Context); err != nil {
			return fmt.Errorf("context: %w", err)
		}
	}
	if rules := mappingValue(root, "rules"); rules != nil {
		for i, n := range rules.Content {
			if checks := mappingValue(n, "checks"); checks != nil {
				for j, check := range checks.Content {
					params := c.Rules[i].Checks[j].Params
					for k := 0; k < len(check.Content); k += 2 {
						key := check.Content[k].Value
						if key == "type" {
							continue
						}
						value, err := exactValue(check.Content[k+1], params[key])
						if err != nil {
							return fmt.Errorf("rule %s argument %s: %w", c.Rules[i].Code, key, err)
						}
						params[key] = value
					}
				}
			}
			if when := mappingValue(n, "when"); when != nil {
				if err := conditionNumbers(when, c.Rules[i].When); err != nil {
					return fmt.Errorf("rule %s when: %w", c.Rules[i].Code, err)
				}
			}
		}
	}
	if rules := mappingValue(root, "dataset_rules"); rules != nil {
		for i, n := range rules.Content {
			r := &c.DatasetRules[i]
			for _, entry := range []struct {
				key    string
				target *any
			}{{"min", &r.Min}, {"max", &r.Max}, {"equals", &r.Equals}} {
				if node := mappingValue(n, entry.key); node != nil {
					v, err := exactValue(node, *entry.target)
					if err != nil {
						return fmt.Errorf("dataset rule %s %s: %w", r.Code, entry.key, err)
					}
					*entry.target = v
				}
			}
		}
	}
	return nil
}

func conditionNumbers(n *yaml.Node, c *Condition) error {
	if c == nil {
		return nil
	}
	if value := mappingValue(n, "value"); value != nil {
		v, err := exactValue(value, c.Value)
		if err != nil {
			return err
		}
		c.Value = v
	}
	for _, entry := range []struct {
		key      string
		children []Condition
	}{{"all", c.All}, {"any", c.Any}} {
		if children := mappingValue(n, entry.key); children != nil {
			for i, child := range children.Content {
				if err := conditionNumbers(child, &entry.children[i]); err != nil {
					return err
				}
			}
		}
	}
	if child := mappingValue(n, "not"); child != nil {
		return conditionNumbers(child, c.Not)
	}
	return nil
}

func exactValue(n *yaml.Node, original any) (any, error) {
	if n.Tag == "!!float" {
		text := strings.ReplaceAll(n.Value, "_", "")
		if _, ok := decimalNumber(text); !ok {
			return nil, fmt.Errorf("numeric literal must be finite decimal with at most 1024 characters and exponent magnitude at most 1000 (line %d)", n.Line)
		}
		sign := ""
		if strings.HasPrefix(text, "-") {
			sign = "-"
			text = text[1:]
		} else if strings.HasPrefix(text, "+") {
			text = text[1:]
		}
		exponent := ""
		if at := strings.IndexAny(text, "eE"); at >= 0 {
			exponent = text[at:]
			text = text[:at]
		}
		whole, fraction, point := strings.Cut(text, ".")
		whole = strings.TrimLeft(whole, "0")
		if whole == "" {
			whole = "0"
		}
		text = sign + whole
		if point {
			if fraction == "" {
				fraction = "0"
			}
			text += "." + fraction
		}
		return json.Number(text + exponent), nil
	}
	switch v := original.(type) {
	case map[string]any:
		for i := 0; i < len(n.Content); i += 2 {
			key := n.Content[i].Value
			value, err := exactValue(n.Content[i+1], v[key])
			if err != nil {
				return nil, err
			}
			v[key] = value
		}
	case []any:
		for i, child := range n.Content {
			value, err := exactValue(child, v[i])
			if err != nil {
				return nil, err
			}
			v[i] = value
		}
	}
	return original, nil
}
