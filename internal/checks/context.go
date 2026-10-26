package checks

import (
	"fmt"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/source"
)

// columnContext is the comparison domain declared for a schema-less column by
// type_check, date_format or schema_matches.
type columnContext struct {
	typ string
	// conflict marks incompatible declarations; the column then compares in
	// neither the numeric nor the temporal domain.
	conflict bool
}

// contextType returns the declared type ("" when conflicting) and whether the
// column has any declaration.
func (c *compiler) contextType(name string) (string, bool) {
	ctx, ok := c.types[name]
	if ctx.conflict {
		return "", ok
	}
	return ctx.typ, ok
}

func (c *compiler) deriveContexts() error {
	c.types = make(map[string]columnContext)
	c.dateFormats = make(map[string][]string)
	record := func(name, typ string) {
		typ = typeName(typ)
		old, ok := c.types[name]
		switch {
		case !ok || (!old.conflict && old.typ == typ):
			c.types[name] = columnContext{typ: typ}
		case old.conflict:
		case !isNumericType(old.typ) || !isNumericType(typ):
			c.types[name] = columnContext{conflict: true}
		}
	}
	for _, rule := range c.cfg.Rules {
		for _, check := range rule.Checks {
			if check.Type == "schema_matches" {
				switch cols := check.Params["columns"].(type) {
				case map[string]any:
					for name, typ := range cols {
						if t, ok := typ.(string); ok {
							record(name, t)
						}
					}
				case map[string]string:
					for name, t := range cols {
						record(name, t)
					}
				}
				continue
			}
			if check.Type != "type_check" && check.Type != "date_format" {
				continue
			}
			names, err := operandNames(check.Params, rule.Column)
			if err != nil {
				return fmt.Errorf("rule %s: %w", rule.Code, err)
			}
			var layouts []string
			if check.Type == "date_format" {
				layouts, err = dateLayoutsParam(check.Params)
				if err != nil {
					return fmt.Errorf("rule %s: %w", rule.Code, err)
				}
			}
			for _, name := range names {
				if check.Type == "type_check" {
					record(name, str(check.Params, "expected_type"))
					continue
				}
				record(name, "date")
				// The immutable layout slice is shared by every operand. The
				// last date_format declaration supplies compare_columns layouts.
				c.dateFormats[name] = layouts
			}
		}
	}
	return nil
}

// timestampInZone reads an instant; text without an explicit offset is local
// to zone and must be unambiguous.
func timestampInZone(v scalar, zone string) (scalar, bool) {
	if v.kind == valTime {
		return v, true
	}
	if v.kind != valString {
		return scalar{}, false
	}
	t, err := source.ParseTimestamp(v.s, config.Column{Timezone: zone})
	if err != nil {
		return scalar{}, false
	}
	return timeScalar(t), true
}
