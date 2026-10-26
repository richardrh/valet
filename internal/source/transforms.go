package source

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/richardrh/valet/internal/config"
	"github.com/richardrh/valet/internal/diagnostics"
)

// renameMapping accumulates one transform's source -> target names.
type renameMapping struct {
	d       *Dataset
	exclude []string
	mapping map[string]string
}

func (m *renameMapping) add(from, to string) error {
	if to == "" || to == "#N/A" || slices.Contains(m.exclude, from) {
		return nil
	}
	if old, ok := m.mapping[from]; ok {
		if old != to {
			return fmt.Errorf("conflicting mapping for column %q", from)
		}
		return nil
	}
	if err := m.d.reserve(int64(len(from)+len(to)+renameEntryBytes), "rename mapping"); err != nil {
		return err
	}
	// Clone so the map does not pin the decoder's whole record string.
	m.mapping[strings.Clone(from)] = strings.Clone(to)
	return nil
}

func (d *Dataset) loadTransforms(ctx context.Context) error {
	for _, t := range d.cfg.Transforms {
		m := &renameMapping{d: d, exclude: t.ExcludeKeys, mapping: map[string]string{}}
		var err error
		switch t.Type {
		case "rename_columns":
			if t.Mapping != nil {
				for from, to := range t.Mapping {
					if err = m.add(from, to); err != nil {
						return err
					}
				}
			} else {
				err = d.loadMappingFile(ctx, t, m)
			}
		case "schema_rename":
			err = d.loadSchemaRename(ctx, t, m)
		default:
			err = fmt.Errorf("unsupported transform %q", t.Type)
		}
		if err != nil {
			return err
		}
		d.transforms = append(d.transforms, m.mapping)
	}
	return nil
}

// loadMappingFile reads a comma-separated, header-bearing CSV mapping file.
func (d *Dataset) loadMappingFile(ctx context.Context, t config.Transform, m *renameMapping) (err error) {
	obj, err := d.store.Stat(ctx, t.MappingFile)
	if err != nil {
		return err
	}
	r, err := openCSV(ctx, d, obj, csvOptions{comma: ','})
	if err != nil {
		return err
	}
	defer closeInto(&err, r)
	target := 1
	if t.TargetCol != nil {
		target = *t.TargetCol
	}
	if t.SourceCol < 0 || target < 0 || t.SourceCol >= len(r.header) || target >= len(r.header) {
		return errors.New("mapping column index outside CSV header")
	}
	for {
		rec, err := r.next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = m.add(rec.values[t.SourceCol].text, rec.values[target].text); err != nil {
			return err
		}
	}
}

// loadSchemaRename streams a {"<key>": {"from": "to", ...}, ...} document and
// applies the entries under t.SourceKey. The whole document is one record for
// max_record_bytes.
func (d *Dataset) loadSchemaRename(ctx context.Context, t config.Transform, m *renameMapping) (err error) {
	obj, err := d.store.Stat(ctx, t.SchemaFile)
	if err != nil {
		return err
	}
	src, err := openTextStream(ctx, d, obj, jsonDecoderFixedBytes, false)
	if err != nil {
		return err
	}
	defer closeInto(&err, src)
	// Charge for the largest document the limiter lets through: the object
	// itself when uncompressed and smaller than max_record_bytes.
	size := d.recordLimit
	if src.zipped == nil && obj.Size < size {
		size = obj.Size
	}
	workspace := highWater{budget: d.budget, phase: "schema rename parser"}
	defer workspace.release()
	if err = workspace.growTo(jsonRecordCopies * size); err != nil {
		return err
	}
	err = parseSchemaMapping(jsontext.NewDecoder(src.limiter), t.SourceKey, m.add)
	if src.limiter.exceeded || src.limiter.delivered > d.recordLimit {
		// The rename document is configuration, not dataset input.
		return diagnostics.PublicError(fmt.Errorf("transforms schema_file %s exceeds execution.max_record_bytes (%d); raise the limit or split the file", t.SchemaFile, d.recordLimit))
	}
	return err
}

func parseSchemaMapping(dec *jsontext.Decoder, key string, add func(string, string) error) error {
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return errors.New("schema rename file must be an object")
	}
	for dec.PeekKind() != '}' {
		tok, err := dec.ReadToken()
		if err != nil {
			return schemaRenameError(err, "invalid schema rename document")
		}
		name := tok.String()
		if tok, err = dec.ReadToken(); err != nil || tok.Kind() != '{' {
			return schemaRenameError(err, "schema rename entries must be string mappings")
		}
		for dec.PeekKind() != '}' {
			if tok, err = dec.ReadToken(); err != nil {
				return schemaRenameError(err, "invalid schema rename mapping")
			}
			from := tok.String()
			if tok, err = dec.ReadToken(); err != nil || tok.Kind() != '"' {
				return schemaRenameError(err, "schema rename targets must be strings")
			}
			if name == key {
				if err = add(from, tok.String()); err != nil {
					return err
				}
			}
		}
		if _, err = dec.ReadToken(); err != nil {
			return schemaRenameError(err, "invalid schema rename mapping")
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return schemaRenameError(err, "invalid schema rename document")
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		return errors.New("trailing schema rename data")
	}
	return nil
}

// schemaRenameError keeps duplicate names distinguishable from other errors.
func schemaRenameError(err error, message string) error {
	if errors.Is(err, jsontext.ErrDuplicateName) {
		return errors.New("duplicate schema rename key")
	}
	return errors.New(message)
}
