package source

//lint:file-ignore ST1005 Error strings here begin with the proper nouns Parquet or Arrow.

import (
	"errors"
	"math"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/thrift/lib/go/thrift"
)

// Thrift field IDs from parquet.thrift.
const (
	fileMetaSchemaField   = 2 // FileMetaData.schema
	fileMetaKeyValueField = 5 // FileMetaData.key_value_metadata

	schemaTypeField        = 1  // SchemaElement.type
	schemaTypeLengthField  = 2  // SchemaElement.type_length
	schemaRepetitionField  = 3  // SchemaElement.repetition_type
	schemaNumChildrenField = 5  // SchemaElement.num_children
	schemaLogicalTypeField = 10 // SchemaElement.logicalType
	schemaFieldCount       = 11 // one past the highest SchemaElement field ID tracked for duplicates
)

// Parquet repetition enum values accepted for flat leaves.
const (
	repetitionRequired = 0
	repetitionOptional = 1
)

func validateSchemaFooter(raw []byte) error {
	c := newRawCompactReader(raw)
	seenSchema := false
	return c.fields(func(id int, kind thrift.TType) error {
		if id != fileMetaSchemaField {
			return c.skip(kind, 1)
		}
		if seenSchema {
			return errors.New("duplicate Parquet schema metadata")
		}
		seenSchema = true
		if kind != thrift.LIST {
			return errors.New("Parquet schema must be a list")
		}
		elem, n, e := c.ReadListBegin(c.ctx)
		if e != nil {
			return e
		}
		if elem != thrift.STRUCT || n == 0 {
			return errors.New("invalid Parquet schema element count")
		}
		count := uint64(n)
		for i := range count {
			physical, children, repetition, width := int64(-1), int64(0), int64(-1), int64(0)
			hasType := false
			seen := [schemaFieldCount]bool{}
			e = c.fields(func(field int, k thrift.TType) error {
				if field < len(seen) {
					if seen[field] {
						return errors.New("duplicate Parquet schema field")
					}
					seen[field] = true
				}
				var e error
				switch field {
				case schemaTypeField:
					hasType = true
					physical, e = c.integer(k)
				case schemaTypeLengthField:
					width, e = c.integer(k)
				case schemaRepetitionField:
					repetition, e = c.integer(k)
				case schemaNumChildrenField:
					children, e = c.integer(k)
				case schemaLogicalTypeField:
					e = c.logicalType(k)
				default:
					e = c.skip(k, 2)
				}
				return e
			})
			if e != nil {
				return e
			}
			if children < 0 || uint64(children) > count-i-1 {
				return errors.New("Parquet schema child count exceeds remaining elements")
			}
			if i == 0 {
				if hasType || uint64(children) != count-1 {
					return errors.New("invalid Parquet root child count")
				}
				continue
			}
			if children != 0 || !hasType {
				return errors.New("nested Parquet schema is unsupported")
			}
			if physical < int64(parquet.Types.Boolean) || physical > int64(parquet.Types.FixedLenByteArray) {
				return errors.New("invalid Parquet physical type")
			}
			if repetition != repetitionRequired && repetition != repetitionOptional {
				return errors.New("repeated or invalid Parquet leaf is unsupported")
			}
			if physical == int64(parquet.Types.FixedLenByteArray) && (width <= 0 || width > math.MaxInt32) {
				return errors.New("invalid Parquet fixed binary width")
			}
		}
		return c.ReadListEnd(c.ctx)
	})
}
