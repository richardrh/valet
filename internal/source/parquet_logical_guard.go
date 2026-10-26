package source

import (
	"errors"

	"github.com/apache/thrift/lib/go/thrift"
)

// LogicalType union member IDs from parquet.thrift.
const (
	logicalString    = 1
	logicalEnum      = 4
	logicalDecimal   = 5
	logicalDate      = 6
	logicalTime      = 7
	logicalTimestamp = 8
	logicalInteger   = 10
	logicalUnknown   = 11
	logicalJSON      = 12
	logicalBSON      = 13
	logicalUUID      = 14
)

// Field IDs inside individual logical annotations.
const (
	decimalScaleField     = 1 // DecimalType.scale
	decimalPrecisionField = 2 // DecimalType.precision
	temporalUTCField      = 1 // TimeType/TimestampType.isAdjustedToUTC
	temporalUnitField     = 2 // TimeType/TimestampType.unit
	timeUnitMillis        = 1 // TimeUnit.MILLIS
	timeUnitNanos         = 3 // TimeUnit.NANOS
	intBitWidthField      = 1 // IntType.bitWidth
	intSignedField        = 2 // IntType.isSigned
	maxDecimalPrecision   = 38
)

func (c *compactReader) logicalType(kind thrift.TType) error {
	if kind != thrift.STRUCT {
		return errors.New("logicalType must be a struct")
	}
	members := 0
	err := c.fields(func(id int, k thrift.TType) error {
		members++
		if members != 1 || k != thrift.STRUCT {
			return errors.New("logicalType must contain exactly one annotation")
		}
		switch id {
		case logicalString, logicalEnum, logicalDate, logicalUnknown, logicalJSON, logicalBSON, logicalUUID:
			return c.fields(func(_ int, k thrift.TType) error { return c.skip(k, 3) })
		case logicalDecimal:
			return c.decimalAnnotation()
		case logicalTime, logicalTimestamp:
			return c.temporalAnnotation()
		case logicalInteger:
			return c.integerAnnotation()
		default:
			return errors.New("unsupported nested or extended Parquet logical annotation")
		}
	})
	if err != nil {
		return err
	}
	if members != 1 {
		return errors.New("empty Parquet logicalType union")
	}
	return nil
}
func (c *compactReader) decimalAnnotation() error {
	scale, precision := int64(-1), int64(-1)
	seenScale, seenPrecision := false, false
	e := c.fields(func(field int, k thrift.TType) error {
		var target *int64
		var seen *bool
		switch field {
		case decimalScaleField:
			target, seen = &scale, &seenScale
		case decimalPrecisionField:
			target, seen = &precision, &seenPrecision
		default:
			return c.skip(k, 3)
		}
		if *seen || k != thrift.I32 {
			return errors.New("invalid decimal annotation field")
		}
		*seen = true
		v, e := c.integer(k)
		*target = v
		return e
	})
	if e != nil {
		return e
	}
	if scale < 0 || precision < 1 || precision > maxDecimalPrecision || scale > precision {
		return errors.New("unsupported decimal precision or scale")
	}
	return nil
}
func (c *compactReader) temporalAnnotation() error {
	seenUTC, seenUnit := false, false
	e := c.fields(func(field int, k thrift.TType) error {
		switch field {
		case temporalUTCField:
			if seenUTC || k != thrift.BOOL {
				return errors.New("invalid temporal UTC annotation")
			}
			seenUTC = true
			_, e := c.boolean(k)
			return e
		case temporalUnitField:
			if seenUnit || k != thrift.STRUCT {
				return errors.New("invalid temporal unit annotation")
			}
			seenUnit = true
			return c.timeUnit()
		default:
			return c.skip(k, 3)
		}
	})
	if e != nil {
		return e
	}
	if !seenUTC || !seenUnit {
		return errors.New("temporal annotation lacks required fields")
	}
	return nil
}
func (c *compactReader) timeUnit() error {
	units := 0
	e := c.fields(func(unit int, k thrift.TType) error {
		units++
		if units != 1 || unit < timeUnitMillis || unit > timeUnitNanos || k != thrift.STRUCT {
			return errors.New("time unit must contain one of MILLIS, MICROS, NANOS")
		}
		return c.fields(func(_ int, k thrift.TType) error { return c.skip(k, 4) })
	})
	if e != nil {
		return e
	}
	if units != 1 {
		return errors.New("empty temporal time unit")
	}
	return nil
}
func (c *compactReader) integerAnnotation() error {
	seenWidth, seenSigned := false, false
	e := c.fields(func(field int, k thrift.TType) error {
		switch field {
		case intBitWidthField:
			if seenWidth || k != thrift.BYTE {
				return errors.New("invalid integer bit-width annotation")
			}
			seenWidth = true
			width, e := c.ReadByte(c.ctx)
			if e != nil {
				return e
			}
			if width != 8 && width != 16 && width != 32 && width != 64 {
				return errors.New("unsupported integer annotation width")
			}
			return nil
		case intSignedField:
			if seenSigned || k != thrift.BOOL {
				return errors.New("invalid integer signedness annotation")
			}
			seenSigned = true
			_, e := c.boolean(k)
			return e
		default:
			return c.skip(k, 3)
		}
	})
	if e != nil {
		return e
	}
	if !seenWidth || !seenSigned {
		return errors.New("integer annotation lacks required fields")
	}
	return nil
}
