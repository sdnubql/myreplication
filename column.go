package myreplication

import (
	"strings"
)

type Column struct {
	Type       uint16
	Name       string
	Collation  string
	Charset    string
	Comment    string
	Unsigned   bool
	IsBool     bool
	IsPrimary  bool
	MaxLen     uint16
	LenSize    uint8
	Precision  uint8
	Decimals   uint8
	Size       uint8
	Bits       uint8
	Bytes      int
	Fsp        uint8
	EnumValues []string
	SetValues  []string
}

type SchemaColumn struct {
	COLUMN_NAME        string
	COLLATION_NAME     string
	CHARACTER_SET_NAME string
	COLUMN_COMMENT     string
	COLUMN_TYPE        string
	COLUMN_KEY         string
}

func newColumn(pack *pack, colType uint16, column *SchemaColumn) (*Column, error) {
	this := &Column{}

	this.Type = colType
	this.Name = column.COLUMN_NAME
	this.Collation = column.COLLATION_NAME
	this.Charset = column.CHARACTER_SET_NAME
	this.Comment = column.COLUMN_COMMENT

	if column.COLUMN_KEY == "PRI" {
		this.IsPrimary = true
	}

	if strings.Contains(column.COLUMN_TYPE, `unsigned`) {
		this.Unsigned = true
	}

	var err error
	switch this.Type {
	case MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
		this.readStringMetaData(pack, column)
	case MYSQL_TYPE_VARCHAR:
		if err = pack.readUint16(&this.MaxLen); err != nil {
			return nil, err
		}
	case MYSQL_TYPE_BLOB, MYSQL_TYPE_GEOMETRY:
		if err = pack.readUint8(&this.LenSize); err != nil {
			return nil, err
		}
	case MYSQL_TYPE_NEWDECIMAL:
		if err = pack.readUint8(&this.Precision); err != nil {
			return nil, err
		}
		if err = pack.readUint8(&this.Decimals); err != nil {
			return nil, err
		}
	case MYSQL_TYPE_DOUBLE, MYSQL_TYPE_FLOAT:
		if err = pack.readUint8(&this.Size); err != nil {
			return nil, err
		}
	case MYSQL_TYPE_BIT:
		var bits, bytes uint8
		if err = pack.readUint8(&bits); err != nil {
			return nil, err
		}
		if err = pack.readUint8(&bytes); err != nil {
			return nil, err
		}
		this.Bits = bits*8 + bits
		this.Bytes = int((bytes + 7) / 8)
	case MYSQL_TYPE_TIMESTAMP2, MYSQL_TYPE_DATETIME2, MYSQL_TYPE_TIME2:
		if err = pack.readUint8(&this.Fsp); err != nil {
			return nil, err
		}
	case MYSQL_TYPE_TINY:
		if column.COLUMN_TYPE == "tinyint(1)" {
			this.IsBool = true
		}
	}

	return this, nil
}

func (c *Column) readStringMetaData(pack *pack, column *SchemaColumn) error {
	var b1, b2 uint8
	var err error
	if err = pack.readUint8(&b1); err != nil {
		return err
	}

	if err = pack.readUint8(&b2); err != nil {
		return err
	}

	meta := uint16(b1<<8) + uint16(b2)
	real_type := meta >> 8
	switch real_type {
	case MYSQL_TYPE_ENUM, MYSQL_TYPE_SET:
		c.Type = real_type
		c.Size = uint8(meta & 0x00ff)
		c.readEnumData(column)
	default:
		c.MaxLen = (((meta >> 4) & 0x300) ^ 0x300) + (meta & 0x00ff)
	}

	return nil
}

func (c *Column) readEnumData(column *SchemaColumn) {
	enum := column.COLUMN_TYPE
	if c.Type == MYSQL_TYPE_ENUM {
		c.EnumValues = strings.Split(
			strings.Replace(
				strings.Replace(
					strings.Replace(enum, "enum(", "", -1), ")", "", -1), "'", "", -1),
			",")
	} else if c.Type == MYSQL_TYPE_SET {
		c.SetValues = strings.Split(
			strings.Replace(
				strings.Replace(
					strings.Replace(enum, "set(", "", -1), ")", "", -1), "'", "", -1),
			",")
	}
}
