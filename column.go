package myreplication

type Column struct {
	Type      int
	Name      string
	Collation string
	Charset   string
	Comment   string
	Unsigned  bool
	IsBool    bool
	IsPrimary bool
	MaxLen    int
	LenSize   uint8
	Precision uint8
	Decimals  uint8
	Size      uint8
	Bits      uint8
	Bytes     int
	Fsp       uint8
}

type SchemaColumn struct {
	COLUMN_NAME        string
	COLLATION_NAME     string
	CHARACTER_SET_NAME string
	COLUMN_COMMENT     string
	COLUMN_TYPE        string
	COLUMN_KEY         string
}

func newColumn(pack *pack, colType int, column *SchemaColumn) (*Column, error) {
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

	switch this.Type {
	case MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
		this.readStringMeta(pack, column)
	case MYSQL_TYPE_VARCHAR:
		if err = pack.readUint16(&this.MaxLength); err != nil {
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
}

func (c *Column) readStringMetaData(pack *pack, column *SchemaColumn) {

}
