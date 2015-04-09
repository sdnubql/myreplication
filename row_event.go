package myreplication

import (
	"math"
	"strings"
)

type rowsEvent struct {
	*eventLogHeader
	tableMapEvent    *TableMapEvent
	postHeaderLength byte

	tableId   uint64
	flags     uint16
	extraData []byte
	values    [][]*RowsEventValue
	newValues [][]*RowsEventValue
}

func (event *rowsEvent) read(pack *pack) {
	isUpdateEvent := event.EventType == _UPDATE_ROWS_EVENTv1 || event.EventType == _UPDATE_ROWS_EVENTv2

	if event.postHeaderLength == 6 {
		var tableId uint32
		pack.readUint32(&tableId)
		event.tableId = uint64(tableId)
	} else {
		pack.readSixByteUint64(&event.tableId)
	}

	pack.readUint16(&event.Flags)

	//If row event == 2
	if event.EventType >= _WRITE_ROWS_EVENTv2 && event.EventType <= _DELETE_ROWS_EVENTv2 {
		var extraDataLength uint16
		pack.readUint16(&extraDataLength)
		extraDataLength -= 2
		event.extraData = pack.Next(int(extraDataLength))
	}

	var (
		columnCount uint64
		isNull      bool
	)

	pack.readIntLengthOrNil(&columnCount, &isNull)
	bitMapLength := int((columnCount + 7) / 8)

	var columnPreset, columnPresentBitmap1, columnPresentBitmap2, nullBitmap []byte
	columnPresentBitmap1 = pack.Next(bitMapLength)
	if isUpdateEvent {
		columnPresentBitmap2 = pack.Next(bitMapLength)
	}

	event.values = [][]*RowsEventValue{}
	event.newValues = [][]*RowsEventValue{}

	switcher := true

	for {
		nullBitmap = pack.Next(bitMapLength)

		row := []*RowsEventValue{}
		for i, column := range event.tableMapEvent.Columns {

			if switcher {
				columnPreset = columnPresentBitmap1
			} else {
				columnPreset = columnPresentBitmap2
			}

			if !isTrue(i, columnPreset) {
				continue
			}

			value := &RowsEventValue{
				columnId: i,
				_type:    column.Type,
			}
			if isTrue(i, nullBitmap) {
				value.value = nil
				value.isNull = true
			} else {
				switch column.Type {
				case MYSQL_TYPE_TINY:
					value.value, _ = pack.ReadByte()
				case MYSQL_TYPE_SHORT:
					var val uint16
					pack.readUint16(&val)
					value.value = val
				case MYSQL_TYPE_LONG:
					var val uint32
					pack.readUint32(&val)
					value.value = val
				case MYSQL_TYPE_INT24:
					var val uint32
					pack.readThreeByteUint32(&val)
					value.value = val
				case MYSQL_TYPE_FLOAT:
					var val uint32
					pack.readUint32(&val)
					value.value = float32(math.Float32frombits(val))
				case MYSQL_TYPE_DOUBLE:
					var val uint64
					pack.readUint64(&val)
					value.value = math.Float64frombits(val)
				case MYSQL_TYPE_LONGLONG:
					var val uint64
					pack.readUint64(&val)
					value.value = val
				case MYSQL_TYPE_STRING, MYSQL_TYPE_VARCHAR:
					var val string
					if column.MaxLen > 255 {
						val, _ = pack.readStringBySize(2)
					} else {
						val, _ = pack.readStringBySize(1)
					}
					value.value = string(val)
				case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL:
					value.value = pack.readNewDecimal(int(column.Precision), int(column.Decimals))
				case MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_BLOB, MYSQL_TYPE_LONG_BLOB:
					value.value, _ = pack.readStringBySize(int(column.LenSize))
				case MYSQL_TYPE_DATE:
					value.value = pack.readDate()
				case MYSQL_TYPE_DATETIME:
					value.value = pack.readDateTime()
				case MYSQL_TYPE_TIMESTAMP:
					value.value = pack.readTimestamp()
				case MYSQL_TYPE_TIME:
					value.value = pack.readTime()
				case MYSQL_TYPE_YEAR:
					b, _ := pack.ReadByte()
					value.value = 1900 + uint32(b)

					// for new date format
				case MYSQL_TYPE_DATETIME2:
					value.value = pack.readDateTime2(column.Fsp)
				case MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_GEOMETRY, MYSQL_TYPE_BIT:
					value.value, _ = pack.readStringLength()
				}
			}
			row = append(row, value)

		}

		if switcher {
			event.values = append(event.values, row)
		} else {
			event.newValues = append(event.newValues, row)
		}

		if isUpdateEvent {
			switcher = !switcher
		}

		if pack.Len() == 0 {
			break
		}
	}
}

func (event *rowsEvent) GetSchema() string {
	return event.tableMapEvent.SchemaName
}

func (event *rowsEvent) GetTable() string {
	return event.tableMapEvent.TableName
}

func (event *rowsEvent) GetRows() [][]*RowsEventValue {
	return event.values
}

func isTrue(columnId int, bitmap []byte) bool {
	return (bitmap[columnId/8]>>uint8(columnId%8))&1 == 1
}

type (
	DeleteEvent struct {
		*rowsEvent
	}

	WriteEvent struct {
		*rowsEvent
	}

	UpdateEvent struct {
		*rowsEvent
	}
)

func (event *UpdateEvent) GetNewRows() [][]*RowsEventValue {
	return event.newValues
}

type RowsEventValue struct {
	columnId int
	isNull   bool
	value    interface{}
	_type    byte
}

func (event *RowsEventValue) GetType() byte {
	return event._type
}

func (event *RowsEventValue) GetValue() interface{} {
	return event.value
}

func (event *RowsEventValue) IsNil() bool {
	return event.isNull
}

func (event *RowsEventValue) GetColumnId() int {
	return event.columnId
}

type (
	TableMapEvent struct {
		*eventLogHeader
		TableId    uint64
		Flags      uint16
		SchemaName string
		TableName  string
		Columns    []*Column

		ctrConn       *Connection
		schemaColumns []*SchemaColumn
		tableMap      map[uint64]*Table
	}

	Column struct {
		Type byte
		//	Nullable   bool
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

	SchemaColumn struct {
		COLUMN_NAME        string
		COLLATION_NAME     string
		CHARACTER_SET_NAME string
		COLUMN_COMMENT     string
		COLUMN_TYPE        string
		COLUMN_KEY         string
	}
)

func newColumn(pack *pack, colType byte /*, column *SchemaColumn*/) (*Column, error) {
	this := &Column{}

	this.Type = colType
	/*	this.Name = column.COLUMN_NAME
		this.Collation = column.COLLATION_NAME
		this.Charset = column.CHARACTER_SET_NAME
		this.Comment = column.COLUMN_COMMENT

		if column.COLUMN_KEY == "PRI" {
			this.IsPrimary = true
		}

		if strings.Contains(column.COLUMN_TYPE, `unsigned`) {
			this.Unsigned = true
		}
	*/
	var err error
	switch this.Type {
	case MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
		this.readStringMetaData(pack /*, column*/)
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
		/*
			if column.COLUMN_TYPE == "tinyint(1)" {
				this.IsBool = true
			}*/
	}
	return this, nil
}

func (c *Column) readStringMetaData(pack *pack /*, column *SchemaColumn*/) error {
	var b1, b2 uint8
	var err error
	if err = pack.readUint8(&b1); err != nil {
		return err
	}

	if err = pack.readUint8(&b2); err != nil {
		return err
	}

	meta := uint16(b1<<8) + uint16(b2)
	real_type := byte(meta >> 8)
	switch real_type {
	case MYSQL_TYPE_ENUM, MYSQL_TYPE_SET:
		c.Type = real_type
		c.Size = uint8(meta & 0x00ff)
		// c.readEnumData(column)
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

func (event *TableMapEvent) read(pack *pack) {
	pack.readSixByteUint64(&event.TableId)
	pack.readUint16(&event.Flags)

	schemaLength, _ := pack.ReadByte()
	event.SchemaName = string(pack.Next(int(schemaLength)))
	filler, _ := pack.ReadByte()
	if filler != 0 {
		panic("incorrect filler")
	}

	tableLength, _ := pack.ReadByte()
	event.TableName = string(pack.Next(int(tableLength)))
	filler, _ = pack.ReadByte()
	if filler != 0 {
		panic("incorrect filler")
	}

	// get schema info
	/*
		var err error
			if _, ok := event.tableMap[event.TableId]; ok {
				event.schemaColumns = event.tableMap[event.TableId].SchemaColumns
			} else if event.schemaColumns, err = event.ctrConn.getSchemaColumns(event.SchemaName, event.TableName); err != nil {
				panic("get schema info err:" + err.Error())
			}
	*/

	var columnCount, metaLen uint64
	var isNull bool

	pack.readIntLengthOrNil(&columnCount, &isNull)

	columnTypeDef := pack.Next(int(columnCount))

	// ignore len
	pack.readIntLengthOrNil(&metaLen, &isNull)

	// columnMetaDef, _ := pack.readStringLength()
	// columnNullBitMap := pack.Bytes()

	event.Columns = make([]*Column, columnCount)

	for i := 0; i < len(columnTypeDef); i++ {
		if column, err := newColumn(pack, columnTypeDef[i] /*, event.schemaColumns[i]*/); err != nil {
			panic(err)
		} else {
			event.Columns[i] = column
		}
	}
}
