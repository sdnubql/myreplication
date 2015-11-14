package myreplication

import ()

type (
	EventLog struct {
		mysqlConnection               *Connection
		binlogVersion                 uint16
		lastRotatePosition            uint32
		lastRotateFileName            []byte
		headerQueryEventLength        byte
		headerDeleteRowsEventV1Length byte
		headerUpdateRowsEventV1Length byte
		headerWriteRowsEventV1Length  byte
		tableMap                      map[uint64]*Table
		lastTableMapEvent             *TableMapEvent
		additionalLength              int
	}

	eventLogHeader struct {
		Timestamp    uint32
		EventType    byte
		ServerId     uint32
		EventSize    uint32
		NextPosition uint32
		Flags        uint16
	}

	logRotateEvent struct {
		*eventLogHeader
		position       uint64
		binlogFileName []byte
	}

	formatDescriptionEvent struct {
		*eventLogHeader
		binlogVersion          uint16
		mysqlServerVersion     []byte
		createTimestamp        uint32
		eventTypeHeaderLengths []byte
	}

	startEventV3Event struct {
		*eventLogHeader
		binlogVersion      uint16
		mysqlServerVersion []byte
		createTimestamp    uint32
	}

	QueryEvent struct {
		*eventLogHeader
		slaveProxyId  uint32
		executionTime uint32
		errorCode     uint16
		statusVars    []byte
		schema        string
		query         string
		binLogVersion uint16
	}

	XidEvent struct {
		*eventLogHeader
		TransactionId uint64
	}

	IntVarEvent struct {
		*eventLogHeader
		_type byte
		value uint64
	}

	BeginLoadQueryEvent struct {
		*eventLogHeader
		fileId    uint32
		blockData string
	}

	ExecuteLoadQueryEvent struct {
		*eventLogHeader
		slaveProxyId     uint32
		executionTime    uint32
		errorCode        uint16
		statusVars       []byte
		schema           string
		fileId           uint32
		startPos         uint32
		endPos           uint32
		dupHandlingFlags byte
		query            string
	}

	UserVarEvent struct {
		*eventLogHeader
		name    string
		isNil   bool
		_type   byte
		charset uint32
		value   string
		flags   byte
	}

	IncidentEvent struct {
		*eventLogHeader
		Type    uint16
		Message string
	}

	RandEvent struct {
		*eventLogHeader
		seed1 uint64
		seed2 uint64
	}

	unknownEvent struct {
		*eventLogHeader
	}

	binLogEvent interface {
		read(*pack)
	}

	AppendBlockEvent struct {
		*BeginLoadQueryEvent
	}

	StopEvent struct {
		*unknownEvent
	}

	slaveEvent struct {
		*unknownEvent
	}

	ignorableEvent struct {
		*unknownEvent
	}

	HeartBeatEvent struct {
		*unknownEvent
	}
)

func (event *RandEvent) GetSeed1() uint64 {
	return event.seed1
}

func (event *RandEvent) GetSeed2() uint64 {
	return event.seed2
}

func (event *RandEvent) read(pack *pack) {
	pack.readUint64(&event.seed1)
	pack.readUint64(&event.seed2)
}

func (event *IncidentEvent) read(pack *pack) {
	pack.readUint16(&event.Type)
	length, _ := pack.ReadByte()
	event.Message = string(pack.Next(int(length)))
}

func (event *unknownEvent) read(pack *pack) {

}

func (event *UserVarEvent) GetName() string {
	return event.name
}

func (event *UserVarEvent) GetType() byte {
	return event._type
}

func (event *UserVarEvent) IsNil() bool {
	return event.isNil
}

func (event *UserVarEvent) GetCharset() uint32 {
	return event.charset
}

func (event *UserVarEvent) GetValue() string {
	return event.value
}

func (event *UserVarEvent) read(pack *pack) {
	var nameLength uint32
	pack.readUint32(&nameLength)
	event.name = string(pack.Next(int(nameLength)))
	isNull, _ := pack.ReadByte()
	event.isNil = isNull == 1
	if event.isNil {
		return
	}

	event._type, _ = pack.ReadByte()
	pack.readUint32(&event.charset)
	var length uint32
	pack.readUint32(&length)
	event.value = string(pack.Next(int(length)))
	event.flags, _ = pack.ReadByte()
}

func (event *ExecuteLoadQueryEvent) GetSchema() string {
	return event.schema
}

func (event *ExecuteLoadQueryEvent) GetQuery() string {
	return event.query
}

func (event *ExecuteLoadQueryEvent) GetExecutionTime() uint32 {
	return event.executionTime
}

func (event *ExecuteLoadQueryEvent) GetErrorCode() uint16 {
	return event.errorCode
}

func (event *ExecuteLoadQueryEvent) read(pack *pack) {
	pack.readUint32(&event.slaveProxyId)
	pack.readUint32(&event.executionTime)

	schemaLength, _ := pack.ReadByte()

	pack.readUint16(&event.errorCode)

	var statusVarsLength uint16
	pack.readUint16(&statusVarsLength)

	pack.readUint32(&event.fileId)
	pack.readUint32(&event.startPos)
	pack.readUint32(&event.endPos)
	event.dupHandlingFlags, _ = pack.ReadByte()

	event.statusVars = pack.Next(int(statusVarsLength))
	event.schema = string(pack.Next(int(schemaLength)))

	splitter, _ := pack.ReadByte()

	if splitter != 0 {
		panic("Incorrect binlog EXECUTE_LOAD_QUERY_EVENT structure")
	}

	event.query = string(pack.Bytes())
}

func (event *BeginLoadQueryEvent) GetData() string {
	return event.blockData
}

func (event *BeginLoadQueryEvent) read(pack *pack) {
	pack.readUint32(&event.fileId)
	event.blockData = string(pack.Bytes())
}

func (event *IntVarEvent) GetValue() uint64 {
	return event.value
}

func (event *IntVarEvent) GetType() byte {
	return event._type
}

func (event *IntVarEvent) read(pack *pack) {
	event._type, _ = pack.ReadByte()
	pack.readUint64(&event.value)
}

func (event *XidEvent) read(pack *pack) {
	pack.readUint64(&event.TransactionId)
}

func (event *QueryEvent) GetQuery() string {
	return event.query
}

func (event *QueryEvent) GetExecutionTime() uint32 {
	return event.executionTime
}

func (event *QueryEvent) GetErrorCode() uint16 {
	return event.errorCode
}

func (event *QueryEvent) GetSchema() string {
	return event.schema
}

func (event *QueryEvent) read(pack *pack) {
	pack.readUint32(&event.slaveProxyId)
	pack.readUint32(&event.executionTime)

	schemaLength, _ := pack.ReadByte()

	pack.readUint16(&event.errorCode)

	if event.binLogVersion >= 4 {
		var statusVarsLength uint16
		pack.readUint16(&statusVarsLength)
		event.statusVars = pack.Next(int(statusVarsLength))
	}

	event.schema = string(pack.Next(int(schemaLength)))
	splitter, _ := pack.ReadByte()

	if splitter != 0 {
		panic("Incorrect binlog QUERY_EVENT structure")
	}

	event.query = string(pack.Bytes())
}

func (event *logRotateEvent) read(pack *pack) {
	pack.readUint64(&event.position)
	event.binlogFileName = pack.Next(pack.Len())
}

func (event *formatDescriptionEvent) read(pack *pack) {
	pack.readUint16(&event.binlogVersion)
	event.mysqlServerVersion = pack.Next(50)
	pack.readUint32(&event.createTimestamp)
	length, _ := pack.ReadByte()
	event.eventTypeHeaderLengths = pack.Next(int(length))
}

func (event *startEventV3Event) read(pack *pack) {
	pack.readUint16(&event.binlogVersion)
	event.mysqlServerVersion = make([]byte, 50)
	pack.Read(event.mysqlServerVersion)

	pack.readUint32(&event.createTimestamp)
}

func (eh *eventLogHeader) readHead(pack *pack) {
	pack.ReadByte()
	pack.readUint32(&eh.Timestamp)
	eh.EventType, _ = pack.ReadByte()
	pack.readUint32(&eh.ServerId)
	pack.readUint32(&eh.EventSize)
	pack.readUint32(&eh.NextPosition)
	pack.readUint16(&eh.Flags)
}

func newEventLog(mysqlConnection *Connection, additionalLength int) *EventLog {
	return &EventLog{
		mysqlConnection:  mysqlConnection,
		additionalLength: additionalLength,
	}
}

func (ev *EventLog) GetLastPosition() uint32 {
	return ev.lastRotatePosition
}

func (ev *EventLog) GetLastLogFileName() string {
	return string(ev.lastRotateFileName)
}

func (ev *EventLog) GetEvent() (interface{}, error) {

	for {
		event, err := ev.readEvent()

		if err != nil {
			return nil, err
		}

		switch e := event.(type) {
		case *startEventV3Event:
			ev.binlogVersion = e.binlogVersion
		case *formatDescriptionEvent:
			ev.binlogVersion = e.binlogVersion
			ev.headerQueryEventLength = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_QUERY_POSITION]

			ev.headerDeleteRowsEventV1Length = 8
			ev.headerUpdateRowsEventV1Length = 8
			ev.headerWriteRowsEventV1Length = 8

			if len(e.eventTypeHeaderLengths) >= 24 {
				ev.headerDeleteRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_DELETEV1_POSITION]
				ev.headerUpdateRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_UPDATEV1_POSITION]
				ev.headerWriteRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_WRITEV1_POSITION]
			}
		case *logRotateEvent:
			ev.lastRotateFileName = e.binlogFileName
		case *QueryEvent:
			return e, nil
		case *XidEvent:
			continue
		case *IntVarEvent:
			return e, nil
		case *BeginLoadQueryEvent:
			return e, nil
		case *AppendBlockEvent:
			return e, nil
		case *ExecuteLoadQueryEvent:
			return e, nil
		case *UserVarEvent:
			return e, nil
		case *RandEvent:
			return e, nil
		case *TableMapEvent:
			ev.lastTableMapEvent = e
		case *rowsEvent:
			switch e.EventType {
			case _DELETE_ROWS_EVENTv0:
				fallthrough
			case _DELETE_ROWS_EVENTv1:
				fallthrough
			case _DELETE_ROWS_EVENTv2:
				return &DeleteEvent{e}, nil
			case _UPDATE_ROWS_EVENTv0:
				fallthrough
			case _UPDATE_ROWS_EVENTv1:
				fallthrough
			case _UPDATE_ROWS_EVENTv2:
				return &UpdateEvent{e}, nil
			case _WRITE_ROWS_EVENTv0:
				fallthrough
			case _WRITE_ROWS_EVENTv1:
				fallthrough
			case _WRITE_ROWS_EVENTv2:
				return &WriteEvent{e}, nil
			}

			////////// trash events
		case *slaveEvent:
			continue
		case *unknownEvent:
			continue
		case *ignorableEvent:
			continue
		case *HeartBeatEvent:
			continue
		case *StopEvent:
			continue
		case *IncidentEvent:
			continue
		default:
			continue
		}
	}

}

func (ev *EventLog) Close() {
	ev.mysqlConnection.Close()
}

func (ev *EventLog) readEvent() (interface{}, error) {
	pack, err := ev.mysqlConnection.packReader.readNextPackWithAdditionalLength(ev.additionalLength)

	if err != nil {
		return nil, err
	}

	header := &eventLogHeader{}
	header.readHead(pack)

	err = pack.isError()

	if err != nil {
		return nil, err
	}

	var event binLogEvent

	switch header.EventType {
	case _START_EVENT_V3:
		event = &startEventV3Event{
			eventLogHeader: header,
		}
	case _FORMAT_DESCRIPTION_EVENT:
		event = &formatDescriptionEvent{
			eventLogHeader: header,
		}
	case _ROTATE_EVENT:
		event = &logRotateEvent{
			eventLogHeader: header,
		}
	case _QUERY_EVENT:
		event = &QueryEvent{
			eventLogHeader: header,
			binLogVersion:  ev.binlogVersion,
		}
	case _XID_EVENT:
		event = &XidEvent{
			eventLogHeader: header,
		}
	case _INTVAR_EVENT:
		event = &IntVarEvent{
			eventLogHeader: header,
		}
	case _BEGIN_LOAD_QUERY_EVENT:
		event = &BeginLoadQueryEvent{
			eventLogHeader: header,
		}
	case _APPEND_BLOCK_EVENT:
		event = &AppendBlockEvent{
			&BeginLoadQueryEvent{
				eventLogHeader: header,
			},
		}
	case _EXECUTE_LOAD_QUERY_EVENT:
		event = &ExecuteLoadQueryEvent{
			eventLogHeader: header,
		}
	case _USER_VAR_EVENT:
		event = &UserVarEvent{
			eventLogHeader: header,
		}
	case _UNKNOWN_EVENT:
		event = &unknownEvent{
			eventLogHeader: header,
		}
	case _IGNORABLE_EVENT:
		event = &ignorableEvent{
			&unknownEvent{
				eventLogHeader: header,
			},
		}
	case _HEARTBEAT_EVENT:
		event = &HeartBeatEvent{
			&unknownEvent{
				eventLogHeader: header,
			},
		}
	case _STOP_EVENT:
		event = &StopEvent{
			&unknownEvent{
				eventLogHeader: header,
			},
		}
	case _INCIDENT_EVENT:
		event = &IncidentEvent{
			eventLogHeader: header,
		}
	case _SLAVE_EVENT:
		event = &slaveEvent{
			&unknownEvent{
				eventLogHeader: header,
			},
		}
	case _RAND_EVENT:
		event = &RandEvent{
			eventLogHeader: header,
		}
	case _TABLE_MAP_EVENT:
		event = &TableMapEvent{
			eventLogHeader: header,
			tableMap:       ev.tableMap,
			ctrConn:        ev.mysqlConnection,
		}
	case _DELETE_ROWS_EVENTv0:
		fallthrough
	case _DELETE_ROWS_EVENTv1:
		fallthrough
	case _DELETE_ROWS_EVENTv2:
		fallthrough
	case _UPDATE_ROWS_EVENTv0:
		fallthrough
	case _UPDATE_ROWS_EVENTv1:
		fallthrough
	case _UPDATE_ROWS_EVENTv2:
		fallthrough
	case _WRITE_ROWS_EVENTv0:
		fallthrough
	case _WRITE_ROWS_EVENTv1:
		fallthrough
	case _WRITE_ROWS_EVENTv2:
		event = &rowsEvent{
			eventLogHeader:   header,
			postHeaderLength: ev.headerWriteRowsEventV1Length,
			tableMapEvent:    ev.lastTableMapEvent,
		}
	default:
		//		println("Unknown event")
		//		println(fmt.Sprintf("% x\n", pack.buff))
		return nil, nil
	}

	ev.lastRotatePosition = header.NextPosition

	event.read(pack)
	return event, nil
}
