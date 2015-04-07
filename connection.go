package myreplication

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"net"
	"strconv"
)

type (
	Connection struct {
		conn       net.Conn
		packReader *packReader
		packWriter *packWriter

		currentDb      string
		masterPosition uint64
		fileName       string

		ctrDB *sql.DB
	}
)

const (
	_DEFAULT_DB = "information_schema"
)

func NewConnection() *Connection {
	return &Connection{
		conn:  nil,
		ctrDB: nil,
	}
}

func (c *Connection) Connection() net.Conn {
	return c.conn
}

func (c *Connection) ConnectAndAuth(host string, port int, username, password string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return err
	}
	c.conn = conn

	c.packReader = newPackReader(conn)
	c.packWriter = newPackWriter(conn)

	if err = c.init(username, password); err != nil {
		return err
	}

	if err = c.initCtrDB(host, port, username, password, _DEFAULT_DB); err != nil {
		return err
	}

	return nil
}

func (c *Connection) initCtrDB(host string, port int, username, password, defaultDB string) error {

	var err error
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=True&loc=Local&interpolateParams=true",
		username, password, host, port, defaultDB)
	if c.ctrDB, err = sql.Open("mysql", dsn); err != nil {
		return err
	}

	return nil
}

func (c *Connection) init(username, password string) (err error) {
	pack, err := c.packReader.readNextPack()
	if err != nil {
		return err
	}
	//receive handshake
	//get handshake data and parse
	handshake := &pkgHandshake{}

	err = handshake.readServer(pack)

	if err != nil {
		return
	}

	//prepare and buff handshake auth response
	pack = handshake.writeServer(username, password)
	pack.setSequence(byte(1))
	err = c.packWriter.flush(pack)

	if err != nil {
		return
	}

	pack, err = c.packReader.readNextPack()
	if err != nil {
		return err
	}

	return pack.isError()
}

func (c *Connection) Close() {
	if c.conn != nil {
		c.Close()
	}

	c = nil
}

func (c *Connection) GetMasterStatus() (pos uint32, filename string, err error) {
	rs, err := c.query("SHOW MASTER STATUS")
	if err != nil {
		return
	}

	pack, err := rs.nextRow()
	if err != nil {
		return
	}

	_fileName, _ := pack.readStringLength()
	_pos, _ := pack.readStringLength()

	filename = string(_fileName)
	pos64, err := strconv.ParseUint(string(_pos), 10, 32)

	if err != nil {
		return
	}

	pos = uint32(pos64)

	rs.nextRow()
	rs = nil
	return
}

func (c *Connection) ChecksumCompatibility() (ok bool, err error) {
	err = c.initDb(_DEFAULT_DB)
	if err != nil {
		return
	}
	rs, err := c.query("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")

	if err != nil {
		return
	}

	pack, err := rs.nextRow()
	if err != nil {
		if err == EOF_ERR {
			return false, nil
		}
		return
	}

	pack.readStringLength()
	_type, _ := pack.readStringLength()
	rs.nextRow()

	if len(_type) == 0 {
		return
	}
	ok = true
	_, err = c.query("set @master_binlog_checksum = @@global.binlog_checksum")
	return
}

func (c *Connection) initDb(schemaName string) error {
	q := &initDb{}
	pack := q.writeServer(schemaName)
	err := c.packWriter.flush(pack)
	if err != nil {
		return err
	}

	pack, err = c.packReader.readNextPack()
	if err != nil {
		return err
	}

	return pack.isError()
}

func (c *Connection) query(command string) (*resultSet, error) {
	q := &query{}
	pack := q.writeServer(command)
	err := c.packWriter.flush(pack)
	if err != nil {
		return nil, err
	}

	rs := &resultSet{}
	rs.setReader(c.packReader)
	err = rs.init()

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (c *Connection) connectDb(db string) error {
	q := &connectDb{}
	pack := q.writeServer(db)
	err := c.packWriter.flush(pack)
	if err != nil {
		return err
	}

	pack, err = c.packReader.readNextPack()

	if err != nil {
		return err
	}

	return pack.isError()
}

func (c *Connection) fieldList(db, table string) (*resultSet, error) {
	if c.currentDb != db {
		err := c.connectDb(db)
		if err != nil {
			return nil, nil
		}
	}

	q := &fieldList{}
	pack := q.writeServer(table)
	err := c.packWriter.flush(pack)
	if err != nil {
		return nil, err
	}

	rs := &resultSet{}
	rs.setReader(c.packReader)
	err = rs.initFieldList()

	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (c *Connection) StartBinlogDump(position uint32, fileName string, serverId uint32) (el *EventLog, err error) {
	ok, err := c.ChecksumCompatibility()
	if err != nil {
		return
	}

	register := &registerSlave{}
	pack := register.writeServer(serverId)
	err = c.packWriter.flush(pack)
	if err != nil {
		return nil, err
	}

	pack, err = c.packReader.readNextPack()

	if err != nil {
		return nil, err
	}

	err = pack.isError()

	if err != nil {
		return nil, err
	}

	startBinLog := &binlogDump{}
	pack = startBinLog.writeServer(position, fileName, serverId)
	err = c.packWriter.flush(pack)
	if err != nil {
		return nil, err
	}

	var additionalLength int

	if ok {
		additionalLength = 4
	}

	el = newEventLog(c, additionalLength)

	return el, nil
}
