package myreplication

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/big"
	"strconv"
	"time"
)

type (
	packReader struct {
		conn io.Reader
	}

	packWriter struct {
		conn io.Writer
	}

	pack struct {
		sequence byte
		length   uint32
		buff     []byte
		*bytes.Buffer
	}
)

var (
	compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
)

func newPackReader(conn io.Reader) *packReader {
	return &packReader{
		conn: conn,
	}
}

func newPackWriter(conn io.Writer) *packWriter {
	return &packWriter{
		conn: conn,
	}
}

func (w *packWriter) flush(p *pack) error {
	_, err := w.conn.Write(p.packBytes())
	return err
}

func newPackWithBuff(buff []byte) *pack {
	pack := &pack{
		buff: buff,
	}
	pack.Buffer = bytes.NewBuffer(pack.buff)
	return pack
}

func newPack() *pack {
	return newPackWithBuff(make([]byte, 4))
}

func (r *packReader) readNextPack() (*pack, error) {
	return r.readNextPackWithAdditionalLength(0)
}

func (r *packReader) readNextPackWithAdditionalLength(addLength int) (*pack, error) {
	buff := make([]byte, 4)
	_, err := r.conn.Read(buff)
	if err != nil {
		return nil, err
	}
	var length uint32

	err = readThreeBytesUint32(buff[0:3], &length)
	if err != nil {
		return nil, err
	}

	pack := &pack{
		sequence: buff[3],
		length:   length,
		buff:     make([]byte, length),
	}

	_, err = r.conn.Read(pack.buff)
	if addLength > 0 {
		pack.buff = pack.buff[0 : len(pack.buff)-addLength]
	}
	pack.Buffer = bytes.NewBuffer(pack.buff)
	if err != nil {
		return nil, err
	}

	return pack, nil
}

func (r *pack) getSequence() byte {
	return r.sequence
}

func (r *pack) setSequence(s byte) {
	r.sequence = s
}

func (r *pack) readByte(dest *byte) (err error) {
	*dest, err = r.ReadByte()
	return
}

func (r *pack) readUint8(dest *uint8) error {
	return readUint8(r.Buffer.Next(1), dest)
}

func (r *pack) readUint16(dest *uint16) error {
	return readUint16(r.Buffer.Next(2), dest)
}

func (r *pack) readThreeByteUint32(dest *uint32) error {
	return readThreeBytesUint32(r.Buffer.Next(3), dest)
}

func (r *pack) readUint32(dest *uint32) error {
	return readUint32(r.Buffer.Next(4), dest)
}

func (r *pack) readSixByteUint64(dest *uint64) error {
	return readSixByteUint64(r.Buffer.Next(6), dest)
}

func (r *pack) readUint64(dest *uint64) error {
	return readUint64(r.Buffer.Next(8), dest)
}

func (r *pack) readUint64BySize(size int) (uint64, error) {

	var ret uint64
	if err := readFixByteUint64(r.Buffer.Next(size), &ret); err != nil {
		return 0, err
	}

	return ret, nil
}

/*
 * YYYY<< 9 + MM << 5 + DD
 */
func (r *pack) readDate() time.Time {
	var value uint32
	r.readThreeByteUint32(&value)
	if value == 0 {
		return time.Time{}.In(time.Local)
	}

	var year int
	if year = int((value & (((1 << 15) - 1) << 9)) >> 9); year == 0 {
		return time.Time{}.In(time.Local)
	}

	month := int((value & (((1 << 4) - 1) << 5)) >> 5)
	day := int(value & ((1 << 5) - 1))

	date := time.Date(
		year, time.Month(month), day, 0, 0, 0, 0, time.Local,
	)

	return date
}

func (r *pack) readDateTime() time.Time {

	var value uint64
	r.readUint64(&value)
	if value == 0 {
		return time.Time{}.In(time.Local)
	}

	date := value / 1000000
	timev := int(value % 1000000)

	year := int(date / 10000)
	month := int((date % 10000) / 100)
	day := int(date % 100)

	if year == 0 || month == 0 || day == 0 {
		return time.Time{}.In(time.Local)
	}

	return time.Date(int(year), time.Month(month), int(day), int(timev/10000), int((timev%10000)/100), int(timev%100), 0, time.Local)
}

func (r *pack) readTimestamp() time.Time {
	var value uint32
	r.readUint32(&value)

	return time.Unix(int64(value), 0)
}

/*
 * doc: http://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
   DATETIME2

   1 bit  sign           (1= non-negative, 0= negative)
   17 bits year*13+month  (year 0-9999, month 0-12)
   5 bits day            (0-31)
   5 bits hour           (0-23)
   6 bits minute         (0-59)
   6 bits second         (0-59)
   ---------------------------
   40 bits = 5 bytes
*/
func (r *pack) readDateTime2(fsp uint8) time.Time {
	data := binary.BigEndian.Uint64(r.Buffer.Next(40))
	year_month := readBinarySlice(data, 1, 17, 40)
	t := time.Date(
		int(year_month/13), time.Month(int(year_month%13)), int(readBinarySlice(data, 18, 5, 40)),
		int(readBinarySlice(data, 23, 5, 40)),
		int(readBinarySlice(data, 28, 6, 40)),
		int(readBinarySlice(data, 34, 6, 40)), 0, time.Local)
	return r.addFspTime(t, fsp)
}

func (r *pack) addFspTime(t time.Time, fsp uint8) time.Time {
	read := 0
	switch fsp {
	case 1, 2:
		read = 1
	case 3, 4:
		read = 2
	case 5, 6:
		read = 3
	}

	if read == 0 {
		return t
	}

	microsec := binary.BigEndian.Uint32(r.Buffer.Next(8 * read))
	if fsp%2 == 1 {
		t.Add(time.Microsecond * time.Duration(int(microsec/10)))
	} else {
		t.Add(time.Microsecond * time.Duration(microsec))
	}

	return t
}

func readBinarySlice(data uint64, start, size, datalen uint32) uint64 {
	data = data >> (datalen - start + size)
	return data & ((1 << size) - 1)
}

func (r *pack) readTime() time.Duration {
	length, _ := r.ReadByte()
	var days uint32
	var hour, minute, second byte
	var microSecond uint32

	if length == 0 {
		return time.Duration(0)
	}

	isNegative, _ := r.ReadByte()

	r.readUint32(&days)
	hour, _ = r.ReadByte()
	minute, _ = r.ReadByte()
	second, _ = r.ReadByte()

	if length == 12 {
		r.readUint32(&microSecond)
	}

	d := time.Duration(
		time.Duration(days)*24*time.Hour +
			time.Duration(hour)*time.Hour +
			time.Duration(minute)*time.Minute +
			time.Duration(second)*time.Second +
			time.Duration(microSecond)*time.Microsecond,
	)

	if isNegative == 1 {
		return -d
	}

	return d
}

//got from https://github.com/whitesock/open-replicator toDecimal method
// and https://github.com/jeremycole/mysql_binlog/blob/master/lib/mysql_binlog/binlog_field_parser.rb#L233
//mysql.com have incorrect manual
func (r *pack) readNewDecimal(precission, scale int) *big.Rat {
	size := getDecimalBinarySize(precission, scale)

	buff := r.Next(size)
	positive := (buff[0] & 0x80) == 0x80
	buff[0] ^= 0x80

	if !positive {
		for i := 0; i < size; i++ {
			buff[i] ^= 0xFF
		}
	}

	decimalPack := newPackWithBuff(buff)

	var value string

	if !positive {
		value += "-"
	}

	x := precission - scale

	unCompIntegral := x / _DIGITS_PER_INTEGER
	unCompFraction := scale / _DIGITS_PER_INTEGER

	compIntegral := x - (unCompIntegral * _DIGITS_PER_INTEGER)
	compFractional := scale - (unCompFraction * _DIGITS_PER_INTEGER)

	size = compressedBytes[compIntegral]

	if size > 0 {
		value += decimalPack.readDecimalStringBySize(size)
	}

	for i := 1; i <= unCompIntegral; i++ {
		value += decimalPack.readDecimalStringBySize(4)
	}

	value += "."

	for i := 1; i <= unCompFraction; i++ {
		value += decimalPack.readDecimalStringBySize(4)
	}

	size = compressedBytes[compFractional]

	if size > 0 {
		value += decimalPack.readDecimalStringBySize(size)
	}

	rat, _ := new(big.Rat).SetString(value)

	return rat
}

func (r *pack) readDecimalStringBySize(size int) string {
	var value int
	switch size {
	case 1:
		val, _ := r.ReadByte()
		value = int(val)
	case 2:
		var val uint16
		readUint16Revert(r.Next(2), &val)
		value = int(val)
	case 3:
		var val uint32
		readThreeBytesUint32Revert(r.Next(3), &val)
		value = int(val)
	case 4:
		var val uint32
		readUint32Revert(r.Next(4), &val)
		value = int(val)
	}
	return strconv.Itoa(value)
}

func (r *pack) readNilString() ([]byte, error) {
	buff, err := r.ReadBytes(byte(0))

	if err != nil {
		return []byte{}, err
	}

	return buff[0 : len(buff)-1], nil
}

func (r *pack) readStringLength() ([]byte, error) {
	var (
		length uint64
		null   bool
	)

	err := r.readIntLengthOrNil(&length, &null)

	if err != nil {
		return []byte{}, err
	}

	if length == 0 {
		return []byte{}, nil
	}

	ret := r.Next(int(length))
	return ret, nil
}

func (r *pack) readIntLengthOrNil(value *uint64, null *bool) error {
	lb, err := r.ReadByte()

	if err != nil {
		return err
	}

	switch lb {
	case 0xFB:
		*null = true
	case 0xFC:
		var val uint16
		r.readUint16(&val)
		*value = uint64(val)
	case 0xFD:
		var val uint32
		r.readThreeByteUint32(&val)
		*value = uint64(val)
	case 0xFE:
		r.readUint64(value)
	default:
		*value = uint64(lb)
	}
	return nil
}

func (r *pack) readStringBySize(size int) (string, error) {
	var i uint64
	var err error
	if err = readFixByteUint64(r.Buffer.Next(size), &i); err != nil {
		return "", err
	} else {
		return string(r.Buffer.Next(int(i))), nil
	}
}

func (r *pack) writeUInt16(data uint16) error {
	buff := make([]byte, 2)

	writeUInt16(buff, data)
	_, err := r.Write(buff)
	return err
}

func (r *pack) writeThreeByteUInt32(data uint32) error {
	buff := make([]byte, 3)

	writeThreeByteUInt32(buff, data)
	_, err := r.Write(buff)
	return err
}

func (r *pack) writeUInt32(data uint32) error {
	buff := make([]byte, 4)

	writeUInt32(buff, data)
	_, err := r.Write(buff)
	return err
}

func (r *pack) writeUInt64(data uint64) error {
	buff := make([]byte, 8, 8)

	writeUInt64(buff, data)

	_, err := r.Write(buff)
	return err
}

func (r *pack) writeStringNil(data string) error {
	_, err := r.Write([]byte(data))
	if err != nil {
		return err
	}

	err = r.WriteByte(byte(0))
	return err
}

func (r *pack) writeStringLength(data string) error {
	length := writeLengthInt(uint64(len(data)))

	_, err := r.Write(length)
	if err != nil {
		return err
	}

	_, err = r.Write([]byte(data))
	if err != nil {
		return err
	}

	return err
}

func (r *pack) packBytes() []byte {
	buff := r.Bytes()
	writeThreeByteUInt32(buff, uint32(len(buff)-4))
	buff[3] = r.getSequence()
	return buff
}

func (r *pack) isError() error {
	if r.buff[0] == _MYSQL_ERR {
		errPack := &errPacket{}
		readUint16(r.buff[1:3], &errPack.code)
		errPack.description = r.buff[3:]
		return errPack
	}

	return nil
}

func (r *pack) isEOF() bool {
	return r.buff[0] == _MYSQL_EOF
}

func getDecimalBinarySize(precission, scale int) int {
	x := precission - scale
	ipDigits := x / _DIGITS_PER_INTEGER
	fpDigits := scale / _DIGITS_PER_INTEGER
	ipDigitsX := x - ipDigits*_DIGITS_PER_INTEGER
	fpDigitsX := scale - fpDigits*_DIGITS_PER_INTEGER
	return (ipDigits << 2) + compressedBytes[ipDigitsX] + (fpDigits << 2) + compressedBytes[fpDigitsX]
}
