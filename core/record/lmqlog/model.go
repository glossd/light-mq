package lmqlog

import (
	"encoding/binary"
	"fmt"
)

const metaDataSize = 8 + 8 + 4 // 20
const metaPositionStart = 8
const metaSizeStart = 8 + 8 // 16

type RecordMetaData struct {
	Offset   uint64
	Position int64
	Size     uint32
}

func (m RecordMetaData) toBytes() []byte {
	buf := make([]byte, metaDataSize)
	binary.LittleEndian.PutUint64(buf, m.Offset)
	binary.LittleEndian.PutUint64(buf[8:], uint64(m.Position))
	binary.LittleEndian.PutUint32(buf[16:], m.Size)
	return buf
}

func metaFromBytes(b []byte) (*RecordMetaData, error) {
	if len(b) != metaDataSize {
		return nil, fmt.Errorf("data inconsistency, excpected bytes length %d, but got %d", metaDataSize, len(b))
	}
	offset := binary.LittleEndian.Uint64(b)
	position := binary.LittleEndian.Uint64(b[metaPositionStart:])
	size := binary.LittleEndian.Uint32(b[metaSizeStart:])
	return &RecordMetaData{Offset: offset, Position: int64(position), Size: size}, nil
}

type Record struct {
	*RecordMetaData
	Body []byte
	// output only
	PartitionID int
}


func (r Record) String() string {
	return fmt.Sprintf("Record{o:%d, p:%d, s:%d, m:%s}", r.Offset, r.Position, r.Size, r.Body)
}

func NewRecord(offset uint64, position int64, size int, body []byte) *Record {
	meta := RecordMetaData{Offset: offset, Position: position, Size: uint32(size)}
	return &Record{RecordMetaData: &meta, Body: body}
}

func GetRecordSize(body []byte) uint32 {
	return uint32(metaDataSize + len(body))
}
