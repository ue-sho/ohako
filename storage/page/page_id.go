package page

import (
	"bytes"
	"encoding/binary"
)

type PageID int32

const InvalidPageID = PageID(-1)

func (id PageID) IsValid() bool {
	return id != InvalidPageID || id >= 0
}

// []byteへ変換する
func (id PageID) Serialize() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	return buf.Bytes()
}

// []byteからPageIDを生成する
func NewPageIDFromBytes(data []byte) (ret PageID) {
	binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &ret)
	return ret
}
