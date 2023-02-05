package index

import (
	"fmt"
	"unsafe"

	"github.com/ue-sho/ohako/storage/page"
)

const uint64BinaryLen = 8

type MetaHeader struct {
	rootPageId page.PageID
}

type MetaData struct {
	header        *MetaHeader
	appAreaLength *uint64
	appArea       []byte
}

// メタ情報を入れる容器を生成する
func NewMeta(bytes []byte) *MetaData {
	meta := MetaData{}

	headerSize := int(unsafe.Sizeof(*meta.header))
	if headerSize+1 > len(bytes) {
		fmt.Println("meta header must be aligned")
		return nil
	}

	meta.header = (*MetaHeader)(unsafe.Pointer(&bytes[0]))
	meta.appAreaLength = (*uint64)(unsafe.Pointer(&bytes[headerSize]))
	meta.appArea = bytes[headerSize+uint64BinaryLen:]
	return &meta
}
