package disk

import (
	"github.com/ue-sho/ohako/types"
)

type DiskManager interface {
	ReadPage(types.PageID, []byte) error
	WritePage(types.PageID, []byte) error
	AllocatePage() types.PageID
	DeallocatePage(types.PageID)
	GetNumWrites() uint64
	ShutDown()
	Size() int64
}