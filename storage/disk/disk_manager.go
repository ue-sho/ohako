package disk

import "github.com/ue-sho/ohako/storage/page"

type DiskManager interface {
	ReadPage(page.PageID, []byte) error
	WritePage(page.PageID, []byte) error
	AllocatePage() page.PageID
	DeallocatePage(page.PageID)
	GetNumWrites() uint64
	ShutDown()
	Size() int64
}
