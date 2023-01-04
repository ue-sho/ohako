package buffer

import (
	"errors"

	"github.com/ue-sho/ohako/storage/disk"
	"github.com/ue-sho/ohako/storage/page"
	"github.com/ue-sho/ohako/types"
)

type BufferPoolManager struct {
	diskManager disk.DiskManager
	pages       []*page.Page
	replacer    *ClockReplacer
	freeList    []FrameID
	pageTable   map[types.PageID]FrameID
}

// バッファプールから要求されたページを取り出す
func (b *BufferPoolManager) FetchPage(pageID types.PageID) *page.Page {
	if frameID, ok := b.pageTable[pageID]; ok {
		pg := b.pages[frameID]
		pg.IncPinCount()
		(*b.replacer).Pin(frameID)
		return pg
	}

	frameID, isFromFreeList := b.getFrameID()
	if frameID == nil {
		return nil
	}

	if !isFromFreeList {
		// 現在のフレームからページを削除する
		currentPage := b.pages[*frameID]
		if currentPage != nil {
			if currentPage.IsDirty() {
				data := currentPage.Data()
				b.diskManager.WritePage(currentPage.ID(), data[:])
			}

			delete(b.pageTable, currentPage.ID())
		}
	}

	data := make([]byte, page.PageSize)
	err := b.diskManager.ReadPage(pageID, data)
	if err != nil {
		return nil
	}
	var pageData [page.PageSize]byte
	copy(pageData[:], data)
	pg := page.New(pageID, false, &pageData)
	b.pageTable[pageID] = *frameID
	b.pages[*frameID] = pg

	return pg
}

// バッファプールからターゲットページのピンを外す
func (b *BufferPoolManager) UnpinPage(pageID types.PageID, isDirty bool) error {
	if frameID, ok := b.pageTable[pageID]; ok {
		pg := b.pages[frameID]
		pg.DecPinCount()

		if pg.PinCount() <= 0 {
			(*b.replacer).Unpin(frameID)
		}

		if pg.IsDirty() || isDirty {
			pg.SetIsDirty(true)
		} else {
			pg.SetIsDirty(false)
		}

		return nil
	}

	return errors.New("could not find page")
}

// ターゲットページをフラッシュする(ディスクへ書き込む)
func (b *BufferPoolManager) FlushPage(pageID types.PageID) bool {
	if frameID, ok := b.pageTable[pageID]; ok {
		pg := b.pages[frameID]
		pg.DecPinCount()

		data := pg.Data()
		b.diskManager.WritePage(pageID, data[:])
		pg.SetIsDirty(false)

		return true
	}

	return false
}

// バッファプールとディスクマネージャーに新しいページを割り当てる。
func (b *BufferPoolManager) NewPage() *page.Page {
	frameID, isFromFreeList := b.getFrameID()
	if frameID == nil {
		// バッファが一杯になると、フレームが見つからなくなる
		return nil
	}

	if !isFromFreeList {
		// 現在のフレームからページを削除する
		currentPage := b.pages[*frameID]
		if currentPage != nil {
			if currentPage.IsDirty() {
				data := currentPage.Data()
				b.diskManager.WritePage(currentPage.ID(), data[:])
			}

			delete(b.pageTable, currentPage.ID())
		}
	}

	pageID := b.diskManager.AllocatePage()
	pg := page.NewEmpty(pageID)

	b.pageTable[pageID] = *frameID
	b.pages[*frameID] = pg

	return pg
}

// バッファプールからページを削除する
func (b *BufferPoolManager) DeletePage(pageID types.PageID) error {
	var frameID FrameID
	var ok bool
	if frameID, ok = b.pageTable[pageID]; !ok {
		return nil
	}

	page := b.pages[frameID]

	if page.PinCount() > 0 {
		return errors.New("Pin count greater than 0")
	}
	delete(b.pageTable, page.ID())
	(*b.replacer).Pin(frameID)
	b.diskManager.DeallocatePage(pageID)

	b.freeList = append(b.freeList, frameID)

	return nil

}

// バッファプール内の全ページをフラッシュする(ディスクに書き込む)
func (b *BufferPoolManager) FlushAllpages() {
	for pageID := range b.pageTable {
		b.FlushPage(pageID)
	}
}

func (b *BufferPoolManager) getFrameID() (*FrameID, bool) {
	if len(b.freeList) > 0 {
		frameID, newFreeList := b.freeList[0], b.freeList[1:]
		b.freeList = newFreeList

		return &frameID, true
	}

	return (*b.replacer).Victim(), false
}

// 空のバッファプールマネージャを生成する
func NewBufferPoolManager(poolSize uint32, DiskManager disk.DiskManager) *BufferPoolManager {
	freeList := make([]FrameID, poolSize)
	pages := make([]*page.Page, poolSize)
	for i := uint32(0); i < poolSize; i++ {
		freeList[i] = FrameID(i)
		pages[i] = nil
	}

	replacer := NewClockReplacer(poolSize)
	return &BufferPoolManager{DiskManager, pages, replacer, freeList, make(map[types.PageID]FrameID)}
}
