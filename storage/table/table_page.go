package table

import (
	"unsafe"

	"github.com/ue-sho/ohako/errors"
	"github.com/ue-sho/ohako/storage/page"
	"github.com/ue-sho/ohako/types"
)

const sizeTablePageHeader = uint32(24)
const sizeTuple = uint32(8)
const offSetPrevPageId = uint32(8)
const offSetNextPageId = uint32(12)
const offsetFreeSpace = uint32(16)
const offSetTupleCount = uint32(20)
const offsetTupleOffset = uint32(24)
const offsetTupleSize = uint32(28)

const ErrEmptyTuple = errors.Error("tuple cannot be empty")
const ErrNotEnoughSpace = errors.Error("there is not enough space")
const ErrNoFreeSlot = errors.Error("could not find a free slot")

// Slotted page format:
//  ---------------------------------------------------------
//  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
//  ---------------------------------------------------------
//                                ^
//                                free space pointer
//  Header format (size in bytes):
//  ----------------------------------------------------------------------------
//  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
//  ----------------------------------------------------------------------------
//  ----------------------------------------------------------------
//  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
//  ----------------------------------------------------------------
type TablePage struct {
	page.Page
}

// CastPageAsTablePage casts the abstract Page struct into TablePage
func CastPageAsTablePage(page *page.Page) *TablePage {
	if page == nil {
		return nil
	}
	return (*TablePage)(unsafe.Pointer(page))
}

// Inserts a tuple into the table
func (tp *TablePage) InsertTuple(tuple *Tuple) (*page.RID, error) {
	if tuple.Size() == 0 {
		return nil, ErrEmptyTuple
	}

	if tp.getFreeSpaceRemaining() < tuple.Size()+sizeTuple {
		return nil, ErrNotEnoughSpace
	}

	var slot uint32

	// try to find a free slot
	for slot = uint32(0); slot < tp.GetTupleCount(); slot++ {
		if tp.GetTupleSize(slot) == 0 {
			break
		}
	}

	if tp.GetTupleCount() == slot && tuple.Size()+sizeTuple > tp.getFreeSpaceRemaining() {
		return nil, ErrNoFreeSlot
	}

	tp.SetFreeSpacePointer(tp.GetFreeSpacePointer() - tuple.Size())
	tp.setTuple(slot, tuple)

	rid := &page.RID{}
	rid.Set(tp.GetTablePageId(), slot)
	if slot == tp.GetTupleCount() {
		tp.SetTupleCount(tp.GetTupleCount() + 1)
	}

	return rid, nil
}

// Init initializes the table header
func (tp *TablePage) Init(pageId types.PageID, prevPageId types.PageID) {
	tp.SetPageId(pageId)
	tp.SetPrevPageId(prevPageId)
	tp.SetNextPageId(types.InvalidPageID)
	tp.SetTupleCount(0)
	tp.SetFreeSpacePointer(page.PageSize) // point to the end of the page
}

func (tp *TablePage) SetPageId(pageId types.PageID) {
	tp.Copy(0, pageId.Serialize())
}

func (tp *TablePage) SetPrevPageId(pageId types.PageID) {
	tp.Copy(offSetPrevPageId, pageId.Serialize())
}

func (tp *TablePage) SetNextPageId(pageId types.PageID) {
	tp.Copy(offSetNextPageId, pageId.Serialize())
}

func (tp *TablePage) SetFreeSpacePointer(freeSpacePointer uint32) {
	tp.Copy(offsetFreeSpace, types.UInt32(freeSpacePointer).Serialize())
}

func (tp *TablePage) SetTupleCount(tupleCount uint32) {
	tp.Copy(offSetTupleCount, types.UInt32(tupleCount).Serialize())
}

func (tp *TablePage) setTuple(slot uint32, tuple *Tuple) {
	fsp := tp.GetFreeSpacePointer()
	tp.Copy(fsp, tuple.data)                                                        // copy tuple to data starting at free space pointer
	tp.Copy(offsetTupleOffset+sizeTuple*slot, types.UInt32(fsp).Serialize())        // set tuple offset at slot
	tp.Copy(offsetTupleSize+sizeTuple*slot, types.UInt32(tuple.Size()).Serialize()) // set tuple size at slot
}

func (tp *TablePage) GetTablePageId() types.PageID {
	return types.NewPageIDFromBytes(tp.Data()[:])
}

func (tp *TablePage) GetNextPageId() types.PageID {
	return types.NewPageIDFromBytes(tp.Data()[offSetNextPageId:])
}

func (tp *TablePage) GetTupleCount() uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offSetTupleCount:]))
}

func (tp *TablePage) GetTupleOffsetAtSlot(slot uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleOffset+sizeTuple*slot:]))
}

func (tp *TablePage) GetTupleSize(slot uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleSize+sizeTuple*slot:]))
}

func (tp *TablePage) getFreeSpaceRemaining() uint32 {
	return tp.GetFreeSpacePointer() - sizeTablePageHeader - sizeTuple*tp.GetTupleCount()
}

func (tp *TablePage) GetFreeSpacePointer() uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetFreeSpace:]))
}

func (tp *TablePage) GetTuple(rid *page.RID) *Tuple {
	slot := rid.GetSlot()
	tupleOffset := tp.GetTupleOffsetAtSlot(slot)
	tupleSize := tp.GetTupleSize(slot)

	tupleData := make([]byte, tupleSize)
	copy(tupleData, tp.Data()[tupleOffset:])

	return &Tuple{rid, tupleSize, tupleData}
}

func (tp *TablePage) GetTupleFirstRID() *page.RID {
	firstRID := &page.RID{}

	tupleCount := tp.GetTupleCount()
	for i := uint32(0); i < tupleCount; i++ {
		// if is deleted
		firstRID.Set(tp.GetTablePageId(), i)
		return firstRID
	}
	return nil
}

func (tp *TablePage) GetNextTupleRID(rid *page.RID) *page.RID {
	firstRID := &page.RID{}

	tupleCount := tp.GetTupleCount()
	for i := rid.GetSlot() + 1; i < tupleCount; i++ {
		// if is deleted
		firstRID.Set(tp.GetTablePageId(), i)
		return firstRID
	}
	return nil
}
