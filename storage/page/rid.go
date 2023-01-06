package page

// 与えられたページ識別子とスロット番号に対応するレコード識別子である
type RID struct {
	pageId  PageID
	slotNum uint32
}

// RIDのsetter
func (r *RID) Set(pageId PageID, slot uint32) {
	r.pageId = pageId
	r.slotNum = slot
}

// ページIDのgetter
func (r *RID) GetPageId() PageID {
	return r.pageId
}

// スロット番号のgetter
func (r *RID) GetSlot() uint32 {
	return r.slotNum
}
