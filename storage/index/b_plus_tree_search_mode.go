package index

import "github.com/ue-sho/ohako/storage/page"

type SearchMode interface {
	childPageId(internalNode *InternalNode) page.PageID
	tupleSlotId(leaf *LeafNode) (int, bool)
}

type SearchModeStart struct {
}

// 子PageIDを取得する
// スタートなので、slotId=0から探す
func (s *SearchModeStart) childPageId(internalNode *InternalNode) page.PageID {
	return internalNode.ChildAt(0)
}

// slotIdを取得する
// スタートなので、0を返す
func (s *SearchModeStart) tupleSlotId(leaf *LeafNode) (int, bool) {
	return 0, false
}

type SearchModeKey struct {
	Key []byte
}

// 指定KeyのPageIDを取得する
func (s *SearchModeKey) childPageId(internalNode *InternalNode) page.PageID {
	return internalNode.SearchChild(s.Key)
}

// 指定KeyのslotIdを取得する
func (s *SearchModeKey) tupleSlotId(leaf *LeafNode) (int, bool) {
	return leaf.SearchSlotId(s.Key)
}
