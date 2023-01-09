package index

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/page"
	"golang.org/x/xerrors"
)

type BPlusTreeIter struct {
	buffer *page.Page
	slotId int
}

// key, valueを取得する
func (it *BPlusTreeIter) Get() ([]byte, []byte, error) {
	leafNode := NewNode(it.buffer.Data()[:])
	leaf := NewLeafNode(leafNode.body)
	if it.slotId < leaf.NumPairs() {
		pair := leaf.PairAt(it.slotId)
		return pair.Key, pair.Value, nil
	}
	return nil, nil, xerrors.New("end of iterator")
}

// 次のイテレータに進む
func (it *BPlusTreeIter) Next(bufmgr *buffer.BufferPoolManager) ([]byte, []byte, error) {
	key, value, err := it.Get()
	if err != nil {
		return nil, nil, err
	}

	it.slotId++
	node := NewNode(it.buffer.Data()[:])
	leaf := NewLeafNode(node.body)
	if it.slotId < leaf.NumPairs() {
		return key, value, nil
	}
	nextPageId, err := leaf.NextPageId()
	if err != nil {
		bufmgr.UnpinPage(it.buffer.ID(), false)
		it.buffer = bufmgr.FetchPage(nextPageId)
		it.slotId = 0
	}
	return key, value, nil
}

// 終了処理
func (it *BPlusTreeIter) Finish(bufmgr *buffer.BufferPoolManager) {
	bufmgr.UnpinPage(it.buffer.ID(), false)
}
