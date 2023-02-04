package index

import (
	"bytes"
	"errors"
	"unsafe"

	"github.com/ue-sho/ohako/storage/page"
	"golang.org/x/xerrors"
)

type LeafNodeHeader struct {
	prevPageId page.PageID
	nextPageId page.PageID
}

type LeafNode struct {
	header *LeafNodeHeader
	body   *Slotted
}

// LeafNodeを作成する
func NewLeafNode(bytes []byte) *LeafNode {
	leaf := LeafNode{}
	headerSize := int(unsafe.Sizeof(*leaf.header))
	if headerSize+1 > len(bytes) {
		panic("leaf header must be aligned")
	}

	leaf.header = (*LeafNodeHeader)(unsafe.Pointer(&bytes[0]))
	leaf.body = NewSlotted(bytes[headerSize:])
	return &leaf
}

// 前のページID
func (l *LeafNode) PrevPageId() (page.PageID, error) {
	if !l.header.prevPageId.IsValid() {
		return page.InvalidPageID, errors.New("invalid page id")
	}
	return l.header.prevPageId, nil
}

// 次のページID
func (l *LeafNode) NextPageId() (page.PageID, error) {
	if !l.header.nextPageId.IsValid() {
		return page.InvalidPageID, errors.New("invalid page id")
	}
	return l.header.nextPageId, nil
}

// Pairの数
func (l *LeafNode) NumPairs() int {
	return l.body.NumSlots()
}

// 指定keyを持ったslotIdを探す
// 見つからなかった場合、挿入されるべき場所のslotIdを返す
func (l *LeafNode) SearchSlotId(key []byte) (int, bool) {
	return BinarySearchBy(l.NumPairs(), func(slotId int) int {
		return bytes.Compare(l.PairAt(slotId).Key, key)
	})
}

// slotIdからPairを取得する
func (l *LeafNode) PairAt(slotId int) *Pair {
	data := l.body.ReadData(slotId)
	return NewPairFromBytes(data)
}

// MaxPairSizeを取得する
func (l *LeafNode) MaxPairSize() int {
	return l.body.Capacity()/2 - int(unsafe.Sizeof(Pointer{}))
}

// LeafNodeの初期化を行う
func (l *LeafNode) Initialize() {
	l.header.prevPageId = page.InvalidPageID
	l.header.nextPageId = page.InvalidPageID
	l.body.Initialize()
}

// 前のページIDを更新する
func (l *LeafNode) SetPrevPageId(prevPageId page.PageID) {
	l.header.prevPageId = prevPageId
}

// 次のページIDを更新する
func (l *LeafNode) SetNextPageId(nextPageId page.PageID) {
	l.header.nextPageId = nextPageId
}

// 指定slotIdにデータを挿入する
func (l *LeafNode) Insert(slotId int, key []byte, value []byte) error {
	pair := Pair{Key: key, Value: value}
	pairBytes := pair.ToBytes()
	if len(pairBytes) > l.MaxPairSize() {
		return errors.New("too long data")
	}
	err := l.body.Insert(slotId, len(pairBytes))
	if err != nil {
		return err
	}
	l.body.WriteData(slotId, pairBytes)
	return nil
}

// Slotの容量が半分を超えているか否か
func (l *LeafNode) isHalfFull() bool {
	return 2*l.body.FreeSpace() < l.body.Capacity()
}

// 自LeafNodeのデータを新規LeafNodeの容量が半分になるまで分割挿入する
func (l *LeafNode) SplitInsert(newLeaf *LeafNode, newKey []byte, newValue []byte) []byte {
	newLeaf.Initialize()
	for {
		if newLeaf.isHalfFull() {
			index, result := l.SearchSlotId(newKey)
			if result {
				panic("key must be unique")
			}
			err := l.Insert(index, newKey, newValue)
			if err != nil {
				panic(xerrors.Errorf("old leaf must have space: %v", err))
			}
			break
		}
		if bytes.Compare(l.PairAt(0).Key, newKey) < 0 {
			l.Transfer(newLeaf)
		} else {
			err := newLeaf.Insert(newLeaf.NumPairs(), newKey, newValue)
			if err != nil {
				panic(xerrors.Errorf("new leaf must have space: %v", err))
			}
			for !newLeaf.isHalfFull() {
				l.Transfer(newLeaf)
			}
			break
		}
	}
	return l.PairAt(0).Key
}

// 先頭データを指定InternalNodeの末尾に移動させる
func (l *LeafNode) Transfer(dest *LeafNode) {
	nextIndex := dest.NumPairs()
	srcBody := l.body.ReadData(0)
	err := dest.body.Insert(nextIndex, len(srcBody))
	if err != nil {
		panic(err)
	}
	dest.body.WriteData(nextIndex, srcBody)
	l.body.Remove(0)
}
