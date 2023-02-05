package index

import (
	"bytes"
	"errors"
	"fmt"
	"unsafe"

	"github.com/ue-sho/ohako/storage/page"
)

type InternalNodeHeader struct {
	rightChild page.PageID
}

type InternalNode struct {
	header *InternalNodeHeader
	body   *Slotted
}

// InternalNodeを作成する
func NewInternalNode(bytes []byte) *InternalNode {
	internalNode := InternalNode{}
	headerSize := int(unsafe.Sizeof(*internalNode.header))
	if headerSize+1 > len(bytes) {
		fmt.Println("Internal header must be aligned")
		return nil
	}

	internalNode.header = (*InternalNodeHeader)(unsafe.Pointer(&bytes[0]))
	internalNode.body = NewSlotted(bytes[headerSize:])
	return &internalNode
}

// Pairの数
func (b *InternalNode) NumPairs() int {
	return b.body.NumSlots()
}

// 指定keyを持ったslotIdを探す
// 見つからなかった場合、挿入されるべき場所のslotIdを返す
func (b *InternalNode) SearchSlotId(key []byte) (int, bool) {
	return BinarySearchBy(b.NumPairs(), func(slotId int) int {
		return bytes.Compare(b.PairAt(slotId).Key, key)
	})
}

// 子Nodeから指定keyのPageIDを取得する
func (b *InternalNode) SearchChild(key []byte) page.PageID {
	childIdx := b.SearchChildIdx(key)
	return b.ChildAt(childIdx)
}

// 指定keyから子NodeのSlotIdを探す
func (b *InternalNode) SearchChildIdx(key []byte) int {
	slotId, result := b.SearchSlotId(key)
	if result {
		return slotId + 1
	} else {
		return slotId
	}
}

// 子NodeのslotIdからPageIDを取得する
func (b *InternalNode) ChildAt(childIdx int) page.PageID {
	if childIdx == b.NumPairs() {
		return b.header.rightChild
	} else {
		return page.NewPageIDFromBytes(b.PairAt(childIdx).Value)
	}
}

// slotIdからPairを取得する
func (b *InternalNode) PairAt(slotId int) *Pair {
	data := b.body.ReadData(slotId)
	return NewPairFromBytes(data)
}

// MaxPairSizeを取得する
func (b *InternalNode) MaxPairSize() int {
	return b.body.Capacity()/2 - int(unsafe.Sizeof(Pointer{}))
}

// InternalNodeの初期化を行う
func (b *InternalNode) Initialize(key []byte, leftChild page.PageID, rightChild page.PageID) error {
	b.body.Initialize()
	err := b.Insert(0, key, leftChild)
	if err != nil {
		return errors.New("new leaf must have space")
	}
	b.header.rightChild = rightChild
	return nil
}

// 末尾のデータを右の子として参照する
// 末尾データは削除する
func (b *InternalNode) FillRightChild() []byte {
	lastId := b.NumPairs() - 1
	pair := b.PairAt(lastId)
	rightChild := page.NewPageIDFromBytes(pair.Value)
	b.body.Remove(lastId)
	b.header.rightChild = rightChild
	return pair.Key
}

// 指定slotIdにデータを挿入する
func (b *InternalNode) Insert(slotId int, key []byte, pageId page.PageID) error {
	pair := Pair{Key: key, Value: pageId.Serialize()}
	pairBytes := pair.ToBytes()
	if len(pairBytes) > b.MaxPairSize() {
		return errors.New("too long data")
	}
	err := b.body.Insert(slotId, len(pairBytes))
	if err != nil {
		return err
	}
	b.body.WriteData(slotId, pairBytes)
	return nil
}

// Slotの容量が半分を超えているか否か
func (b *InternalNode) isHalfFull() bool {
	return 2*b.body.FreeSpace() < b.body.Capacity()
}

// 自InternalNodeのデータを新規InternalNodeの容量が半分になるまで分割挿入する
func (b *InternalNode) SplitInsert(newInternal *InternalNode, newKey []byte, newPageId page.PageID) ([]byte, error) {
	newInternal.body.Initialize()
	for {
		if newInternal.isHalfFull() {
			index, result := b.SearchSlotId(newKey)
			if result {
				return nil, errors.New("key must be unique")
			}
			err := b.Insert(index, newKey, newPageId)
			if err != nil {
				fmt.Println(err)
				return nil, errors.New("old Internal must have space")
			}
			break
		}
		if bytes.Compare(b.PairAt(0).Key, newKey) < 0 {
			b.Transfer(newInternal)
		} else {
			err := newInternal.Insert(newInternal.NumPairs(), newKey, newPageId)
			if err != nil {
				fmt.Println(err)
				return nil, errors.New("new Internal must have space")
			}
			for !newInternal.isHalfFull() {
				b.Transfer(newInternal)
			}
			break
		}
	}
	return newInternal.FillRightChild(), nil
}

// 先頭データを指定InternalNodeの末尾に移動させる
func (b *InternalNode) Transfer(dest *InternalNode) error {
	nextIndex := dest.NumPairs()
	srcBody := b.body.ReadData(0)
	err := dest.body.Insert(nextIndex, len(srcBody))
	if err != nil {
		fmt.Println(err)
		return errors.New("no space in dest Internal")
	}
	dest.body.WriteData(nextIndex, srcBody)
	b.body.Remove(0)
	return nil
}
