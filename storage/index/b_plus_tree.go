package index

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/page"
	"golang.org/x/xerrors"
)

type SearchMode interface {
	childPageId(internalNode *InternalNode) page.PageID
	tupleSlotId(leaf *LeafNode) (int, bool)
}

type SearchModeStart struct {
}

func (s *SearchModeStart) childPageId(internalNode *InternalNode) page.PageID {
	return internalNode.ChildAt(0)
}

func (s *SearchModeStart) tupleSlotId(leaf *LeafNode) (int, bool) {
	return 0, false
}

type SearchModeKey struct {
	Key []byte
}

func (s *SearchModeKey) childPageId(internalNode *InternalNode) page.PageID {
	return internalNode.SearchChild(s.Key)
}

func (s *SearchModeKey) tupleSlotId(leaf *LeafNode) (int, bool) {
	return leaf.SearchSlotId(s.Key)
}

type BPlusTree struct {
	MetaPageId page.PageID
}

func CreateBPlusTree(bufmgr *buffer.BufferPoolManager) (*BPlusTree, error) {
	metaBuffer := bufmgr.NewPage()
	defer bufmgr.UnpinPage(metaBuffer.ID(), false)
	meta := NewMeta(metaBuffer.Data()[:])

	rootBuffer := bufmgr.NewPage()
	defer bufmgr.UnpinPage(rootBuffer.ID(), false)

	root := NewNode(rootBuffer.Data()[:])
	root.SetNodeType(NodeTypeLeaf)

	leaf := NewLeafNode(root.body)
	leaf.Initialize()

	meta.header.rootPageId = rootBuffer.ID()
	return NewBPlusTree(metaBuffer.ID()), nil
}

func NewBPlusTree(metaPageId page.PageID) *BPlusTree {
	return &BPlusTree{metaPageId}
}

func (t *BPlusTree) ReadMetaAppArea(bufmgr *buffer.BufferPoolManager) ([]byte, error) {
	metaBuffer := t.fetchMetaPage(bufmgr)
	defer bufmgr.UnpinPage(metaBuffer.ID(), false)

	meta := NewMeta(metaBuffer.Data()[:])
	data := make([]byte, *(meta.appAreaLength))
	copy(data, meta.appArea)
	return data, nil
}

func (t *BPlusTree) WriteMetaAppArea(bufmgr *buffer.BufferPoolManager, data []byte) error {
	metaBuffer := t.fetchMetaPage(bufmgr)
	defer bufmgr.UnpinPage(metaBuffer.ID(), true)

	meta := NewMeta(metaBuffer.Data()[:])
	if len(meta.appArea) < len(data) {
		return xerrors.Errorf("too long data")
	}
	copy(meta.appArea, data)
	*(meta.appAreaLength) = uint64(len(data))
	return nil
}

func (t *BPlusTree) fetchMetaPage(bufmgr *buffer.BufferPoolManager) *page.Page {
	metaBuffer := bufmgr.FetchPage(t.MetaPageId)
	return metaBuffer
}

func (t *BPlusTree) fetchRootPage(bufmgr *buffer.BufferPoolManager) (*page.Page, error) {
	metaBuffer := bufmgr.FetchPage(t.MetaPageId)
	defer bufmgr.UnpinPage(metaBuffer.ID(), false)

	meta := NewMeta(metaBuffer.Data()[:])
	rootPageId := meta.header.rootPageId
	rootBuffer := bufmgr.FetchPage(rootPageId)
	return rootBuffer, nil
}

func (t *BPlusTree) searchInternal(bufmgr *buffer.BufferPoolManager, nodeBuffer *page.Page, searchMode SearchMode) (*BPlusTreeIter, error) {
	node := NewNode(nodeBuffer.Data()[:])
	switch node.header.nodeType {
	case NodeTypeLeaf:
		leaf := NewLeafNode(node.body)
		slotId, _ := searchMode.tupleSlotId(leaf)
		node = nil
		return &BPlusTreeIter{nodeBuffer, slotId}, nil
	case NodeTypeInternal:
		internalNode := NewInternalNode(node.body)
		childPageId := searchMode.childPageId(internalNode)
		node = nil
		bufmgr.UnpinPage(nodeBuffer.ID(), false)
		childNodePage := bufmgr.FetchPage(childPageId)
		return t.searchInternal(bufmgr, childNodePage, searchMode)
	default:
		panic("unreachable")
	}
}

func (t *BPlusTree) Search(bufmgr *buffer.BufferPoolManager, searchMode SearchMode) (*BPlusTreeIter, error) {
	rootPage, err := t.fetchRootPage(bufmgr)
	if err != nil {
		return nil, err
	}
	return t.searchInternal(bufmgr, rootPage, searchMode)
}

func (t *BPlusTree) insertInternal(bufmgr *buffer.BufferPoolManager, buffer *page.Page, key []byte, value []byte) (bool, []byte, page.PageID, error) {
	node := NewNode(buffer.Data()[:])
	switch node.header.nodeType {
	case NodeTypeLeaf:
		leaf := NewLeafNode(node.body)
		slotId, result := leaf.SearchSlotId(key)
		if result {
			return false, nil, page.InvalidPageID, xerrors.New("duplicate key")
		}
		if err := leaf.Insert(slotId, key, value); err == nil {
			buffer.SetIsDirty(true)
			return false, nil, page.InvalidPageID, nil
		} else {
			// overflowした場合
			// 新しいleafのBufferを作成
			newLeafBuffer := bufmgr.NewPage()
			defer bufmgr.UnpinPage(newLeafBuffer.ID(), true)

			// leaf.prevLeafとleafの間に入れる
			prevLeafPageId, err := leaf.PrevPageId()
			if err == nil {
				prevLeafBuffer := bufmgr.FetchPage(prevLeafPageId)
				defer bufmgr.UnpinPage(prevLeafBuffer.ID(), true)

				node := NewNode(prevLeafBuffer.Data()[:])
				prefLeaf := NewLeafNode(node.body)
				prefLeaf.SetNextPageId(newLeafBuffer.ID())
				prevLeafBuffer.SetIsDirty(true)
			}

			leaf.SetPrevPageId(newLeafBuffer.ID())

			// 新しいleafを初期化
			// leafと新しいleafにSplitInsert
			newLeafNode := NewNode(newLeafBuffer.Data()[:])
			newLeafNode.SetNodeType(NodeTypeLeaf)
			newLeaf := NewLeafNode(newLeafNode.body)
			newLeaf.Initialize()
			overflowKey := leaf.SplitInsert(newLeaf, key, value)
			newLeaf.SetNextPageId(buffer.ID())
			newLeaf.SetPrevPageId(prevLeafPageId)
			buffer.SetIsDirty(true)
			return true, overflowKey, newLeafBuffer.ID(), nil
		}

	case NodeTypeInternal:
		internalNode := NewInternalNode(node.body)
		childIdx := internalNode.SearchChildIdx(key)
		childPageId := internalNode.ChildAt(childIdx)
		childNodeBuffer := bufmgr.FetchPage(childPageId)
		defer bufmgr.UnpinPage(childNodeBuffer.ID(), true)

		overflow, overflowKeyFromChild, overflowChildPageId, err := t.insertInternal(bufmgr, childNodeBuffer, key, value)
		if err != nil {
			return false, nil, page.InvalidPageID, err
		}
		if overflow {
			// overflowした場合
			// branchにInsert
			if err := internalNode.Insert(childIdx, overflowKeyFromChild, overflowChildPageId); err == nil {
				buffer.SetIsDirty(true)
				return false, nil, page.InvalidPageID, nil
			} else {
				// それも入りきらなかった場合
				// 新しいbranchを作成し、SplitInsert
				newBranchBuffer := bufmgr.NewPage()
				defer bufmgr.UnpinPage(newBranchBuffer.ID(), true)

				newBranchNode := NewNode(newBranchBuffer.Data()[:])
				newBranchNode.SetNodeType(NodeTypeInternal)
				NewInternalNode := NewInternalNode(newBranchNode.body)
				overflowKey := internalNode.SplitInsert(NewInternalNode, overflowKeyFromChild, overflowChildPageId)
				buffer.SetIsDirty(true)
				newBranchBuffer.SetIsDirty(true)
				return true, overflowKey, newBranchBuffer.ID(), nil
			}
		} else {
			return false, nil, page.InvalidPageID, nil
		}

	default:
		panic("unreachable")
	}
}

func (t *BPlusTree) Insert(bufmgr *buffer.BufferPoolManager, key []byte, value []byte) error {
	metaBuffer := bufmgr.FetchPage(t.MetaPageId)
	defer bufmgr.UnpinPage(metaBuffer.ID(), true)
	meta := NewMeta(metaBuffer.Data()[:])

	rootPageId := meta.header.rootPageId
	rootBuffer := bufmgr.FetchPage(rootPageId)
	defer bufmgr.UnpinPage(rootBuffer.ID(), true)

	overflow, key, childPageId, err := t.insertInternal(bufmgr, rootBuffer, key, value)
	if err != nil {
		return err
	}
	if overflow {
		// overflowした場合
		// rootの下に新しいbranchを作成
		newRootBuffer := bufmgr.NewPage()
		defer bufmgr.UnpinPage(newRootBuffer.ID(), true)

		node := NewNode(newRootBuffer.Data()[:])
		node.SetNodeType(NodeTypeInternal)
		internalNode := NewInternalNode(node.body)
		internalNode.Initialize(key, childPageId, rootPageId)
		meta.header.rootPageId = newRootBuffer.ID()
		metaBuffer.SetIsDirty(true)
	}
	return nil
}

type BPlusTreeIter struct {
	buffer *page.Page
	slotId int
}

func (it *BPlusTreeIter) Get() ([]byte, []byte, error) {
	leafNode := NewNode(it.buffer.Data()[:])
	leaf := NewLeafNode(leafNode.body)
	if it.slotId < leaf.NumPairs() {
		pair := leaf.PairAt(it.slotId)
		return pair.Key, pair.Value, nil
	}
	return nil, nil, xerrors.New("end of iterator")
}

func (it *BPlusTreeIter) Next(bufmgr *buffer.BufferPoolManager) ([]byte, []byte, error) {
	key, value, err := it.Get()
	if err != nil {
		return nil, nil, err
	}

	it.slotId++
	leafNode := NewNode(it.buffer.Data()[:])
	leaf := NewLeafNode(leafNode.body)
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

func (it *BPlusTreeIter) Finish(bufmgr *buffer.BufferPoolManager) {
	bufmgr.UnpinPage(it.buffer.ID(), false)
}
