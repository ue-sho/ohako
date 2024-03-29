package index

import (
	"errors"

	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/page"
)

type BPlusTree struct {
	MetaPageId page.PageID // メタ情報が書くのされたページのID
}

// メタ情報のページIDからBPlusTreeを生成する
func NewBPlusTree(metaPageId page.PageID) *BPlusTree {
	return &BPlusTree{metaPageId}
}

// BPlusTreeインスタンスを生成する
func CreateBPlusTree(bufmgr *buffer.BufferPoolManager) (*BPlusTree, error) {
	metaPage := bufmgr.NewPage()
	if metaPage == nil {
		return nil, errors.New("failed to retrieve new page")
	}
	defer bufmgr.UnpinPage(metaPage.ID(), false)

	meta := NewMeta(metaPage.Data()[:])
	if meta == nil {
		return nil, errors.New("meta page creation failed")
	}

	rootPage := bufmgr.NewPage()
	if metaPage == nil {
		return nil, errors.New("failed to retrieve new page")
	}
	defer bufmgr.UnpinPage(rootPage.ID(), false)

	root := NewNode(rootPage.Data()[:])
	if meta == nil {
		return nil, errors.New("root page creation failed")
	}

	root.SetNodeType(NodeTypeLeaf)

	leaf := NewLeafNode(root.body)
	if leaf == nil {
		return nil, errors.New("leaf node creation failed")
	}
	leaf.Initialize()

	meta.header.rootPageId = rootPage.ID()
	return NewBPlusTree(metaPage.ID()), nil
}

// メタデータからアプリケーションで使う保存領域を読み出す
func (t *BPlusTree) ReadMetaAppArea(bufmgr *buffer.BufferPoolManager) ([]byte, error) {
	metaBuffer := t.fetchMetaPage(bufmgr)
	defer bufmgr.UnpinPage(metaBuffer.ID(), false)

	meta := NewMeta(metaBuffer.Data()[:])
	data := make([]byte, *(meta.appAreaLength))
	copy(data, meta.appArea)
	return data, nil
}

// メタデータのアプリケーションを使う保存領域に書き込む
func (t *BPlusTree) WriteMetaAppArea(bufmgr *buffer.BufferPoolManager, data []byte) error {
	metaBuffer := t.fetchMetaPage(bufmgr)
	defer bufmgr.UnpinPage(metaBuffer.ID(), true)

	meta := NewMeta(metaBuffer.Data()[:])
	if len(meta.appArea) < len(data) {
		return errors.New("too long data")
	}
	copy(meta.appArea, data)
	*(meta.appAreaLength) = uint64(len(data))
	return nil
}

// メタページを取得する
func (t *BPlusTree) fetchMetaPage(bufmgr *buffer.BufferPoolManager) *page.Page {
	metaBuffer := bufmgr.FetchPage(t.MetaPageId)
	return metaBuffer
}

// ルートページを取得する
func (t *BPlusTree) fetchRootPage(bufmgr *buffer.BufferPoolManager) (*page.Page, error) {
	metaBuffer := bufmgr.FetchPage(t.MetaPageId)
	if metaBuffer == nil {
		return nil, errors.New("failed to fetch root page")
	}
	defer bufmgr.UnpinPage(metaBuffer.ID(), false)

	meta := NewMeta(metaBuffer.Data()[:])
	rootPageId := meta.header.rootPageId
	rootBuffer := bufmgr.FetchPage(rootPageId)
	return rootBuffer, nil
}

// 引数page(Node)からsearchModeで指定されたデータ見つかるまで再帰で探す
func (t *BPlusTree) searchNode(bufmgr *buffer.BufferPoolManager, page *page.Page, searchMode SearchMode) (*BPlusTreeIter, error) {
	node := NewNode(page.Data()[:])
	if node == nil {
		return nil, errors.New("node creation failed")
	}

	switch node.header.nodeType {
	case NodeTypeLeaf:
		leaf := NewLeafNode(node.body)
		slotId, _ := searchMode.tupleSlotId(leaf)
		node = nil
		return &BPlusTreeIter{page, slotId}, nil
	case NodeTypeInternal:
		internalNode := NewInternalNode(node.body)
		childPageId := searchMode.childPageId(internalNode)
		node = nil
		bufmgr.UnpinPage(page.ID(), false)
		childNodePage := bufmgr.FetchPage(childPageId)
		return t.searchNode(bufmgr, childNodePage, searchMode)
	default:
		return nil, errors.New("unreachable")
	}
}

// B+TreeからsearchModeで指定されたデータを探す
func (t *BPlusTree) Search(bufmgr *buffer.BufferPoolManager, searchMode SearchMode) (*BPlusTreeIter, error) {
	rootPage, err := t.fetchRootPage(bufmgr)
	if err != nil {
		return nil, err
	}
	return t.searchNode(bufmgr, rootPage, searchMode)
}

// 各NodeでのB+treeの挿入
func (t *BPlusTree) insertNode(bufmgr *buffer.BufferPoolManager, buffer *page.Page, key []byte, value []byte) (bool, []byte, page.PageID, error) {
	node := NewNode(buffer.Data()[:])
	if node == nil {
		return false, nil, page.InvalidPageID, errors.New("node creation failed")
	}

	switch node.header.nodeType {
	case NodeTypeLeaf:
		leaf := NewLeafNode(node.body)
		slotId, result := leaf.SearchSlotId(key)
		if result {
			return false, nil, page.InvalidPageID, errors.New("duplicate key")
		}

		if err := leaf.Insert(slotId, key, value); err == nil {
			buffer.SetIsDirty(true)
			return false, nil, page.InvalidPageID, nil
		} else {
			// overflowした場合
			// 新しいLeafNodeを作成
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
			overflowKey, err := leaf.SplitInsert(newLeaf, key, value)
			if err != nil {
				return false, nil, page.InvalidPageID, err
			}
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

		overflow, overflowKeyFromChild, overflowChildPageId, err := t.insertNode(bufmgr, childNodeBuffer, key, value)
		if err != nil {
			return false, nil, page.InvalidPageID, err
		}

		if overflow {
			// internalNodeにInsert
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
				overflowKey, err := internalNode.SplitInsert(NewInternalNode, overflowKeyFromChild, overflowChildPageId)
				if err != nil {
					return false, nil, page.InvalidPageID, err
				}
				buffer.SetIsDirty(true)
				newBranchBuffer.SetIsDirty(true)
				return true, overflowKey, newBranchBuffer.ID(), nil
			}
		} else {
			return false, nil, page.InvalidPageID, nil
		}

	default:
		return false, nil, page.InvalidPageID, errors.New("unreachable")
	}
}

// 指定されたkey, valueをB+Treeに挿入する
func (t *BPlusTree) Insert(bufmgr *buffer.BufferPoolManager, key []byte, value []byte) error {
	metaBuffer := bufmgr.FetchPage(t.MetaPageId)
	defer bufmgr.UnpinPage(metaBuffer.ID(), true)
	meta := NewMeta(metaBuffer.Data()[:])

	rootPageId := meta.header.rootPageId
	rootBuffer := bufmgr.FetchPage(rootPageId)
	defer bufmgr.UnpinPage(rootBuffer.ID(), true)

	overflow, key, childPageId, err := t.insertNode(bufmgr, rootBuffer, key, value)
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
