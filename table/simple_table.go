package table

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/index"
	"github.com/ue-sho/ohako/storage/page"
)

type SimpleTable struct {
	MetaPageId  page.PageID
	NumKeyElems int // 左からいくつの列がプライマリキーなのかを表す
}

// シンプルなテーブルを作成する
func (t *SimpleTable) Create(bufmgr *buffer.BufferPoolManager) error {
	tree, err := index.CreateBPlusTree(bufmgr)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId
	return nil
}

// レコードを挿入する
func (t *SimpleTable) Insert(bufmgr *buffer.BufferPoolManager, record Tuple) error {
	tree := index.NewBPlusTree(t.MetaPageId)
	key := EncodeTuple(record[:t.NumKeyElems])
	value := EncodeTuple(record[t.NumKeyElems:])
	if err := tree.Insert(bufmgr, key, value); err != nil {
		return err
	}
	return nil
}
