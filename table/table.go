package table

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/index"
	"github.com/ue-sho/ohako/storage/page"
)

type Table struct {
	MetaPageId    page.PageID
	NumKeyElems   int // 左からいくつの列がプライマリキーなのかを表す
	UniqueIndices []UniqueIndex
}

// テーブルを作成する
func (t *Table) Create(bufmgr *buffer.BufferPoolManager) error {
	tree, err := index.CreateBPlusTree(bufmgr)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId

	for i := range t.UniqueIndices {
		t.UniqueIndices[i].Create(bufmgr)
	}
	return nil
}

// レコードを挿入する
func (t *Table) Insert(bufmgr *buffer.BufferPoolManager, record [][]byte) error {
	tree := index.NewBPlusTree(t.MetaPageId)
	key := EncodeTuple(record[:t.NumKeyElems])
	value := EncodeTuple(record[t.NumKeyElems:])
	if err := tree.Insert(bufmgr, key, value); err != nil {
		return err
	}
	for _, uniqueIndex := range t.UniqueIndices {
		err := uniqueIndex.Insert(bufmgr, key, record)
		if err != nil {
			return err
		}
	}
	return nil
}
