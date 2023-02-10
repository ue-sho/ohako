package table

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/index"
	"github.com/ue-sho/ohako/storage/page"
)

// セカンダリインデックス用 (重複は許容しない)
type UniqueIndex struct {
	MetaPageId page.PageID
	SKey       []int // セカンダリキーを含める列を指定する
}

// Index(B+tree)を作成する
func (idx *UniqueIndex) Create(bufmgr *buffer.BufferPoolManager) error {
	tree, err := index.CreateBPlusTree(bufmgr)
	if err != nil {
		return err
	}
	idx.MetaPageId = tree.MetaPageId
	return nil
}

// キーとレコードを挿入する
func (idx *UniqueIndex) Insert(bufmgr *buffer.BufferPoolManager, pkey []byte, record [][]byte) error {
	tree := index.NewBPlusTree(idx.MetaPageId)

	skeyElems := [][]byte{}
	for _, k := range idx.SKey {
		skeyElems = append(skeyElems, record[k])
	}
	skey := EncodeTuple(skeyElems)

	// key=セカンダリキー, value=プライマリーキーを挿入
	if err := tree.Insert(bufmgr, skey, pkey); err != nil {
		return err
	}
	return nil
}
