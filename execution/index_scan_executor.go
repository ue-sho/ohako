package execution

import (
	"errors"

	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/index"
	"github.com/ue-sho/ohako/table"
)

type IndexScanExecutor struct {
	tableTree *index.BPlusTree
	indexIter *index.BPlusTreeIter
	whileCond WhileCondFunc
}

func (es *IndexScanExecutor) Next(bufmgr *buffer.BufferPoolManager) (table.Tuple, error) {
	// セカンダリインデックスの検索を進める
	skeyBytes, pkeyBytes, err := es.indexIter.Next(bufmgr)
	if err != nil {
		return nil, err
	}
	skey := [][]byte{}
	skey = table.DecodeTuple(skeyBytes, skey)
	if !(es.whileCond)(skey) {
		return nil, errors.New("end of iterator")
	}

	// プライマリキーでテーブルを検索
	tableIter, err := es.tableTree.Search(bufmgr, &index.SearchModeKey{Key: pkeyBytes})
	if err != nil {
		return nil, err
	}
	defer tableIter.Finish(bufmgr)

	pkeyBytes, tupleBytes, err := tableIter.Next(bufmgr)
	if err != nil {
		return nil, err
	}
	tuple := [][]byte{}
	tuple = table.DecodeTuple(pkeyBytes, tuple)
	tuple = table.DecodeTuple(tupleBytes, tuple)
	return tuple, nil
}

func (es *IndexScanExecutor) Finish(bufmgr *buffer.BufferPoolManager) {
	es.indexIter.Finish(bufmgr)
}
