package execution

import (
	"errors"

	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/index"
	"github.com/ue-sho/ohako/table"
)

type SeqScanExecutor struct {
	tableIter *index.BPlusTreeIter
	whileCond WhileCondFunc
}

func (es *SeqScanExecutor) Next(bufmgr *buffer.BufferPoolManager) (table.Tuple, error) {
	pkeyBytes, tupleBytes, err := es.tableIter.Next(bufmgr)
	if err != nil {
		return nil, err
	}
	pkey := [][]byte{}
	pkey = table.DecodeTuple(pkeyBytes, pkey)
	if !(es.whileCond)(pkey) {
		return nil, errors.New("end of iterator")
	}
	tuple := pkey
	tuple = table.DecodeTuple(tupleBytes, tuple)
	return tuple, nil
}

func (es *SeqScanExecutor) Finish(bufmgr *buffer.BufferPoolManager) {
	es.tableIter.Finish(bufmgr)
}
