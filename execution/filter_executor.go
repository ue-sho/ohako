package execution

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/table"
)

type ExecFilter struct {
	innerIter Executor
	cond      WhileCondFunc
}

func (ef *ExecFilter) Next(bufmgr *buffer.BufferPoolManager) (table.Tuple, error) {
	for {
		tuple, err := ef.innerIter.Next(bufmgr)
		if err != nil {
			return nil, err
		}
		if (ef.cond)(tuple) {
			return tuple, nil
		}
	}
}

func (ef *ExecFilter) Finish(bufmgr *buffer.BufferPoolManager) {
	ef.innerIter.Finish(bufmgr)
}
