package execution

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/index"
	"github.com/ue-sho/ohako/storage/page"
	"github.com/ue-sho/ohako/table"
)

type SeqScanPlanNode struct {
	TableMetaPageId page.PageID
	SearchMode      table.TupleSearchMode
	WhileCond       WhileCondFunc
}

func (s *SeqScanPlanNode) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	tree := index.NewBPlusTree(s.TableMetaPageId)
	tableIter, err := tree.Search(bufmgr, s.SearchMode.Encode())
	if err != nil {
		return nil, err
	}
	return &SeqScanExecutor{
		tableIter,
		s.WhileCond,
	}, nil
}

func (s *SeqScanPlanNode) Explain() []string {
	return []string{"SeqScan"}
}
