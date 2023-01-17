package execution

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/index"
	"github.com/ue-sho/ohako/storage/page"
	"github.com/ue-sho/ohako/table"
)

type IndexScanPlanNode struct {
	TableMetaPageId page.PageID
	IndexMetaPageId page.PageID
	SearchMode      table.TupleSearchMode
	WhileCond       WhileCondFunc
}

func (s *IndexScanPlanNode) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	tableTree := index.NewBPlusTree(s.TableMetaPageId)
	indexTree := index.NewBPlusTree(s.IndexMetaPageId)
	indexIter, err := indexTree.Search(bufmgr, s.SearchMode.Encode())
	if err != nil {
		return nil, err
	}
	return &IndexScanExecutor{
		tableTree,
		indexIter,
		s.WhileCond,
	}, nil
}

func (s *IndexScanPlanNode) Explain() []string {
	return []string{"IndexScan"}
}
