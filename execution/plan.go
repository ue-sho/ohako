package execution

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/table"
)

type WhileCondFunc func(table.Tuple) bool

type PlanNode interface {
	// 実行計画開始
	Start(bufmgr *buffer.BufferPoolManager) (Executor, error)

	// 実行計画説明
	Explain() []string
}
