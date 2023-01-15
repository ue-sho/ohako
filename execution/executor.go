package execution

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/table"
)

type Executor interface {
	// 次の実行を進める
	Next(bufmgr *buffer.BufferPoolManager) (table.Tuple, error)

	// 終了処理
	Finish(bufmgr *buffer.BufferPoolManager)
}
