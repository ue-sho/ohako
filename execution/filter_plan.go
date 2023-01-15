package execution

import (
	"github.com/ue-sho/ohako/storage/buffer"
)

type FilterPlanNode struct {
	InnerPlan PlanNode
	Cond      WhileCondFunc
}

func (f *FilterPlanNode) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	innerIter, err := f.InnerPlan.Start(bufmgr)
	if err != nil {
		return nil, err
	}
	return &ExecFilter{
		innerIter,
		f.Cond,
	}, nil
}

func (f *FilterPlanNode) Explain() (ret []string) {
	ret = []string{"Filter"}
	ret = append(ret, f.InnerPlan.Explain()...)
	return
}
