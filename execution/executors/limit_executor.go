package executors

import (
	"github.com/ue-sho/ohako/execution/plans"
	"github.com/ue-sho/ohako/storage/table"
)

// LimitExecutor implements the limit/offset operation
type LimitExecutor struct {
	context *ExecutorContext
	plan    *plans.LimitPlanNode // contains information about limit and offset
	child   Executor             // the child executor that will provide tuples to the limit executor
	emitted uint32               // counts the number of tuples processed. It is compared to the LIMIT
	skipped uint32               // counts the number of tuples skiped. It is compared to the OFFSET
}

func NewLimitExecutor(context *ExecutorContext, plan *plans.LimitPlanNode, child Executor) Executor {
	return &LimitExecutor{context, plan, child, 0, 0}
}

func (e *LimitExecutor) Init() {
	e.child.Init()
}

func (e *LimitExecutor) Next() (*table.Tuple, Done, error) {
	tuple, done, err := e.child.Next()
	if err != nil {
		return nil, done, err
	}

	if e.skipped < e.plan.GetOffset() {
		e.skipped++
		return nil, false, nil
	}

	e.emitted++
	if e.emitted > e.plan.GetLimit() {
		return nil, true, nil
	}

	return tuple, false, nil
}
