package plans

import "github.com/ue-sho/ohako/storage/table"

type PlanType int

const (
	SeqScan PlanType = iota
	Insert
	Limit
)

type Plan interface {
	OutputSchema() *table.Schema
	GetChildAt(childIndex uint32) Plan
	GetChildren() []Plan
	GetType() PlanType
}

type AbstractPlanNode struct {
	outputSchema *table.Schema
	children     []Plan
}

func (p *AbstractPlanNode) GetChildAt(childIndex uint32) Plan {
	return p.children[childIndex]
}

func (p *AbstractPlanNode) GetChildren() []Plan {
	return p.children
}

func (p *AbstractPlanNode) OutputSchema() *table.Schema {
	return p.outputSchema
}
