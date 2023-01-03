package expression

import (
	"github.com/ue-sho/ohako/storage/table"
	"github.com/ue-sho/ohako/types"
)

type ConstantValue struct {
	value types.Value
}

func NewConstantValue(value types.Value) Expression {
	return &ConstantValue{value}
}

func (c *ConstantValue) Evaluate(tuple *table.Tuple, schema *table.Schema) types.Value {
	return c.value
}
