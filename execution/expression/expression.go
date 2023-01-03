package expression

import (
	"github.com/ue-sho/ohako/storage/table"
	"github.com/ue-sho/ohako/types"
)

type Expression interface {
	Evaluate(*table.Tuple, *table.Schema) types.Value
}
