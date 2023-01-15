package table

import "github.com/ue-sho/ohako/storage/index"

type TupleSearchMode interface {
	Encode() index.SearchMode
}

type TupleSearchModeStart struct {
}

// B+treeのSearchModeにエンコードする
func (ts *TupleSearchModeStart) Encode() index.SearchMode {
	return &index.SearchModeStart{}
}

type TupleSearchModeKey struct {
	Key [][]byte
}

// B+treeのSearchModeにエンコードする
func (ts *TupleSearchModeKey) Encode() index.SearchMode {
	return &index.SearchModeKey{Key: EncodeTuple(ts.Key)}
}
