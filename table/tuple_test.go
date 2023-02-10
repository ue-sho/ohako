package table

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestTuple(t *testing.T) {
	// given
	org := [][]byte{
		[]byte("helloworld!memcmpable"),
		[]byte("foobarbazhogehuga"),
	}

	// when
	enc := EncodeTuple(org)

	// then
	elems := make([][]byte, 0)
	elems = DecodeTuple(enc, elems)
	for i, r := range elems {
		testingpkg.Equals(t, r, org[i])
	}
}
