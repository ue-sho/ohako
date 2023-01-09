package index

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestPairByte(t *testing.T) {
	// given
	pair := NewPair([]byte("hello"), []byte("world"))

	// when
	bytes := pair.ToBytes()
	newPair := NewPairFromBytes(bytes)

	// then
	testingpkg.Equals(t, "hello", string(newPair.Key))
	testingpkg.Equals(t, "world", string(newPair.Value))
}
