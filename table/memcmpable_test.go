package table

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestMemCmpAble(t *testing.T) {
	// given:
	org1 := []byte("helloworld!memcmpable")
	org2 := []byte("foobarbazhogehuga")

	// when: enc変数にエンコードしたデータを入れる
	encSize := MemcomparableEncodedSize(len(org1)) + MemcomparableEncodedSize(len(org2))
	enc := make([]byte, 0, encSize) // 長さ0でcapacity=encSize

	enc = MemcomparableEncode(org1, enc)
	enc = MemcomparableEncode(org2, enc)
	rest := enc

	// then
	dec1 := make([]byte, 0, len(rest))
	rest, dec1 = MemcomparableDecode(rest, dec1)
	testingpkg.Equals(t, org1, dec1)

	dec2 := make([]byte, 0, len(rest))
	_, dec2 = MemcomparableDecode(rest, dec2)
	testingpkg.Equals(t, org2, dec2)
}
