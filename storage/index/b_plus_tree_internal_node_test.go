package index

import (
	"encoding/binary"
	"testing"

	"github.com/ue-sho/ohako/storage/page"
	testingpkg "github.com/ue-sho/ohako/testing"
)

func uint64ToBytes(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf[:]
}

func TestInternalNodeInsert(t *testing.T) {
	// given
	data := make([]byte, 100)
	internalNode := NewInternalNode(data)

	// { key=5, value=PageID(1) } を挿入し、rightNodeはPageID(2)を参照させる
	internalNode.Initialize(uint64ToBytes(5), page.PageID(1), page.PageID(2))

	// when:
	// { key=8, value=PageID(3) } を挿入する
	err := internalNode.Insert(1, uint64ToBytes(8), page.PageID(3))
	if err != nil {
		panic(err)
	}
	// { key=11, value=PageID(4) } を挿入する
	err = internalNode.Insert(2, uint64ToBytes(11), page.PageID(4))
	if err != nil {
		panic(err)
	}

	// then
	tests := []struct {
		key    uint64
		pageID page.PageID
	}{
		// 挿入されたkey [ 5, 8, 11 ]
		{1, 1},  // key=5より小さいので、5のPageIDを参照
		{5, 3},  // key=5と一致しているので、5の次のPageIDを参照
		{6, 3},  // key=5より大きく、key=8より小さいので、8のPageIDを参照
		{8, 4},  // key=8と一致しているので、8の次のPageIDを参照
		{10, 4}, // key=8より大きく、key=11より小さいので、11のPageIDを参照
		{11, 2}, // key=11と一致しているので、11の次のPageIDを参照 → RightNodeを参照する
		{12, 2}, // 挿入された値の中で最大値のkey=11より大きいので、RightNodeを参照する
	}
	for _, tt := range tests {
		actual := internalNode.SearchChild(uint64ToBytes(tt.key))
		testingpkg.Equals(t, tt.pageID, actual)
	}

}

func TestInternalNodeSplit(t *testing.T) {
	// given
	data := make([]byte, 100)
	internalNode := NewInternalNode(data)

	// { key=5, value=PageID(1) } を挿入し、rightNodeはPageID(2)を参照させる
	internalNode.Initialize(uint64ToBytes(5), page.PageID(1), page.PageID(2))

	// { key=8, value=PageID(3) } を挿入する
	err := internalNode.Insert(1, uint64ToBytes(8), page.PageID(3))
	if err != nil {
		panic(err)
	}
	// { key=11, value=PageID(4) } を挿入する
	err = internalNode.Insert(2, uint64ToBytes(11), page.PageID(4))
	if err != nil {
		panic(err)
	}

	// 2つ目のInternalNodeを作成する
	data2 := make([]byte, 100)
	internalNode2 := NewInternalNode(data2)

	// when: { key=10, value=PageID(5) }を起点に分割挿入する
	midKey := internalNode.SplitInsert(internalNode2, uint64ToBytes(10), page.PageID(5))

	// then: 分割の分岐点はkey=8 (8がRootとなり分割される)
	// 初期のInternalNodeのペア数は2, 新規InternalNodeのペア数は1に分割される
	testingpkg.Equals(t, midKey, uint64ToBytes(8))
	testingpkg.Equals(t, 2, internalNode.NumPairs())
	testingpkg.Equals(t, 1, internalNode2.NumPairs())

	tests := []struct {
		key    uint64
		pageID page.PageID
	}{
		// 挿入されたkey [ 10, 11 ]
		{9, 5},
		{10, 4},
		{11, 2},
		{12, 2},
	}
	for _, tt := range tests {
		actual := internalNode.SearchChild(uint64ToBytes(tt.key))
		if actual != tt.pageID {
			t.Fatalf("internalNode.SearchChild(%v) = %v, want %v", tt.key, actual, tt.pageID)
		}
	}

	tests = []struct {
		key    uint64
		pageID page.PageID
	}{
		// 挿入されたkey [ 5 ]
		{1, 1},
		{5, 3},
		{6, 3},
	}
	for _, tt := range tests {
		actual := internalNode2.SearchChild(uint64ToBytes(tt.key))
		if actual != tt.pageID {
			t.Fatalf("internalNode2.SearchChild(%v) = %v, want %v", tt.key, actual, tt.pageID)
		}
	}

}
