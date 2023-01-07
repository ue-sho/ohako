package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
		{5, 3},  // 5と一致している、次のPageIDを参照
		{6, 3},  // 5と一致している、次のPageIDを参照
		{8, 4},  // 5と一致している、次のPageIDを参照
		{10, 4}, // 5と一致している、次のPageIDを参照
		{11, 2}, // 5と一致している、次のPageIDを参照
		{12, 2}, // 5と一致している、次のPageIDを参照
	}
	for _, tt := range tests {
		actual := internalNode.SearchChild(uint64ToBytes(tt.key))
		fmt.Println("internalNode.SearchChild ", tt.key, tt.pageID)

		testingpkg.Equals(t, tt.pageID, actual)
	}

}

func TestInterNewInternalNode(t *testing.T) {
	uint64ToBytes := func(n uint64) []byte {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n)
		return buf[:]
	}

	t.Run("Insert", func(t *testing.T) {

	})

	t.Run("Split", func(t *testing.T) {
		var err error

		data := make([]byte, 100)
		internalNode := NewInternalNode(data)

		internalNode.Initialize(uint64ToBytes(5), page.PageID(1), page.PageID(2))
		err = internalNode.Insert(1, uint64ToBytes(8), page.PageID(3))
		if err != nil {
			panic(err)
		}
		err = internalNode.Insert(2, uint64ToBytes(11), page.PageID(4))
		if err != nil {
			panic(err)
		}

		data2 := make([]byte, 100)
		internalNode2 := NewInternalNode(data2)
		{
			midKey := internalNode.SplitInsert(internalNode2, uint64ToBytes(10), page.PageID(5))
			expect := uint64ToBytes(8)
			if !bytes.Equal(midKey, expect) {
				t.Fatalf("internalNode.SplitInsert() = %v, want %v", midKey, expect)
			}
		}
		{
			actual := internalNode.NumPairs()
			expect := 2
			if actual != expect {
				t.Fatalf("internalNode.NumPairs() = %v, want %v", actual, expect)
			}
		}
		{
			actual := internalNode2.NumPairs()
			expect := 1
			if actual != expect {
				t.Fatalf("internalNode2.NumPairs() = %v, want %v", actual, expect)
			}
		}
		{
			tests := []struct {
				key          uint64
				pageIDPageID page.PageID
			}{
				{1, 1},
				{5, 3},
				{6, 3},
			}
			for _, tt := range tests {
				actual := internalNode2.SearchChild(uint64ToBytes(tt.key))
				if actual != tt.pageIDPageID {
					t.Fatalf("internalNode2.SearchChild(%v) = %v, want %v", tt.key, actual, tt.pageIDPageID)
				}
			}
		}
		{
			tests := []struct {
				key          uint64
				pageIDPageID page.PageID
			}{
				{9, 5},
				{10, 4},
				{11, 2},
				{12, 2},
			}
			for _, tt := range tests {
				actual := internalNode.SearchChild(uint64ToBytes(tt.key))
				if actual != tt.pageIDPageID {
					t.Fatalf("internalNode.SearchChild(%v) = %v, want %v", tt.key, actual, tt.pageIDPageID)
				}
			}
		}
	})
}
