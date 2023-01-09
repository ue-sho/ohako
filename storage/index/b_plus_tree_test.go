package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/disk"
	"github.com/ue-sho/ohako/storage/page"
)

// BTreeの中身をlogに出力
func (t *BPlusTree) dump(bufmgr *buffer.BufferPoolManager) {
	metaBuffer := bufmgr.FetchPage(t.MetaPageId)
	defer bufmgr.UnpinPage(metaBuffer.ID(), false)
	meta := NewMeta(metaBuffer.Data()[:])

	rootPageId := meta.header.rootPageId
	rootBuffer := bufmgr.FetchPage(rootPageId)
	defer bufmgr.UnpinPage(rootBuffer.ID(), false)

	t.dumpInternal(bufmgr, rootBuffer)
}

func (t *BPlusTree) dumpInternal(bufmgr *buffer.BufferPoolManager, buffer *page.Page) {
	node := NewNode(buffer.Data()[:])
	switch node.header.nodeType {
	case NodeTypeLeaf:
		leaf := NewLeafNode(node.body)
		fmt.Println("***** [leaf] ", buffer.ID())
		fmt.Print("leaf.PrevPageId() = ")
		fmt.Println(leaf.PrevPageId())
		fmt.Print("leaf.NextPageId() = ")
		fmt.Println(leaf.NextPageId())
		for i := 0; i < leaf.NumPairs(); i++ {
			pair := leaf.PairAt(i)
			fmt.Printf("leaf.PairAt(%d) = ", i)
			fmt.Println(pair.Key[:1], pair.Value[:1])
		}
	case NodeTypeInternal:
		branch := NewInternalNode(node.body)
		fmt.Println("***** [branch] ", buffer.ID())
		for i := 0; i < branch.NumPairs(); i++ {
			pair := branch.PairAt(i)
			fmt.Printf("leaf.PairAt(%d) = ", i)
			fmt.Println(pair.Key[:1], pair.Value[:1])
		}
		fmt.Print("branch.header.rightChild = ")
		fmt.Println(branch.header.rightChild)
		for i := 0; i < branch.NumPairs(); i++ {
			func() {
				childPageId := branch.ChildAt(i)
				childNodeBuffer := bufmgr.FetchPage(childPageId)
				defer bufmgr.UnpinPage(childNodeBuffer.ID(), false)

				t.dumpInternal(bufmgr, childNodeBuffer)
			}()
		}
		childNodeBuffer := bufmgr.FetchPage(branch.header.rightChild)
		defer bufmgr.UnpinPage(childNodeBuffer.ID(), false)
		t.dumpInternal(bufmgr, childNodeBuffer)
	default:
		panic("?")
	}
}

func TestBTree(t *testing.T) {
	uint64ToBytes := func(n uint64) []byte {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n)
		return buf[:]
	}

	t.Run("Search", func(t *testing.T) {
		disk := disk.NewDiskManagerTest()
		defer disk.ShutDown()

		bufmgr := buffer.NewBufferPoolManager(10, disk)

		btree, err := CreateBPlusTree(bufmgr)
		if err != nil {
			panic(err)
		}

		err = btree.Insert(bufmgr, uint64ToBytes(6), []byte("world"))
		if err != nil {
			panic(err)
		}
		err = btree.Insert(bufmgr, uint64ToBytes(3), []byte("hello"))
		if err != nil {
			panic(err)
		}
		err = btree.Insert(bufmgr, uint64ToBytes(8), []byte("!"))
		if err != nil {
			panic(err)
		}
		err = btree.Insert(bufmgr, uint64ToBytes(4), []byte(","))
		if err != nil {
			panic(err)
		}
		{
			iter, err := btree.Search(bufmgr, &SearchModeKey{uint64ToBytes(3)})
			if err != nil {
				panic(err)
			}
			defer iter.Finish(bufmgr)

			_, value, err := iter.Get()
			if err != nil {
				panic(err)
			}
			expect := []byte("hello")
			if !bytes.Equal(expect, value) {
				t.Fatalf("btree.search() = %v, want = %v", value, expect)
			}
		}
		{
			iter, err := btree.Search(bufmgr, &SearchModeKey{uint64ToBytes(8)})
			if err != nil {
				panic(err)
			}
			defer iter.Finish(bufmgr)

			_, value, err := iter.Get()
			if err != nil {
				panic(err)
			}
			expect := []byte("!")
			if !bytes.Equal(expect, value) {
				t.Fatalf("btree.search() = %v, want = %v", value, expect)
			}
		}
	})

	t.Run("Split", func(t *testing.T) {
		arrayRepeat := func(value byte, length int) []byte {
			longData := make([]byte, length)
			for j := 0; j < length; j++ {
				longData[j] = value
			}
			return longData
		}

		disk := disk.NewDiskManagerTest()
		defer disk.ShutDown()

		bufmgr := buffer.NewBufferPoolManager(5, disk)

		btree, err := CreateBPlusTree(bufmgr)
		if err != nil {
			panic(err)
		}

		longDataList := [][]byte{
			arrayRepeat(0xC0, 1000),
			arrayRepeat(0x01, 1000),
			arrayRepeat(0xCA, 1000),
			arrayRepeat(0xFE, 1000),
			arrayRepeat(0xDE, 1000),
			arrayRepeat(0xAD, 1000),
			arrayRepeat(0xBE, 1000),
			arrayRepeat(0xAE, 1000),
		}

		for _, data := range longDataList {
			// fmt.Println("=============== ", i)
			err := btree.Insert(bufmgr, data, data)
			if err != nil {
				panic(err)
			}
			// btree.dump(bufmgr)
		}

		// 先頭からすべて検索
		func() {
			sortedLongDataList := longDataList
			sort.SliceStable(sortedLongDataList, func(i, j int) bool {
				return sortedLongDataList[i][0] < sortedLongDataList[j][0]
			})

			iter, err := btree.Search(bufmgr, &SearchModeStart{})
			if err != nil {
				panic(err)
			}
			defer iter.Finish(bufmgr)

			i := 0
			for {
				k, v, err := iter.Next(bufmgr)
				if err != nil {
					break
				}

				data := sortedLongDataList[i]
				if !bytes.Equal(data, k) {
					t.Fatalf("bytes.Equal(data, k) = %v, want = %v", k[0], data[0])
				}
				if !bytes.Equal(data, v) {
					t.Fatalf("bytes.Equal(data, v) = %v, want = %v", v[0], data[0])
				}
				i++
			}
		}()

		// 個別に検索
		for _, data := range longDataList {
			func() {
				iter, err := btree.Search(bufmgr, &SearchModeKey{data})
				if err != nil {
					panic(err)
				}
				defer iter.Finish(bufmgr)

				k, v, err := iter.Get()
				if err != nil {
					panic(err)
				}
				if !bytes.Equal(data, k) {
					t.Fatalf("bytes.Equal(data, k) = %v, want = %v", k[0], data[0])
				}
				if !bytes.Equal(data, v) {
					t.Fatalf("bytes.Equal(data, v) = %v, want = %v", v[0], data[0])
				}
			}()
		}
	})

	t.Run("Insert: キーが重複", func(t *testing.T) {
		disk := disk.NewDiskManagerTest()
		defer disk.ShutDown()

		bufmgr := buffer.NewBufferPoolManager(10, disk)

		btree, err := CreateBPlusTree(bufmgr)
		if err != nil {
			panic(err)
		}

		if err = btree.Insert(bufmgr, uint64ToBytes(6), []byte("world")); err != nil {
			panic(err)
		}
		if err = btree.Insert(bufmgr, uint64ToBytes(6), []byte("world")); err == nil {
			t.Fatalf("btree.Insert() = %v, want ErrDuplicateKey", err)
		}
	})
}
