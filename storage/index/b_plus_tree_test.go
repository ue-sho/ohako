package index

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/disk"
	"github.com/ue-sho/ohako/storage/page"
	testingpkg "github.com/ue-sho/ohako/testing"
)

// BPlusTreeの中身を出力
func (t *BPlusTree) dump(bufmgr *buffer.BufferPoolManager) {
	metaBuffer := bufmgr.FetchPage(t.metaPageId)
	defer bufmgr.UnpinPage(metaBuffer.ID(), false)
	meta := NewMeta(metaBuffer.Data()[:])

	rootPageId := meta.header.rootPageId
	rootBuffer := bufmgr.FetchPage(rootPageId)
	defer bufmgr.UnpinPage(rootBuffer.ID(), false)

	t.dumpNode(bufmgr, rootBuffer)
}

// Node情報を出力
func (t *BPlusTree) dumpNode(bufmgr *buffer.BufferPoolManager, buffer *page.Page) {
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

				t.dumpNode(bufmgr, childNodeBuffer)
			}()
		}
		childNodeBuffer := bufmgr.FetchPage(branch.header.rightChild)
		defer bufmgr.UnpinPage(childNodeBuffer.ID(), false)
		t.dumpNode(bufmgr, childNodeBuffer)
	default:
		panic("?")
	}
}

func TestBPlusTree_Insert_Search(t *testing.T) {
	// given
	disk := disk.NewDiskManagerTest()
	defer disk.ShutDown()

	bufmgr := buffer.NewBufferPoolManager(10, disk)
	bPlusTree, err := CreateBPlusTree(bufmgr)
	if err != nil {
		panic(err)
	}

	err = bPlusTree.Insert(bufmgr, testingpkg.Uint64ToBytes(6), []byte("world"))
	if err != nil {
		panic(err)
	}
	err = bPlusTree.Insert(bufmgr, testingpkg.Uint64ToBytes(3), []byte("hello"))
	if err != nil {
		panic(err)
	}
	err = bPlusTree.Insert(bufmgr, testingpkg.Uint64ToBytes(8), []byte("!"))
	if err != nil {
		panic(err)
	}
	err = bPlusTree.Insert(bufmgr, testingpkg.Uint64ToBytes(4), []byte(","))
	if err != nil {
		panic(err)
	}

	// when
	// key=3のデータを取得
	iter1, err := bPlusTree.Search(bufmgr, &SearchModeKey{testingpkg.Uint64ToBytes(3)})
	if err != nil {
		panic(err)
	}
	defer iter1.Finish(bufmgr)

	_, value1, err := iter1.Get()
	if err != nil {
		panic(err)
	}

	iter2, err := bPlusTree.Search(bufmgr, &SearchModeKey{testingpkg.Uint64ToBytes(8)})
	if err != nil {
		panic(err)
	}
	defer iter2.Finish(bufmgr)

	_, value2, err := iter2.Get()
	if err != nil {
		panic(err)
	}

	// then
	testingpkg.Equals(t, []byte("hello"), value1)
	testingpkg.Equals(t, []byte("!"), value2)
}

func TestBPlusTreeSplitSearch(t *testing.T) {
	// given: 長さ1000のデータを複数挿入する
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
	bPlusTree, err := CreateBPlusTree(bufmgr)
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
		err := bPlusTree.Insert(bufmgr, data, data)
		if err != nil {
			panic(err)
		}
		// bPlusTree.dump(bufmgr)
	}

	// when: 先頭からすべて検索
	iter, err := bPlusTree.Search(bufmgr, &SearchModeStart{})
	if err != nil {
		panic(err)
	}

	// tehn: ソートされた順番で取り出される
	sortedLongDataList := longDataList
	sort.SliceStable(sortedLongDataList, func(i, j int) bool {
		return sortedLongDataList[i][0] < sortedLongDataList[j][0]
	})
	for i := 0; ; i++ {
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
	}
	iter.Finish(bufmgr)

	// then: 個別に検索しても取り出すことができる
	for _, data := range longDataList {
		iter, err := bPlusTree.Search(bufmgr, &SearchModeKey{data})
		if err != nil {
			panic(err)
		}

		k, v, err := iter.Get()
		if err != nil {
			panic(err)
		}
		testingpkg.Equals(t, data, k)
		testingpkg.Equals(t, data, v)

		iter.Finish(bufmgr)
	}

}

func TestInsert_DuplicateKey(t *testing.T) {
	// given
	disk := disk.NewDiskManagerTest()
	defer disk.ShutDown()

	bufmgr := buffer.NewBufferPoolManager(10, disk)
	bPlusTree, err := CreateBPlusTree(bufmgr)
	if err != nil {
		panic(err)
	}

	// when: 同じkeyを挿入する
	err1 := bPlusTree.Insert(bufmgr, testingpkg.Uint64ToBytes(6), []byte("world"))
	err2 := bPlusTree.Insert(bufmgr, testingpkg.Uint64ToBytes(6), []byte("world"))

	// then: 2つ目はエラーとなる
	testingpkg.Equals(t, true, err1 == nil)
	testingpkg.Equals(t, false, err2 == nil)
}
