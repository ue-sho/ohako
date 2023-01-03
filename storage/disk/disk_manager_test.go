package disk

import (
	"testing"

	"github.com/ue-sho/ohako/storage/page"
	testingpkg "github.com/ue-sho/ohako/testing"
	"github.com/ue-sho/ohako/types"
)

func TestReadWritePage(t *testing.T) {
	// given: pageID=0 に 適当な文字列を書き込む
	dm := NewDiskManagerTest()
	defer dm.ShutDown()

	data := make([]byte, page.PageSize)
	buffer := make([]byte, page.PageSize)

	copy(data, "A test string.")

	// when: dataを書き込み、bufferに読み出しする
	dm.ReadPage(0, buffer) // 空読みでエラーが出ない
	dm.WritePage(0, data)
	dm.ReadPage(0, buffer)

	// then: ページサイズは4KB, 読み込み書き込みができていること
	testingpkg.Equals(t, int64(4096), dm.Size())
	testingpkg.Equals(t, data, buffer)
}

func TestPageSize(t *testing.T) {
	// given: pageID=0, 5に適当な文字列を書き込む
	dm := NewDiskManagerTest()
	defer dm.ShutDown()

	data := make([]byte, page.PageSize)
	buffer := make([]byte, page.PageSize)

	copy(data, "A test string.")
	dm.WritePage(0, data)
	dm.ReadPage(0, buffer)

	memset(buffer, 0)
	copy(data, "Another test string.")
	dm.WritePage(5, data)
	dm.ReadPage(5, buffer)

	// when
	size := dm.Size()

	// then: 6ページ(ID: 0~5)あるのでディスクサイズは24576バイトになる
	testingpkg.Equals(t, data, buffer)
	testingpkg.Equals(t, int64(24576), size)
}

func TestAllocatePage(t *testing.T) {
	// given
	dm := NewDiskManagerTest()
	defer dm.ShutDown()

	// when
	pageId_1 := dm.AllocatePage()
	pageId_2 := dm.AllocatePage()
	pageId_3 := dm.AllocatePage()

	// then: インクリメントしてIDが割り当てられる
	testingpkg.Equals(t, types.PageID(0), pageId_1)
	testingpkg.Equals(t, types.PageID(1), pageId_2)
	testingpkg.Equals(t, types.PageID(2), pageId_3)
}

func memset(buffer []byte, value int) {
	for i := range buffer {
		buffer[i] = 0
	}
}
