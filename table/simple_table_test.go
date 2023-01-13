package table

import (
	"os"
	"testing"

	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/disk"
	"github.com/ue-sho/ohako/storage/index"
	"github.com/ue-sho/ohako/storage/page"
	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestSimpleTableCreateInsert(t *testing.T) {
	// given
	file, err := os.Create("simple.ohk")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	poolSize := uint32(10)
	dm := disk.NewDiskManagerImpl(file.Name())
	defer dm.ShutDown()
	bufmgr := buffer.NewBufferPoolManager(poolSize, dm)

	// テーブル作成
	tbl := SimpleTable{
		MetaPageId:  page.InvalidPageID,
		NumKeyElems: 1,
	}
	err = tbl.Create(bufmgr)
	if err != nil {
		panic(err)
	}

	rows := [][][]byte{
		{[]byte("z"), []byte("Alice"), []byte("Smith")},
		{[]byte("x"), []byte("Bob"), []byte("Johnson")},
		{[]byte("y"), []byte("Charlie"), []byte("Williams")},
		{[]byte("w"), []byte("Dave"), []byte("Miller")},
		{[]byte("v"), []byte("Eve"), []byte("Brown")},
	}
	// 書き込みを行う
	for _, row := range rows {
		// when
		err = tbl.Insert(bufmgr, row)

		// then: エラーが発生しない
		testingpkg.Equals(t, nil, err)
	}
	bufmgr.FlushAllpages()
}

// TestSimpleTableCreateInsert()で作成したデータからテストするため、必ず一緒に行う
func TestSimpleTableExact(t *testing.T) {
	// given: TestSimpleTableCreateInsert作成したファイルからDiskManagerを作成
	dm := disk.NewDiskManagerImpl("simple.ohk")
	defer dm.ShutDown()
	poolSize := uint32(10)
	bufmgr := buffer.NewBufferPoolManager(poolSize, dm)

	tree := index.NewBPlusTree(page.PageID(0)) // metaPageIDはテーブルがひとつしかない場合は基本0になる

	// when: key=xのデータを探す
	searchKey := EncodeTuple([][]byte{[]byte("x")})
	iter, err := tree.Search(bufmgr, &index.SearchModeKey{Key: searchKey})
	if err != nil {
		panic(err)
	}
	defer iter.Finish(bufmgr)

	// then: key=x ~ 最後まで(z)を探索する
	tests := []struct {
		key    []byte
		value1 []byte
		value2 []byte
	}{
		{[]byte("x"), []byte("Bob"), []byte("Johnson")},
		{[]byte("y"), []byte("Charlie"), []byte("Williams")},
		{[]byte("z"), []byte("Alice"), []byte("Smith")},
	}
	for _, tt := range tests {
		key, value, err := iter.Next(bufmgr)
		if err != nil {
			break
		}
		record := make([][]byte, 0)
		record = DecodeTuple(key, record)
		record = DecodeTuple(value, record)

		// testingpkg.PrintTableRecord(record)
		testingpkg.Equals(t, tt.key, record[0])
		testingpkg.Equals(t, tt.value1, record[1])
		testingpkg.Equals(t, tt.value2, record[2])
	}

	os.Remove("simple.ohk")
}
