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

func tableCreate(fileName string, uniqIdxs []UniqueIndex) *Table {
	file, err := os.Create(fileName)
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
	tbl := Table{
		MetaPageId:    page.InvalidPageID,
		NumKeyElems:   1,
		UniqueIndices: uniqIdxs,
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
		err = tbl.Insert(bufmgr, row)
		if err != nil {
			panic(err)
		}
	}
	bufmgr.FlushAllpages()
	return &tbl
}

func deleteTable(fileName string) {
	os.Remove(fileName)
}

func TestTableExact(t *testing.T) {
	// given
	dbFile := "table_test.ohk"
	table := tableCreate(dbFile, []UniqueIndex{})
	defer deleteTable(dbFile)

	// tableCreateで作成したDBファイルからDiskManagerを作成
	dm := disk.NewDiskManagerImpl(dbFile)
	defer dm.ShutDown()
	poolSize := uint32(10)
	bufmgr := buffer.NewBufferPoolManager(poolSize, dm)

	tree := index.NewBPlusTree(table.MetaPageId)

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
}

func TestTableSecondaryIndex(t *testing.T) {
	// given
	dbFile := "table_test.ohk"

	uniqIdxs := []UniqueIndex{
		UniqueIndex{
			MetaPageId: page.InvalidPageID,
			SKey:       []int{2},
		},
	}

	table := tableCreate(dbFile, uniqIdxs)
	defer deleteTable(dbFile)

	dm := disk.NewDiskManagerImpl(dbFile)
	defer dm.ShutDown()
	poolSize := uint32(10)
	bufmgr := buffer.NewBufferPoolManager(poolSize, dm)

	tree := index.NewBPlusTree(table.UniqueIndices[0].MetaPageId) // UniqueIndexのMetaPageID

	// when: secondrayKey=Brownのデータを探す
	searchKey := EncodeTuple([][]byte{[]byte("Brown")})
	iter, err := tree.Search(bufmgr, &index.SearchModeKey{Key: searchKey})
	if err != nil {
		panic(err)
	}
	defer iter.Finish(bufmgr)

	// then: key=x ~ 最後まで(z)を探索する
	tests := []struct {
		secondaryKey []byte
		primaryKey   []byte
	}{
		{[]byte("Brown"), []byte("v")},
		{[]byte("Johnson"), []byte("x")},
		{[]byte("Miller"), []byte("w")},
		{[]byte("Smith"), []byte("z")},
		{[]byte("Williams"), []byte("y")},
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
		testingpkg.Equals(t, tt.secondaryKey, record[0])
		testingpkg.Equals(t, tt.primaryKey, record[1])
	}
}
