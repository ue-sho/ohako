package table

import (
	"testing"

	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/disk"
	"github.com/ue-sho/ohako/storage/page"
	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestSimpleTableCreateInsert(t *testing.T) {
	// given
	poolSize := uint32(10)
	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	bufmgr := buffer.NewBufferPoolManager(poolSize, dm)

	// テーブル作成
	tbl := SimpleTable{
		MetaPageId:  page.InvalidPageID,
		NumKeyElems: 1,
	}
	err := tbl.Create(bufmgr)
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

	for _, row := range rows {
		// when
		err = tbl.Insert(bufmgr, row)

		// then: エラーが発生しない
		testingpkg.Equals(t, nil, err)
	}
	bufmgr.FlushAllpages()
}
