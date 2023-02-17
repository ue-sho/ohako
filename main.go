package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"fmt"

	"github.com/ue-sho/ohako/execution"
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/disk"
	"github.com/ue-sho/ohako/storage/page"
	"github.com/ue-sho/ohako/table"
	testingpkg "github.com/ue-sho/ohako/testing"
)

const NumRows int = 10000

func tableCreate(bufmgr *buffer.BufferPoolManager) {
	tbl := table.Table{
		MetaPageId:  page.InvalidPageID,
		NumKeyElems: 1,
		UniqueIndices: []table.UniqueIndex{
			{
				MetaPageId: page.InvalidPageID,
				SKey:       []int{2},
			},
		},
	}
	err := tbl.Create(bufmgr)
	if err != nil {
		panic(err)
	}
	fmt.Println(tbl)

	rows := [][][]byte{
		{[]byte("z"), []byte("Alice"), []byte("Smith")},
		{[]byte("x"), []byte("Bob"), []byte("Johnson")},
		{[]byte("y"), []byte("Charlie"), []byte("Williams")},
		{[]byte("w"), []byte("Dave"), []byte("Miller")},
		{[]byte("v"), []byte("Eve"), []byte("Brown")},
	}
	for _, row := range rows {
		err = tbl.Insert(bufmgr, row)
		if err != nil {
			panic(err)
		}
	}

	insertLargeData(bufmgr, &tbl)

	bufmgr.FlushAllpages()
	fmt.Println("flush Ok")
}

func insertLargeData(bufmgr *buffer.BufferPoolManager, tbl *table.Table) {
	for i := 0; i <= NumRows; i++ {
		// fmt.Println(i)
		pkey := make([]byte, 4)
		binary.BigEndian.PutUint32(pkey, uint32(i))
		md5Hash := md5.Sum(pkey)
		sha1Hash := sha1.Sum(pkey)
		if err := tbl.Insert(bufmgr, [][]byte{
			pkey[:],
			md5Hash[:],
			sha1Hash[:],
		}); err != nil {
			panic(err)
		}
	}
}

func fetchData(bufmgr *buffer.BufferPoolManager) {
	plan := execution.IndexScanPlanNode{
		TableMetaPageId: page.PageID(0),
		IndexMetaPageId: page.PageID(2),
		SearchMode:      &table.TupleSearchModeKey{Key: [][]byte{[]byte("Smith")}},
		WhileCond: func(skey table.Tuple) bool {
			return bytes.Equal(skey[0], []byte("Smith"))
		},
	}
	exec, err := plan.Start(bufmgr)
	if err != nil {
		panic(err)
	}
	defer exec.Finish(bufmgr)

	for {
		record, err := exec.Next(bufmgr)
		if err != nil {
			break
		}
		testingpkg.PrintTableRecord(record)
	}
}

func main() {
	diskManager := disk.NewDiskManagerImpl("table_large.ohk")
	poolSize := uint32(NumRows)
	bufmgr := buffer.NewBufferPoolManager(poolSize, diskManager)

	tableCreate(bufmgr)
	// fetchData(bufmgr)
}
