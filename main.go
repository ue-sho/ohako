package main

import (
	"bytes"
	"fmt"

	"github.com/ue-sho/ohako/execution"
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/disk"
	"github.com/ue-sho/ohako/storage/page"
	"github.com/ue-sho/ohako/table"
	testingpkg "github.com/ue-sho/ohako/testing"
)

func tableCreate(bufmgr *buffer.BufferPoolManager) {
	tbl := table.SimpleTable{
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
		err = tbl.Insert(bufmgr, row)
		if err != nil {
			panic(err)
		}
	}
}

func fetchData(bufmgr *buffer.BufferPoolManager) {
	plan := execution.FilterPlanNode{
		Cond: func(record table.Tuple) bool {
			return bytes.Compare(record[1], []byte("Dave")) < 0
		},
		InnerPlan: &execution.SeqScanPlanNode{
			TableMetaPageId: page.PageID(0),
			SearchMode:      &table.TupleSearchModeKey{Key: [][]byte{[]byte("w")}},
			WhileCond: func(pkey table.Tuple) bool {
				return bytes.Compare(pkey[0], []byte("z")) < 0
			},
		},
	}
	fmt.Println("plan: ", plan.Explain())

	exec, err := plan.Start(bufmgr)
	if err != nil {
		panic(err)
	}
	defer exec.Finish(bufmgr)

	for {
		record, err := exec.Next(bufmgr)
		if err != nil {
			fmt.Println(err)
			break
		}
		testingpkg.PrintTableRecord(record)
	}
}

func main() {
	diskManager := disk.NewDiskManagerImpl("./plan.ohk")
	poolSize := uint32(10)
	bufmgr := buffer.NewBufferPoolManager(poolSize, diskManager)

	tableCreate(bufmgr)
	fetchData(bufmgr)
}
