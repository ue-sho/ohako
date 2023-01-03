package access

import (
	"github.com/ue-sho/ohako/storage/table"
)

// TableHeapIterator is the access method for table heaps
//
// It iterates through a table heap when Next is called
// The tuple that it is being pointed to can be accessed with the method Current
type TableHeapIterator struct {
	tableHeap *TableHeap
	tuple     *table.Tuple
}

// NewTableHeapIterator creates a new table heap operator for the given table heap
// It points to the first tuple of the table heap
func NewTableHeapIterator(tableHeap *TableHeap) *TableHeapIterator {
	return &TableHeapIterator{tableHeap, tableHeap.GetFirstTuple()}
}

// Current points to the current tuple
func (it *TableHeapIterator) Current() *table.Tuple {
	return it.tuple
}

// End checks if the iterator is at the end
func (it *TableHeapIterator) End() bool {
	return it.Current() == nil
}

// Next advances the iterator trying to find the next tuple
// The next tuple can be inside the same page of the current tuple
// or it can be in the next page
func (it *TableHeapIterator) Next() *table.Tuple {
	bpm := it.tableHeap.bpm
	currentPage := table.CastPageAsTablePage(bpm.FetchPage(it.tuple.GetRID().GetPageId()))

	nextTupleRID := currentPage.GetNextTupleRID(it.tuple.GetRID())
	if nextTupleRID == nil {
		for currentPage.GetNextPageId().IsValid() {
			nextPage := table.CastPageAsTablePage(bpm.FetchPage(currentPage.GetNextPageId()))
			bpm.UnpinPage(currentPage.GetTablePageId(), false)
			currentPage = nextPage
			nextTupleRID = currentPage.GetNextTupleRID(it.tuple.GetRID())
			if nextTupleRID != nil {
				break
			}
		}
	}

	if nextTupleRID != nil && nextTupleRID.GetPageId().IsValid() {
		it.tuple = it.tableHeap.GetTuple(nextTupleRID)
	} else {
		it.tuple = nil
	}

	bpm.UnpinPage(currentPage.GetTablePageId(), false)
	return it.tuple
}
