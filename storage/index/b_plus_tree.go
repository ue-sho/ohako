package index

import (
	"github.com/ue-sho/ohako/storage/buffer"
	"github.com/ue-sho/ohako/storage/page"
	"github.com/ue-sho/ohako/types"
)

const LeafPageHeaderSize = 28
const LEAF_PAGE_SIZE = ((page.PageSize - LeafPageHeaderSize) / 8)

type BPlusTree struct {
	indexName           string
	pageId              types.PageID
	buffer_pool_manager *buffer.BufferPoolManager
	leafMaxSize         int
	internalMaxSize     int
}

// Returns true if this B+ tree has no keys and values.
func (b *BPlusTree) IsEmpty() bool {
	return true
}

// Insert a key-value pair into this B+ tree.
func (b *BPlusTree) Insert(key interface{}, value interface{}) bool {
	return true
}

// Remove a key and its value from this B+ tree.
func (b *BPlusTree) Remove(key interface{}) {}

// return the value associated with a given key
func (b *BPlusTree) GetValue(key interface{}, result []interface{}) bool {
	return true
}

// return the page id of the root node
func (b *BPlusTree) GetRootPageId() types.PageID {
	return types.InvalidPageID
}

// index iterator
// func (b *BPlusTree) Begin() -> INDEXITERATOR_TYPE;
// func (b *BPlusTree) Begin(key interface{}) -> INDEXITERATOR_TYPE;
// func (b *BPlusTree) End() -> INDEXITERATOR_TYPE;

// print the B+ tree
func (b *BPlusTree) Print(bpm *buffer.BufferPoolManager) {}

// draw the B+ tree
func (b *BPlusTree) Draw(bpm *buffer.BufferPoolManager, outf string)

// read data from file and insert one by one
func (b *BPlusTree) InsertFromFile(file_name string)

// read data from file and remove one by one
func (b *BPlusTree) RemoveFromFile(file_name string)

func (b *BPlusTree) updateRootPageId(insertRecord int) {}

func NewBPlusTree(name string, bpm *buffer.BufferPoolManager, leaf_max_size int, internal_max_size int) {
}
