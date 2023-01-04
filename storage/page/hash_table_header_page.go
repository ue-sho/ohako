package page

import "github.com/ue-sho/ohako/types"

/**
 *
 * ヘッダーフォーマット (size in byte, 16 bytes in total):
 * -------------------------------------------------------------------------------
 * | PageId(4) | LSN (4) | NextBlockIndex(4) | Size (4) | BlockPageIds (4) x 1020
 * -------------------------------------------------------------------------------
 *
 * ヘッダー16Byte + BlockPageIds 4*1020Byte = 4096Byte
 */
type HashTableHeaderPage struct {
	pageId       types.PageID
	lsn          int // log sequence number
	nextIndex    int // blockPageIdsに新規追加する次のインデックス。
	size         int // ハッシュテーブルが保持できるキーと値のペアの数
	blockPageIds [1020]types.PageID
}

func (page *HashTableHeaderPage) GetBlockPageId(index int) types.PageID {
	return page.blockPageIds[index]
}

func (page *HashTableHeaderPage) GetPageId() types.PageID {
	return page.pageId
}

func (page *HashTableHeaderPage) SetPageId(pageId types.PageID) {
	page.pageId = pageId
}

func (page *HashTableHeaderPage) GetLSN() int {
	return page.lsn
}

func (page *HashTableHeaderPage) SetLSN(lsn int) {
	page.lsn = lsn
}

func (page *HashTableHeaderPage) AddBlockPageId(pageId types.PageID) {
	page.blockPageIds[page.nextIndex] = pageId
	page.nextIndex++
}

func (page *HashTableHeaderPage) NumBlocks() int {
	return page.nextIndex
}

func (page *HashTableHeaderPage) SetSize(size int) {
	page.size = size
}

func (page *HashTableHeaderPage) GetSize() int {
	return page.size
}
