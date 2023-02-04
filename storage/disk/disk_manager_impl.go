package disk

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ue-sho/ohako/storage/page"
)

// DiskManagerインターフェースの実装
type DiskManagerImpl struct {
	db         *os.File
	fileName   string
	nextPageID page.PageID
	numWrites  uint64
	size       int64
}

// DiskManagerImplインスタンスを生成する
func NewDiskManagerImpl(dbFilename string) DiskManager {
	file, err := os.OpenFile(dbFilename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("can't open db file")
		return nil
	}

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("file info error")
		return nil
	}

	fileSize := fileInfo.Size()
	nPages := fileSize / page.PageSize

	nextPageID := page.PageID(0)
	if nPages > 0 {
		nextPageID = page.PageID(int32(nPages + 1))
	}

	return &DiskManagerImpl{file, dbFilename, nextPageID, 0, fileSize}
}

// データベースファイルからページを読み込む
func (d *DiskManagerImpl) ReadPage(pageID page.PageID, pageData []byte) error {
	offset := int64(pageID * page.PageSize)

	fileInfo, err := d.db.Stat()
	if err != nil {
		return errors.New("file info error")
	}

	if offset > fileInfo.Size() {
		return errors.New("I/O error past end of file")
	}

	d.db.Seek(offset, io.SeekStart)

	bytesRead, err := d.db.Read(pageData)
	if err != nil {
		return errors.New("I/O error while reading")
	}

	if bytesRead < page.PageSize {
		for i := 0; i < page.PageSize; i++ {
			pageData[i] = 0
		}
	}
	return nil
}

// データベースファイルにページデータを書き込む
func (d *DiskManagerImpl) WritePage(pageId page.PageID, pageData []byte) error {
	offset := int64(pageId * page.PageSize)
	d.db.Seek(offset, io.SeekStart)
	bytesWritten, err := d.db.Write(pageData)
	if err != nil {
		return err
	}

	if bytesWritten != page.PageSize {
		return errors.New("bytes written not equals page size")
	}

	if offset >= d.size {
		d.size = offset + int64(bytesWritten)
	}

	d.db.Sync()
	return nil
}

//  新しいページを割り当てる
//  実際に行っていることは、ページIDカウンターを増やすだけ
func (d *DiskManagerImpl) AllocatePage() page.PageID {
	ret := d.nextPageID
	d.nextPageID++
	return ret
}

// ページを解放する
// MEMO: 今のところ何もする必要がない
func (d *DiskManagerImpl) DeallocatePage(pageID page.PageID) {
}

// ディスクの書き込み回数を取得する
func (d *DiskManagerImpl) GetNumWrites() uint64 {
	return d.numWrites
}

// データベースファイルを閉じる
func (d *DiskManagerImpl) ShutDown() {
	d.db.Close()
}

// ディスクファイルのサイズ
func (d *DiskManagerImpl) Size() int64 {
	return d.size
}
