package page

import "github.com/ue-sho/ohako/types"

// ページサイズ固定(4KB)
// Linuxで使われているポピュラーなファイルシステムext4のデフォルトのブロックサイズが4096バイトであるため
const PageSize = 4096

// ディスク上の抽象的なページ
type Page struct {
	id       types.PageID    // ページを識別するID。ディスク上のページのオフセットを見つけるために使用される
	pinCount uint32          // アクセスするゴルーチンの数
	isDirty  bool            // ページが変更されたが、フラッシュされているかどうか
	data     *[PageSize]byte // ディスクに格納されたデータ
}

// IncPinCount decrements pin count
func (p *Page) IncPinCount() {
	p.pinCount++
}

// DecPinCount decrements pin count
func (p *Page) DecPinCount() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// PinCount retunds the pin count
func (p *Page) PinCount() uint32 {
	return p.pinCount
}

// ID retunds the page id
func (p *Page) ID() types.PageID {
	return p.id
}

// Data returns the data of the page
func (p *Page) Data() *[PageSize]byte {
	return p.data
}

// SetIsDirty sets the isDirty bit
func (p *Page) SetIsDirty(isDirty bool) {
	p.isDirty = isDirty
}

// IsDirty check if the page is dirty
func (p *Page) IsDirty() bool {
	return p.isDirty
}

// Copy copies data to the page's data. It is mainly used for testing
func (p *Page) Copy(offset uint32, data []byte) {
	copy(p.data[offset:], data)
}

// New creates a new page
func New(id types.PageID, isDirty bool, data *[PageSize]byte) *Page {
	return &Page{id, uint32(1), isDirty, data}
}

// New creates a new empty page
func NewEmpty(id types.PageID) *Page {
	return &Page{id, uint32(1), false, &[PageSize]byte{}}
}
