package page

import "github.com/ue-sho/ohako/types"

// ページサイズ固定(4KB)
// Linuxで使われているポピュラーなファイルシステムext4のデフォルトのブロックサイズが4096byteであるため
const PageSize = 4096

// ディスク上の抽象的なページ
type Page struct {
	id       types.PageID    // ページを識別するID。ディスク上のページのオフセットを見つけるために使用される
	pinCount uint32          // アクセス数 セマフォ的な役割
	isDirty  bool            // ページが変更されたが、フラッシュされているかどうか
	data     *[PageSize]byte // ディスクに格納されたデータ
}

// ピンカウントをインクリメントする
func (p *Page) IncPinCount() {
	p.pinCount++
}

// ピンカウントをデクリメントする
func (p *Page) DecPinCount() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// ピンカウント
func (p *Page) PinCount() uint32 {
	return p.pinCount
}

// ページID
func (p *Page) ID() types.PageID {
	return p.id
}

// ページデータ
func (p *Page) Data() *[PageSize]byte {
	return p.data
}

// isDirtyのsertter
func (p *Page) SetIsDirty(isDirty bool) {
	p.isDirty = isDirty
}

// isDirty(ページが変更されたが、フラッシュされているかどうか)
func (p *Page) IsDirty() bool {
	return p.isDirty
}

// ページのデータを引数で受けたdataにコピーする
// 主にテストに使用される
func (p *Page) Copy(offset uint32, data []byte) {
	copy(p.data[offset:], data)
}

// ページを生成する
func New(id types.PageID, isDirty bool, data *[PageSize]byte) *Page {
	return &Page{id, uint32(1), isDirty, data}
}

// 空ページを生成する
func NewEmpty(id types.PageID) *Page {
	return &Page{id, uint32(1), false, &[PageSize]byte{}}
}
