package index

import (
	"unsafe"

	"golang.org/x/xerrors"
)

type SlottedHeader struct {
	numSlots        uint16
	freeSpaceOffset uint16
	_padding        uint32 // 64bitコンピュータが基本なので、8byteになるように調整
}

type Pointer struct {
	offset uint16
	length uint16
}

const pointerSize int = int(unsafe.Sizeof(Pointer{}))

func (p *Pointer) getRange() (int, int) {
	start := int(p.offset)
	end := start + int(p.length)
	return start, end
}

/**
 * LeafNode, InternalNodeのスペース管理
 *
 * ヘッダーフォーマット 8byte
 *  ---------------------------------------------------
 * | numSlots (2) | freeSpaceOffset (2) | padding (4) |
 *  ---------------------------------------------------
 *
 *  ボディフォーマット (データの位置は順不同)
 *                                                                Pointer1 offset
 *                                                                      v
 *  -------------------------------------------------------------------------------------------
 * | Pointer1 (4) | Pointer2 (4) | ... |   freeSpace   | Pointer2のData | Pointer1のData | ... |
 *  -------------------------------------------------------------------------------------------
 *                                                     ^
 *                                               freeSpaceOffset
 */
type Slotted struct {
	header *SlottedHeader
	body   []byte
}

// スロットの容量
func (s *Slotted) Capacity() int {
	return len(s.body)
}

// スロット数
func (s *Slotted) NumSlots() int {
	return int(s.header.numSlots)
}

// 空きスペース(byte列の数)
func (s *Slotted) FreeSpace() int {
	return int(s.header.freeSpaceOffset) - s.pointersSize()
}

// 合計のPinterのサイズ
func (s *Slotted) pointersSize() int {
	return int(pointerSize * s.NumSlots())
}

// 全てのpointerを取り出す
func (s *Slotted) pointers() []*Pointer {
	ret := make([]*Pointer, s.NumSlots())
	for i := 0; i < s.NumSlots(); i++ {
		ret[i] = (*Pointer)(unsafe.Pointer(&s.body[i*pointerSize]))
	}
	return ret
}

// 指定Pointerのデータを取り出す
func (s *Slotted) data(pointer *Pointer) []byte {
	start, end := pointer.getRange()
	return s.body[start:end]
}

// 初期化を行う
func (s *Slotted) Initialize() {
	s.header.numSlots = 0
	s.header.freeSpaceOffset = uint16(len(s.body))
}

// 指定indexの場所に容量を確保する
// データの書き込みはWriteDataを使用する
func (s *Slotted) Insert(index int, length int) error {
	if s.FreeSpace() < pointerSize+length {
		return xerrors.New("no free space")
	}

	numSlotsOrig := s.NumSlots()
	if index < 0 || numSlotsOrig < index {
		return xerrors.New("invalid index")
	}

	s.header.freeSpaceOffset -= uint16(length)
	s.header.numSlots++
	freeSpaceOffset := s.header.freeSpaceOffset
	pointers := s.pointers()
	for i := numSlotsOrig - 1; i >= index; i-- {
		*pointers[i+1] = *pointers[i]
	}
	pointer := pointers[index]
	pointer.offset = freeSpaceOffset
	pointer.length = uint16(length)
	return nil
}

// indexのデータを削除する
func (s *Slotted) Remove(index int) {
	numSlots := s.NumSlots()
	if index < 0 || numSlots <= index {
		panic("invalid index")
	}

	if err := s.Resize(index, 0); err != nil {
		panic(err)
	}

	pointers := s.pointers()
	for i := index + 1; i < numSlots; i++ {
		*pointers[i-1] = *pointers[i]
	}
	s.header.numSlots--
}

// 指定indexのデータ長さを変更する
func (s *Slotted) Resize(index int, lenNew int) error {
	if index < 0 || s.NumSlots() <= index {
		xerrors.New("invalid index")
	}

	pointers := s.pointers()
	lenIncr := lenNew - int(pointers[index].length)
	if lenIncr == 0 {
		return nil
	}
	if lenIncr > s.FreeSpace() {
		return xerrors.New("no free space")
	}

	freeSpaceOffset := s.header.freeSpaceOffset
	offsetOrig := pointers[index].offset
	shiftStart := int(freeSpaceOffset)
	shiftEnd := int(offsetOrig)
	freeSpaceOffsetNew := int(freeSpaceOffset) - lenIncr
	s.header.freeSpaceOffset = uint16(freeSpaceOffsetNew)

	buf := make([]byte, shiftEnd-shiftStart)
	copy(buf, s.body[shiftStart:shiftEnd])
	copy(s.body[freeSpaceOffsetNew:], buf)

	for _, pointer := range pointers {
		if pointer.offset <= offsetOrig {
			pointer.offset = uint16(int(pointer.offset) - lenIncr)
		}
	}

	pointer := pointers[index]
	pointer.length = uint16(lenNew)
	if lenNew == 0 {
		pointer.offset = uint16(freeSpaceOffsetNew)
	}
	return nil
}

// 指定indexのデータを読み込む
func (s *Slotted) ReadData(index int) []byte {
	return s.data(s.pointers()[index])
}

// 指定indexに書き込みを行う
func (s *Slotted) WriteData(index int, buf []byte) {
	data := s.ReadData(index)
	copy(data, buf)
}

// byte列からSlottedを生成する
func NewSlotted(bytes []byte) *Slotted {
	slotted := Slotted{}
	headerSize := int(unsafe.Sizeof(*slotted.header))
	if headerSize+1 > len(bytes) {
		panic("slotted header must be aligned")
	}

	slotted.header = (*SlottedHeader)(unsafe.Pointer(&bytes[0]))
	slotted.body = bytes[headerSize:]
	return &slotted
}
