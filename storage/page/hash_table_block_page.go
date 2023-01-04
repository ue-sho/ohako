package page

type HashTablePair struct {
	key   int
	value int
}

// key(int32 = 4byte) + value(int32 = 4byte)
const sizeOfHashTablePair = 8

const BlockArraySize = 4 * 4096 / (4*sizeOfHashTablePair + 1) // 496

/**
 * ブロックページ内にインデックスされたキーと値を一緒に格納します。一意でないキーをサポートします。
 *
 * ブロックページフォーマット
 *  ----------------------------------------------------------------
 * | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------
 */
type HashTableBlockPage struct {
	occuppied [(BlockArraySize-1)/8 + 1]byte // 62 bytes (496 bits)
	readable  [(BlockArraySize-1)/8 + 1]byte // 62 bytes (496 bits)
	array     [BlockArraySize]HashTablePair  // 496 * 8 byte
}

// ブロック内のインデックスにあるKeyを取得する
func (page *HashTableBlockPage) KeyAt(index int) int {
	return page.array[index].key
}

// ブロック内のインデックスにあるValueを取得する
func (page *HashTableBlockPage) ValueAt(index int) int {
	return page.array[index].value
}

// ブロック内のインデックスにキーと値を挿入する
func (page *HashTableBlockPage) Insert(index int, key int, value int) bool {
	if page.IsOccupied(index) {
		return false
	}

	page.array[index] = HashTablePair{key, value}
	page.occuppied[index/8] |= (1 << (index % 8))
	page.readable[index/8] |= (1 << (index % 8))
	return true
}

// ブロック内のインデックスにあるデータを削除する
func (page *HashTableBlockPage) Remove(index int) {
	if !page.IsReadable(index) {
		return
	}

	page.readable[index/8] &= ^(1 << (index % 8))
}

// インデックスが占有されているか否か判定する
func (page *HashTableBlockPage) IsOccupied(index int) bool {
	return (page.occuppied[index/8] & (1 << (index % 8))) != 0
}

// インデックスが読み取り可能であるか否か判定する
func (page *HashTableBlockPage) IsReadable(index int) bool {
	return (page.readable[index/8] & (1 << (index % 8))) != 0
}
