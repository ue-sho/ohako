package table

/*
 Memcomparable format:
 複合キーをつくるときにソート可能な方法でエンコードする手法
 a, b の複合キーをエンコードして得られる値はソート順が保たれる
*/

// 8byteごとに区切る
const separationLen = 9

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MemcomparableEncodedSize(length int) int {
	return (length + separationLen) / separationLen * (separationLen + 1)
}

// srcをエンコード(8文字区切りのデータに)する
func MemcomparableEncode(src []byte, dst []byte) []byte {
	for {
		copyLen := min(separationLen, len(src))
		dst = append(dst, src[0:copyLen]...)
		src = src[copyLen:]
		if len(src) == 0 {
			padSize := separationLen - copyLen
			if padSize > 0 {
				dst = append(dst, make([]byte, padSize)...)
			}
			dst = append(dst, byte(copyLen))
			break
		}
		// 8byte区切りごとに何文字挿入したか記録する
		dst = append(dst, byte(separationLen+1))
	}
	return dst
}

// srcをデコード(8文字区切りのデータを元の形に)する
func MemcomparableDecode(src []byte, dst []byte) ([]byte, []byte) {
	for {
		// 何文字のデータか確認
		extra := src[separationLen]
		length := min(separationLen, int(extra))

		dst = append(dst, src[:length]...)
		src = src[separationLen+1:]
		if extra < byte(separationLen+1) {
			break
		}
	}
	return src, dst
}
