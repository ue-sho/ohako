package table

type Tuple [][]byte

// バイナリデータリストをエンコードする
func EncodeTuple(elems Tuple) []byte {
	encSize := 0
	for _, elem := range elems {
		encSize += MemcomparableEncodedSize(len(elem))
	}
	bytes := make([]byte, 0, encSize)
	for _, elem := range elems {
		bytes = MemcomparableEncode(elem, bytes)
	}
	return bytes
}

// バイナリデータリストにデコードする
func DecodeTuple(bytes []byte, elems Tuple) Tuple {
	rest := bytes
	for len(rest) > 0 {
		elem := make([]byte, 0, len(bytes))
		rest, elem = MemcomparableDecode(rest, elem)
		elems = append(elems, elem)
	}
	return elems
}
