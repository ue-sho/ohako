package testing

import (
	"encoding/binary"
	"fmt"
)

func Uint64ToBytes(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf[:]
}

func PrintTableRecord(record [][]byte) {
	s := ""
	for _, col := range record {
		s += string(col) + "\t"
	}
	fmt.Println(s)
}
