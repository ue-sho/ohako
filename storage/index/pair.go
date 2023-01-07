package index

import (
	"encoding/binary"
)

const uint32BinaryLen = 4

type Pair struct {
	Key   []byte
	Value []byte
}

func (p *Pair) ToBytes() []byte {
	keyLen := uint32(len(p.Key))
	valueLen := uint32(len(p.Value))

	buf := make([]byte, keyLen+valueLen+uint32BinaryLen*2)

	// 最初4byteに長さを書き込み、その後ろに実際のデータ
	binary.BigEndian.PutUint32(buf[0:], keyLen)
	copy(buf[uint32BinaryLen:uint32BinaryLen+keyLen], p.Key[:])

	binary.BigEndian.PutUint32(buf[uint32BinaryLen+keyLen:keyLen+uint32BinaryLen*2], valueLen)
	copy(buf[keyLen+uint32BinaryLen*2:], p.Value[:])

	return buf
}

func NewPair(k, v []byte) *Pair {
	return &Pair{
		Key:   k,
		Value: v,
	}
}

func NewPairFromBytes(buf []byte) *Pair {
	keyLen := binary.BigEndian.Uint32(buf[:uint32BinaryLen])
	key := make([]byte, keyLen)
	copy(key, buf[uint32BinaryLen:uint32BinaryLen+keyLen])

	valueLen := binary.BigEndian.Uint32(buf[uint32BinaryLen+keyLen : keyLen+uint32BinaryLen*2])
	value := make([]byte, valueLen)
	copy(value, buf[keyLen+uint32BinaryLen*2:])

	return NewPair(key, value)
}
