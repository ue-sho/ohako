package types

import (
	"bytes"
	"encoding/binary"
)

type Value struct {
	valueType TypeID
	integer   *int32
	boolean   *bool
	varchar   *string
}

func NewInteger(value int32) Value {
	return Value{Integer, &value, nil, nil}
}

func NewBoolean(value bool) Value {
	return Value{Boolean, nil, &value, nil}
}

func NewVarchar(value string) Value {
	return Value{Varchar, nil, nil, &value}
}

// NewValueFromBytes is used for deserialization
func NewValueFromBytes(data []byte, valueType TypeID) (ret *Value) {
	switch valueType {
	case Integer:
		v := new(int32)
		binary.Read(bytes.NewBuffer(data), binary.LittleEndian, v)
		vInteger := NewInteger(*v)
		ret = &vInteger
	case Varchar:
		lengthInBytes := data[0:2]
		length := new(int16)
		binary.Read(bytes.NewBuffer(lengthInBytes), binary.LittleEndian, length)
		varchar := NewVarchar(string(data[2:(*length + 2)]))
		ret = &varchar
	}
	return ret
}

func (v Value) CompareEquals(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer == *right.integer
	case Varchar:
		return *v.varchar == *right.varchar
	}
	return false
}

func (v Value) CompareNotEquals(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer != *right.integer
	case Varchar:
		return *v.varchar != *right.varchar
	}
	return false
}

func (v Value) Serialize() []byte {
	switch v.valueType {
	case Integer:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, v.ToInteger())
		return buf.Bytes()
	case Varchar:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint16(len(v.ToVarchar())))
		lengthInBytes := buf.Bytes()
		return append(lengthInBytes, []byte(v.ToVarchar())...)
	}
	return []byte{}
}

// Size returns the size in bytes that the type will occupy inside the tuple
func (v Value) Size() uint32 {
	switch v.valueType {
	case Integer:
		return 4
	case Varchar:
		return uint32(len(*v.varchar)) + 2 // varchar occupies the size of the string + 2 bytes for length storage
	}
	return 0
}

func (v Value) ToBoolean() bool {
	return *v.boolean
}

func (v Value) ToInteger() int32 {
	return *v.integer
}

func (v Value) ToVarchar() string {
	return *v.varchar
}
