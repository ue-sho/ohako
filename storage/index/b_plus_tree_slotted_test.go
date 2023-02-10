package index

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestNewSlotted(t *testing.T) {
	// given
	buf := make([]byte, 128)

	// when
	slotted := NewSlotted(buf)
	slotted.Initialize()

	// then: ヘッダー8byteを除いたデータがcapacityとなる
	testingpkg.Equals(t, 120, slotted.Capacity())
	testingpkg.Equals(t, 120, slotted.FreeSpace())
	testingpkg.Equals(t, 0, slotted.NumSlots())
	testingpkg.Equals(t, 0, slotted.pointersSize())
}

func TestInsertWiteData(t *testing.T) {
	// given
	buf := make([]byte, 128)
	slotted := NewSlotted(buf)
	slotted.Initialize()
	index := 0
	insertStr := []byte("Hello, World")

	// when
	err := slotted.Insert(index, len(insertStr))
	if err != nil {
		panic(err)
	}

	var beforeData []byte
	readData := slotted.ReadData(index)
	copy(beforeData, readData)
	slotted.WriteData(index, insertStr)
	afterData := slotted.ReadData(index)

	// then: 初期はから文字列
	testingpkg.Equals(t, "", string(beforeData))
	testingpkg.Equals(t, "Hello, World", string(afterData))
}

func TestInvalidIndex(t *testing.T) {
	// given: indexがNumSlotsより大きい
	buf := make([]byte, 128)
	slotted := NewSlotted(buf)
	slotted.Initialize()
	index := 6

	// when:
	err := slotted.Insert(index, 10)
	if err == nil {
		panic(err)
	}

	// then: エラーが発生する
	testingpkg.Equals(t, true, err != nil)
}

func TestSenario(t *testing.T) {
	// given
	buf := make([]byte, 128)
	slotted := NewSlotted(buf)
	slotted.Initialize()

	// when: 同じindexにinsert
	insertData1 := []byte("world")
	err := slotted.Insert(0, len(insertData1))
	if err != nil {
		panic(err)
	}
	slotted.WriteData(0, insertData1)

	insertData2 := []byte("hello")
	err = slotted.Insert(0, len(insertData2))
	if err != nil {
		panic(err)
	}
	slotted.WriteData(0, insertData2)

	// then: index=0にはhelloが出力される
	tests := []string{
		"hello",
		"world",
	}
	for index, data := range tests {
		actual := slotted.ReadData(index)
		expect := []byte(data)
		testingpkg.Equals(t, actual, expect)
	}

	// when
	insertData3 := []byte(", ")
	err = slotted.Insert(1, len(insertData3))
	if err != nil {
		panic(err)
	}
	slotted.WriteData(1, insertData3)

	insertData4 := []byte("!")
	num := slotted.NumSlots()
	err = slotted.Insert(num, len(insertData4))
	if err != nil {
		panic(err)
	}
	slotted.WriteData(num, insertData4)

	// then:
	tests = []string{
		"hello",
		", ",
		"world",
		"!",
	}
	for index, data := range tests {
		actual := slotted.ReadData(index)
		expect := []byte(data)
		testingpkg.Equals(t, actual, expect)
	}

	// "hello, world!"(13byte) + pointersSize(16byte)使用している
	testingpkg.Equals(t, 120, slotted.Capacity())
	testingpkg.Equals(t, 16, slotted.pointersSize())
	testingpkg.Equals(t, 91, slotted.FreeSpace())
	testingpkg.Equals(t, 4, slotted.NumSlots())

	// when: Remove, Resizeを実行
	// ","を削除
	slotted.Remove(1)

	// "hello"をリサイズ
	slotted.Resize(0, 2)
	slotted.WriteData(0, []byte("hi"))

	// "world"をリサイズ
	slotted.Resize(1, 10)
	slotted.WriteData(1, []byte("ohako dbms"))

	// then
	tests = []string{
		"hi",
		"ohako dbms",
		"!",
	}
	for index, data := range tests {
		actual := slotted.ReadData(index)
		expect := []byte(data)
		testingpkg.Equals(t, actual, expect)
	}

}
