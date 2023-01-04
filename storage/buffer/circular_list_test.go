package buffer

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestInsert_hasKey(t *testing.T) {
	// given
	cl := newCircularList(10)

	// when: 数値も文字列も挿入できる
	cl.insert(1, 1)
	cl.insert("key", "value")

	// then
	testingpkg.Equals(t, true, cl.hasKey(1))
	testingpkg.Equals(t, true, cl.hasKey("key"))
}

func TestInsertRemove(t *testing.T) {
	// given
	cl := newCircularList(10)

	// when
	cl.insert("key1", "value1")
	cl.insert("key2", "value2")
	cl.insert("key3", "value3")
	size1 := cl.size

	cl.remove("key2")
	size2 := cl.size

	// then
	testingpkg.Equals(t, uint32(3), size1)
	testingpkg.Equals(t, uint32(2), size2)
	testingpkg.Equals(t, false, cl.hasKey("key2"))
}

func TestIsFull(t *testing.T) {
	// given
	cl := newCircularList(2)
	cl.insert(1, 1)
	cl.insert(2, 2)

	// when
	ret1 := cl.isFull()
	cl.remove(1)
	ret2 := cl.isFull()

	// then: maxSize=2なので2つinsertしたらFullになる
	testingpkg.Equals(t, true, ret1)

	// 削除したらFullは解消される
	testingpkg.Equals(t, false, ret2)
}

func TestFind(t *testing.T) {
	// given
	cl := newCircularList(10)
	cl.insert(1, 10)
	cl.insert(2, 20)

	// when
	node := cl.find(1)

	// then: maxSize=2なので2つinsertしたらFullになる
	testingpkg.Equals(t, 1, node.key)
	testingpkg.Equals(t, 10, node.value)
}

func TestPrint(t *testing.T) {
	// given
	cl := newCircularList(10)
	cl.insert("key1", "value1")
	cl.insert("key2", "value2")
	cl.insert("key3", "value3")
	cl.insert("key4", "value3")
	cl.insert("key5", "value3")

	// when
	cl.print()
	size := cl.size

	// then: maxSize=2なので2つinsertしたらFullになる
	testingpkg.Equals(t, uint32(5), size)
}
