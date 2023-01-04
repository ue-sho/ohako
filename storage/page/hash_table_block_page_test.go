package page

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestInsert_KeyAt_ValueAt(t *testing.T) {
	// given
	bp := HashTableBlockPage{}
	index := 0
	initKey := 1
	initValue := 10

	// when
	isReadable1 := bp.IsReadable(index)
	isOccupied1 := bp.IsOccupied(index)

	bp.Insert(index, initKey, initValue)

	isReadable2 := bp.IsReadable(index)
	isOccupied2 := bp.IsOccupied(index)

	key := bp.KeyAt(index)
	value := bp.ValueAt(index)

	// then
	testingpkg.Equals(t, initKey, key)
	testingpkg.Equals(t, initValue, value)

	testingpkg.Equals(t, false, isReadable1)
	testingpkg.Equals(t, true, isReadable2)

	testingpkg.Equals(t, false, isOccupied1)
	testingpkg.Equals(t, true, isOccupied2)
}

func TestInsert_Remove_IsReadable_IsOccupied(t *testing.T) {
	// given
	bp := HashTableBlockPage{}
	index := 0
	initKey := 1
	initValue := 10

	// when
	bp.Insert(index, initKey, initValue)

	isReadable1 := bp.IsReadable(index)
	isOccupied1 := bp.IsOccupied(index)

	bp.Remove(index)
	isReadable2 := bp.IsReadable(index)
	isOccupied2 := bp.IsOccupied(index)

	// then: 削除後は読み取り不可になる, 削除後も占有されたまま
	testingpkg.Equals(t, true, isReadable1)
	testingpkg.Equals(t, false, isReadable2)

	testingpkg.Equals(t, true, isOccupied1)
	testingpkg.Equals(t, true, isOccupied2)
}
