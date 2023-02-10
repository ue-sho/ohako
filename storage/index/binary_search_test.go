package index

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestBinarySearchBy(t *testing.T) {
	// given
	sortedList := []int{1, 2, 3, 5, 8, 13, 21}

	// when
	target := func(data []int, searchValue int) (int, bool) {
		// return=0が正解となるようにする
		f := func(idx int) int {
			return data[idx] - searchValue
		}

		return BinarySearchBy(len(data), f)
	}

	// then
	actual, result := target(sortedList, 1)
	testingpkg.Equals(t, 0, actual)
	testingpkg.Equals(t, true, result)

	actual, result = target(sortedList, 0)
	testingpkg.Equals(t, 0, actual)
	testingpkg.Equals(t, false, result)

	actual, result = target(sortedList, 2)
	testingpkg.Equals(t, 1, actual)
	testingpkg.Equals(t, true, result)

	actual, result = target(sortedList, 21)
	testingpkg.Equals(t, 6, actual)
	testingpkg.Equals(t, true, result)

	actual, result = target(sortedList, 50)
	testingpkg.Equals(t, 7, actual)
	testingpkg.Equals(t, false, result)
}
