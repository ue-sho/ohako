package page

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestRID(t *testing.T) {
	// given
	rid := RID{}

	// when
	rid.Set(PageID(0), uint32(0))

	// then
	testingpkg.Equals(t, PageID(0), rid.GetPageId())
	testingpkg.Equals(t, uint32(0), rid.GetSlot())
}
