package page

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
	"github.com/ue-sho/ohako/types"
)

func TestNewPage(t *testing.T) {
	// when
	p := New(types.PageID(0), false, &[PageSize]byte{})

	// then
	testingpkg.Equals(t, types.PageID(0), p.ID())
	testingpkg.Equals(t, uint32(1), p.PinCount())
}
func TestPinCount(t *testing.T) {
	// given
	p := New(types.PageID(0), false, &[PageSize]byte{})
	initPinCount := p.PinCount()

	// when
	p.IncPinCount()
	incrementPinCount := p.PinCount()
	p.DecPinCount()
	p.DecPinCount()
	p.DecPinCount()
	decrementPinCount := p.PinCount()

	// then
	testingpkg.Equals(t, uint32(1), initPinCount)
	testingpkg.Equals(t, uint32(2), incrementPinCount)
	testingpkg.Equals(t, uint32(0), decrementPinCount) // 0以上は小さくならない
}

func TestIsDirty(t *testing.T) {
	// given
	p := New(types.PageID(0), false, &[PageSize]byte{})

	// when
	p.SetIsDirty(true)

	// then
	testingpkg.Equals(t, true, p.IsDirty())
}

func TestPageCopy(t *testing.T) {
	// given
	p := New(types.PageID(0), false, &[PageSize]byte{})

	// when
	p.Copy(0, []byte{'H', 'E', 'L', 'L', 'O'})

	// then
	testingpkg.Equals(t, [PageSize]byte{'H', 'E', 'L', 'L', 'O'}, *p.Data())
}

func TestEmptyPage(t *testing.T) {
	// when
	p := NewEmpty(types.PageID(0))

	// then: 空のページが生成される
	testingpkg.Equals(t, types.PageID(0), p.ID())
	testingpkg.Equals(t, uint32(1), p.PinCount())
	testingpkg.Equals(t, false, p.IsDirty())
	testingpkg.Equals(t, [PageSize]byte{}, *p.Data())
}
