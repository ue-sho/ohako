package index

import (
	"testing"

	"github.com/ue-sho/ohako/storage/page"
	testingpkg "github.com/ue-sho/ohako/testing"
)

func (l *LeafNode) SearchPair(key []byte) *Pair {
	slotId, result := l.SearchSlotId(key)
	if !result {
		panic("miss")
	}
	return l.PairAt(slotId)
}

func TestLeafNodeInsert(t *testing.T) {
	// given
	pageData := make([]byte, 100)
	leafPage := NewLeafNode(pageData)
	leafPage.Initialize()

	key1 := []byte("slasher is ")
	value1 := []byte("a great team.")
	key2 := []byte("hello")
	value2 := []byte("world")
	key3 := []byte("ohako")
	value3 := []byte("dbms")

	// when
	slotId1, result1 := leafPage.SearchSlotId(key1)
	if result1 {
		t.Fatalf("leafPage.SearchSlotId() unexpected hit")
	}
	err := leafPage.Insert(slotId1, key1, value1)
	if err != nil {
		t.Fatalf("leafPage.Insert(): %v", err)
	}

	slotId2, result2 := leafPage.SearchSlotId(key2)
	if result2 {
		t.Fatalf("leafPage.SearchSlotId() unexpected hit")
	}
	err = leafPage.Insert(slotId2, key2, value2)
	if err != nil {
		t.Fatalf("leafPage.Insert(): %v", err)
	}

	slotId3, result3 := leafPage.SearchSlotId(key3)
	if result3 {
		t.Fatalf("leafPage.SearchSlotId() unexpected hit")
	}
	err = leafPage.Insert(slotId3, key3, value3)
	if err != nil {
		t.Fatalf("leafPage.Insert(): %v", err)
	}

	// then
	testingpkg.Equals(t, 0, slotId1)
	testingpkg.Equals(t, 0, slotId2)
	testingpkg.Equals(t, 1, slotId3)
	testingpkg.Equals(t, 3, leafPage.NumPairs())

	tests := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("hello"), []byte("world")},
		{[]byte("ohako"), []byte("dbms")},
		{[]byte("slasher is "), []byte("a great team.")},
	}
	for idx, tt := range tests {
		pair := leafPage.PairAt(idx)
		testingpkg.Equals(t, tt.key, pair.Key)
		testingpkg.Equals(t, tt.value, pair.Value)
	}

}
func TestLeafNodeSplitInsert(t *testing.T) {
	// given
	pageData := make([]byte, 100)
	leafPage := NewLeafNode(pageData)
	leafPage.Initialize()

	key1 := []byte("slasher is ")
	value1 := []byte("a great team.")
	slotId1, result1 := leafPage.SearchSlotId(key1)
	if result1 {
		t.Fatalf("leafPage.SearchSlotId() unexpected hit")
	}
	err := leafPage.Insert(slotId1, key1, value1)
	if err != nil {
		t.Fatalf("leafPage.Insert(): %v", err)
	}

	key2 := []byte("hello")
	value2 := []byte("world")
	slotId2, result2 := leafPage.SearchSlotId(key2)
	if result2 {
		t.Fatalf("leafPage.SearchSlotId() unexpected hit")
	}
	err = leafPage.Insert(slotId2, key2, value2)
	if err != nil {
		t.Fatalf("leafPage.Insert(): %v", err)
	}

	key3 := []byte("ohako")
	value3 := []byte("dbms")
	slotId3, result3 := leafPage.SearchSlotId(key3)
	if result3 {
		t.Fatalf("leafPage.SearchSlotId() unexpected hit")
	}
	err = leafPage.Insert(slotId3, key3, value3)
	if err != nil {
		t.Fatalf("leafPage.Insert(): %v", err)
	}

	newPageData := make([]byte, 88)
	newLeafPage := NewLeafNode(newPageData)

	// when
	mid := leafPage.SplitInsert(newLeafPage, []byte("hoge"), []byte("fuga"))

	// then: key=ohakoを起点に分割挿入
	testingpkg.Equals(t, []byte("ohako"), mid)

	originNodetests := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("ohako"), []byte("dbms")},
		{[]byte("slasher is "), []byte("a great team.")},
	}
	for idx, tt := range originNodetests {
		pair := leafPage.PairAt(idx)
		testingpkg.Equals(t, tt.key, pair.Key)
		testingpkg.Equals(t, tt.value, pair.Value)
	}

	newNodetests := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("hello"), []byte("world")},
		{[]byte("hoge"), []byte("fuga")},
	}
	for idx, tt := range newNodetests {
		pair := newLeafPage.PairAt(idx)
		testingpkg.Equals(t, tt.key, pair.Key)
		testingpkg.Equals(t, tt.value, pair.Value)
	}
}

func TestLeafPageId(t *testing.T) {
	// given
	pageData := make([]byte, 88)
	leafPage := NewLeafNode(pageData)
	leafPage.Initialize()
	prevId := page.PageID(1)
	nextId := page.PageID(5)

	// when
	leafPage.SetPrevPageId(prevId)
	actualPrevId, err1 := leafPage.PrevPageId()

	leafPage.SetNextPageId(nextId)
	actualNextId, err2 := leafPage.NextPageId()

	// then
	testingpkg.Equals(t, prevId, actualPrevId)
	testingpkg.Equals(t, true, err1 == nil)

	testingpkg.Equals(t, nextId, actualNextId)
	testingpkg.Equals(t, true, err2 == nil)
}
