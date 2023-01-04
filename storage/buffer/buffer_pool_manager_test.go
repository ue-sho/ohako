package buffer

import (
	"crypto/rand"
	"testing"

	"github.com/ue-sho/ohako/storage/disk"
	"github.com/ue-sho/ohako/storage/page"
	testingpkg "github.com/ue-sho/ohako/testing"
	"github.com/ue-sho/ohako/types"
)

func TestBinaryData(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	bpm := NewBufferPoolManager(poolSize, dm)

	// Scenario: バッファプールは空。新しいページを作成する。
	page0 := bpm.NewPage()
	testingpkg.Equals(t, types.PageID(0), page0.ID())

	// ランダムなバイナリーデータを作る
	randomBinaryData := make([]byte, page.PageSize)
	rand.Read(randomBinaryData)

	// 中間と末尾に終端文字を入れる
	randomBinaryData[page.PageSize/2] = '0'
	randomBinaryData[page.PageSize-1] = '0'

	var fixedRandomBinaryData [page.PageSize]byte
	copy(fixedRandomBinaryData[:], randomBinaryData[:page.PageSize])

	// Scenario: ページができれば、コンテンツの読み書きができる
	page0.Copy(0, randomBinaryData)
	testingpkg.Equals(t, fixedRandomBinaryData, *page0.Data())

	// Scenario: バッファプールが一杯になるまで、新しいページを作ることができる
	for i := uint32(1); i < poolSize; i++ {
		p := bpm.NewPage()
		testingpkg.Equals(t, types.PageID(i), p.ID())
	}

	// Scenario: バッファプールが一杯になったら、新しいページを作ることはできない
	for i := poolSize; i < poolSize*2; i++ {
		testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	}

	// Scenario: ページ{0, 1, 2, 3, 4}のピンを解除する
	// さらに4つの新しいページを固定した後は、ページ0を読むためのキャッシュフレームがまだ1つ残る
	for i := 0; i < 5; i++ {
		testingpkg.Ok(t, bpm.UnpinPage(types.PageID(i), true))
		bpm.FlushPage(types.PageID(i))
	}
	// 4つをページ0を読むためのキャッシュフレームがまだ1つ残っています。
	for i := 0; i < 4; i++ {
		p := bpm.NewPage()
		bpm.UnpinPage(p.ID(), false)
	}

	// Scenario: 先ほど書いたデータを取り出せる
	page0 = bpm.FetchPage(types.PageID(0))
	testingpkg.Equals(t, fixedRandomBinaryData, *page0.Data())
	testingpkg.Ok(t, bpm.UnpinPage(types.PageID(0), true))
}

func TestSample(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	bpm := NewBufferPoolManager(poolSize, dm)

	// Scenario: バッファプールは空。新しいページを作成する。
	page0 := bpm.NewPage()
	testingpkg.Equals(t, types.PageID(0), page0.ID())

	// Scenario: ページができれば、コンテンツの読み書きができる
	page0.Copy(0, []byte("Hello"))
	testingpkg.Equals(t, [page.PageSize]byte{'H', 'e', 'l', 'l', 'o'}, *page0.Data())

	// Scenario: バッファプールが一杯になるまで、新しいページを作ることができる
	for i := uint32(1); i < poolSize; i++ {
		p := bpm.NewPage()
		testingpkg.Equals(t, types.PageID(i), p.ID())
	}

	// Scenario: バッファプールが一杯になったら、新しいページを作ることはできない
	for i := poolSize; i < poolSize*2; i++ {
		testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	}

	// Scenario: ページ{0, 1, 2, 3, 4}のピンを解除する
	// さらに4つの新しいページを固定した後は、ページ0を読むためのキャッシュフレームがまだ1つ残る
	for i := 0; i < 5; i++ {
		testingpkg.Ok(t, bpm.UnpinPage(types.PageID(i), true))
		bpm.FlushPage(types.PageID(i))
	}
	for i := 0; i < 4; i++ {
		bpm.NewPage()
	}
	// Scenario: 先ほど書いたデータを取り出せる
	page0 = bpm.FetchPage(types.PageID(0))
	testingpkg.Equals(t, [page.PageSize]byte{'H', 'e', 'l', 'l', 'o'}, *page0.Data())

	// Scenario: ページ0のピンを解除し、新しいページを作成すると、バッファのすべてのページが固定される. ページ0の取得は失敗する
	testingpkg.Ok(t, bpm.UnpinPage(types.PageID(0), true))

	testingpkg.Equals(t, types.PageID(14), bpm.NewPage().ID())
	testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	testingpkg.Equals(t, (*page.Page)(nil), bpm.FetchPage(types.PageID(0)))
}
