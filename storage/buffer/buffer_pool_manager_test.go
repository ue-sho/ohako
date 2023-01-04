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

	// シナリオ: バッファプールは空。最初のページはpageID=0が作成される
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

	// シナリオ: ページができれば、コンテンツの読み書きができる
	page0.Copy(0, randomBinaryData)
	testingpkg.Equals(t, fixedRandomBinaryData, *page0.Data())

	// シナリオ: バッファプールが一杯になるまで、新しいページを作ることができる
	for i := uint32(1); i < poolSize; i++ {
		p := bpm.NewPage()
		testingpkg.Equals(t, types.PageID(i), p.ID())
	}

	// シナリオ: バッファプールが一杯になったら、新しいページを作ることはできない
	for i := poolSize; i < poolSize*2; i++ {
		testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	}

	// シナリオ: ページ{0, 1, 2, 3, 4}のピンを解除し、ディスクへ書き込む
	for i := 0; i < 5; i++ {
		testingpkg.Ok(t, bpm.UnpinPage(types.PageID(i), true))
		bpm.FlushPage(types.PageID(i))
	}
	// 4つUnpinPageしたので、新規ページが作成できる
	for i := 0; i < 4; i++ {
		p := bpm.NewPage()
		testingpkg.Equals(t, true, bpm.NewPage() != (*page.Page)(nil))
		bpm.UnpinPage(p.ID(), false)
	}

	// シナリオ: 先ほど書いたデータを取り出せる
	page0 = bpm.FetchPage(types.PageID(0))
	testingpkg.Equals(t, fixedRandomBinaryData, *page0.Data())
	testingpkg.Ok(t, bpm.UnpinPage(types.PageID(0), true))
}

func TestSample(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	defer dm.ShutDown()
	bpm := NewBufferPoolManager(poolSize, dm)

	// シナリオ: バッファプールは空。最初のページはpageID=0が作成される
	page0 := bpm.NewPage()
	testingpkg.Equals(t, types.PageID(0), page0.ID())

	// シナリオ: ページができれば、コンテンツの読み書きができる
	page0.Copy(0, []byte("Hello"))
	testingpkg.Equals(t, [page.PageSize]byte{'H', 'e', 'l', 'l', 'o'}, *page0.Data())

	// シナリオ: バッファプールが一杯になるまで、新しいページを作ることができる
	for i := uint32(1); i < poolSize; i++ {
		p := bpm.NewPage()
		testingpkg.Equals(t, types.PageID(i), p.ID())
	}

	// シナリオ: バッファプールが一杯になったら、新しいページを作ることはできない
	for i := poolSize; i < poolSize*2; i++ {
		testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	}

	// シナリオ: ページ{0, 1, 2, 3, 4}のピンを解除する
	for i := 0; i < 5; i++ {
		testingpkg.Ok(t, bpm.UnpinPage(types.PageID(i), true))
		bpm.FlushPage(types.PageID(i))
	}
	// さらに4つの新しいページ作成. キャッシュフレームがまだ1つ残っている状態
	for i := 0; i < 4; i++ {
		bpm.NewPage()
	}
	// シナリオ: 先ほど書いたデータを取り出せる
	page0 = bpm.FetchPage(types.PageID(0))
	testingpkg.Equals(t, [page.PageSize]byte{'H', 'e', 'l', 'l', 'o'}, *page0.Data())

	// シナリオ: ページ0のピンを解除し、新しいページを2つ作成すると、バッファのすべてのページが固定されページ0の取得は失敗する
	testingpkg.Ok(t, bpm.UnpinPage(types.PageID(0), true))

	testingpkg.Equals(t, types.PageID(14), bpm.NewPage().ID())
	testingpkg.Equals(t, (*page.Page)(nil), bpm.NewPage())
	testingpkg.Equals(t, (*page.Page)(nil), bpm.FetchPage(types.PageID(0)))
}
