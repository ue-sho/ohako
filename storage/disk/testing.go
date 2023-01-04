package disk

import (
	"os"
)

// テスト用のDiskManager
type DiskManagerTest struct {
	path string
	DiskManager
}

// テスト用のDiskManagerインスタンスを生成する
func NewDiskManagerTest() DiskManager {
	f, err := os.CreateTemp("", "")
	if err != nil {
		panic(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)

	diskManager := NewDiskManagerImpl(path)
	return &DiskManagerTest{path, diskManager}
}

// データベースファイルをクローズする
func (d *DiskManagerTest) ShutDown() {
	defer os.Remove(d.path)
	d.DiskManager.ShutDown()
}
