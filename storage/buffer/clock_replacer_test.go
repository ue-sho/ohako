package buffer

import (
	"testing"

	testingpkg "github.com/ue-sho/ohako/testing"
)

func TestClockReplacer(t *testing.T) {
	clockReplacer := NewClockReplacer(7)

	// シナリオ: 6つの要素をアンピンする. つまりリプレーサーに追加する
	clockReplacer.Unpin(1)
	clockReplacer.Unpin(2)
	clockReplacer.Unpin(3)
	clockReplacer.Unpin(4)
	clockReplacer.Unpin(5)
	clockReplacer.Unpin(6)
	clockReplacer.Unpin(1) // 同じフレームは新規追加されない
	testingpkg.Equals(t, uint32(6), clockReplacer.Size())

	// シナリオ: 3つのVictimを取得する
	var value *FrameID
	value = clockReplacer.Victim()
	testingpkg.Equals(t, FrameID(1), *value)
	value = clockReplacer.Victim()
	testingpkg.Equals(t, FrameID(2), *value)
	value = clockReplacer.Victim()
	testingpkg.Equals(t, FrameID(3), *value)

	// シナリオ: リプレイサーの要素を2つピンする
	// 3はすでにVictimになっているので、3をピン留めしても効果はない
	clockReplacer.Pin(3)
	clockReplacer.Pin(4)
	testingpkg.Equals(t, uint32(2), clockReplacer.Size())

	// シナリオ: 4のピンを外す
	clockReplacer.Unpin(4)

	// シナリオ: さらにVictimを探す.
	value = clockReplacer.Victim()
	testingpkg.Equals(t, FrameID(5), *value)
	value = clockReplacer.Victim()
	testingpkg.Equals(t, FrameID(6), *value)
	value = clockReplacer.Victim()
	testingpkg.Equals(t, FrameID(4), *value)
}
