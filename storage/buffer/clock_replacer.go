package buffer

type FrameID uint32

// クロック置換(ページ置換)アルゴリズム
// Least Recently Used(最後に使われてからの経過時間が最も長い: LRU)を採用
type ClockReplacer struct {
	cList     *circularList // 使わなくなったnodeを入れる円環リスト
	clockHand **node        // 先頭nodeのポインタ
}

// LRUアルゴリズムを元にVictimフレームを取得する(リストからは削除する)
func (c *ClockReplacer) Victim() *FrameID {
	if c.cList.size == 0 {
		return nil
	}

	var victimFrameID *FrameID
	currentNode := (*c.clockHand)
	for {
		if currentNode.value.(bool) {
			currentNode.value = false
			c.clockHand = &currentNode.next
		} else {
			frameID := currentNode.key.(FrameID)
			victimFrameID = &frameID

			c.clockHand = &currentNode.next

			c.cList.remove(currentNode.key)
			return victimFrameID
		}
	}
}

// フレームのピンを外す
// フレームを使用しなくなったのでリストに追加する
func (c *ClockReplacer) Unpin(id FrameID) {
	if !c.cList.hasKey(id) {
		c.cList.insert(id, true)
		if c.cList.size == 1 {
			c.clockHand = &c.cList.head
		}
	}
}

// フレームをピンする
// フレームを使っているのでリストから削除する
func (c *ClockReplacer) Pin(id FrameID) {
	node := c.cList.find(id)
	if node == nil {
		return
	}

	if (*c.clockHand) == node {
		c.clockHand = &(*c.clockHand).next
	}
	c.cList.remove(id)

}

// サイズ
func (c *ClockReplacer) Size() uint32 {
	return c.cList.size
}

// ClockReplacerを生成する
func NewClockReplacer(poolSize uint32) *ClockReplacer {
	cList := newCircularList(poolSize)
	return &ClockReplacer{cList, &cList.head}
}
