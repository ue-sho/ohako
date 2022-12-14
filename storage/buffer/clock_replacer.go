package buffer

type FrameID uint32

// Clock-Sweepアルゴリズム
// Least Recently Used(最後に使われてからの経過時間が最も長い: LRU)の近似アルゴリズム
type ClockReplacer struct {
	cList     *circularList[FrameID, int32] // 使わなくなったnodeを入れる円環リスト
	clockHand **node[FrameID, int32]        // 先頭nodeのポインタ
}

// LRUアルゴリズムを元にVictimフレームを取得する
func (c *ClockReplacer) Victim() *FrameID {
	if c.cList.size == 0 {
		return nil
	}

	var victimFrameID *FrameID
	currentNode := (*c.clockHand)
	for {
		if currentNode.value > 0 {
			currentNode.value -= 1
			c.clockHand = &currentNode.next
		} else {
			frameID := currentNode.key
			victimFrameID = &frameID

			c.clockHand = &currentNode.next

			// リストからは削除する
			c.cList.remove(currentNode.key)
			return victimFrameID
		}
	}
}

// フレームのピンを外す
func (c *ClockReplacer) Unpin(id FrameID) {
	if c.cList.hasKey(id) {
		return
	}

	// フレームを使用しなくなったのでリストに追加する
	c.cList.insert(id, 1)
	if c.cList.size == 1 {
		c.clockHand = &c.cList.head
	}
}

// フレームをピンする
func (c *ClockReplacer) Pin(id FrameID) {
	node := c.cList.find(id)
	if node == nil {
		return
	}

	// フレームを使っているのでリストから削除する
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
	cList := newCircularList[FrameID, int32](poolSize)
	return &ClockReplacer{cList, &cList.head}
}
