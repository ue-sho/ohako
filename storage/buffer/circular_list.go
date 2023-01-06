package buffer

import (
	"errors"
	"fmt"
)

type node[T, U any] struct {
	key   T
	value U
	next  *node[T, U]
	prev  *node[T, U]
}

type circularList[T comparable, U any] struct {
	head     *node[T, U]
	tail     *node[T, U]
	size     uint32
	capacity uint32 // 最大サイズ
}

// headから辿って指定のkeyのnodeを探す
func (c *circularList[T, U]) find(key T) *node[T, U] {
	ptr := c.head
	for i := uint32(0); i < c.size; i++ {
		if ptr.key == key {
			return ptr
		}
		ptr = ptr.next
	}
	return nil
}

// keyを持っているか否か
func (c *circularList[T, U]) hasKey(key T) bool {
	return c.find(key) != nil
}

// nodeを挿入する
func (c *circularList[T, U]) insert(key T, value U) error {
	if c.isFull() {
		return errors.New("capacity is full")
	}

	newNode := &node[T, U]{key, value, nil, nil}
	if c.size == 0 {
		newNode.next = newNode
		newNode.prev = newNode
		c.head = newNode
		c.tail = newNode
		c.size++
		return nil
	}

	node := c.find(key)
	if node != nil {
		// keyが存在する場合valueを更新
		node.value = value
		return nil
	}

	newNode.next = c.head
	newNode.prev = c.tail

	c.tail.next = newNode
	if c.head == c.tail {
		// MEMO: 上記行と同じことをしているので、意味なさそう？
		c.head.next = newNode
	}

	c.tail = newNode
	c.head.prev = c.tail

	c.size++

	return nil
}

// keyのnodeを削除する
func (c *circularList[T, U]) remove(key T) {
	node := c.find(key)
	if node == nil {
		return
	}

	if c.size == 1 {
		c.head = nil
		c.tail = nil
		c.size--
		return
	}

	if node == c.head {
		c.head = c.head.next
	}

	if node == c.tail {
		c.tail = c.tail.prev
	}

	node.next.prev = node.prev
	node.prev.next = node.next

	c.size--
}

func (c *circularList[T, U]) isFull() bool {
	return c.size == c.capacity
}

// debug用
func (c *circularList[T, U]) print() {
	if c.size == 0 {
		fmt.Println(nil)
	}
	ptr := c.head
	for i := uint32(0); i < c.size; i++ {
		fmt.Println(ptr.key, ptr.value, ptr.prev.key, ptr.next.key)
		ptr = ptr.next
	}
}

func newCircularList[T, U comparable](maxSize uint32) *circularList[T, U] {
	return &circularList[T, U]{nil, nil, 0, maxSize}
}
