package index

import (
	"fmt"
	"unsafe"
)

type NodeType int

const (
	NodeTypeUnknown NodeType = iota
	NodeTypeRoot
	NodeTypeInternal
	NodeTypeLeaf
)

type NodeHeader struct {
	nodeType NodeType
}

type Node struct {
	header *NodeHeader
	body   []byte
}

func NewNode(data []byte) *Node {
	node := Node{}
	headerSize := int(unsafe.Sizeof(*node.header))
	if headerSize+1 > len(data) {
		fmt.Println("Node header must be aligned")
		return nil
	}

	node.header = (*NodeHeader)(unsafe.Pointer(&data[0]))
	node.body = data[headerSize:]
	return &node
}

func (n *Node) SetNodeType(nodeType NodeType) {
	n.header.nodeType = nodeType
}
