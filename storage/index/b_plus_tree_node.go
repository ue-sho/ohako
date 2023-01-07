package index

import (
	"unsafe"
)

type NodeType string

const (
	NodeTypeUnknown  NodeType = "UNKNOWN "
	NodeTypeRoot              = "ROOT    "
	NodeTypeInternal          = "INTERNAL"
	NodeTypeLeaf              = "LEAF    "
)

type NodeHeader struct {
	nodeType [8]byte
}

func (h *NodeHeader) NodeTypeString() string {
	return string(h.nodeType[:])
}

type Node struct {
	header *NodeHeader
	body   []byte
}

func NewNode(data []byte) *Node {
	node := Node{}
	headerSize := int(unsafe.Sizeof(*node.header))
	if headerSize+1 > len(data) {
		panic("node header must be aligned")
	}

	node.header = (*NodeHeader)(unsafe.Pointer(&data[0]))
	node.body = data[headerSize:]
	return &node
}

func (n *Node) SetHeader(nodeType NodeType) {
	copy(n.header.nodeType[:], []byte(nodeType))
}
