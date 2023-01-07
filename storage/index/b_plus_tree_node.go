package index

type NodeType uint8

const (
	NodeTypeUnknown NodeType = iota
	NodeTypeRoot
	NodeTypeInternal
	NodeTypeLeaf
)

type Node struct {
	nodeType NodeType
	body     []byte
}

func NewNode(data []byte) *Node {
	return &Node{
		nodeType: NodeTypeUnknown,
		body:     data,
	}
}

func (n *Node) IsNodeTypeLeaf() bool {
	return n.nodeType == NodeTypeLeaf
}

func (n *Node) IsNodeTypeInternal() bool {
	return n.nodeType == NodeTypeInternal
}

func (n *Node) SetNodeType(nodeType NodeType) {
	n.nodeType = nodeType
}
