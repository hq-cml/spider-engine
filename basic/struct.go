package basic

import "unsafe"

type DocNode struct {
	Docid  uint32
	Weight uint32
}

var DOC_NODE_SIZE int

func init() {
	DOC_NODE_SIZE = int(unsafe.Sizeof(DocNode{}))
}
