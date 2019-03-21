package basic

import "unsafe"

type DocNode struct {
	Docid  uint32
	Weight uint32
}

var DocSize uint32

func init() {
	DocSize = uint32(unsafe.Sizeof(DocNode{}))
}
