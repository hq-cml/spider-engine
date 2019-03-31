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

// 过滤类型，对应filtertype
const (
	FILT_EQ          = 1  //等于
	FILT_OVER        = 2  //大于
	FILT_LESS        = 3  //小于
	FILT_RANGE       = 4  //范围内
	FILT_NOT         = 5  //不等于
	FILT_STR_PREFIX  = 11 //前缀
	FILT_STR_SUFFIX  = 12 //后缀
	FILT_STR_RANGE   = 13 //之内
	FILT_STR_ALL     = 14 //全词
)
