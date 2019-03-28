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
	FILT_EQ         uint64 = 1  //等于
	FILT_OVER       uint64 = 2  //大于
	FILT_LESS       uint64 = 3  //小于
	FILT_RANGE      uint64 = 4  //范围内
	FILT_NOT        uint64 = 5  //不等于
	FILT_STR_PREFIX uint64 = 11 //前缀
	FILT_STR_SUFFIX uint64 = 12 //后缀
	FILT_STR_RANGE  uint64 = 13 //之内
	FILT_STR_ALL    uint64 = 14 //全词
)
