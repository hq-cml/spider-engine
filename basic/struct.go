package basic

import (
	"unsafe"
)

type DocNode struct {
	DocId  uint32
	Weight uint32
}

type DocInfo struct {
	Key    string
	Detail map[string]interface{}
}

var DOC_NODE_SIZE int

func init() {
	DOC_NODE_SIZE = int(unsafe.Sizeof(DocNode{}))
}

// 过滤类型，对应filtertype
const (
	FILT_EQ          = 1  //等于
	FILT_NEQ 		 = 2  //不等于
	FILT_MORE_THAN   = 3  //大于, 仅数字支持
	FILT_LESS_THAN   = 4  //小于, 仅数字支持
	FILT_IN          = 5  //IN
	FILT_NOTIN       = 6  //NOT IN
	FILT_BETWEEN     = 7  //范围内

	FILT_STR_PREFIX  = 11 //前缀, 仅数字字符
	FILT_STR_SUFFIX  = 12 //后缀, 仅数字字符
	FILT_STR_CONTAIN = 13 //包含
)

const (
	IDX_FILENAME_SUFFIX_BTREE  = ".btdb"
	IDX_FILENAME_SUFFIX_FWD    = ".fwd"
	IDX_FILENAME_SUFFIX_FWDEXT = ".ext"
	IDX_FILENAME_SUFFIX_INVERT = ".ivt"
	IDX_FILENAME_SUFFIX_META   = ".meta"
	IDX_FILENAME_SUFFIX_BITMAP = ".btmp"
)

/*************************************************************************
索引查询接口
索引查询分为 查询和过滤, 统计，子查询四种
查询：倒排索引匹配
过滤：正排索引过滤
统计：汇总某个字段，然后进行统计计算
子查询：必须是有父子
************************************************************************/
//查询接口数据结构[用于倒排索引查询]，内部都是求交集
type SearchQuery struct {
	FieldName string `json:"_field"`   //要过滤的字段
	Value     string `json:"_value"`   //要过滤的值
	Type      uint64 `json:"_type"`    //过滤类型
}

type SearchFilter struct {
	FieldName       string
	FilterType      uint8
	StrVal          string   //用于字符的==/!=
	IntVal          int64    //用于数字的==/!=/>/<
	Begin           int64    //用于数字between
	End             int64    //用于数字between
	RangeNums       []int64  //用于数字in或not in
	RangeStrs       []string //用于字符in或not in
}

const (
	RET_CODE_OK  = iota
	RET_CODE_FAILED
	RET_CODE_ERROR
)

//Http response body
type Result struct {
	Code int			`json:"code"`
	Msg  string			`json:"msg"`
	Data interface{}	`json:"data"`
}

func NewOkResult(data interface{}) *Result {
	return &Result{
		Code: RET_CODE_OK,
		Msg: "ok",
		Data:data,
	}
}

func NewFailedResult(data interface{}) *Result {
	return &Result{
		Code: RET_CODE_FAILED,
		Msg: "failed",
		Data:data,
	}
}

func NewErrorResult(data interface{}) *Result {
	return &Result{
		Code: RET_CODE_ERROR,
		Msg: "error",
		Data:data,
	}
}

//抽象的请求
type SpiderRequest struct {
	Type  uint8
	Req   interface{}
	Resp  chan interface{}  //用于结果接收
}

const (
	REQ_TYPE_DDL_ADD_FIELD = 10
	REQ_TYPE_DDL_DEL_FIELD = 11

	REQ_TYPE_DML_ADD_DOC   = 20
	REQ_TYPE_DML_DEL_DOC   = 21
	REQ_TYPE_DML_EDIT_DOC  = 22
)