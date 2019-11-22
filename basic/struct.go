package basic

type DocNode struct {
	DocId  uint32
	Weight uint16           //权重，存储的时候是词频tf，搜索的时候，实时计算出tf-idf值
}

type DocInfo struct {
	Key    string
	Detail map[string]interface{}
}

var DOC_NODE_SIZE int

func init() {
	//DOC_NODE_SIZE = int(unsafe.Sizeof(DocNode{}))
	DOC_NODE_SIZE = 6
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

var FilterTypeMap = map[string]int{
	"=" 		: FILT_EQ,
	"!=" 		: FILT_NEQ,
	">" 		: FILT_MORE_THAN,
	"<" 		: FILT_LESS_THAN,
	"in" 		: FILT_IN,
	"not in"	: FILT_NOTIN,
	"between" 	: FILT_BETWEEN,

	"prefix" 	: FILT_STR_PREFIX,
	"suffix" 	: FILT_STR_SUFFIX,
	"contain" 	: FILT_STR_CONTAIN,
}

const (
	IDX_FILENAME_SUFFIX_BTREE  = ".btdb"
	IDX_FILENAME_SUFFIX_FWD    = ".fwd"
	IDX_FILENAME_SUFFIX_FWDEXT = ".ext"
	IDX_FILENAME_SUFFIX_INVERT = ".ivt"
	IDX_FILENAME_SUFFIX_META   = ".meta"
	IDX_FILENAME_SUFFIX_BITMAP = ".btmp"
)

type SearchFilter struct {
	FieldName       string   `json:"field"`   //需要过滤的字段，和搜索字段不是一个东西
	FilterType      string   `json:"type"` 	  //过滤类型: =, !=, >, <, in, not in, between, prefix, suffix, contain
	StrVal          string   `json:"str"` 	  //用于字符的==/!=, prefix, suffix
	IntVal          int64    `json:"int"` 	  //用于数字的==/!=/>/<
	Begin           int64    `json:"begin"`   //用于数字between
	End             int64    `json:"end"` 	  //用于数字between
	RangeNums       []int64  `json:"iranges"` //用于数字in或not in
	RangeStrs       []string `json:"sranges"` //用于字符in或not in
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
	Resp  chan *SpiderResponse  //用于结果接收
}

//抽象的返回结果
type SpiderResponse struct {
	Err   error              //表示是否有错误
	Data  interface{}        //实际结果
}

const (
	REQ_TYPE_DDL_ADD_FIELD = 10
	REQ_TYPE_DDL_DEL_FIELD = 11

	REQ_TYPE_DML_ADD_DOC   = 20
	REQ_TYPE_DML_DEL_DOC   = 21
	REQ_TYPE_DML_EDIT_DOC  = 22
)

func NewRequest(typ uint8, p interface{}) *SpiderRequest {
	return &SpiderRequest{
		Type: typ,
		Req:  p,
		Resp: make(chan *SpiderResponse),
	}
}

func NewResponse(err error, data interface{}) *SpiderResponse {
	return &SpiderResponse{
		Err : err,
		Data: data,
	}
}

//分区自动落地和合并参数
var (
	PART_PERSIST_MIN_DOC_CNT uint32  //1w个文档，内存分区满1w个文档，就落地一次
	PART_MERGE_MIN_DOC_CNT   uint32  //10w个文档，分区合并的一个参考值，合并一个分区至少拥有10w个Doc

	//Test
	//PART_PERSIST_MIN_DOC_CNT uint32 = 2
	//PART_MERGE_MIN_DOC_CNT uint32 = 6
)