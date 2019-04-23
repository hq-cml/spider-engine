package basic

import "unsafe"

type DocNode struct {
	DocId  uint32
	Weight uint32
}

var DOC_NODE_SIZE int

func init() {
	DOC_NODE_SIZE = int(unsafe.Sizeof(DocNode{}))
}

// 过滤类型，对应filtertype
const (
	FILT_EQ          = 1  //等于
	FILT_NEQ 		 = 2  //不等于
	FILT_OVER        = 3  //大于, 仅数字支持
	FILT_LESS        = 4  //小于, 仅数字支持
	FILT_IN          = 5  //IN
	FILT_NOTIN       = 6  //NOT IN
	FILT_BETWEEN     = 7  //范围内

	FILT_STR_PREFIX  = 11 //前缀
	FILT_STR_SUFFIX  = 12 //后缀
	FILT_STR_RANGE   = 13 //之内
	FILT_STR_ALL     = 14 //全词
)

const (
	IDX_FILENAME_SUFFIX_BTREE  = ".btdb"
	IDX_FILENAME_SUFFIX_FWD    = ".fwd"
	IDX_FILENAME_SUFFIX_FWDEXT = ".ext"
	IDX_FILENAME_SUFFIX_INVERT = ".ivt"
	IDX_FILENAME_SUFFIX_META   = ".meta"
	IDX_FILENAME_SUFFIX_BITMAP = ".btmp"
)

const (
	MODIFY_TYPE_ADD    uint8 = 1
	MODIFY_TYPE_UPDATE uint8 = 2
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

//过滤接口数据结构，内部都是求交集
type SearchFilted struct {
	FieldName string   `json:"_field"`
	Start     int64    `json:"_start"`
	End       int64    `json:"_end"`
	Range     []int64  `json:"_range"`
	Type      uint8    `json:"_type"`
	MatchStr  string   `json:"_matchstr"`
	RangeStr  []string `json:"_rangestr"`
}
