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

const (
	IDX_FILENAME_SUFFIX_BTREE  = ".btdb"
	IDX_FILENAME_SUFFIX_FWD    = ".fwd"
	IDX_FILENAME_SUFFIX_FWDEXT = ".ext"
	IDX_FILENAME_SUFFIX_INVERT = ".ivt"
	IDX_FILENAME_SUFFIX_META = ".meta"
)

/*************************************************************************
索引查询接口
索引查询分为 查询和过滤,统计，子查询四种
查询：倒排索引匹配
过滤：正排索引过滤
统计：汇总某个字段，然后进行统计计算
子查询：必须是有父子
************************************************************************/
//查询接口数据结构[用于倒排索引查询]，内部都是求交集
type SearchQuery struct {
	FieldName string `json:"_field"`
	Value     string `json:"_value"`
	Type      uint64 `json:"_type"`
}

//过滤接口数据结构，内部都是求交集
type SearchFilted struct {
	FieldName string   `json:"_field"`
	Start     int64    `json:"_start"`
	End       int64    `json:"_end"`
	Range     []int64  `json:"_range"`
	Type      uint64   `json:"_type"`
	MatchStr  string   `json:"_matchstr"`
	RangeStr  []string `json:"_rangestr"`
}
