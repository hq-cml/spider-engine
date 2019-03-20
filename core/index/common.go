package index

import "github.com/hq-cml/spider-engine/utils/spliter"

// 索引类型说明
const (
	IDX_TYPE_STRING        = 1 //字符型索引[全词匹配]
	IDX_TYPE_STRING_SEG    = 2 //字符型索引[切词匹配，全文索引,hash存储倒排]
	IDX_TYPE_STRING_LIST   = 3 //字符型索引[列表类型，分号切词，直接切分,hash存储倒排]
	IDX_TYPE_STRING_SINGLE = 4 //字符型索引[单字切词]

	IDX_TYPE_NUMBER = 11 //数字型索引，只支持整数，数字型索引只建立正排

	IDX_TYPE_DATE = 15 //日期型索引 '2015-11-11 00:11:12'，日期型只建立正排，转成时间戳存储

	IDX_TYPE_PK = 21 //主键类型，倒排正排都需要，倒排使用B树存储
	GATHER_TYPE = 22 //汇总类型，倒排正排都需要[后续使用]

	IDX_ONLYSTORE = 30 //只保存详情，不参与检索
)

var Spliter spliter.Spliter

func init() {
	Spliter = spliter.NewSpliter("jieba")
}

//单个词模式分词, 将一个string分解成为一个个的rune (去重)
func SplitWordsSingle(content string) []string{
	rstr := []rune(content)
	tempMap := make(map[rune]bool)
	for _, r := range rstr {
		tempMap[r] = true
	}
	result := []string{}
	for k := range tempMap {
		result = append(result, string(k))
	}
	return result
}