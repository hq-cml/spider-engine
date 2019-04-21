package index

import (
	"github.com/hq-cml/spider-engine/splitter"
	"github.com/hq-cml/spider-engine/basic"
	"strings"
)

// 索引类型说明
const (
	IDX_TYPE_PK            = 101 //主键类型，特殊，只有倒排，使用独立的B+树存储

	IDX_TYPE_STRING        = 201 //字符型索引[全词匹配，倒排，用于姓名等字段]
	IDX_TYPE_STRING_SEG    = 202 //字符型索引[切词匹配，倒排，用于全文索引]
	IDX_TYPE_STRING_LIST   = 203 //字符型索引[列表类型，倒排，分号切词]
	IDX_TYPE_STRING_SINGLE = 204 //字符型索引[单字切词，倒排，自然语言单个字母或者汉字]

	IDX_TYPE_NUMBER        = 301 //数字型索引，只支持整数，数字型索引只建立正排
	IDX_TYPE_DATE          = 302 //日期型索引 '2019-01-01 00:11:12'，日期型只建立正排，转成时间戳存储

	GATHER_TYPE            = 401 //汇总类型，倒排正排都需要[后续使用]

	IDX_ONLYSTORE          = 501 //只保存详情，不参与检索
)

//全局分词器
var Splitter splitter.Splitter

func init() {
	Splitter = splitter.NewSplitter("jieba")
}

//全词分词
func SplitWholeWords(docId uint32, content string) map[string]basic.DocNode {

	m := map[string]basic.DocNode {
		content: basic.DocNode {
			DocId: docId,
		},
	}
	return m
}

//分号分词
func SplitSemicolonWords(docId uint32, content string) map[string]basic.DocNode {
	terms := strings.Split(content, ";")
	m := map[string]basic.DocNode {}
	for _, term := range terms {
		node := basic.DocNode {
			DocId: docId,
		}
		m[term] = node
	}
	return m
}

//单个词模式分词, 将一个string分解成为一个个的rune (去重), 计算词频TF=0
func SplitRuneWords(docId uint32, content string) map[string]basic.DocNode {
	rstr := []rune(content)
	uniqMap := make(map[rune]bool)
	for _, r := range rstr {   //去重
		uniqMap[r] = true
	}

	m := map[string]basic.DocNode {}
	for term := range uniqMap {
		node := basic.DocNode {
			DocId:  docId,
			Weight: 0,
		}
		m[string(term)] = node
	}
	return m
}

//真分词, 利用分词器, 同时, 计算词频TF
func SplitTrueWords(docId uint32, content string) map[string]basic.DocNode {

	terms :=  Splitter.DoSplit(content, false) //TODO true config
	totalTerm := len(terms)

	uniqMap := make(map[string]int)
	for _, term := range terms {
		if _, ok := uniqMap[term]; !ok {
			uniqMap[term] = 1
		} else {
			uniqMap[term] ++
		}
	}
	m := map[string]basic.DocNode {}
	for term, tf := range uniqMap {
		node := basic.DocNode {
			DocId:  docId,
			Weight: uint32((float32(tf)/float32(totalTerm)) * 10000), //TODO 这个10000是个魔幻数字
		}
		m[term] = node
	}
	return m
}
