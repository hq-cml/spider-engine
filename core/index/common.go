package index

import (
	"github.com/hq-cml/spider-engine/utils/splitter"
	"github.com/hq-cml/spider-engine/basic"
	"strings"
)

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

const DOCNODE_SIZE int = 8 //12 TODO ??

var Splitter splitter.Splitter

func init() {
	Splitter = splitter.NewSplitter("jieba")
}

//全词分词
func SplitWholeWords(docId uint32, content string) map[string]basic.DocNode {

	m := map[string]basic.DocNode {
		content: basic.DocNode {
			Docid: docId,
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
			Docid: docId,
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
			Docid: docId,
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
			Docid: docId,
			Weight: uint32((float32(tf)/float32(totalTerm)) * 10000), //TODO 这个10000是个魔幻数字
		}
		m[term] = node
	}
	return m
}
