package index

import (
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/splitter"
	"strings"
)

// 索引类型说明
const (
	IDX_TYPE_PK = 101 //主键类型，特殊，primarykey和docid一一映射，正排和倒排都维护在一颗独立的B+树中

	IDX_TYPE_STR_WHOLE   = 201 //字符型索引, 全词匹配，倒排搜索，用于姓名等字段
	IDX_TYPE_STR_SPLITER = 202 //字符型索引, 切词匹配，倒排搜索，用于全文索引
	IDX_TYPE_STR_LIST    = 203 //字符型索引, 列表类型，倒排搜索，分号切词
	IDX_TYPE_STR_WORD    = 204 //字符型索引, 单字切词，倒排搜索，自然语言单个字母或者汉字

	IDX_TYPE_INTEGER = 301 //数字型索引，只支持整数，数字型索引只建立正排

	IDX_TYPE_DATE = 401 //日期型索引，数字类型的变种，只建立正排，转成时间戳存储

	IDX_TYPE_GOD = 501 //上帝视角索引, 特殊隐藏字段用于跨字段搜索, 只有倒排索引, 无正排

	IDX_TYPE_PURE_TEXT = 601 //纯文本字符类型, 只保存详情，不参与检索, 只有正排没有倒排
)

const (
	IDX_TYPE_NAME_PRIME = "primary"
	IDX_TYPE_NAME_WHOLE = "whole"
	IDX_TYPE_NAME_WORDS = "words"
	IDX_TYPE_NAME_PURE  = "pure"
	IDX_TYPE_NAME_TIME  = "time"
	IDX_TYPE_NAME_INT   = "number"
)

var IDX_MAP = map[string]uint16{
	IDX_TYPE_NAME_PRIME: IDX_TYPE_PK,
	IDX_TYPE_NAME_WHOLE: IDX_TYPE_STR_WHOLE,
	IDX_TYPE_NAME_WORDS: IDX_TYPE_STR_SPLITER,
	IDX_TYPE_NAME_PURE:  IDX_TYPE_PURE_TEXT,
	IDX_TYPE_NAME_TIME:  IDX_TYPE_DATE,
	IDX_TYPE_NAME_INT:   IDX_TYPE_INTEGER,
}

var RE_IDX_MAP = map[uint16]string{
	IDX_TYPE_PK:          IDX_TYPE_NAME_PRIME,
	IDX_TYPE_STR_WHOLE:   IDX_TYPE_NAME_WHOLE,
	IDX_TYPE_STR_SPLITER: IDX_TYPE_NAME_WORDS,
	IDX_TYPE_PURE_TEXT:   IDX_TYPE_NAME_PURE,
	IDX_TYPE_DATE:        IDX_TYPE_NAME_TIME,
	IDX_TYPE_INTEGER:     IDX_TYPE_NAME_INT,
}

//全局分词器
var Splitter splitter.Splitter

var punctuationMap = map[string]bool{
	" ": true,
	".": true, "。": true,
	",": true, "，": true,
	"\\": true, "、": true,
	";": true, "；": true,
	":": true, "：": true,
	"?": true, "？": true,
	"!": true, "！": true,
	"\"": true, "“": true, "”": true,
	"(": true, ")": true, "（": true, "）": true,
	"《": true, "》": true,
	"-": true, "—": true, "": true,
	"~": true, "#": true, "$": true,
	"%": true, "&": true, "`": true, "^": true,
	"+": true, "=": true,
	"【": true, "】": true,
}

const (
	BIGGER_MULTIPLE = 10000 //词频放大倍数，存储浮点不太方便，将词频放大10000倍取整存储，使用的时候再缩减回来
)

func init() {
	Splitter = splitter.NewSplitter("jieba")
}

//全词分词
func SplitWholeWords(docId uint32, content string) map[string]basic.DocNode {

	m := map[string]basic.DocNode{
		content: basic.DocNode{
			DocId: docId,
		},
	}
	return m
}

//分号分词
func SplitSemicolonWords(docId uint32, content string) map[string]basic.DocNode {
	terms := strings.Split(content, ";")
	m := map[string]basic.DocNode{}
	for _, term := range terms {
		node := basic.DocNode{
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
	for _, r := range rstr { //去重
		uniqMap[r] = true
	}

	m := map[string]basic.DocNode{}
	for term := range uniqMap {
		node := basic.DocNode{
			DocId:  docId,
			Weight: 0,
		}
		m[string(term)] = node
	}
	return m
}

//真分词, 利用分词器, 同时, 计算词频TF
func SplitTrueWords(docId uint32, content string) map[string]basic.DocNode {

	terms := Splitter.DoSplit(content, false)

	terms = trimPunctuation(terms) //过滤无意义的标点
	totalCnt := len(terms)

	//统计单词term的个数
	uniqMap := make(map[string]int)
	for _, term := range terms {
		if _, ok := uniqMap[term]; !ok {
			uniqMap[term] = 1
		} else {
			uniqMap[term]++
		}
	}
	m := map[string]basic.DocNode{}
	for term, termCnt := range uniqMap {
		node := basic.DocNode{
			DocId:  docId,
			Weight: uint16((float32(termCnt) / float32(totalCnt)) * BIGGER_MULTIPLE), //词频放大10000倍
		}
		m[term] = node
	}
	return m
}

//去除掉标点符号, 空格等
func trimPunctuation(in []string) []string {
	out := []string{}
	for _, v := range in {
		if _, ok := punctuationMap[v]; !ok {
			out = append(out, v)
		}
	}

	return out
}
