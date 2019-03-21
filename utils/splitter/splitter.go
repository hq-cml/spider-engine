package splitter

import (
	"github.com/hq-cml/spider-engine/utils/splitter/sego"
	"github.com/hq-cml/spider-engine/utils/splitter/jieba"
)

/*
 * 分词器接口
 */
type Splitter interface {
	DoSplit(content string, searchMode bool) []string
}

//工厂
func NewSplitter(name string) Splitter {
	if name == "sego" {
		return sego.NewSegoWrapper("/tmp/dic.txt")
	} else {
		return jieba.NewJiebaWrapper()
	}
}
