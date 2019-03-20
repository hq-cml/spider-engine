package spliter

import (
	"github.com/hq-cml/spider-engine/utils/spliter/sego"
	"github.com/hq-cml/spider-engine/utils/spliter/jieba"
)

/*
 * 分词器接口
 */
type Spliter interface {
	DoSplit(content string, searchMode bool) []string
}

//工厂
func NewSpliter(name string) Spliter {
	if name == "sego" {
		return sego.NewSegoWrapper("/tmp/dic.txt")
	} else {
		return jieba.NewJiebaWrapper()
	}
}
