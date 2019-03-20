package jieba

/*
 * 结巴分词器包装
 */
import (
	"github.com/yanyiwu/gojieba"
)

type JiebaWrapper struct {
	*gojieba.Jieba
}

func NewJiebaWrapper() *JiebaWrapper {
	return &JiebaWrapper {
		Jieba: gojieba.NewJieba(),
	}
}

func (jw *JiebaWrapper)DoSplit(content string, searchMode bool) []string {
	use_hmm := true
	if searchMode {
		return jw.Jieba.CutForSearch(content, use_hmm)
	} else {
		return jw.Jieba.Cut(content, use_hmm)
	}
}


//更多牛逼用法....

//var s string
//var words []string
//use_hmm := true
//x := gojieba.NewJieba()
//defer x.Free()
//
//s = "我来到北京清华大学"
//fmt.Println(s)
//words = x.CutAll(s)
//fmt.Println("全模式:", strings.Join(words, "/"))
//
//words = x.Cut(s, use_hmm)
//fmt.Println("精确模式:", strings.Join(words, "/"))
//fmt.Println()
//
//s = "比特币"
//fmt.Println(s)
//words = x.Cut(s, use_hmm)
//fmt.Println("精确模式:", strings.Join(words, "/"))
//
//x.AddWord("比特币")
//words = x.Cut(s, use_hmm)
//fmt.Println("添加词典后,精确模式:", strings.Join(words, "/"))
//fmt.Println()
//
//s = "他来到了网易杭研大厦"
//fmt.Println(s)
//words = x.Cut(s, use_hmm)
//fmt.Println("新词识别:", strings.Join(words, "/"))
//fmt.Println()
//
//s = "小明硕士毕业于中国科学院计算所，后在日本京都大学深造"
//fmt.Println(s)
//words = x.CutForSearch(s, use_hmm)
//fmt.Println("搜索引擎模式:", strings.Join(words, "/"))
//fmt.Println()
//
//s = "长春市长春药店"
//fmt.Println(s)
//words = x.Tag(s)
//fmt.Println("词性标注:", strings.Join(words, ","))
//fmt.Println()
//
//s = "区块链"
//fmt.Println(s)
//words = x.Tag(s)
//fmt.Println("词性标注:", strings.Join(words, ","))
//fmt.Println()
//
//s = "长江大桥"
//fmt.Println(s)
//words = x.CutForSearch(s, !use_hmm)
//fmt.Println("搜索引擎模式:", strings.Join(words, "/"))
//wordinfos := x.Tokenize(s, gojieba.SearchMode, !use_hmm)
//fmt.Println("Tokenize:(搜索引擎模式)", wordinfos)
//wordinfos = x.Tokenize(s, gojieba.DefaultMode, !use_hmm)
//fmt.Println("Tokenize:(默认模式)", wordinfos)
//keywords := x.ExtractWithWeight(s, 5)
//fmt.Println("Extract:", keywords)
//fmt.Println()