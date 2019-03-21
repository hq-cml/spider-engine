package index

import (
	"testing"
	"encoding/json"
	"github.com/hq-cml/spider-engine/basic"
)

func TestGetDocNodeSize(t *testing.T) {
	t.Log("DocNode Size:", basic.DocSize)

}

func TestSplitWordsRune(t *testing.T) {
	t.Skip()
	ret := SplitRuneWords(0, "我爱北京天安门, Hello world!")
	r, _ := json.Marshal(ret)
	t.Log(string(r))
}

func TestSplitWords(t *testing.T) {
	ret := SplitTrueWords(0, "我爱北京天安门, Hello world!")
	r, _ := json.Marshal(ret)
	t.Log(string(r))
}

func TestAddDoc(t *testing.T) {
	rIdx := NewReverseIndex(IDX_TYPE_STRING_SEG, 0, "/tmp/xx")
	rIdx.addDocument(0, "我爱北京天安门")
	rIdx.addDocument(1, "天安门上太阳升")
	r, _ := json.Marshal(rIdx.termMap)
	t.Log(string(r))
}