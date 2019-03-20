package index

import "testing"

func TestSplitWordsSingle(t *testing.T) {
	ret := SplitWordsSingle("我爱北京天安门, Hello world!")
	t.Log(ret)
}