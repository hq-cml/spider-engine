package jieba

import (
	"testing"
	"strings"
)

func TestJieba(t *testing.T) {
	jw := NewJiebaWrapper()
	s := []string{}
	//s = jw.DoSegment("我爱北京天安门hello world", true)
	//s = jw.DoSegment("我住在北京, 我的家乡是江苏", true)
	s = jw.DoSplit("我的名字是张二小", true)
	//s = jw.DoSegment("中华人民共和国", true)
	t.Log(strings.Join(s, " | "))
}