package jieba

import (
	"testing"
	"strings"
)

func TestJieba(t *testing.T) {
	jw := NewJiebaWrapper()
	s := []string{}
	//s = jw.DoSplit("我爱北京天安门hello world", true)
	//s = jw.DoSplit("我住在北京, 我的家乡是江苏", true)
	//s = jw.DoSplit("我的名字是张二小", true)
	//s = jw.DoSplit("中华人民共和国", true)
	//s = jw.DoSplit("中华人民共和国", false)
	//s = jw.DoSplit("我爱北京天安门", false)
	s = jw.DoSplit("我爱北京天安门", true)
	t.Log(strings.Join(s, " | "))
}