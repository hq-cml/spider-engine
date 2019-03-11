package sego

import (
	"testing"
	"strings"
)

func TestSego(t *testing.T) {
	sego := NewSegoWrapper("/tmp/dic.txt")
	s := []string{}
	//s = sego.DoSegment("我爱北京天安门hello world", true)
	//s = sego.DoSegment("我住在北京, 我的家乡是江苏", true)
	s = sego.DoSegment("我的名字是张二小", true)
	//s = sego.DoSegment("中华人民共和国", true)
	t.Log(strings.Join(s, " | "))
}