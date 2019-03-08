package sego

import (
	"testing"
)

func TestSego(t *testing.T) {
	sego := NewSegoSegmenter()
	s := sego.DoSegment([]byte("hello world"))
	for _, v := range s {
		t.Log(v)
	}

}