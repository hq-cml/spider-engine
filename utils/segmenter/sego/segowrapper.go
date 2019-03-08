package sego

import (
	"github.com/huichen/sego"
)

type SegoSgememter struct{
	sego.Segmenter
}

func NewSegoSegmenter() SegoSgememter {
	ss := SegoSgememter{}
	ss.Segmenter.LoadDictionary("/tmp/dic.txt")
	return ss
}

func (ss *SegoSgememter)DoSegment(content []byte) []string {
	result := ss.Segmenter.Segment(content)

	return sego.SegmentsToSlice(result, false)
}