package sego

import (
	"github.com/hq-cml/spider-engine/utils/segmenter"
	"github.com/huichen/sego"
)

type SegoWrapper struct{
	sego.Segmenter
}

func NewSegoWrapper(dictFile string) segmenter.Segmenter {
	ss := SegoWrapper{}
	ss.Segmenter.LoadDictionary(dictFile)
	return &ss
}

func (ss *SegoWrapper)DoSegment(content string, searchMode bool) []string {
	result := ss.Segmenter.Segment([]byte(content))

	return sego.SegmentsToSlice(result, searchMode)
}
