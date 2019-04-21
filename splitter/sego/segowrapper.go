package sego

/*
 * Sego分词器包装
 */
import (
	"github.com/huichen/sego"
)

type SegoWrapper struct{
	sego.Segmenter
}

func NewSegoWrapper(dictFile string) *SegoWrapper {
	ss := SegoWrapper{}
	ss.Segmenter.LoadDictionary(dictFile)
	return &ss
}

func (ss *SegoWrapper)DoSplit(content string, searchMode bool) []string {
	result := ss.Segmenter.Segment([]byte(content))

	return sego.SegmentsToSlice(result, searchMode)
}
