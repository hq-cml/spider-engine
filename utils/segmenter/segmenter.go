package segmenter

/*
 * 分词器接口
 */
type Segmenter interface {
	DoSegment(content string, searchMode bool) []string
}
