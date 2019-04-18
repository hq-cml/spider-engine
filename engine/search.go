package engine
/*
 * 搜索引擎的引擎部分
 * 负责搜索功能实现
 */

/*
 //从numbers判断pos指定的数,如果
// type:EQ 只要有一个==, 就算ok
// type:NEQ 必须全部都是!=, 就算ok
func (fwdIdx *ForwardIndex) FilterNums(pos uint32, filterType uint8, numbers []int64) bool {
	var value int64
	if fwdIdx.fake {   //TODO ??
		return false
	}

	//仅支持数值型
	if fwdIdx.indexType != IDX_TYPE_NUMBER {
		return false
	}

	if fwdIdx.inMemory {
		value = fwdIdx.memoryNum[pos]
	} else {
		if fwdIdx.baseMmap == nil {
			return false
		}

		offset := fwdIdx.fwdOffset + uint64(pos) * DATA_BYTE_CNT
		value = fwdIdx.baseMmap.ReadInt64(offset)
	}

	switch filterType {
	case basic.FILT_EQ:
		for _, num := range numbers {
			if (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == num) {
				return true
			}
		}
		return false
	case basic.FILT_NEQ:
		for _, num := range numbers {
			if (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == num) {
				return false
			}
		}
		return true

	default:
		return false
	}
}

//过滤
func (fwdIdx *ForwardIndex) Filter(pos uint32, filterRype uint8, start, end int64, str string) bool {

	var value int64
	if fwdIdx.fake {
		return false
	}

	if fwdIdx.indexType == IDX_TYPE_NUMBER {
		if fwdIdx.inMemory {
			value = fwdIdx.memoryNum[pos]
		} else {
			if fwdIdx.baseMmap == nil {
				return false
			}
			offset := fwdIdx.fwdOffset + uint64(pos) * DATA_BYTE_CNT
			value = fwdIdx.baseMmap.ReadInt64(offset)
		}

		switch filterRype {
		case basic.FILT_EQ:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == start)
		case basic.FILT_OVER:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value >= start)
		case basic.FILT_LESS:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value <= start)
		case basic.FILT_RANGE:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value >= start && value <= end)
		case basic.FILT_NEQ:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value != start)
		default:
			return false
		}
	} else if fwdIdx.indexType == IDX_TYPE_STRING_SINGLE || fwdIdx.indexType == IDX_TYPE_STRING{
		vl := strings.Split(str, ",")
		switch filterRype {

		case basic.FILT_STR_PREFIX:
			if vstr, ok := fwdIdx.GetString(pos); ok {
				for _, v := range vl {
					if strings.HasPrefix(vstr, v) {
						return true
					}
				}
			}
			return false
		case basic.FILT_STR_SUFFIX:
			if vstr, ok := fwdIdx.GetString(pos); ok {
				for _, v := range vl {
					if strings.HasSuffix(vstr, v) {
						return true
					}
				}
			}
			return false
		case basic.FILT_STR_RANGE:
			if vstr, ok := fwdIdx.GetString(pos); ok {
				for _, v := range vl {
					if !strings.Contains(vstr, v) {
						return false
					}
				}
				return true
			}
			return false
		case basic.FILT_STR_ALL:
			if vstr, ok := fwdIdx.GetString(pos); ok {
				for _, v := range vl {
					if vstr == v {
						return true
					}
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}

 */

/*

//过滤（针对的是正排索引）
func (fld *Field) Filter(docId uint32, filterType uint8, start, end int64, numbers []int64, str string) bool {
	if docId >= fld.StartDocId && docId < fld.NextDocId && fld.FwdIdx != nil {

		//Pos是docId在本索引中的位置
		pos := docId - fld.StartDocId

		if len(numbers) == 0 {
			return fld.FwdIdx.Filter(pos, filterType, start, end, str)
		} else {
			return fld.FwdIdx.FilterNums(pos, filterType, numbers)
		}
	}
	return false
}

 */


/*

//搜索（单query）
//根据query搜索结果, 再通过filter进行过滤
//bitmap从更高层传入
func (part *Partition) SearchDocs(query basic.SearchQuery, filters []basic.SearchFilted,
	bitmap *bitmap.Bitmap) ([]basic.DocNode, bool) {

	retDocs := []basic.DocNode{}
	//校验
	if filters != nil && len(filters) > 0 {
		for _, filter := range filters {
			if _, hasField := part.Fields[filter.FieldName]; hasField {
				return retDocs, false
			}
		}
	}

	//先用query查询, 如果为空, 则取出所有未删除的节点
	if query.Value == "" {
		for i := part.StartDocId; i < part.NextDocId; i++ {
			retDocs = append(retDocs, basic.DocNode{DocId: i})
		}
	} else {
		var match bool
		retDocs, match = part.Fields[query.FieldName].Query(query.Value)
		if !match {
			return retDocs, false
		}
	}

	//再用bitmap去掉已删除的数据
	if bitmap != nil {
		idx := 0
		for _, doc := range retDocs{
			//保留未删除的
			if bitmap.GetBit(uint64(doc.DocId)) == 0 {
				retDocs[idx] = doc
				idx++
			}
		}
		retDocs = retDocs[:idx]
	}

	//再使用过滤器
	if filters != nil && len(filters) > 0 {
		idx := 0
		for _, doc := range retDocs {
			match := true
			//必须全部的过滤器都满足
			for _, filter := range filters {
				if !part.Fields[filter.FieldName].Filter(doc.DocId, filter.Type, filter.Start, filter.End, filter.Range, filter.MatchStr) {
					match = false
					break
				}
				log.Debugf("Partition[%v] QUERY  %v", part.PartitionName, doc)
			}
			if match {
				retDocs[idx] = doc
				idx++
			}
		}
	}

	return retDocs, true
}

 */



/*
var GetDocIDsChan chan []basic.DocNode
var GiveDocIDsChan chan []basic.DocNode

func InteractionWithStartAndDf(a []basic.DocNode, b []basic.DocNode, start int, df int, maxdoc uint32) ([]basic.DocNode, bool) {

	if a == nil || b == nil {
		return a, false
	}

	lena := len(a)
	lenb := len(b)
	lenc := start
	ia := start
	ib := 0
	idf := math.Log10(float64(maxdoc) / float64(df))
	for ia < lena && ib < lenb {

		if a[ia].DocId == b[ib].DocId {
			a[lenc] = a[ia]
			//uint32((float64(a[ia].Weight) / 10000 * idf ) * 10000)
			a[lenc].Weight += uint32(float64(a[ia].Weight) * idf)
			lenc++
			ia++
			ib++
			continue
			//c = append(c, a[ia])
		}

		if a[ia].DocId < b[ib].DocId {
			ia++
		} else {
			ib++
		}
	}

	return a[:lenc], true
}

func (tbl *Table) SearchDocIds(querys []basic.SearchQuery, filteds []basic.SearchFilted) ([]basic.DocNode, bool) {

	var ok bool
	docids := <- GetDocIDsChan

	if len(querys) == 0 || querys == nil {
		for _, prt := range tbl.partitions {
			docids, _ = prt.SearchDocs(basic.SearchQuery{}, filteds, tbl.bitMap, docids)
		}
		if len(docids) > 0 {
			for _, doc := range docids {
				if tbl.bitMap.GetBit(uint64(doc.DocId)) == 1 {
					log.Infof("bitMap is 1 %v", doc.DocId)
				}
			}
			return docids, true
		}
		GiveDocIDsChan <- docids
		return nil, false
	}

	if len(querys) >= 1 {
		for _, prt := range tbl.partitions {
			docids, _ = prt.SearchDocs(querys[0], filteds, tbl.bitMap, docids)
		}
	}

	if len(querys) == 1 {
		if len(docids) > 0 {
			return docids, true
		}
		GiveDocIDsChan <- docids
		return nil, false
	}

	for _, query := range querys[1:] {

		subdocids := <- GetDocIDsChan
		for _, prt := range tbl.partitions {
			subdocids, _ = prt.SearchDocs(query, filteds, tbl.bitMap, subdocids)
		}

		//tbl.Logger.Info("[INFO] key[%v] doclens:%v", query.Value, len(subdocids))
		docids, ok = InteractionWithStartAndDf(docids, subdocids, 0, len(subdocids), tbl.NextDocId)
		GiveDocIDsChan <- subdocids
		if !ok {
			GiveDocIDsChan <- docids
			return nil, false
		}
	}

	if len(docids) > 0 {
		return docids, true
	}
	GiveDocIDsChan <- docids
	return nil, false

}
*/
