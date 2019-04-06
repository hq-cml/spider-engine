
package partition
/*
 * 分区, 类比于Mysql的分区的概念
 * 每个分区都拥有全量的filed, 但是数据是整体表数据的一部分,
 * 每个分区是独立的索引单元, 所有的分区合在一起, 就是一张完整的表
 */
import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/bitmap"
)

const (
	PARTITION_MAX_DOC_CNT = 1000000   //10w个文档，组成一个partition
)

// Segment description:段结构
type Partition struct {
	StartDocId  	uint32 							 `json:"startdocid"`
	NextDocId   	uint32 							 `json:"nextdocid"`      //下次的DocId（所以Max的DocId是NextId-1）
	SegmentName 	string                           `json:"segmentname"`
	FieldSummaries  map[string]field.FieldSummary    `json:"fields"`
	Fields     		map[string]*field.Field
	isMemory   		bool
	ivtMmap     	*mmap.Mmap
	btreeDb     	btree.Btree
	baseMmap    	*mmap.Mmap
	extMmap     	*mmap.Mmap
}

//新建一个空分区, 包含字段
func NewEmptyPartitionWithFieldsInfo(partitionName string, start uint32, fields []field.FieldSummary) *Partition {

	part := &Partition{
		btreeDb: nil,
		StartDocId: start,
		NextDocId: start,
		SegmentName: partitionName,
		ivtMmap: nil,
		extMmap: nil,
		baseMmap: nil,
		Fields: make(map[string]*field.Field),
		FieldSummaries: make(map[string]field.FieldSummary),
		isMemory: true,
	}

	for _, fld := range fields {
		fieldSummary := field.FieldSummary{
			FieldName: fld.FieldName,
			FieldType: fld.FieldType,
		}
		part.FieldSummaries[fld.FieldName] = fieldSummary
		emptyField := field.NewEmptyField(fld.FieldName, start, fld.FieldType)
		part.Fields[fld.FieldName] = emptyField
	}

	log.Infof("Make New Segment [%v] Success ", partitionName)
	return part
}

//从文件加载一个分区
//TODO ?? partition自己的mmap和b+树
func LoadPartition(partitionName string) *Partition {

	part := &Partition{
		btreeDb: nil,
		StartDocId: 0,
		NextDocId: 0,
		SegmentName: partitionName,
		ivtMmap: nil,
		extMmap: nil,
		baseMmap: nil,
		Fields: make(map[string]*field.Field),
		FieldSummaries: make(map[string]field.FieldSummary),
		isMemory: false,
	}

	metaFileName := fmt.Sprintf("%v.meta", partitionName)
	buffer, err := helper.ReadFile(metaFileName)
	if err != nil {
		return part
	}

	err = json.Unmarshal(buffer, part)
	if err != nil {
		return part
	}

	btdbPath := fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_BTREE, partitionName)
	if helper.Exist(btdbPath) {
		log.Debugf("Load B+Tree File : %v", btdbPath)
		part.btreeDb = btree.NewBtree("", btdbPath)
	}

	//TODO 改变较大
	//part.ivtMmap, err = utils.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_INVERT, partitionName), utils.MODE_APPEND)
	part.ivtMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_INVERT, partitionName), true, 0)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	//part.ivtMmap.SetFileEnd(0)
	log.Debugf("Load Invert File : %v.idx ", partitionName)

	//part.baseMmap, err = utils.NewMmap(fmt.Sprintf("%v.pfl", partitionName), utils.MODE_APPEND)
	part.baseMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWD, partitionName), true, 0)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	//part.baseMmap.SetFileEnd(0)
	log.Debugf("Load Profile File : %v.pfl", partitionName)

	//part.extMmap, err = utils.NewMmap(fmt.Sprintf("%v.dtl", partitionName), utils.MODE_APPEND)
	part.extMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWDEXT, partitionName), true, 0)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	//part.extMmap.SetFileEnd(0)
	log.Debugf("[INFO] Load Detail File : %v.dtl", partitionName)

	//TODO 从文件加载进来的??
	//for _, field := range part.FieldInfos {
	for _, fld := range part.FieldSummaries {
		if fld.FwdDocCnt == 0 {
			newField := field.NewEmptyField(fld.FieldName, part.StartDocId, fld.FieldType)
			part.Fields[fld.FieldName] = newField
		} else {
			oldField := field.LoadField(fld.FieldName, part.StartDocId,
				part.NextDocId, fld.FieldType, fld.FwdOffset, fld.FwdDocCnt,
				part.ivtMmap, part.baseMmap, part.extMmap, false, part.btreeDb)
			part.Fields[fld.FieldName] = oldField
		}
	}
	return part
}

//判断为空
func (prt *Partition) IsEmpty() bool {
	return prt.StartDocId == prt.NextDocId
}

//添加字段
func (prt *Partition) AddField(summary field.FieldSummary) error {

	if _, ok := prt.FieldSummaries[summary.FieldName]; ok {
		log.Warnf("Segment --> AddField Already has field [%v]", summary.FieldName)
		return errors.New("Already has field..")
	}

	if prt.isMemory && !prt.IsEmpty() {
		log.Warnf("Segment --> AddField field [%v] fail..", summary.FieldName)
		return errors.New("memory segment can not add field..")
	}

	prt.FieldSummaries[summary.FieldName] = summary
	newFiled := field.NewEmptyField(summary.FieldName, prt.NextDocId, summary.FieldType)
	prt.Fields[summary.FieldName] = newFiled
	return nil
}

//删除字段
func (prt *Partition) DeleteField(fieldname string) error {
	if _, exist := prt.FieldSummaries[fieldname]; !exist {
		log.Warnf("Partition --> DeleteField not found field [%v]", fieldname)
		return errors.New("not found field")
	}


	//TODO why ??
	if prt.isMemory && !prt.IsEmpty() {
		log.Warnf("Segment --> deleteField field [%v] fail..", fieldname)
		return errors.New("memory segment can not delete field..")
	}

	prt.Fields[fieldname].Destroy()
	delete(prt.Fields, fieldname)
	delete(prt.FieldSummaries, fieldname)
	log.Infof("Segment --> DeleteField[%v] :: Success ", fieldname)
	return nil
}

//更新文档
//content, 一篇文档的各个字段的值
func (prt *Partition) UpdateDocument(docid uint32, content map[string]string) error {
	//TODO >=???
	if docid >= prt.NextDocId || docid < prt.StartDocId {
		log.Errf("Partition --> UpdateDocument :: Wrong DocId[%v]  NextDocId[%v]", docid, prt.NextDocId)
		return errors.New("Partition --> UpdateDocument :: Wrong DocId Number")
	}

	for name, _ := range prt.Fields {
		if _, ok := content[name]; !ok {
			if err := prt.Fields[name].UpdateDocument(docid, ""); err != nil {
				log.Errf("Partition --> UpdateDocument :: %v", err)
			}
		} else {
			if err := prt.Fields[name].UpdateDocument(docid, content[name]); err != nil {
				log.Errf("Partition --> UpdateDocument :: field[%v] value[%v] error[%v]", name, content[name], err)
			}
		}
	}

	return nil
}

//添加文档
//content, 一篇文档的各个字段的值
func (prt *Partition) AddDocument(docid uint32, content map[string]string) error {

	if docid != prt.NextDocId {
		log.Errf("Partition --> AddDocument :: Wrong DocId[%v]  NextDocId[%v]", docid, prt.NextDocId)
		return errors.New("Partition --> AddDocument :: Wrong DocId Number")
	}

	for name, _ := range prt.Fields {
		if _, ok := content[name]; !ok {
			if err := prt.Fields[name].AddDocument(docid, ""); err != nil {
				log.Errf("Partition --> AddDocument [%v] :: %v", prt.SegmentName, err)
			}
			continue
		}

		if err := prt.Fields[name].AddDocument(docid, content[name]); err != nil {
			log.Errf("Partition --> AddDocument :: field[%v] value[%v] error[%v]", name, content[name], err)
		}

	}
	prt.NextDocId++
	return nil
}

//落地持久化
func (prt *Partition) Persist() error {

	btdbPath := prt.SegmentName + basic.IDX_FILENAME_SUFFIX_BTREE
	if prt.btreeDb == nil {
		prt.btreeDb = btree.NewBtree("", btdbPath)
	}
	log.Debugf("[INFO] Serialization Segment File : [%v] Start", prt.SegmentName)
	for name, summary := range prt.FieldSummaries {
		//TODO 用同一颗B树???
		if err := prt.Fields[name].Persist(prt.SegmentName, prt.btreeDb); err != nil {
			log.Errf("Partition --> Serialization %v", err)
			return err
		}
		summary.FwdOffset, summary.FwdDocCnt = prt.Fields[name].FwdOffset, prt.Fields[name].FwdDocCnt
		prt.FieldSummaries[summary.FieldName] = summary
		log.Debugf("%v %v %v", name, summary.FwdOffset, summary.FwdDocCnt)
	}

	//存储源信息
	if err := prt.StoreMeta(); err != nil {
		return err
	}

	prt.isMemory = false

	var err error
	prt.ivtMmap, err = mmap.NewMmap(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_INVERT, false, 0) //创建默认大小的mmap
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}
	//prt.ivtMmap.SetFileEnd(0)

	prt.baseMmap, err = mmap.NewMmap(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_FWD, false, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}
	//prt.baseMmap.SetFileEnd(0)

	prt.extMmap, err = mmap.NewMmap(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_FWDEXT, false, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}
	//prt.extMmap.SetFileEnd(0)

	//TODO 各个Filed公用同一套mmap ??
	for name := range prt.Fields {
		prt.Fields[name].SetMmap(prt.baseMmap, prt.extMmap, prt.ivtMmap)
	}
	log.Infof("[INFO] Serialization Segment File : [%v] Finish", prt.SegmentName)
	return nil
}

func (prt *Partition) StoreMeta() error {
	metaFileName := prt.SegmentName + basic.IDX_FILENAME_SUFFIX_META
	data := helper.JsonEncode(prt)
	if data != "" {
		if err := helper.WriteToFile([]byte(data), metaFileName); err != nil {
			return err
		}
	} else {
		return errors.New("Json error")
	}

	return nil
}

//关闭Partition
func (prt *Partition) Close() error {
	for _, field := range prt.Fields {
		field.Destroy()
	}

	if prt.ivtMmap != nil {
		prt.ivtMmap.Unmap()
	}

	if prt.baseMmap != nil {
		prt.baseMmap.Unmap()
	}

	if prt.extMmap != nil {
		prt.extMmap.Unmap()
	}

	if prt.btreeDb != nil {
		prt.btreeDb.Close()
	}

	return nil
}

//销毁分区
func (prt *Partition) Destroy() error {

	for _, field := range prt.Fields {
		field.Destroy()
	}

	if prt.ivtMmap != nil {
		prt.ivtMmap.Unmap()
	}

	if prt.baseMmap != nil {
		prt.baseMmap.Unmap()
	}

	if prt.extMmap != nil {
		prt.extMmap.Unmap()
	}

	if prt.btreeDb != nil {
		prt.btreeDb.Close()
	}
	//TODO ??
	//posFilename := fmt.Sprintf("%v.pos", prt.SegmentName)
	//os.Remove(posFilename)

	os.Remove(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_META)
	os.Remove(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_INVERT)
	os.Remove(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_FWD)
	os.Remove(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_FWDEXT)
	os.Remove(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_BTREE)
	return nil

}

func (prt *Partition) findFieldDocs(key, field string) ([]basic.DocNode, bool) {
	if _, hasField := prt.Fields[field]; !hasField {
		log.Infof("[INFO] Field %v not found", field)
		return nil, false
	}
	docids, match := prt.Fields[field].Query(key)
	if !match {
		return nil, false
	}
	return docids, true

}

//查询
func (prt *Partition) Query(fieldname string, key interface{}) ([]basic.DocNode, bool) {
	if _, hasField := prt.Fields[fieldname]; !hasField {
		log.Warnf("[WARN] Field[%v] not found", fieldname)
		return nil, false
	}

	return prt.Fields[fieldname].Query(key)
}

//获取详情，单个字段
func (prt *Partition) GetFieldValue(docid uint32, fieldname string) (string, bool) {

	if docid < prt.StartDocId || docid >= prt.NextDocId {
		return "", false
	}

	if _, ok := prt.Fields[fieldname]; !ok {
		return "", false
	}
	return prt.Fields[fieldname].GetString(docid)

}

//获取整篇文档详情，全部字段
func (prt *Partition) GetDocument(docid uint32) (map[string]string, bool) {

	if docid < prt.StartDocId || docid >= prt.NextDocId {
		return nil, false
	}

	res := make(map[string]string)

	for name, field := range prt.Fields {
		res[name], _ = field.GetString(docid)
	}

	return res, true

}

//获取详情，部分字段
func (prt *Partition) GetValueWithFields(docid uint32, fields []string) (map[string]string, bool) {

	if fields == nil {
		return prt.GetDocument(docid)
	}

	if docid < prt.StartDocId || docid >= prt.NextDocId {
		return nil, false
	}
	flag := false

	res := make(map[string]string)
	for _, field := range fields {
		if _, ok := prt.Fields[field]; ok {
			res[field], _ = prt.GetFieldValue(docid, field)
			flag = true
		} else {
			res[field] = ""
		}

	}

	return res, flag
}

//搜索（单query）
//搜索的结果将append到indocids中
func (prt *Partition) SearchDocs(query basic.SearchQuery,
	filters []basic.SearchFilted, bitmap *bitmap.Bitmap,
	indocids []basic.DocNode) ([]basic.DocNode, bool) {

	start := len(indocids)
	//query查询
	if query.Value == "" {
		docids := make([]basic.DocNode, 0)
		for i := prt.StartDocId; i < prt.NextDocId; i++ {
			if bitmap.GetBit(uint64(i)) == 0 {
				docids = append(docids, basic.DocNode{Docid: i})
			}
		}
		indocids = append(indocids, docids...)
	} else {
		docids, match := prt.Fields[query.FieldName].Query(query.Value)
		if !match {
			return indocids, false
		}

		indocids = append(indocids, docids...)
	}

	//bitmap去掉数据
	index := start
	if (filters == nil || len(filters) == 0) && bitmap != nil {
		for _, docid := range indocids[start:] {
			//去掉bitmap删除的
			if bitmap.GetBit(uint64(docid.Docid)) == 0 {
				indocids[index] = docid
				index++
			}
		}
		return indocids[:index], true
	}

	//过滤操作
	index = start
	for _, docidinfo := range indocids[start:] {
		match := true
		for _, filter := range filters {
			if _, hasField := prt.Fields[filter.FieldName]; hasField {
				if (bitmap != nil && bitmap.GetBit(uint64(docidinfo.Docid)) == 1) ||
					(!prt.Fields[filter.FieldName].Filter(docidinfo.Docid, filter.Type, filter.Start, filter.End, filter.Range, filter.MatchStr)) {
					match = false
					break
				}
				log.Debugf("Partition[%v] QUERY  %v", prt.SegmentName, docidinfo)
			} else {
				log.Debugf("Partition[%v] FILTER FIELD[%v] NOT FOUND", prt.SegmentName, filter.FieldName)
				return indocids[:start], true
			}
		}
		if match {
			indocids[index] = docidinfo
			index++
		}

	}

	return indocids[:index], true
}

//TODO 求交集？？
func interactionWithStart(a []basic.DocNode, b []basic.DocNode, start int) ([]basic.DocNode, bool) {

	if a == nil || b == nil {
		return a, false
	}

	lena := len(a)
	lenb := len(b)
	lenc := start
	ia := start
	ib := 0

	//fmt.Printf("a:%v,b:%v,c:%v\n",lena,lenb,lenc)
	for ia < lena && ib < lenb {

		if a[ia].Docid == b[ib].Docid {
			a[lenc] = a[ia]
			lenc++
			ia++
			ib++
			continue
			//c = append(c, a[ia])
		}

		if a[ia].Docid < b[ib].Docid {
			ia++
		} else {
			ib++
		}
	}

	//fmt.Printf("a:%v,b:%v,c:%v\n",lena,lenb,lenc)
	return a[:lenc], true

}

//搜索（多query）
//TODO 再看
func (prt *Partition) SearchUnitDocIds(querys []basic.SearchQuery, filteds []basic.SearchFilted,
		bitmap *bitmap.Bitmap, indocids []basic.DocNode) ([]basic.DocNode, bool) {

	start := len(indocids)
	flag := false
	var ok bool

	if len(querys) == 0 || querys == nil {
		docids := make([]basic.DocNode, 0)
		for i := prt.StartDocId; i < prt.NextDocId; i++ {
			docids = append(docids, basic.DocNode{Docid: i})
		}
		indocids = append(indocids, docids...)
	} else {
		for _, query := range querys {
			if _, hasField := prt.Fields[query.FieldName]; !hasField {
				log.Infof("Field %v not found", query.FieldName)
				return indocids[:start], false
			}
			docids, match := prt.Fields[query.FieldName].Query(query.Value)
			if !match {
				return indocids[:start], false
			}

			if !flag {
				flag = true
				indocids = append(indocids, docids...)
			} else {
				indocids, ok = interactionWithStart(indocids, docids, start)
				if !ok {
					return indocids[:start], false
				}
			}
		}
	}
	log.Infof("ResLen[%v] ", len(indocids))

	//bitmap去掉数据
	index := start

	if filteds == nil && bitmap != nil {
		for _, docid := range indocids[start:] {
			//去掉bitmap删除的
			if bitmap.GetBit(uint64(docid.Docid)) == 0 {
				indocids[index] = docid
				index++
			}
		}
		if index == start {
			return indocids[:start], false
		}
		return indocids[:index], true
	}

	//过滤操作
	index = start
	for _, docidinfo := range indocids[start:] {
		match := true
		for _, filter := range filteds {
			if _, hasField := prt.Fields[filter.FieldName]; hasField {
				if (bitmap != nil && bitmap.GetBit(uint64(docidinfo.Docid)) == 1) ||
					(!prt.Fields[filter.FieldName].Filter(docidinfo.Docid, filter.Type, filter.Start, filter.End, filter.Range, filter.MatchStr)) {
					match = false
					break
				}
				log.Debugf("SEGMENT[%v] QUERY  %v", prt.SegmentName, docidinfo)
			} else {
				log.Errf("SEGMENT[%v] FILTER FIELD[%v] NOT FOUND", prt.SegmentName, filter.FieldName)
				return indocids[:start], false
			}
		}
		if match {
			indocids[index] = docidinfo
			index++
		}

	}

	if index == start {
		return indocids[:start], false
	}
	return indocids[:index], true
}

func (prt *Partition) MergePartitions(parts []*Partition) error {

	log.Infof("MergePartitions [%v] Start", prt.SegmentName)
	btdbname := fmt.Sprintf("%v.bt", prt.SegmentName)
	if prt.btreeDb == nil {
		prt.btreeDb = btree.NewBtree("", btdbname)
	}

	for name, summary := range prt.FieldSummaries {
		//prt.Logger.Info("[INFO] Merge Field[%v]", name)
		fs := make([]*field.Field, 0)
		for _, pt := range parts {
			if _, ok := pt.Fields[name]; !ok {
				fakefield := field.NewEmptyFakeField(prt.Fields[name].FieldName, pt.StartDocId,
					prt.Fields[name].FieldType, pt.NextDocId-pt.StartDocId)
				fs = append(fs, fakefield)
				continue
			}
			fs = append(fs, pt.Fields[name])
		}
		prt.Fields[name].MergeField(fs, prt.SegmentName, prt.btreeDb)
		summary.FwdOffset = prt.Fields[name].FwdOffset
		summary.FwdDocCnt = prt.Fields[name].FwdDocCnt
		prt.FieldSummaries[name] = summary
	}

	prt.isMemory = false
	var err error
	prt.ivtMmap, err = mmap.NewMmap(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_INVERT, false, 0)
	if err != nil {
		log.Errf("[ERROR] mmap error : %v \n", err)
	}
	//prt.idxMmap.SetFileEnd(0)

	prt.baseMmap, err = mmap.NewMmap(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_FWD,false, 0)
	if err != nil {
		log.Errf("[ERROR] mmap error : %v \n", err)
	}
	//prt.pflMmap.SetFileEnd(0)

	prt.extMmap, err = mmap.NewMmap(prt.SegmentName + basic.IDX_FILENAME_SUFFIX_FWDEXT, false, 0)
	if err != nil {
		log.Errf("[ERROR] mmap error : %v \n", err)
	}
	//prt.dtlMmap.SetFileEnd(0)

	for name := range prt.Fields {
		prt.Fields[name].SetMmap(prt.baseMmap, prt.extMmap, prt.ivtMmap)
	}
	log.Infof("MergeSegments [%v] Finish", prt.SegmentName)
	prt.NextDocId = parts[len(parts)-1].NextDocId

	return prt.StoreMeta()
}