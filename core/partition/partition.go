
package partition
/*
 * 分区, 类比于Mysql的分区的概念
 * 每个分区都拥有全量的filed, 但是数据是整体表数据的一部分,
 * 每个分区是独立的索引单元, 所有的分区合在一起, 就是一张完整的表
 * Note:
 * 同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
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
	StartDocId    uint32                      `json:"startDocId"`
	NextDocId     uint32                      `json:"nextDocId"`      //下次的DocId（所以Max的DocId是NextId-1）
	PartitionName string                      `json:"partitionName"`
	BasicFields   map[string]field.BasicField `json:"fields"`         //分区各个字段的最基础信息，落盘用
	Fields        map[string]*field.Field	  `json:"-"`
	inMemory      bool                        `json:"-"`
	btdb          btree.Btree                 `json:"-"`			  //四套文件，本分区所有字段公用
	ivtMmap       *mmap.Mmap				  `json:"-"`
	baseMmap      *mmap.Mmap				  `json:"-"`
	extMmap       *mmap.Mmap                  `json:"-"`
}

//新建一个空分区, 包含字段
//相当于建立了一个完整的空骨架，分区=>字段=>索引
func NewEmptyPartitionWithBasicFields(partitionName string, start uint32, basicFields []field.BasicField) *Partition {

	part := &Partition{
		StartDocId:    start,
		NextDocId:     start,
		PartitionName: partitionName,
		Fields:        make(map[string]*field.Field),
		BasicFields:   make(map[string]field.BasicField),
		inMemory:      true,
	}

	for _, fld := range basicFields {
		basicField := field.BasicField{
			FieldName: fld.FieldName,
			IndexType: fld.IndexType,
		}
		part.BasicFields[fld.FieldName] = basicField
		emptyField := field.NewEmptyField(fld.FieldName, start, fld.IndexType)
		part.Fields[fld.FieldName] = emptyField
	}

	log.Infof("Make New Partition [%v] Success ", partitionName)
	return part
}

//从文件加载一个分区
func LoadPartition(partitionName string) (*Partition, error) {

	part := Partition {
		PartitionName: partitionName,
		Fields:        make(map[string]*field.Field),
		BasicFields:   make(map[string]field.BasicField),
	}

	//从meta文件加载partition信息到part
	metaFileName := partitionName + basic.IDX_FILENAME_SUFFIX_META
	buffer, err := helper.ReadFile(metaFileName)
	if err != nil {
		return nil ,err
	}
	err = json.Unmarshal(buffer, &part)
	if err != nil {
		return nil ,err
	}

	//加载btree
	btdbPath := partitionName + basic.IDX_FILENAME_SUFFIX_BTREE
	if helper.Exist(btdbPath) {
		log.Debugf("Load B+Tree File : %v", btdbPath)
		part.btdb = btree.NewBtree("", btdbPath)
	}

	//加载倒排文件
	part.ivtMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_INVERT, partitionName), true, 0)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
		return nil, err
	}
	log.Debugf("Load Invert File : %v.idx ", partitionName)

	//加载正排文件
	part.baseMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWD, partitionName), true, 0)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
		return nil, err
	}
	log.Debugf("Load Profile File : %v.pfl", partitionName)

	//加载正排辅助文件
	part.extMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWDEXT, partitionName), true, 0)
	if err != nil {
		fmt.Printf("mmap error : %v \n", err)
	}
	log.Debugf("[INFO] Load Detail File : %v.dtl", partitionName)

	//加载各个Field
	for _, basicField := range part.BasicFields {
		if basicField.DocCnt == 0 {
			//TODO ?? 这里会进入吗
			newField := field.NewEmptyField(basicField.FieldName, part.StartDocId, basicField.IndexType)
			part.Fields[basicField.FieldName] = newField
		} else {
			oldField := field.LoadField(basicField.FieldName, part.StartDocId,
				part.NextDocId, basicField.IndexType, basicField.FwdOffset, basicField.DocCnt,
				part.ivtMmap, part.baseMmap, part.extMmap, part.btdb)
			part.Fields[basicField.FieldName] = oldField
		}
	}
	return &part, nil
}

//判断为空
func (part *Partition) IsEmpty() bool {
	return part.StartDocId == part.NextDocId
}

//添加字段
func (part *Partition) AddField(basicField field.BasicField) error {

	if _, ok := part.BasicFields[basicField.FieldName]; ok {
		log.Warnf("Partition --> AddField Already has field [%v]", basicField.FieldName)
		return errors.New("Already has field..")
	}

	//TODO why ?? 必须是空的part或者是磁盘态的part, 才能新增字段
	if part.inMemory && !part.IsEmpty() {
		log.Warnf("Partition --> AddField field [%v] fail..", basicField.FieldName)
		return errors.New("memory Partition can not add field..")
	}

	part.BasicFields[basicField.FieldName] = basicField
	newFiled := field.NewEmptyField(basicField.FieldName, part.NextDocId, basicField.IndexType)
	part.Fields[basicField.FieldName] = newFiled
	return nil
}

//删除字段
func (part *Partition) DeleteField(fieldname string) error {
	if _, exist := part.BasicFields[fieldname]; !exist {
		log.Warnf("Partition --> DeleteField not found field [%v]", fieldname)
		return errors.New("not found field")
	}

	//TODO why ?? 必须是空的part或者是磁盘态的part, 删除新增字段
	if part.inMemory && !part.IsEmpty() {
		log.Warnf("Partition --> deleteField field [%v] fail..", fieldname)
		return errors.New("memory Partition can not delete field..")
	}

	part.Fields[fieldname].Destroy()
	delete(part.Fields, fieldname)
	delete(part.BasicFields, fieldname)
	log.Infof("Partition --> DeleteField[%v] :: Success ", fieldname)
	return nil
}

//更新文档
//content, 一篇文档的各个字段的值
func (part *Partition) UpdateDocument(docId uint32, content map[string]string) error {
	//校验
	if docId >= part.NextDocId || docId < part.StartDocId {
		log.Errf("Partition --> UpdateDocument :: Wrong DocId[%v]  NextDocId[%v]", docId, part.NextDocId)
		return errors.New("Partition --> UpdateDocument :: Wrong DocId Number")
	}

	//各个字段分别改
	for fieldName, _ := range part.Fields {
		if _, ok := content[fieldName]; !ok {
			//如果某个字段没传, 则会清空字段
			if err := part.Fields[fieldName].UpdateDocument(docId, ""); err != nil {
				log.Errf("Partition --> UpdateDocument :: %v", err)
			}
		} else {

			if err := part.Fields[fieldName].UpdateDocument(docId, content[fieldName]); err != nil {
				log.Errf("Partition --> UpdateDocument :: field[%v] value[%v] error[%v]", fieldName, content[fieldName], err)
			}
		}
	}

	return nil
}

//添加文档
//content, 一篇文档的各个字段的值
func (part *Partition) AddDocument(docId uint32, content map[string]string) error {

	if docId != part.NextDocId {
		log.Errf("Partition --> AddDocument :: Wrong DocId[%v]  NextDocId[%v]", docId, part.NextDocId)
		return errors.New("Partition --> AddDocument :: Wrong DocId Number")
	}

	for name, _ := range part.Fields {
		if _, ok := content[name]; !ok {
			//如果某个字段没传, 则会是空值
			if err := part.Fields[name].AddDocument(docId, ""); err != nil {
				log.Errf("Partition --> AddDocument [%v] :: %v", part.PartitionName, err)
			}
		} else {
			if err := part.Fields[name].AddDocument(docId, content[name]); err != nil {
				log.Errf("Partition --> AddDocument :: field[%v] value[%v] error[%v]", name, content[name], err)
			}
		}
	}
	part.NextDocId++
	return nil
}

//关闭Partition
func (part *Partition) Close() error {
	for _, field := range part.Fields {
		field.Destroy()
	}

	//统一unmmap掉mmap
	if part.ivtMmap != nil {
		if err := part.ivtMmap.Unmap(); err != nil {log.Errf("Unmap Error:", err)}
	}
	if part.baseMmap != nil {
		if err := part.baseMmap.Unmap(); err != nil {log.Errf("Unmap Error:", err)}
	}
	if part.extMmap != nil {
		if err := part.extMmap.Unmap(); err != nil {log.Errf("Unmap Error:", err)}
	}
	if part.btdb != nil {
		if err := part.btdb.Close(); err != nil {log.Errf("Btree Close Error:", err)}
	}

	return nil
}

//销毁分区
func (part *Partition) Destroy() error {
	//先关闭
	part.Close()

	//删除文件
	os.Remove(part.PartitionName + basic.IDX_FILENAME_SUFFIX_META)
	os.Remove(part.PartitionName + basic.IDX_FILENAME_SUFFIX_INVERT)
	os.Remove(part.PartitionName + basic.IDX_FILENAME_SUFFIX_FWD)
	os.Remove(part.PartitionName + basic.IDX_FILENAME_SUFFIX_FWDEXT)
	os.Remove(part.PartitionName + basic.IDX_FILENAME_SUFFIX_BTREE)
	return nil
}

//查询
func (part *Partition) Query(fieldName string, key interface{}) ([]basic.DocNode, bool) {
	if _, exist := part.Fields[fieldName]; !exist {
		log.Errf("Field [%v] not found", fieldName)
		return nil, false
	}

	return part.Fields[fieldName].Query(key)
}

//获取详情，单个字段
func (part *Partition) GetFieldValue(docId uint32, fieldName string) (string, bool) {
	//校验
	if docId < part.StartDocId || docId >= part.NextDocId {
		return "", false
	}
	if _, ok := part.Fields[fieldName]; !ok {
		return "", false
	}

	//获取
	return part.Fields[fieldName].GetString(docId)
}

//获取整篇文档详情，全部字段
func (part *Partition) GetDocument(docid uint32) (map[string]string, bool) {
	//校验
	if docid < part.StartDocId || docid >= part.NextDocId {
		return nil, false
	}

	//获取
	ret := make(map[string]string)
	for fieldName, field := range part.Fields {
		ret[fieldName], _ = field.GetString(docid)
	}
	return ret, true
}

//获取详情，部分字段
func (part *Partition) GetValueWithFields(docId uint32, fieldNames []string) (map[string]string, bool) {
	//校验
	if docId < part.StartDocId || docId >= part.NextDocId {
		return nil, false
	}

	if fieldNames == nil {
		return part.GetDocument(docId)
	}

	flag := false
	ret := make(map[string]string)
	for _, fieldName := range fieldNames {
		if _, ok := part.Fields[fieldName]; ok {
			ret[fieldName], _ = part.GetFieldValue(docId, fieldName)
			flag = true
		} else {
			ret[fieldName] = ""
		}
	}
	return ret, flag
}

//存储元信息
func (part *Partition) StoreMeta() error {
	metaFileName := part.PartitionName + basic.IDX_FILENAME_SUFFIX_META
	data := helper.JsonEncode(part)
	if data != "" {
		if err := helper.WriteToFile([]byte(data), metaFileName); err != nil {
			return err
		}
	} else {
		return errors.New("Json error")
	}

	return nil
}

//落地持久化
func (part *Partition) Persist() error {

	btdbPath := part.PartitionName + basic.IDX_FILENAME_SUFFIX_BTREE
	if part.btdb == nil {
		part.btdb = btree.NewBtree("", btdbPath)
	}
	log.Debugf("Persist Partition File : [%v] Start", part.PartitionName)
	for name, basicField := range part.BasicFields {
		//当前分区的各个字段分别落地
		//Note: field.Persist不会自动加载回mmap，但是设置了倒排的btdb和正排的fwdOffset和docCnt
		if err := part.Fields[name].Persist(part.PartitionName, part.btdb); err != nil {
			log.Errf("Partition --> Persist %v", err)
			return err
		}
		//设置basicField的fwdOffset和docCnt
		basicField.FwdOffset, basicField.DocCnt = part.Fields[name].FwdOffset, part.Fields[name].DocCnt
		part.BasicFields[basicField.FieldName] = basicField
		log.Debugf("%v %v %v", name, basicField.FwdOffset, basicField.DocCnt)
	}

	//存储源信息
	if err := part.StoreMeta(); err != nil {
		return err
	}

	//内存态 => 磁盘态
	part.inMemory = false

	//加载回mmap
	var err error
	part.ivtMmap, err = mmap.NewMmap(part.PartitionName+ basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}
	part.baseMmap, err = mmap.NewMmap(part.PartitionName+ basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}
	part.extMmap, err = mmap.NewMmap(part.PartitionName+ basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}

	//统一设置回来, 因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
	for name := range part.Fields {
		part.Fields[name].SetMmap(part.baseMmap, part.extMmap, part.ivtMmap)
	}
	log.Infof("Persist Partition File : [%v] Finish", part.PartitionName)
	return nil
}

//将分区合并然后落地
//Note:
// 这个和底层的MergePersist有不同, 函数有接受者, 初始是一个骨架,
// 函数会完整的填充接收者, 加载btdb和mmap, 使之成为一个可用的磁盘态分区
func (part *Partition) MergePersistPartitions(parts []*Partition) error {

	log.Infof("MergePartitions [%v] Start", part.PartitionName)
	btdbname := part.PartitionName + basic.IDX_FILENAME_SUFFIX_BTREE
	if part.btdb == nil {
		part.btdb = btree.NewBtree("", btdbname)
	}

	//挨个字段进行merge
	//TODO 理论上各个字段的nextId, docId应该相同, 但是fwdOffset应该不同, 待测试
	for fieldName, basicField := range part.BasicFields {
		fs := make([]*field.Field, 0)
		for _, pt := range parts {
			if _, exist := pt.Fields[fieldName]; exist {
				fs = append(fs, pt.Fields[fieldName])
			} else {
				fakefield := field.NewEmptyFakeField(part.Fields[fieldName].FieldName, pt.StartDocId,
					part.Fields[fieldName].IndexType, pt.NextDocId-pt.StartDocId)
				fs = append(fs, fakefield)
			}
		}
		offset, cnt, nextId, err := field.MergePersistField(fs, part.PartitionName, part.btdb)
		if err != nil {
			log.Errln("MergePartitions Error:", err)
			return err
		}
		part.Fields[fieldName].FwdOffset = offset
		part.Fields[fieldName].DocCnt = cnt
		part.Fields[fieldName].NextDocId = nextId

		basicField.FwdOffset = part.Fields[fieldName].FwdOffset
		basicField.DocCnt = part.Fields[fieldName].DocCnt
		part.BasicFields[fieldName] = basicField
	}

	//内存态 => 磁盘态
	part.inMemory = false

	//加载回mmap
	var err error
	part.ivtMmap, err = mmap.NewMmap(part.PartitionName+ basic.IDX_FILENAME_SUFFIX_INVERT, false, 0)
	if err != nil {
		log.Errln("MergePartitions Error:", err)
		return err
	}
	part.baseMmap, err = mmap.NewMmap(part.PartitionName+ basic.IDX_FILENAME_SUFFIX_FWD,false, 0)
	if err != nil {
		log.Errln("MergePartitions Error:", err)
		return err
	}
	part.extMmap, err = mmap.NewMmap(part.PartitionName+ basic.IDX_FILENAME_SUFFIX_FWDEXT, false, 0)
	if err != nil {
		log.Errln("MergePartitions Error:", err)
		return err
	}
	for name := range part.Fields {
		part.Fields[name].SetMmap(part.baseMmap, part.extMmap, part.ivtMmap)
	}

	//最后设置一下nextDocId
	part.NextDocId = parts[len(parts)-1].NextDocId

	log.Infof("MergeSegments [%v] Finish", part.PartitionName)
	return part.StoreMeta()
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
				log.Debugf("Partition[%v] QUERY  %v", prt.PartitionName, docidinfo)
			} else {
				log.Debugf("Partition[%v] FILTER FIELD[%v] NOT FOUND", prt.PartitionName, filter.FieldName)
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
				log.Debugf("SEGMENT[%v] QUERY  %v", prt.PartitionName, docidinfo)
			} else {
				log.Errf("SEGMENT[%v] FILTER FIELD[%v] NOT FOUND", prt.PartitionName, filter.FieldName)
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
