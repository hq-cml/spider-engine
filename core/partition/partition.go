
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
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/bitmap"
	"github.com/hq-cml/spider-engine/core/index"
	"strings"
)

//TODO 配置化
var (
	PARTITION_MIN_DOC_CNT uint32 = 100000 //10w个文档，分区合并的一个参考值
	//PARTITION_MIN_DOC_CNT uint32 = 3
)

const (
	GOD_FIELD_NAME = "#O@H!M*Y&G%O(D#"
)

// Partition description:段结构
type Partition struct {
	StartDocId      uint32                     `json:"startDocId"`
	NextDocId       uint32                     `json:"nextDocId"`      //下次的DocId（所以Max的DocId是NextId-1）
	DocCnt          uint32                     `json:"docCnt"` 	       //分区文档个数
	PrtPathName     string                     `json:"prtPathName"`
	CoreFields      map[string]field.CoreField `json:"fields"`         //分区各个字段的最基础信息，落盘用
	GodBaseField    field.BasicField           `json:"godCoreField"`   //上帝视角字段, 用于跨字段倒排索引搜索
	Fields          map[string]*field.Field    `json:"-"`
	GodField        *field.Field               `json:"-"`
	inMemory        bool                       `json:"-"`
	btdb            btree.Btree                `json:"-"`			   //四套文件，本分区所有字段公用
	ivtMmap         *mmap.Mmap                 `json:"-"`
	baseMmap        *mmap.Mmap                 `json:"-"`
	extMmap         *mmap.Mmap                 `json:"-"`
}

//新建一个空分区, 包含字段
//相当于建立了一个完整的空骨架，分区=>字段=>索引
func NewEmptyPartitionWithBasicFields(PrtPathName string, start uint32, basicFields []field.BasicField) *Partition {

	part := &Partition{
		StartDocId:  start,
		NextDocId:   start,
		PrtPathName: PrtPathName,
		Fields:      make(map[string]*field.Field),
		CoreFields:  make(map[string]field.CoreField),
		inMemory:    true,
	}

	for _, fld := range basicFields {
		coreField := field.CoreField{
			BasicField: field.BasicField{
				FieldName: fld.FieldName,
				IndexType: fld.IndexType,
			},
		}
		part.CoreFields[fld.FieldName] = coreField
		emptyField := field.NewEmptyField(fld.FieldName, start, fld.IndexType)
		part.Fields[fld.FieldName] = emptyField
	}

	//上帝字段
	part.GodBaseField = field.BasicField{
		FieldName: GOD_FIELD_NAME,
		IndexType: index.IDX_TYPE_GOD,
	}
	part.GodField = field.NewEmptyGodField(GOD_FIELD_NAME, start)

	log.Infof("Make New Partition [%v] Success ", PrtPathName)
	return part
}

//从文件加载一个分区
func LoadPartition(prtPathName string) (*Partition, error) {

	part := Partition {
		PrtPathName: prtPathName,
		Fields:      make(map[string]*field.Field),
		//CoreFields:  make(map[string]field.CoreField),
	}

	//从meta文件加载partition信息到part
	metaFileName := prtPathName + basic.IDX_FILENAME_SUFFIX_META
	buffer, err := helper.ReadFile(metaFileName)
	if err != nil {
		return nil ,err
	}
	err = json.Unmarshal(buffer, &part)
	if err != nil {
		return nil ,err
	}

	//加载btree
	btdbPath := prtPathName + basic.IDX_FILENAME_SUFFIX_BTREE
	if helper.Exist(btdbPath) {
		log.Infof("Load B+Tree File : %v", btdbPath)
		part.btdb = btree.NewBtree("", btdbPath)
	}

	//加载倒排文件
	part.ivtMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_INVERT, prtPathName), true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
		return nil, err
	}
	log.Debugf("Load Invert File : %v.idx ", prtPathName)

	//加载正排文件
	part.baseMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWD, prtPathName), true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
		return nil, err
	}
	log.Debugf("Load Profile File : %v.pfl", prtPathName)

	//加载正排辅助文件
	part.extMmap, err = mmap.NewMmap(fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWDEXT, prtPathName), true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}
	log.Debugf("Load Detail File : %v.dtl", prtPathName)

	//加载各个Field
	for _, coreField := range part.CoreFields {
		if part.DocCnt == 0 {
			//TODO ?? 这里会进入吗, 做一个测试，刚刚load的table，直接关闭，看看内存分区是否回落地一个0 的分区
			panic("Unknow error")
			newField := field.NewEmptyField(coreField.FieldName, part.StartDocId, coreField.IndexType)
			part.Fields[coreField.FieldName] = newField
		} else {
			oldField := field.LoadField(coreField.FieldName, part.StartDocId,
				part.NextDocId, coreField.IndexType, coreField.FwdOffset, part.DocCnt,
				part.baseMmap, part.extMmap, part.ivtMmap, part.btdb)
			part.Fields[coreField.FieldName] = oldField
		}
	}

	//加载上帝字段
	part.GodField = field.LoadField(GOD_FIELD_NAME, part.StartDocId,
		part.NextDocId, index.IDX_TYPE_GOD, 0, 0,
		nil, nil, part.ivtMmap, part.btdb)

	return &part, nil
}

//判断为空
func (part *Partition) IsEmpty() bool {
	return part.StartDocId == part.NextDocId
}

//添加字段
func (part *Partition) AddField(basicField field.BasicField) error {
	//校验
	if _, exist := part.CoreFields[basicField.FieldName]; exist {
		log.Warnf("Partition --> AddField Already has field [%v]", basicField.FieldName)
		return errors.New("Already has field..")
	}

	//分区只能是内存态并且为空的时候，才能变更字段(因为已经有部分的doc,新字段没法处理)
	if !part.inMemory || !part.IsEmpty() {
		log.Warnf("Partition --> AddField field [%v] fail..", basicField.FieldName)
		return errors.New("Only memory and enmpty partition can add field..")
	}

	//新增
	part.CoreFields[basicField.FieldName] = field.CoreField{
		BasicField: field.BasicField{
			FieldName: basicField.FieldName,
			IndexType: basicField.IndexType,
		},
	}
	newFiled := field.NewEmptyField(basicField.FieldName, part.NextDocId, basicField.IndexType)
	part.Fields[basicField.FieldName] = newFiled
	return nil
}

//删除字段
func (part *Partition) DeleteField(fieldname string) error {
	//校验
	if _, exist := part.CoreFields[fieldname]; !exist {
		log.Warnf("Partition --> DeleteField not found field [%v]", fieldname)
		return errors.New("not found field")
	}

	//分区只能是内存态并且为空的时候，才能变更字段
	if !part.inMemory || !part.IsEmpty() {
		log.Warnf("Partition --> deleteField field [%v] fail..", fieldname)
		return errors.New("Only memory and enmpty partition can delete field..")
	}

	part.Fields[fieldname].DoClose()
	delete(part.Fields, fieldname)
	delete(part.CoreFields, fieldname)
	log.Infof("Partition --> DeleteField[%v] :: Success ", fieldname)
	return nil
}

//添加文档
//content, 一篇文档的各个字段的值
func (part *Partition) AddDocument(docId uint32, content map[string]interface{}) error {

	if docId != part.NextDocId {
		log.Errf("Partition --> AddDocument :: Wrong DocId[%v]  NextDocId[%v]", docId, part.NextDocId)
		return errors.New("Partition --> AddDocument :: Wrong DocId Number")
	}

	//各个字段分别新增文档的对应部分
	godStrs := []string{}
	for fieldName, iField := range part.Fields {
		if _, ok := content[fieldName]; !ok {
			//如果某个字段没传, 则会是空值
			if err := part.Fields[fieldName].AddDocument(docId, ""); err != nil {
				log.Errf("Partition --> AddDocument [%v] :: %v", part.PrtPathName, err)
				return err
			}
		} else {
			if err := part.Fields[fieldName].AddDocument(docId, content[fieldName]); err != nil {
				log.Errf("Partition --> AddDocument :: field[%v] value[%v] error[%v]", fieldName, content[fieldName], err)
				return err
			}
		}

		//字符类型的字段内容, 汇入上帝视角
		if iField.IndexType == index.IDX_TYPE_STRING ||
			iField.IndexType == index.IDX_TYPE_STRING_SEG ||
			iField.IndexType == index.IDX_TYPE_STRING_LIST ||
			iField.IndexType == index.IDX_TYPE_STRING_SINGLE {
			if val, ok := content[fieldName]; ok {
				str, _ := val.(string)
				godStrs = append(godStrs, str)
			}
		}
	}

	strVal := ""
	if len(godStrs) > 0 {
		strVal = strings.Join(godStrs, "。") //汇总, 然后增加倒排
	}
	if err := part.GodField.AddDocument(docId, strVal); err != nil {
		log.Errf("Partition --> AddDocument :: field[%v] value[%v] error[%v]", GOD_FIELD_NAME, strVal, err)
		return err
	}


	part.NextDocId++
	part.DocCnt++
	return nil
}

//更高层采用先删后增的方式，变相得实现了update
//更新文档
//content, 一篇文档的各个字段的值
//func (part *Partition) UpdateDocument(docId uint32, content map[string]string) error {
//	//校验
//	if docId >= part.NextDocId || docId < part.StartDocId {
//		log.Errf("Partition --> UpdateDocument :: Wrong DocId[%v]  NextDocId[%v]", docId, part.NextDocId)
//		return errors.New("Partition --> UpdateDocument :: Wrong DocId Number")
//	}
//
//	//各个字段分别改
//	for fieldName, _ := range part.Fields {
//		if _, ok := content[fieldName]; !ok {
//			//如果某个字段没传, 则会清空字段
//			if err := part.Fields[fieldName].UpdateDocument(docId, ""); err != nil {
//				log.Errf("Partition --> UpdateDocument :: %v", err)
//			}
//		} else {
//
//			if err := part.Fields[fieldName].UpdateDocument(docId, content[fieldName]); err != nil {
//				log.Errf("Partition --> UpdateDocument :: field[%v] value[%v] error[%v]", fieldName, content[fieldName], err)
//			}
//		}
//	}
//
//	return nil
//}

//关闭Partition
func (part *Partition) DoClose() error {
	//各个字段关闭
	for _, fld := range part.Fields {
		fld.DoClose()
	}
	part.GodField.DoClose()

	//统一unmmap掉
	if part.ivtMmap != nil {
		if err := part.ivtMmap.Unmap(); err != nil {log.Errf("Unmap Error:", err)}
	}
	if part.baseMmap != nil {
		if err := part.baseMmap.Unmap(); err != nil {log.Errf("Unmap Error:", err)}
	}
	if part.extMmap != nil {
		if err := part.extMmap.Unmap(); err != nil {log.Errf("Unmap Error:", err)}
	}
	//统一关闭btdb
	if part.btdb != nil {
		if err := part.btdb.Close(); err != nil {log.Errf("Btree Close Error:", err)}
	}

	return nil
}

//销毁分区
func (part *Partition) Destroy() error {
	//先关闭
	part.DoClose()

	//删除文件
	part.Remove()
	return nil
}

//销毁分区
func (part *Partition) Remove() error {
	//删除文件
	if err := helper.Remove(part.PrtPathName + basic.IDX_FILENAME_SUFFIX_META); err != nil {return err}
	if err := helper.Remove(part.PrtPathName + basic.IDX_FILENAME_SUFFIX_INVERT); err != nil {return err}
	if err := helper.Remove(part.PrtPathName + basic.IDX_FILENAME_SUFFIX_FWD); err != nil {return err}
	if err := helper.Remove(part.PrtPathName + basic.IDX_FILENAME_SUFFIX_FWDEXT); err != nil {return err}
	if err := helper.Remove(part.PrtPathName + basic.IDX_FILENAME_SUFFIX_BTREE); err != nil {return err}
	return nil
}

//获取详情，单个字段
func (part *Partition) GetFieldValue(docId uint32, fieldName string) (interface{}, bool) {
	//校验
	if docId < part.StartDocId || docId >= part.NextDocId {
		return "", false
	}
	if _, ok := part.Fields[fieldName]; !ok {
		return "", false
	}

	//获取
	return part.Fields[fieldName].GetValue(docId)
}

//获取整篇文档详情，全部字段
func (part *Partition) GetDocument(docId uint32) (map[string]interface{}, bool) {
	//校验
	if docId < part.StartDocId || docId >= part.NextDocId {
		return nil, false
	}

	//获取
	ret := make(map[string]interface{})
	for fieldName, fld := range part.Fields {
		ret[fieldName], _ = fld.GetValue(docId)
	}
	return ret, true
}

//获取详情，部分字段
func (part *Partition) GetValueWithFields(docId uint32, fieldNames []string) (map[string]interface{}, bool) {
	//校验
	if docId < part.StartDocId || docId >= part.NextDocId {
		return nil, false
	}

	if fieldNames == nil {
		return part.GetDocument(docId)
	}

	flag := false
	ret := make(map[string]interface{})
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
func (part *Partition) storeMeta() error {
	metaFileName := part.PrtPathName + basic.IDX_FILENAME_SUFFIX_META
	data := helper.JsonEncodeIndent(part)
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
//Note:
//  和底层的Persit有一些不同，这里会自动加载回来mmap
//  因为四个文件的公用分为是分区级别，这里已经可以统一加载mmap了
func (part *Partition) Persist() error {

	btdbPath := part.PrtPathName + basic.IDX_FILENAME_SUFFIX_BTREE
	if part.btdb == nil {
		part.btdb = btree.NewBtree("", btdbPath)
	}
	log.Debugf("Persist Partition File : [%v] Start", part.PrtPathName)
	var docCnt uint32
	//当前分区的各个字段分别落地
	for name, coreField := range part.CoreFields {
		//Note: field.Persist不会自动加载回mmap，但是设置了倒排的btdb和正排的fwdOffset和docCnt
		if err := part.Fields[name].Persist(part.PrtPathName, part.btdb); err != nil {
			log.Errf("Partition --> Persist %v", err)
			return err
		}
		//设置coreField的fwdOffset和docCnt
		coreField.FwdOffset, docCnt = part.Fields[name].FwdOffset, part.Fields[name].DocCnt
		part.CoreFields[coreField.FieldName] = coreField
		log.Debugf("%v %v %v", name, coreField.FwdOffset, docCnt)
		if part.DocCnt != docCnt {
			log.Errf("Doc cnt not same!!. %v, %v", part.DocCnt, docCnt)
			return errors.New("Doc cnt not same!!")
		}
	}
	//上帝视角字段落地
	part.GodField.Persist(part.PrtPathName, part.btdb)

	//存储源信息
	if err := part.storeMeta(); err != nil {
		return err
	}

	//内存态 => 磁盘态
	part.inMemory = false

	//加载回mmap
	var err error
	part.ivtMmap, err = mmap.NewMmap(part.PrtPathName+ basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}
	part.baseMmap, err = mmap.NewMmap(part.PrtPathName+ basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}
	part.extMmap, err = mmap.NewMmap(part.PrtPathName+ basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		log.Errf("mmap error : %v \n", err)
	}

	//统一设置回来, 因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
	for name := range part.Fields {
		part.Fields[name].SetMmap(part.baseMmap, part.extMmap, part.ivtMmap)
	}
	part.GodField.SetMmap(nil, nil, part.ivtMmap)

	log.Infof("Persist Partition File : [%v] Finish", part.PrtPathName)
	return nil
}

//将分区合并然后落地
//Note:
// 这个和底层的MergePersist有不同, 因为四个文件是按照分区级别公用，所以函数会完整的填充接收者
// 接受者初始是一个骨架，加载btdb和mmap以及其他控制字段，使之成为一个可用的磁盘态分区
func (part *Partition) MergePersistPartitions(parts []*Partition) error {
	//一些校验，顺序必须正确
	l := len(parts)
	for i:=0; i<(l-1); i++ {
		if parts[i].NextDocId != parts[i+1].StartDocId {
			return errors.New("Partitions order wrong")
		}
	}

	log.Infof("MergePartitions [%v] Start", part.PrtPathName)
	btdbname := part.PrtPathName + basic.IDX_FILENAME_SUFFIX_BTREE
	if part.btdb == nil {
		part.btdb = btree.NewBtree("", btdbname)
	}

	//逐个字段进行merge
	tmp := map[uint32]bool{}
	for fieldName, coreField := range part.CoreFields {
		fs := make([]*field.Field, 0)
		for _, pt := range parts {
			if _, exist := pt.Fields[fieldName]; exist {
				fs = append(fs, pt.Fields[fieldName])
			} else {
				//fmt.Println("A---------", fieldName)
				//特殊情况
				//如果新的分区拥有一些新字段,但是老分区没有这个字段,此时,需要生成一个假的字段来占位
				fakefield := field.NewFakeField(part.Fields[fieldName].FieldName, pt.StartDocId,
					pt.NextDocId, part.Fields[fieldName].IndexType)
				fs = append(fs, fakefield)
			}
		}
		//fmt.Println("A---------", len(fs))
		err := part.Fields[fieldName].MergePersistField(fs, part.PrtPathName, part.btdb)
		if err != nil {
			log.Errln("MergePartitions Error1:", err)
			return err
		}

		coreField.FwdOffset = part.Fields[fieldName].FwdOffset
		tmp[part.Fields[fieldName].DocCnt] = true
		part.CoreFields[fieldName] = coreField
	}
	if len(tmp) > 1 {
		log.Errf("Doc cnt not consistent!!. %v", tmp)
		return errors.New("Doc cnt not same!!")
	}

	//上帝字段合并
	fs := make([]*field.Field, 0)
	for _, pt := range parts {
		fs = append(fs, pt.GodField)
	}
	err := part.GodField.MergePersistField(fs, part.PrtPathName, part.btdb)
	if err != nil {
		log.Errln("Merge God Partitions failed:", err)
		return err
	}

	//加载回mmap
	part.ivtMmap, err = mmap.NewMmap(part.PrtPathName+ basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		log.Errln("MergePartitions Error2:", err)
		return err
	}
	part.baseMmap, err = mmap.NewMmap(part.PrtPathName+ basic.IDX_FILENAME_SUFFIX_FWD,true, 0)
	if err != nil {
		log.Errln("MergePartitions Error3:", err)
		return err
	}
	part.extMmap, err = mmap.NewMmap(part.PrtPathName+ basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		log.Errln("MergePartitions Error4:", err)
		return err
	}
	for name := range part.Fields {
		part.Fields[name].SetMmap(part.baseMmap, part.extMmap, part.ivtMmap)
	}
	part.GodField.SetMmap(nil, nil, part.ivtMmap)

	//内存态 => 磁盘态
	part.inMemory = false

	//最后设置startId和nextDocId
	part.StartDocId = parts[0].StartDocId
	part.NextDocId = parts[l-1].NextDocId
	part.DocCnt = parts[l-1].NextDocId - parts[0].StartDocId

	log.Infof("MergePartitions [%v] Finish", part.PrtPathName)
	return part.storeMeta()
}

//查询
func (part *Partition) query(fieldName string, key interface{}) ([]basic.DocNode, bool) {
	//校验
	fld, exist := part.Fields[fieldName]
	if !exist {
		if fieldName == GOD_FIELD_NAME {
			fld = part.GodField
		} else {
			log.Errf("Field [%v] not found", fieldName)
			return nil, false
		}
	}

	return fld.Query(key)
}

//搜索, 如果keyWord为空, 则取出所有未删除的节点
//根据搜索结果, 再通过bitmap进行过滤
func (part *Partition) SearchDocs(fieldName, keyWord string, bitmap *bitmap.Bitmap,filters []basic.SearchFilter) ([]basic.DocNode, bool) {
	//校验
	_, exist := part.Fields[fieldName]
	if !exist && fieldName != GOD_FIELD_NAME{
		log.Errf("Field [%v] not found", fieldName)
		return nil, false
	}

	retDocs := []basic.DocNode{}
	//如果keyWord为空, 则取出所有未删除的节点
	if keyWord == "" {
		for i := part.StartDocId; i < part.NextDocId; i++ {
			retDocs = append(retDocs, basic.DocNode{DocId: i})
		}
	} else {
		var match bool
		retDocs, match = part.query(fieldName, keyWord)
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

	finalRetDocs := []basic.DocNode{}
	//再使用过滤器
	if filters != nil && len(filters) > 0 {
		for _, doc := range retDocs {
			match := true
			//必须全部的过滤器都满足
			for _, filter := range filters {
				if !part.Fields[filter.FieldName].Filter(doc.DocId, filter) {
					match = false
					break
				}
				log.Debugf("Partition[%v] QUERY  %v", part.PrtPathName, doc)
			}
			if match {
				finalRetDocs = append(finalRetDocs, doc)
			}
		}
	} else {
		finalRetDocs = retDocs
	}
	return finalRetDocs, len(finalRetDocs)>0
}
