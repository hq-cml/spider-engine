
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
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/prometheus/common/log"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/basic"
)

// Segment description:段结构
type Partition struct {
	StartDocId  uint32 `json:"startdocid"`
	NextDocId   uint32 `json:"nextdocid"`
	SegmentName string                           `json:"segmentname"`
	//FieldInfos  map[string]utils.SimpleFieldInfo `json:"fields"`
	Fields      map[string]*field.Field
	isMemory    bool
	ivtMmap     *mmap.Mmap
	btreeDb     btree.Btree
	baseMmap    *mmap.Mmap
	extMmap     *mmap.Mmap
}

//新建一个空分区, 包含字段
func NewEmptyPartitionWithFieldsInfo(partitionName string, start uint32, fields []*field.Field) *Partition {

	part := &Partition{
		btreeDb: nil,
		StartDocId: start,
		NextDocId: start,
		SegmentName: partitionName,
		ivtMmap: nil,
		extMmap: nil,
		baseMmap: nil,
		Fields: make(map[string]*field.Field),
		isMemory: true,
	}

	for _, fld := range fields {
		indexer := field.NewEmptyField(fld.FieldName, start, fld.FieldType)
		part.Fields[fld.FieldName] = indexer
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
	for _, fld := range part.Fields {
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

//添加字段
func (part *Partition) AddField(fieldName string, fieldType uint8) error {

	if _, ok := part.Fields[fieldName]; ok {
		log.Warnf("Segment --> AddField Already has field [%v]", fieldName)
		return errors.New("Already has field..")
	}

	if part.isMemory && !part.IsEmpty() {
		log.Warnf("Segment --> AddField field [%v] fail..", fieldName)
		return errors.New("memory segment can not add field..")
	}

	newFiled := field.NewEmptyField(fieldName, part.NextDocId, fieldType)
	part.Fields[fieldName] = newFiled
	return nil
}

//删除字段
func (part *Partition) DeleteField(fieldname string) error {
	//TODO why ??
	if part.isMemory && !part.IsEmpty() {
		log.Warnf("Segment --> deleteField field [%v] fail..", fieldname)
		return errors.New("memory segment can not delete field..")
	}

	part.Fields[fieldname].Destroy()
	delete(part.Fields, fieldname)
	log.Infof("Segment --> DeleteField[%v] :: Success ", fieldname)
	return nil
}

//更新文档
//content, 一篇文档的各个字段的值
func (part *Partition) UpdateDocument(docid uint32, content map[string]string) error {
	//TODO >=???
	if docid >= part.NextDocId || docid < part.StartDocId {
		log.Errorf("Partition --> UpdateDocument :: Wrong DocId[%v]  MaxDocId[%v]", docid, part.NextDocId)
		return errors.New("Partition --> UpdateDocument :: Wrong DocId Number")
	}

	for name, _ := range part.Fields {
		if _, ok := content[name]; !ok {
			if err := part.Fields[name].UpdateDocument(docid, ""); err != nil {
				log.Errorf("Partition --> UpdateDocument :: %v", err)
			}
		} else {
			if err := part.Fields[name].UpdateDocument(docid, content[name]); err != nil {
				log.Errorf("Partition --> UpdateDocument :: field[%v] value[%v] error[%v]", name, content[name], err)
			}
		}
	}

	return nil
}

//添加文档
//content, 一篇文档的各个字段的值
func (part *Partition) AddDocument(docid uint32, content map[string]string) error {

	if docid != part.NextDocId {
		log.Errorf("Partition --> AddDocument :: Wrong DocId[%v]  MaxDocId[%v]", docid, part.NextDocId)
		return errors.New("Partition --> AddDocument :: Wrong DocId Number")
	}

	for name, _ := range part.Fields {
		if _, ok := content[name]; !ok {
			if err := part.Fields[name].AddDocument(docid, ""); err != nil {
				log.Errorf("Partition --> AddDocument [%v] :: %v", part.SegmentName, err)
			}
			continue
		}

		if err := part.Fields[name].AddDocument(docid, content[name]); err != nil {
			log.Errorf("Partition --> AddDocument :: field[%v] value[%v] error[%v]", name, content[name], err)
		}

	}
	part.NextDocId++
	return nil
}

