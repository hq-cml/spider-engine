package field

/*
 * 字段的实现, 一个字段，类比于Mysql中表的一列
 * 一个field可以包含一个正排索引（必须）和一个倒排索引（可选）
 */
import (
	"errors"
	"fmt"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/hq-cml/spider-engine/utils/log"
)

//字段的结构定义
type Field struct {
	fieldName  string
	startDocId uint32               //TODO 是否存在不为0的情况
	nextDocId  uint32
	fieldType  uint8
	isMemory   bool
	ivtIdx     *index.InvertedIndex //倒排索引
	fwdIdx     *index.ForwardIndex  //正排索引
	fwdOffset  uint64               //此正排索引的数据，在文件中的起始偏移（这个东西不能自增）
	fwdDocCnt  uint32               //正排索引文档个数
	btree      btree.Btree
}

//TODO ??
func NewEmptyFakeField(fieldname string, start uint32, fieldtype uint8, docCnt uint32) *Field {
	fwdIdx := index.NewEmptyFakeForwardIndex(fieldtype, start, docCnt)
	return &Field{
		fieldName:  fieldname,
		startDocId: start,
		nextDocId:  start,
		fieldType:  fieldtype,
		isMemory:   true,
		ivtIdx:     nil,
		fwdIdx:     fwdIdx,
		fwdOffset:  0,
		fwdDocCnt:  0,
		btree:      nil,
	}
}

//新建空字段
func NewEmptyField(fieldname string, start uint32, fieldType uint8) *Field {
	var ivtIdx *index.InvertedIndex
	if fieldType == index.IDX_TYPE_STRING ||
		fieldType == index.IDX_TYPE_STRING_SEG ||
		fieldType == index.IDX_TYPE_STRING_LIST ||
		fieldType == index.IDX_TYPE_STRING_SINGLE ||
		fieldType == index.GATHER_TYPE {
		ivtIdx = index.NewInvertedIndex(fieldType, start, fieldname)
	}
	fwdIdx := index.NewEmptyForwardIndex(fieldType, start)
	return &Field{
		fieldName:  fieldname,
		startDocId: start,
		nextDocId:  start,
		fieldType:  fieldType,
		isMemory:   true,
		ivtIdx:     ivtIdx,
		fwdIdx:     fwdIdx,
		fwdOffset:  0,
		fwdDocCnt:  0,
		btree:      nil,
	}
}

//加载重建字段索引
func LoadField(fieldname string, start, max uint32, fieldtype uint8, fwdOffset uint64,
	fwdDocCnt uint32, idxMmap , baseMmap, extMmap *mmap.Mmap, isMomery bool, btree btree.Btree) *Field {

	var ivtIdx *index.InvertedIndex
	if fieldtype == index.IDX_TYPE_STRING ||
		fieldtype == index.IDX_TYPE_STRING_SEG ||
		fieldtype == index.IDX_TYPE_STRING_LIST ||
		fieldtype == index.IDX_TYPE_STRING_SINGLE ||
		fieldtype == index.GATHER_TYPE {
		ivtIdx = index.LoadInvertedIndex(btree, fieldtype, fieldname, idxMmap)
	}

	fwdIdx := index.LoadForwardIndex(fieldtype, baseMmap, extMmap,
		fwdOffset, fwdDocCnt, false)

	return &Field{
		fieldName:  fieldname,
		startDocId: start,
		nextDocId:  max,
		fieldType:  fieldtype,
		isMemory:   isMomery,
		fwdDocCnt:  fwdDocCnt,
		fwdOffset:  fwdOffset,
		ivtIdx:     ivtIdx,
		fwdIdx:     fwdIdx,
		btree:      btree,
	}
}

//增加一个doc
//TODO 如何保证一致性？？？
func (fld *Field) AddDocument(docId uint32, content string) error {

	if docId != fld.nextDocId || fld.isMemory == false || fld.fwdIdx == nil {
		log.Errf("AddDocument :: Wrong docId %v fld.nextDocId %v fld.profile %v", docId, fld.nextDocId, fld.fwdIdx)
		return errors.New("[ERROR] Wrong docId")
	}

	if err := fld.fwdIdx.AddDocument(docId, content); err != nil {
		log.Errf("Field --> AddDocument :: Add Document Error %v", err)
		return err
	}

	//数字型和时间型不能加倒排索引
	if fld.fieldType != index.IDX_TYPE_NUMBER &&
		fld.fieldType != index.IDX_TYPE_DATE &&
		fld.ivtIdx != nil {
		if err := fld.ivtIdx.AddDocument(docId, content); err != nil {
			log.Errf("Field --> AddDocument :: Add Invert Document Error %v", err)
			// return err
			//TODO 一致性？？
		}
	}

	fld.nextDocId++
	return nil
}

//更新
//TODO 倒排索引怎么办?不更新了吗
func (fld *Field) UpdateDocument(docid uint32, contentstr string) error {
	if docid < fld.startDocId || docid >= fld.nextDocId || fld.fwdIdx == nil {
		log.Errf("Field --> UpdateDocument :: Wrong docid %v", docid)
		return errors.New("[ERROR] Wrong docid")
	}
	if fld.fieldType == index.IDX_TYPE_NUMBER || fld.fieldType == index.IDX_TYPE_DATE{
		if err := fld.fwdIdx.UpdateDocument(docid, contentstr); err != nil {
			log.Errf("Field --> UpdateDocument :: Update Document Error %v", err)
			return err
		}
	}

	return nil
}

//落地持久化
func (fld *Field) Persist(segmentName string, btree btree.Btree) error {

	var err error
	if fld.fwdIdx != nil {
		fld.fwdOffset, fld.fwdDocCnt, err = fld.fwdIdx.Persist(segmentName)
		if err != nil {
			log.Errf("Field --> Persist :: Serialization Error %v", err)
			return err
		}
	}

	if fld.ivtIdx != nil {
		fld.btree = btree //TODO 为什么是使用传进来的，而不能直接使用自己的额？？
		err = fld.ivtIdx.Persist(segmentName, fld.btree)
		if err != nil {
			log.Errf("Field --> Serialization :: Serialization Error %v", err)
			return err
		}
	}

	log.Infof("Field[%v] --> Serialization OK...", fld.fieldName)
	return nil
}

//给定一个查询词query，找出doc的列表（标准操作）
//这个就是利用倒排索引
func (fld *Field) Query(key interface{}) ([]basic.DocNode, bool) {

	if fld.ivtIdx == nil {
		return nil, false
	}

	return fld.ivtIdx.QueryTerm(fmt.Sprintf("%v", key))
}

//获取字符值
func (fld *Field) GetString(docId uint32) (string, bool) {

	if docId >= fld.startDocId && docId < fld.nextDocId && fld.fwdIdx != nil {
		return fld.fwdIdx.GetString(docId - fld.startDocId)
	}

	return "", false
}

//过滤（针对的是正排索引）
func (fld *Field) Filter(docId uint32, filterType uint8, start, end int64, numbers []int64, str string) bool {

	if docId >= fld.startDocId && docId < fld.nextDocId && fld.fwdIdx != nil {
		if len(numbers) == 0 {
			return fld.fwdIdx.Filter(docId - fld.startDocId, filterType, start, end, str)
		} else {
			return fld.fwdIdx.FilterNums(docId - fld.startDocId, filterType, numbers)
		}
	}
	return false
}

//字段归并
func (fld *Field) MergeField(fields []*Field, segmentName string, btree btree.Btree) (uint64, uint32, error) {
	var err error
	if fld.fwdIdx != nil {
		fwds := make([]*index.ForwardIndex, 0)

		for _, fd := range fields {
			fwds = append(fwds, fd.fwdIdx)
		}
		fld.fwdOffset, fld.fwdDocCnt, err = fld.fwdIdx.MergeIndex(fwds, segmentName)
		if err != nil {
			log.Errf("Field --> mergeField :: Serialization Error %v", err)
			return 0, 0, err
		}
		fld.nextDocId += uint32(fld.fwdDocCnt)
	}

	if fld.ivtIdx != nil {
		fld.btree = btree

		ivts := make([]*index.InvertedIndex, 0)
		for _, fd := range fields {
			if fd.ivtIdx != nil {
				ivts = append(ivts, fd.ivtIdx)
			} else {
				log.Infof("invert is nil ")
			}
		}
		if err := fld.ivtIdx.MergeIndex(ivts, segmentName, btree); err != nil {
			return 0, 0, err
		}

	}

	return fld.fwdOffset, fld.fwdDocCnt, nil
}

//销毁字段
func (fld *Field) destroy() error {
	if fld.fwdIdx != nil {
		fld.fwdIdx.Destroy()
	}

	if fld.ivtIdx != nil {
		fld.ivtIdx.Destroy()
	}
	return nil
}

func (fld *Field) SetBaseMmap(mmap *mmap.Mmap) {

	if fld.fwdIdx != nil {
		fld.fwdIdx.SetBaseMmap(mmap)
	}

}

func (fld *Field) SetExtMmap(mmap *mmap.Mmap) {
	if fld.fwdIdx != nil {
		fld.fwdIdx.SetExtMmap(mmap)
	}
}

func (fld *Field) SetIdxMmap(mmap *mmap.Mmap) {
	if fld.ivtIdx != nil {
		fld.ivtIdx.SetIdxMmap(mmap)
	}
}

func (fld *Field) SetBtree(btdb btree.Btree) {
	if fld.ivtIdx != nil {
		fld.ivtIdx.SetBtree(btdb)
	}
}

func (fld *Field) SetMmap(base, ext, idx *mmap.Mmap) {
	fld.SetBaseMmap(base)
	fld.SetExtMmap(ext)
	fld.SetIdxMmap(idx)
}

