package field

/*
 * 字段的实现, 一个字段，类比于Mysql中表的一列
 * 一个field可以包含一个正排索引（必须）和一个倒排索引（可选）
 */
import (
	"errors"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/hq-cml/spider-engine/utils/log"
)

//字段的结构定义
type Field struct {
	fieldName  string
	startDocId uint32
	maxDocId   uint32
	fieldType  uint8
	isMemory   bool
	ivtIdx     *index.InvertedIndex //倒排索引
	fwdIdx     *index.ForwardIndex  //正排索引
	fwdOffset  uint64               //正排索引的数据，在文件中的起始偏移
	fwdDocCnt  uint32               //正排索引文档个数
	btree      btree.Btree
}

//TODO ??
func NewEmptyFakeField(fieldname string, start uint32, fieldtype uint8, docCnt uint32) *Field {
	fwdIdx := index.NewEmptyFakeForwardIndex(fieldtype, start, docCnt)
	return &Field{
		fieldName:  fieldname,
		startDocId: start,
		maxDocId:   start,
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
func NewEmptyField(fieldname string, start uint32, fieldtype uint8) *Field {
	var ivtIdx *index.InvertedIndex
	if fieldtype == index.IDX_TYPE_STRING ||
		fieldtype == index.IDX_TYPE_STRING_SEG ||
		fieldtype == index.IDX_TYPE_STRING_LIST ||
		fieldtype == index.IDX_TYPE_STRING_SINGLE ||
		fieldtype == index.GATHER_TYPE {
		ivtIdx = index.NewInvertedIndex(fieldtype, start, fieldname)
	}
	fwdIdx := index.NewEmptyForwardIndex(fieldtype, start)
	return &Field{
		fieldName:  fieldname,
		startDocId: start,
		maxDocId:   start,
		fieldType:  fieldtype,
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
		fieldName: fieldname,
		startDocId: start,
		maxDocId: max,
		fieldType: fieldtype,
		isMemory: isMomery,
		fwdDocCnt: fwdDocCnt,
		fwdOffset: fwdOffset,
		ivtIdx: ivtIdx,
		fwdIdx: fwdIdx,
		btree: btree,
	}
}

//增加一个doc
//TODO 如何保证一致性？？？
func (fld *Field) AddDocument(docId uint32, content string) error {

	if docId != fld.maxDocId || fld.isMemory == false || fld.fwdIdx == nil {
		log.Errf("AddDocument :: Wrong docId %v fld.maxDocId %v fld.profile %v", docId, fld.maxDocId, fld.fwdIdx)
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

	fld.maxDocId++
	return nil
}

//更新
//TODO 倒排索引怎么办?
func (fld *Field) UpdateDocument(docid uint32, contentstr string) error {
	if docid < fld.startDocId || docid >= fld.maxDocId || fld.fwdIdx == nil {
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

