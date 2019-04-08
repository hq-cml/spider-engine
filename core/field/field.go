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
	"github.com/hq-cml/spider-engine/basic"
	"fmt"
)

//字段的结构定义
type Field struct {
	FieldName  string `json:"fieldname"`
	startDocId uint32               	 //和它所拥有的正排索引一致
	nextDocId  uint32				 	 //和它所拥有的正排索引一致
	IndexType  uint8  `json:"indextype"`
	inMemory   bool
	ivtIdx     *index.InvertedIndex      //倒排索引
	fwdIdx     *index.ForwardIndex       //正排索引
	FwdOffset  uint64 `json:"fwdOffset"` //此正排索引的数据，在文件中的起始偏移
	DocCnt     uint32 `json:"docCnt"`    //正排索引文档个数
	btdb       btree.Btree
}

// 字段的描述信息
type FieldSummary struct {
	FieldName string `json:"fieldname"`
	FieldType uint8  `json:"fieldtype"`
	FwdOffset uint64 `json:"fwdOffset"` //正排索引的偏移量
	DocCnt    uint32 `json:"docCnt"` 	//正排索引文档个数
}

//假字段，高层占位用
func NewEmptyFakeField(fieldname string, start uint32, IndexType uint8, docCnt uint32) *Field {
	fwdIdx := index.NewEmptyFakeForwardIndex(IndexType, start, docCnt)
	return &Field{
		FieldName:  fieldname,
		startDocId: start,
		nextDocId:  start,
		IndexType:  IndexType,
		fwdIdx:     fwdIdx,    //主要是为了这个假索引
	}
}

//新建空字段
func NewEmptyField(fieldName string, start uint32, indexType uint8) *Field {
	//建立反向索引，如果需要的话
	var ivtIdx *index.InvertedIndex
	if indexType == index.IDX_TYPE_STRING ||
		indexType == index.IDX_TYPE_STRING_SEG ||
		indexType == index.IDX_TYPE_STRING_LIST ||
		indexType == index.IDX_TYPE_STRING_SINGLE ||
		indexType == index.GATHER_TYPE {
		ivtIdx = index.NewEmptyInvertedIndex(indexType, start, fieldName)
	}
	//建立正向索引
	fwdIdx := index.NewEmptyForwardIndex(indexType, start)

	return &Field{
		FieldName:  fieldName,
		startDocId: start,
		nextDocId:  start,
		IndexType:  indexType,
		inMemory:   true,
		ivtIdx:     ivtIdx,
		fwdIdx:     fwdIdx,
		FwdOffset:  0,
		DocCnt:     0,
		btdb:       nil,
	}
}

//加载字段索引
//这里并未真的从磁盘加载，mmap都是从外部直接传入的，因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
func LoadField(fieldname string, start, next uint32, fieldtype uint8, fwdOffset uint64,
	fwdDocCnt uint32, ivtMmap, baseMmap, extMmap *mmap.Mmap, isMomery bool, btree btree.Btree) *Field {

	var ivtIdx *index.InvertedIndex
	if fieldtype == index.IDX_TYPE_STRING ||
		fieldtype == index.IDX_TYPE_STRING_SEG ||
		fieldtype == index.IDX_TYPE_STRING_LIST ||
		fieldtype == index.IDX_TYPE_STRING_SINGLE ||
		fieldtype == index.GATHER_TYPE {
		ivtIdx = index.LoadInvertedIndex(btree, fieldtype, fieldname, ivtMmap)
	}

	fwdIdx := index.LoadForwardIndex(fieldtype, baseMmap, extMmap, fwdOffset, fwdDocCnt, start, next)

	return &Field{
		FieldName:  fieldname,
		startDocId: start,
		nextDocId:  next,
		IndexType:  fieldtype,
		inMemory:   isMomery,
		DocCnt:     fwdDocCnt,
		FwdOffset:  fwdOffset,
		ivtIdx:     ivtIdx,
		fwdIdx:     fwdIdx,
		btdb:       btree,
	}
}

//增加一个doc
//TODO 如何保证一致性？？？
func (fld *Field) AddDocument(docId uint32, content string) error {

	if docId != fld.nextDocId || fld.inMemory == false || fld.fwdIdx == nil {
		log.Errf("AddDocument :: Wrong docId %v fld.nextDocId %v fld.profile %v", docId, fld.nextDocId, fld.fwdIdx)
		return errors.New("[ERROR] Wrong docId")
	}

	if err := fld.fwdIdx.AddDocument(docId, content); err != nil {
		log.Errf("Field --> AddDocument :: Add Document Error %v", err)
		return err
	}

	//数字型和时间型不能加倒排索引
	if fld.IndexType != index.IDX_TYPE_NUMBER &&
		fld.IndexType != index.IDX_TYPE_DATE &&
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
//Note:
//只更新正排索引，倒排索引在上层通过bitmap来更新
func (fld *Field) UpdateDocument(docid uint32, contentstr string) error {
	if fld.fwdIdx == nil {
		return errors.New("fwdIdx is nil")
	}
	if err := fld.fwdIdx.UpdateDocument(docid, contentstr); err != nil {
		log.Errf("Field --> UpdateDocument :: Update Document Error %v", err)
		return err
	}

	return nil
}

//给定一个查询词query，找出doc的列表
//Note：这个就是利用倒排索引
func (fld *Field) Query(key interface{}) ([]basic.DocNode, bool) {
	if fld.ivtIdx == nil {
		return nil, false
	}

	return fld.ivtIdx.QueryTerm(fmt.Sprintf("%v", key))
}

//获取字符值
//Note：利用正排索引
func (fld *Field) GetString(docId uint32) (string, bool) {
	//Pos是docId在本索引中的位置
	pos := docId - fld.startDocId

	if docId >= fld.startDocId && docId < fld.nextDocId && fld.fwdIdx != nil {
		return fld.fwdIdx.GetString(pos)
	}

	return "", false
}

//销毁字段
func (fld *Field) Destroy() error {
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

func (fld *Field) SetIvtMmap(mmap *mmap.Mmap) {
	if fld.ivtIdx != nil {
		fld.ivtIdx.SetIvtMmap(mmap)
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
	fld.SetIvtMmap(idx)
}

//过滤（针对的是正排索引）
func (fld *Field) Filter(docId uint32, filterType uint8, start, end int64, numbers []int64, str string) bool {
	if docId >= fld.startDocId && docId < fld.nextDocId && fld.fwdIdx != nil {

		//Pos是docId在本索引中的位置
		pos := docId - fld.startDocId

		if len(numbers) == 0 {
			return fld.fwdIdx.Filter(pos, filterType, start, end, str)
		} else {
			return fld.fwdIdx.FilterNums(pos, filterType, numbers)
		}
	}
	return false
}

//落地持久化
//Note:
// 因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)，所以这里直接用分区的路径文件名做前缀
// 这里一个设计的问题，函数并未自动加载回mmap，但是设置了倒排的btdb和正排的fwdOffset和docCnt
func (fld *Field) Persist(partitionPathName string, btdb btree.Btree) error {

	var err error
	if fld.fwdIdx != nil {
		fld.FwdOffset, fld.DocCnt, err = fld.fwdIdx.Persist(partitionPathName)
		if err != nil {
			log.Errf("Field --> Persist :: Error %v", err)
			return err
		}
	}

	if fld.ivtIdx != nil {
		fld.btdb = btdb
		err = fld.ivtIdx.Persist(partitionPathName, fld.btdb)
		if err != nil {
			log.Errf("Field --> Persist :: Error %v", err)
			return err
		}
	}

	log.Infof("Field[%v] --> Persist OK...", fld.FieldName)
	return nil
}

//字段归并
//TODO 这些操作, 完全不闭合, 而且还依赖顺序, 后续要大改
//TODO 目前只能合并出完整的磁盘版本, 但是filed并不能直接用
func MergePersistField(fields []*Field, segmentName string, btree btree.Btree) (uint64, uint32, uint32, error) {
	//一些校验, index的类型，顺序必须完整正确
	if fields == nil || len(fields) == 0 {
		return 0, 0, 0, errors.New("Nil []*Field")
	}

	var offset uint64
	var docCnt uint32
	var nextId uint32
	var err error

	//合并正排索引
	fwds := make([]*index.ForwardIndex, 0)
	for _, fd := range fields {
		fwds = append(fwds, fd.fwdIdx)
	}
	offset, docCnt, nextId, err = index.MergePersistFwdIndex(fwds, segmentName)
	if err != nil {
		log.Errf("Field --> mergeField :: Serialization Error %v", err)
		return 0, 0, 0, err
	}

	//如果有倒排索引，则合并
	if fields[0].ivtIdx != nil {
		ivts := make([]*index.InvertedIndex, 0)
		for _, fd := range fields {
			if fd.ivtIdx != nil {
				ivts = append(ivts, fd.ivtIdx)
			} else {
				log.Infof("invert is nil ")
			}
		}
		err := index.MergePersistIvtIndex(ivts, segmentName, btree)
		if  err != nil {
			//如果此处出错，则会不一致...
			log.Errf("MergePersistIvtIndex Error: ", err, ". Danger!!!!")
			return 0, 0, 0, err
		}
	}

	return offset, docCnt, nextId, nil
}


