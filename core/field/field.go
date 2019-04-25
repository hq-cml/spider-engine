package field

/*
 * 字段的实现, 一个字段，对等于Mysql中表的一列
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
	FieldName  string `json:"fieldName"`
	StartDocId uint32 `json:"startDocId"`                  //和它所拥有的正排索引一致
	NextDocId  uint32 `json:"startDocId"`
	IndexType  uint16 `json:"indexType"`
	inMemory   bool
	IvtIdx     *index.InvertedIndex    `json:"-"`  //倒排索引
	FwdIdx     *index.ForwardIndex      `json:"-"` //正排索引
	FwdOffset  uint64 `json:"fwdOffset"` //此正排索引的数据，在文件中的起始偏移
	DocCnt     uint32 `json:"docCnt"`    //正排索引文档个数
	btdb       btree.Btree  `json:"-"`
}

// 字段的核心描述信息，用于分区的落盘与加载
type CoreField struct {
	BasicField
	FwdOffset uint64 `json:"fwdOffset"` //正排索引的偏移量
	//DocCnt    uint32 `json:"docCnt"` 	//没必要：正排索引文档个数
}

// 字段的基本描述信息，用于除了CoreFiled场景之外的场景
type BasicField struct {
	FieldName string `json:"fieldName"`
	IndexType uint16  `json:"indexType"`
}

//假字段，高层合并落地时, 可能会出现部分新的分区拥有新字段
//此时, 老分区用FakeField占位用
func NewFakeField(fieldname string, start uint32, next uint32, indexType uint16) *Field {
	fwdIdx := index.NewFakeForwardIndex(indexType, start, next)
	var ivtIdx *index.InvertedIndex
	if indexType == index.IDX_TYPE_STRING ||
		indexType == index.IDX_TYPE_STRING_SEG ||
		indexType == index.IDX_TYPE_STRING_LIST ||
		indexType == index.IDX_TYPE_STRING_SINGLE {
		ivtIdx = index.NewFakeInvertedIndex(indexType, start, fieldname)
	}

	return &Field {
		FieldName:  fieldname,
		StartDocId: start,
		NextDocId:  next,
		IndexType:  indexType,
		FwdIdx:     fwdIdx,    //主要是为了这个假索引
		IvtIdx:     ivtIdx,    //主要是为了这个假索引
	}
}

//新建空字段
func NewEmptyField(fieldName string, start uint32, indexType uint16) *Field {
	//建立反向索引，如果需要的话
	var ivtIdx *index.InvertedIndex
	if indexType == index.IDX_TYPE_STRING ||
		indexType == index.IDX_TYPE_STRING_SEG ||
		indexType == index.IDX_TYPE_STRING_LIST ||
		indexType == index.IDX_TYPE_STRING_SINGLE {
		ivtIdx = index.NewEmptyInvertedIndex(indexType, start, fieldName)
	}
	//建立正向索引
	fwdIdx := index.NewEmptyForwardIndex(indexType, start)

	return &Field{
		FieldName:  fieldName,
		StartDocId: start,
		NextDocId:  start,
		IndexType:  indexType,
		inMemory:   true,
		IvtIdx:     ivtIdx,
		FwdIdx:     fwdIdx,
		FwdOffset:  0,
		DocCnt:     0,
		btdb:       nil,
	}
}

//新建空字段
func NewEmptyGodField(fieldName string, start uint32) *Field {
	//建立反向索引，如果需要的话
	ivtIdx := index.NewEmptyInvertedIndex(index.IDX_TYPE_GOD, start, fieldName)

	return &Field{
		FieldName:  fieldName,
		StartDocId: start,
		NextDocId:  start,
		IndexType:  index.IDX_TYPE_GOD,
		inMemory:   true,
		IvtIdx:     ivtIdx,
	}
}

//加载字段索引
//这里并未真的从磁盘加载，mmap都是从外部直接传入的，因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
func LoadField(fieldname string, start, next uint32, indexType uint16, fwdOffset uint64,
	fwdDocCnt uint32, baseMmap, extMmap, ivtMmap *mmap.Mmap, btdb btree.Btree) *Field {

	//加载倒排
	var ivtIdx *index.InvertedIndex
	if indexType == index.IDX_TYPE_STRING ||
		indexType == index.IDX_TYPE_STRING_SEG ||
		indexType == index.IDX_TYPE_STRING_LIST ||
		indexType == index.IDX_TYPE_STRING_SINGLE ||
		indexType == index.IDX_TYPE_GOD {
		ivtIdx = index.LoadInvertedIndex(btdb, indexType, fieldname, ivtMmap, next)
	}

	//加载正排
	var fwdIdx *index.ForwardIndex
	if indexType != index.IDX_TYPE_GOD { //上帝字段没有正排
		fwdIdx = index.LoadForwardIndex(indexType, baseMmap, extMmap, fwdOffset, fwdDocCnt, start, next)
	}

	return &Field{
		FieldName:  fieldname,
		StartDocId: start,
		NextDocId:  next,
		IndexType:  indexType,
		inMemory:   false,
		DocCnt:     fwdDocCnt,
		FwdOffset:  fwdOffset,
		IvtIdx:     ivtIdx,
		FwdIdx:     fwdIdx,
		btdb:       btdb,
	}
}

//增加一个doc
//Note:
//只有内存态的字段才能增加Doc
func (fld *Field) AddDocument(docId uint32, content interface{}) error {

	if docId != fld.NextDocId || fld.inMemory == false || fld.FwdIdx == nil {
		log.Errf("AddDocument :: Wrong docId %v fld.NextDocId %v fld.profile %v", docId, fld.NextDocId, fld.FwdIdx)
		return errors.New("[ERROR] Wrong docId")
	}

	if err := fld.FwdIdx.AddDocument(docId, content); err != nil {
		log.Errf("Field --> AddDocument :: Add Document Error %v", err)
		return err
	}

	//数字型和时间型不添加倒排索引
	if fld.IndexType != index.IDX_TYPE_INTEGER &&
		fld.IndexType != index.IDX_TYPE_DATE &&
		fld.IvtIdx != nil {
		contentStr, ok := content.(string)
		if !ok {
			return errors.New("Invert index must string")
		}
		if err := fld.IvtIdx.AddDocument(docId, contentStr); err != nil {
			log.Errf("Field --> AddDocument :: Add Invert Document Error %v", err)
			// return err
			//TODO 一致性？？
		}
	}
	fld.DocCnt++
	fld.NextDocId++
	return nil
}

//更高层采用先删后增的方式，变相得实现了update
//更新
//Note:
//只更新正排索引，倒排索引在上层通过bitmap来更新
//func (fld *Field) UpdateDocument(docid uint32, content string) error {
//	if fld.FwdIdx == nil {
//		return errors.New("fwdIdx is nil")
//	}
//	if err := fld.FwdIdx.UpdateDocument(docid, content); err != nil {
//		log.Errf("Field --> UpdateDocument :: Update Document Error %v", err)
//		return err
//	}
//
//	return nil
//}

//给定一个查询词query，找出doc的列表
//Note：这个就是利用倒排索引
func (fld *Field) Query(key interface{}) ([]basic.DocNode, bool) {
	if fld.IvtIdx == nil {
		return nil, false
	}

	return fld.IvtIdx.QueryTerm(fmt.Sprintf("%v", key))
}

//获取字符值
//Note：利用正排索引
func (fld *Field) GetString(docId uint32) (string, bool) {
	//Pos是docId在本索引中的位置
	pos := docId - fld.StartDocId
	if docId >= fld.StartDocId && docId < fld.NextDocId && fld.FwdIdx != nil {
		return fld.FwdIdx.GetString(pos)
	}

	return "", false
}

func (fld *Field) GetInt(docId uint32) (int64, bool) {
	//Pos是docId在本索引中的位置
	pos := docId - fld.StartDocId
	if docId >= fld.StartDocId && docId < fld.NextDocId && fld.FwdIdx != nil {
		return fld.FwdIdx.GetInt(pos)
	}

	return index.MaxInt64, false
}

func (fld *Field) GetValue(docId uint32) (interface{}, bool) {
	//Pos是docId在本索引中的位置
	if fld.IndexType == index.IDX_TYPE_INTEGER || fld.IndexType == index.IDX_TYPE_DATE {
		return fld.GetInt(docId)
	} else {
		return fld.GetString(docId)
	}
}

//销毁字段
func (fld *Field) DoClose() error {
	if fld.FwdIdx != nil {
		fld.FwdIdx.DoClose()
	}

	if fld.IvtIdx != nil {
		fld.IvtIdx.DoClose()
	}
	return nil
}

func (fld *Field) SetBaseMmap(mmap *mmap.Mmap) {
	if fld.FwdIdx != nil {
		fld.FwdIdx.SetBaseMmap(mmap)
	}
}

func (fld *Field) SetExtMmap(mmap *mmap.Mmap) {
	if fld.FwdIdx != nil {
		fld.FwdIdx.SetExtMmap(mmap)
	}
}

func (fld *Field) SetIvtMmap(mmap *mmap.Mmap) {
	if fld.IvtIdx != nil {
		fld.IvtIdx.SetIvtMmap(mmap)
	}
}

func (fld *Field) SetBtree(btdb btree.Btree) {
	if fld.IvtIdx != nil {
		fld.IvtIdx.SetBtree(btdb)
	}
}

func (fld *Field) SetMmap(base, ext, ivt *mmap.Mmap) {
	fld.SetBaseMmap(base)
	fld.SetExtMmap(ext)
	fld.SetIvtMmap(ivt)
}

//落地持久化
//Note:
// 因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)，所以这里直接用分区的路径文件名做前缀
// 这里一个设计的问题，函数并未自动加载回mmap，但是设置了倒排的btdb和正排的fwdOffset和docCnt
func (fld *Field) Persist(partitionPathName string, btdb btree.Btree) error {

	var err error
	if fld.FwdIdx != nil {
		//落地, 并设置了field的信息
		fld.FwdOffset, fld.DocCnt, err = fld.FwdIdx.Persist(partitionPathName)
		if err != nil {
			log.Errf("Field --> Persist :: Error %v", err)
			return err
		}
	}

	if fld.IvtIdx != nil {
		//落地, 并设置了btdb
		fld.btdb = btdb
		err = fld.IvtIdx.Persist(partitionPathName, fld.btdb)
		if err != nil {
			log.Errf("Field --> Persist :: Error %v", err)
			return err
		}
	}

	log.Infof("Field[%v] --> Persist OK...", fld.FieldName)
	return nil
}

//字段归并
//和底层逻辑一致，同样mmap不会加载，其他控制数据包括btdb会加载
func (fld *Field) MergePersistField(fields []*Field, partitionName string, btdb btree.Btree) (error) {
	//一些校验, index的类型，顺序必须完整正确
	if fields == nil || len(fields) == 0 {
		return errors.New("Nil []*Field")
	}
	l := len(fields)
	for i:=0; i<(l-1); i++ {
		if fields[i].NextDocId != fields[i+1].StartDocId {
			return errors.New("Indexes order wrong")
		}
	}
	var err error

	//合并正排索引(上帝字段没有正排索引)
	if fld.IndexType != index.IDX_TYPE_GOD {
		fwds := make([]*index.ForwardIndex, 0)
		for _, fd := range fields {
			fwds = append(fwds, fd.FwdIdx)
		}
		err = fld.FwdIdx.MergePersistFwdIndex(fwds, partitionName)
		//fmt.Println("B--------", partitionName, fields[0].FieldName, offset, docCnt, nextId)
		if err != nil {
			log.Errf("Field --> mergeField :: Serialization Error %v", err)
			return err
		}
	}

	//如果有倒排索引，则合并
	if fld.IndexType == index.IDX_TYPE_STRING ||
		fld.IndexType == index.IDX_TYPE_STRING_SEG ||
		fld.IndexType == index.IDX_TYPE_STRING_LIST ||
		fld.IndexType == index.IDX_TYPE_STRING_SINGLE ||
		fld.IndexType == index.IDX_TYPE_GOD {

		ivts := make([]*index.InvertedIndex, 0)
		for _, fd := range fields {
			if fd.IvtIdx != nil {
				ivts = append(ivts, fd.IvtIdx)
			} else {
				log.Infof("invert is nil ")
				panic("invert is nil")
			}
		}
		err := fld.IvtIdx.MergePersistIvtIndex(ivts, partitionName, btdb)
		if  err != nil {
			//如果此处出错，则会不一致...
			log.Errf("MergePersistIvtIndex Error: ", err, ". Danger!!!!")
			return err
		}
	}

	//加载回控制数据
	fld.btdb = btdb
	if fld.FwdIdx != nil {
		fld.FwdOffset = fld.FwdIdx.GetFwdOffset()
		fld.DocCnt = fld.FwdIdx.GetDocCnt()
		fld.StartDocId = fld.FwdIdx.GetStartId()
		fld.NextDocId = fld.FwdIdx.GetNextId()
	} else {
		//这里只会是上帝字段
		fld.StartDocId = fields[0].StartDocId
		fld.NextDocId = fields[l-1].NextDocId
		fld.DocCnt = fld.NextDocId - fld.StartDocId
	}

	return nil
}


//过滤（针对的是正排索引）
func (fld *Field) Filter(docId uint32, filter basic.SearchFilter) bool {
	if docId >= fld.StartDocId && docId < fld.NextDocId && fld.FwdIdx != nil {
		//Pos是docId在本索引中的位置
		pos := docId - fld.StartDocId
		return fld.FwdIdx.Filter(pos, filter)
	}
	return false
}

