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
	FieldName  string 				`json:"fieldName"`
	StartDocId uint32 				`json:"startDocId"`  //字段归属的分区的起始DocId（和它所拥有的底层正排索引起始DocId一致）
	NextDocId  uint32			    `json:"nextDocId"`
	IndexType  uint16			    `json:"indexType"`
	inMemory   bool
	IvtIdx     *index.InvertedIndex `json:"-"`           //倒排索引
	FwdIdx     *index.ForwardIndex  `json:"-"`           //正排索引
	btdb       btree.Btree          `json:"-"`
}

// 字段的基本描述信息，用于除了CoreFiled场景之外的场景
type BasicField struct {
	FieldName string `json:"fieldName"`
	IndexType uint16  `json:"indexType"`
}

// 字段的核心描述信息，用于分区的落盘与加载
type CoreField struct {
	BasicField
	FwdOffset uint64 `json:"fwdOffset"` //正排索引的偏移量
}

type BasicStatus struct {
	FieldName  string   `json:"name"`
	IndexType  string   `json:"type"`
}

type FieldStatus struct {
	FieldName     string `json:"name"`
	StartDocId    uint32 `json:"startDocId"`
	NextDocId     uint32 `json:"nextDocId"`
	FwdNextDocId  int64  `json:"fwdNextDocId"`
	FwdDocCnt     int64  `json:"fwdDocCnt"`
	IvtNextDocId  int64  `json:"ivtNextDocId"`
}

//假字段，高层合并落地时, 可能会出现部分新的分区拥有新字段
//此时, 老分区用FakeField占位用
func NewFakeField(fieldname string, start uint32, next uint32, indexType uint16, docCnt uint32) *Field {
	fwdIdx := index.NewFakeForwardIndex(indexType, docCnt, next)
	var ivtIdx *index.InvertedIndex
	if indexType == index.IDX_TYPE_STR_WHOLE ||
		indexType == index.IDX_TYPE_STR_SPLITER ||
		indexType == index.IDX_TYPE_STR_LIST ||
		indexType == index.IDX_TYPE_STR_WORD ||
		indexType == index.IDX_TYPE_GOD {
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
	if indexType == index.IDX_TYPE_STR_WHOLE ||
		indexType == index.IDX_TYPE_STR_SPLITER ||
		indexType == index.IDX_TYPE_STR_LIST ||
		indexType == index.IDX_TYPE_STR_WORD { //上帝视角有专门的函数去创建, 这里不创建
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
		btdb:       nil,
	}
}

//新建空上帝视角字段
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
func LoadField(fieldname string, startDocId, nextDocId uint32, indexType uint16, fwdOffset uint64,
	fwdDocCnt uint32, baseMmap, extMmap, ivtMmap *mmap.Mmap, btdb btree.Btree) *Field {

	//加载倒排
	var ivtIdx *index.InvertedIndex
	if indexType == index.IDX_TYPE_STR_WHOLE ||
		indexType == index.IDX_TYPE_STR_SPLITER ||
		indexType == index.IDX_TYPE_STR_LIST ||
		indexType == index.IDX_TYPE_STR_WORD ||
		indexType == index.IDX_TYPE_GOD {
		ivtIdx = index.LoadInvertedIndex(btdb, indexType, fieldname, ivtMmap, nextDocId)
	}

	//加载正排
	var fwdIdx *index.ForwardIndex
	if indexType != index.IDX_TYPE_GOD { //上帝字段没有正排
		fwdIdx = index.LoadForwardIndex(indexType, baseMmap, extMmap, fwdOffset, fwdDocCnt, nextDocId)
	}

	return &Field{
		FieldName:  fieldname,
		StartDocId: startDocId,
		NextDocId:  nextDocId,
		IndexType:  indexType,
		inMemory:   false,
		IvtIdx:     ivtIdx,
		FwdIdx:     fwdIdx,
		btdb:       btdb,
	}
}

//增加一个doc
//Note:
//	只有内存态的字段才能增加Doc
//Note:
//  为了保证一致性，错误不中断，继续进行，交给高层去废除
func (fld *Field) AddDocument(docId uint32, content interface{}) error {
	var checkErr error
	var fwdErr error
	var ivtErr error
	var ok bool
	var contentStr string

	//校验
	if docId != fld.NextDocId || fld.inMemory == false {
		log.Warnf("Field AddDoc. Wrong docId %v, NextDocId: %v, FwdIdx: %v", docId, fld.NextDocId, fld.FwdIdx)
		//return errors.New("[ERROR] Wrong docId")
		checkErr = errors.New("Field AddDoc. Wrong docId.")
	}

	//正排新增(上帝视角没有正排)
	if fld.IndexType != index.IDX_TYPE_GOD {
		fwdErr = fld.FwdIdx.AddDocument(docId, content)
		if fwdErr != nil {
			fwdErr = errors.New(fmt.Sprintf("Add Fwd Error %v", fwdErr.Error()))
			log.Warnf("Add Fwd Error: %v", fwdErr.Error())
		}
	}

	//倒排新增: 数字型和时间型不添加倒排索引, 纯文本模式也不需要倒排
	if fld.IndexType != index.IDX_TYPE_INTEGER &&
		fld.IndexType != index.IDX_TYPE_DATE &&
		fld.IndexType != index.IDX_TYPE_PURE_TEXT &&
		fld.IndexType != index.IDX_TYPE_PK &&      //主键只在高层Table起作用
		fld.IvtIdx != nil {
		contentStr, ok = content.(string)
		if !ok {
			ivtErr = errors.New("Invert index must string.")
			log.Warn("Invert index must string.")
			contentStr = ""
		}

		err := fld.IvtIdx.AddDocument(docId, contentStr)
		if err != nil {
			ivtErr = errors.New(fmt.Sprintf("Add Invert Doc Error %v", err.Error()))
			log.Warnf(fmt.Sprintf("Add Invert Doc Error %v", err.Error()))
		}
	}

	if checkErr == nil && fwdErr == nil && ivtErr == nil {
		fld.NextDocId++
		return nil
	} else {
		//为了保证一致性，将错就错
		fld.NextDocId++
		checkInfo, fwdInfo, ivtInfo := "", "", ""
		if checkErr != nil { checkInfo = checkErr.Error() }
		if fwdErr != nil { fwdInfo = fwdErr.Error() }
		if ivtErr != nil { ivtInfo = ivtErr.Error() }
		return errors.New(fmt.Sprintf(
			"Field-->AddDoc. CheckErr:%v, FwdErr: %v, IvtErr: %v", checkInfo, fwdInfo, ivtInfo))
	}
}

//其他字段增加文档出现失败的时候，用来将当前字段回滚
//Note:
// 目前来讲回滚啥卵事没干，先预留
func (fld *Field) AddDocRollback(docId uint32) error {
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
	if fld.IndexType == index.IDX_TYPE_INTEGER ||
		fld.IndexType == index.IDX_TYPE_DATE ||
		fld.IndexType == index.IDX_TYPE_PURE_TEXT ||
		fld.IndexType == index.IDX_TYPE_PK ||  //主键只在高层Table起作用
		fld.IvtIdx == nil {
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
func (fld *Field) Persist(partitionPathName string, btdb btree.Btree) (uint64, uint32, error) {
	var err error
	var docCnt uint32
	var fwdOffset uint64
	if fld.FwdIdx != nil {
		//落地, 并设置了field的信息
		fwdOffset, docCnt, err = fld.FwdIdx.Persist(partitionPathName)
		if err != nil {
			log.Errf("Field--> Persist. Error %v", err)
			return 0, 0, err
		}
	}

	if fld.IvtIdx != nil {
		//落地, 并设置了btdb
		fld.btdb = btdb
		err = fld.IvtIdx.Persist(partitionPathName, fld.btdb)
		if err != nil {
			log.Errf("Field--> Persist. Error %v", err)
			return 0, 0, err
		}
	}

	log.Infof("Field[%v]--> Persist OK...", fld.FieldName)
	return fwdOffset, docCnt, nil
}

//字段归并
//和底层逻辑一致，同样mmap不会加载，其他控制数据包括btdb会加载
func (fld *Field) MergePersistField(fields []*Field, partitionName string, btdb btree.Btree) (uint64, uint32, error) {
	//一些校验, index的类型，顺序必须完整正确
	if fields == nil || len(fields) == 0 {
		return 0, 0, errors.New("Nil []*Field")
	}
	l := len(fields)
	for i:=0; i<(l-1); i++ {
		if fields[i].NextDocId != fields[i+1].StartDocId {
			return 0, 0, errors.New("Indexes order wrong")
		}
	}
	var err error
	var docCnt uint32
	var fwdOffset uint64
	//合并正排索引(上帝字段没有正排索引)
	if fld.IndexType != index.IDX_TYPE_GOD {
		fwds := make([]*index.ForwardIndex, 0)
		for _, fd := range fields {
			fwds = append(fwds, fd.FwdIdx)
		}
		fwdOffset, docCnt, err = fld.FwdIdx.MergePersistFwdIndex(fwds, partitionName)
		//fmt.Println("B--------", partitionName, fields[0].FieldName, offset, docCnt, nextId)
		if err != nil {
			log.Errf("Field--> mergeField. Serialization Error %v", err)
			return 0, 0, err
		}
	}

	//如果有倒排索引，则合并
	if fld.IndexType == index.IDX_TYPE_STR_WHOLE ||
		fld.IndexType == index.IDX_TYPE_STR_SPLITER ||
		fld.IndexType == index.IDX_TYPE_STR_LIST ||
		fld.IndexType == index.IDX_TYPE_STR_WORD ||
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
			return 0, 0, err
		}
	}

	//加载回控制数据
	fld.btdb = btdb
	fld.StartDocId = fields[0].StartDocId
	fld.NextDocId = fields[l-1].NextDocId

	return fwdOffset, docCnt, nil
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

func (fld *Field) GetStatus() *FieldStatus {
	var fwdNextId int64 = -1
	var ivtStartId int64 = -1
	var fwdDocCnt int64 = -1

	if fld.FwdIdx != nil {
		fwdNextId = int64(fld.FwdIdx.GetNextId())
		fwdDocCnt = int64(fld.FwdIdx.GetDocCnt())
	}

	if fld.IvtIdx != nil {
		ivtStartId = int64(fld.IvtIdx.GetNextId())
	}

	return &FieldStatus{
		FieldName:     fld.FieldName,
		StartDocId:    fld.StartDocId,
		NextDocId :    fld.NextDocId,
		FwdNextDocId:  fwdNextId,
		FwdDocCnt:     fwdDocCnt,
		IvtNextDocId:  ivtStartId,
	}
}

func (fld *BasicField) GetBasicStatus() *BasicStatus {
	return &BasicStatus {
		FieldName : fld.FieldName,
		IndexType : index.RE_IDX_MAP[fld.IndexType],
	}
}