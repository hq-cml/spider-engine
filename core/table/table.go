package table

/**
 * 表的实现，类比于Mysql的表的概念
 * 一张表的构成：
 *   逻辑上由多个字段Field构成
 *   物理上，是由多个Partiton构成（各个Partition都拥有相同的字段）
 *
 * 一张表，拥有一套完整的索引系统
 * 其每个字段都会默认建立正排索引，并根据需要可选的建立倒排索引
 *
 * 一张表，必须拥有自己的主键，主键是锁定文档的唯一Key，而不是docId
 * 一个文档如果编辑，那么底层docId可能发生变化，但是Key是不变的
 */

import (
	"sync"
	"fmt"
	"encoding/json"
	"time"
	"errors"
	"math"
	"github.com/hq-cml/spider-engine/core/partition"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/bitmap"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/core/field"
)

//表的原则：
// 主键和其他字段不同，单独用一个btree实例存储，不分区
// 最新的文档增加只会操作内存态分区，到达阈值后回落地或者和其他分区合并落地
// 文档的删除采用假删除，通过bitmap标记
type Table struct {
	TableName      string                      `json:"tableName"`
	Path           string                      `json:"pathName"`
	BasicFields    map[string]field.BasicField `json:"fields"`
	PrimaryKey     string                      `json:"primaryKey"`
	StartDocId     uint32                      `json:"startDocId"`
	NextDocId      uint32                      `json:"nextDocId"`
	Prefix         uint64                      `json:"prefix"`
	PartitionNames []string                    `json:"partitionNames"`

	memPartition   *partition.Partition   //内存态的分区
	partitions     []*partition.Partition //磁盘态的分区列表
	primaryBtdb    btree.Btree            //主键专用倒排索引（内存态）
	primaryMap     map[string]string      //主键专用倒排索引（磁盘态），主键不会重复，直接map[string]string
	bitMap         *bitmap.Bitmap         //用于文档删除标记
	mutex          sync.Mutex             //锁，当分区持久化到或者合并时使用或者新建分区时使用
}

//产出一块空的内存分区
func (tbl *Table) generateMemPartition() {
	segmentname := fmt.Sprintf("%v%v_%010v", tbl.Path, tbl.TableName, tbl.Prefix) //10位数补零
	var basicFields []field.BasicField
	for _, f := range tbl.BasicFields {
		if f.IndexType != index.IDX_TYPE_PK { //剔除主键，其他字段建立架子
			basicFields = append(basicFields, f)
		}
	}

	tbl.memPartition = partition.NewEmptyPartitionWithBasicFields(segmentname, tbl.NextDocId, basicFields)
	tbl.Prefix++
}

//新建空表
func NewEmptyTable(path, name string) *Table {
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}
	table := Table{
		TableName:      name,
		PartitionNames: make([]string, 0),
		partitions:     make([]*partition.Partition, 0),
		Path:           path,
		BasicFields:    make(map[string]field.BasicField),
		primaryMap:     make(map[string]string),
	}

	//bitmap文件新建
	btmpName := fmt.Sprintf("%v%v%v",path, name, basic.IDX_FILENAME_SUFFIX_BITMAP)
	table.bitMap = bitmap.NewBitmap(btmpName, false)

	return &table
}

//从文件加载一张表
func LoadTable(name, path string) (*Table, error) {
	tbl := Table{}
	metaFileName := fmt.Sprintf("%v%v%v",path, name, basic.IDX_FILENAME_SUFFIX_META)
	buffer, err := helper.ReadFile(metaFileName)
	if err != nil {
		return nil, err
	}
	//json.Unmarshal仅会覆盖大写的公开字段，小写字段包括锁都不受影响
	err = json.Unmarshal(buffer, &tbl)
	if err != nil {
		return nil, err
	}

	//分别加载各个分区
	for _, partitionName := range tbl.PartitionNames {
		segment, err := partition.LoadPartition(partitionName)
		if err != nil {
			log.Errf("partition.LoadPartition Error:%v", err)
			return nil, err
		}
		tbl.partitions = append(tbl.partitions, segment)
	}

	//产出一块空的内存分区
	tbl.generateMemPartition()

	//加载bitmap
	btmpPath := path + name + basic.IDX_FILENAME_SUFFIX_BITMAP
	tbl.bitMap = bitmap.NewBitmap(btmpPath, true)

	//如果表存在主键，则直接加载主键专用btree
	if tbl.PrimaryKey != "" {
		primaryname := fmt.Sprintf("%v%v_primary%v", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_BTREE)
		tbl.primaryBtdb = btree.NewBtree("", primaryname)
	}

	log.Infof("Load Table %v success", tbl.TableName)
	return &tbl, nil
}

//落地表的元信息
func (tbl *Table) StoreMeta() error {
	metaFileName := fmt.Sprintf("%v%v%s", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_META)
	data := helper.JsonEncode(tbl)
	if data != "" {
		if err := helper.WriteToFile([]byte(data), metaFileName); err != nil {
			return err
		}
	} else {
		return errors.New("Json error")
	}

	//将内存态的主键，全部落盘到btree
	if tbl.PrimaryKey != "" {
		tbl.primaryBtdb.MutiSet(tbl.PrimaryKey, tbl.primaryMap)
		tbl.primaryMap = make(map[string]string)
	}
	return nil
}

//新增字段
//Note:
// 新增的字段只会在最新的空分区生效，如果新增的时候有非空的分区，会先落地，然后产出新分区
// 分区的变动，会锁表
func (tbl *Table) AddField(basicField field.BasicField) error {
	//校验
	if _, exist := tbl.BasicFields[basicField.FieldName]; exist {
		log.Warnf("Field %v have Exist ", basicField.FieldName)
		return errors.New(fmt.Sprintf("Field %v have Exist ", basicField.FieldName))
	}

	//实施新增
	tbl.BasicFields[basicField.FieldName] = basicField
	if basicField.IndexType == index.IDX_TYPE_PK {
		//主键新增单独处理
		if tbl.PrimaryKey != "" {
			return errors.New("Primary key has exist!")
		}
		tbl.PrimaryKey = basicField.FieldName
		primaryname := fmt.Sprintf("%v%v_primary%v", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_BTREE)
		tbl.primaryBtdb = btree.NewBtree("", primaryname)
		tbl.primaryBtdb.AddTree(basicField.FieldName)
	} else {
		//锁表
		tbl.mutex.Lock()
		defer tbl.mutex.Unlock()

		if tbl.memPartition == nil {
			//如果内存分区为nil，则直接新增一个内存分区，新增出来的分区已经包含了新的新增字段
			tbl.generateMemPartition()

		} else if tbl.memPartition.IsEmpty() {
			//如果内存分区为空架子，则直接在内存分区新增字段
			err := tbl.memPartition.AddField(basicField)
			if err != nil {
				log.Errf("Add Field Error  %v", err)
				return err
			}
		} else {
			//将当前的内存分区落地，然后新建内存分区，新增出来的分区已经包含了新的新增字段
			tmpPartition := tbl.memPartition
			//分区落地
			if err := tmpPartition.Persist(); err != nil {
				return err
			}
			//归档分区
			tbl.partitions = append(tbl.partitions, tmpPartition)
			tbl.PartitionNames = append(tbl.PartitionNames, tmpPartition.PartitionName)
			//新分区（包含新字段）生成
			tbl.generateMemPartition()
		}
	}

	//元信息落地
	err := tbl.StoreMeta()
	if err != nil {
		return errors.New("StoreMeta Error:" + err.Error())
	}

	return nil
}

//删除分区
func (tbl *Table) DeleteField(fieldname string) error {
	//校验
	if _, exist := tbl.BasicFields[fieldname]; !exist {
		log.Warnf("Field %v not found ", fieldname)
		return nil
	}
	if fieldname == tbl.PrimaryKey {
		log.Warnf("Field %v is primaryBtdb key can not delete ", fieldname)
		return nil
	}

	//锁表
	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()

	delete(tbl.BasicFields, fieldname)

	if tbl.memPartition == nil {
		//啥也不需要干
		log.Info("Delete field. memPartition is nil. do nothing~")
	} else if tbl.memPartition.IsEmpty() {
		//删除内存分区的字段
		tbl.memPartition.DeleteField(fieldname)
	} else {
		//当前内存分区先落地
		tmpPartition := tbl.memPartition
		if err := tmpPartition.Persist(); err != nil {
			return err
		}
		tbl.partitions = append(tbl.partitions, tmpPartition)
		tbl.PartitionNames = append(tbl.PartitionNames, tmpPartition.PartitionName)
		//产出新的分区(字段已删除）
		tbl.generateMemPartition()
	}

	//元信息落地
	err := tbl.StoreMeta()
	if err != nil {
		return errors.New("StoreMeta Error:" + err.Error())
	}
	return nil
}


func (tbl *Table) AddOrUpdateDoc(content map[string]string, updateType uint8) (uint32, error) {

	if len(tbl.BasicFields) == 0 {
		log.Errf("No Field or Partiton is nil")
		return 0, errors.New("no field or segment is nil")
	}

	//如果memPart为空，则新建
	if tbl.memPartition == nil {
		tbl.mutex.Lock()
		tbl.generateMemPartition()
		if err := tbl.StoreMeta(); err != nil {
			tbl.mutex.Unlock()
			return 0, err
		}
		tbl.mutex.Unlock()
	}

	newDocId := tbl.NextDocId
	tbl.NextDocId++

	if updateType == basic.UPDATE_TYPE_ADD {
		//直接添加主键，不检查
		if tbl.PrimaryKey != "" {
			tbl.primaryMap[content[tbl.PrimaryKey]] = fmt.Sprintf("%v", newDocId)
			//if err := tbl.changePrimaryDocId(content[tbl.PrimaryKey], newDocId); err != nil {
			//	return 0, err
			//}
			if tbl.NextDocId%50000 == 0 {
				startTime := time.Now()
				log.Debugf("start muti set %v", startTime)
				tbl.primaryBtdb.MutiSet(tbl.PrimaryKey, tbl.primaryMap)
				endTime := time.Now()
				log.Debugf("[INFO] cost  muti set  %v", endTime.Sub(startTime))
				tbl.primaryMap = make(map[string]string)
			}
		}
		//无主键的表直接添加
		return newDocId, tbl.memPartition.AddDocument(newDocId, content)
	} else {
		//
		if _, hasPrimary := content[tbl.PrimaryKey]; !hasPrimary {
			log.Errf("Primary Key Not Found %v", tbl.PrimaryKey)
			return 0, errors.New("No Primary Key")
		}

		//先删除docId
		oldDocid, founddoc := tbl.findPrimaryDockId(content[tbl.PrimaryKey])
		if founddoc {
			tbl.bitMap.Set(uint64(oldDocid.Docid))
		}

		//再新增docId
		if err := tbl.changePrimaryDocId(content[tbl.PrimaryKey], newDocId); err != nil {
			return 0, err
		}
		//实质内容本质上还是新增，主键不变，但是doc变了
		return newDocId, tbl.memPartition.AddDocument(newDocId, content)
	}
}

func (tbl *Table) changePrimaryDocId(key string, docid uint32) error {

	err := tbl.primaryBtdb.Set(tbl.PrimaryKey, key, uint64(docid))

	if err != nil {
		log.Errf("[ERROR] update Put key error  %v", err)
		return err
	}

	return nil
}

func (tbl *Table) findPrimaryDockId(key string) (basic.DocNode, bool) {

	val, ok := tbl.primaryBtdb.GetInt(tbl.PrimaryKey, key)
	if !ok /*|| val >= uint64(tbl.memPartition.StartDocId)*/ {
		return basic.DocNode{}, false
	}
	return basic.DocNode{Docid: uint32(val)}, true
}

//将内存分区落盘
func (tbl *Table) SyncMemoryPartition() error {

	if tbl.memPartition == nil {
		return nil
	}
	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()

	//memPartition为空， 则退出
	if tbl.memPartition.NextDocId == tbl.memPartition.StartDocId {
		return nil
	}

	//持久化内存分区
	if err := tbl.memPartition.Persist(); err != nil {
		log.Errf("SyncMemoryPartition Error %v", err)
		return err
	}
	partitionName := tbl.memPartition.PartitionName
	tbl.memPartition.Close()
	tbl.memPartition = nil
	newPartition := partition.LoadPartition(partitionName)
	tbl.partitions = append(tbl.partitions, newPartition)
	tbl.PartitionNames = append(tbl.PartitionNames, partitionName)

	return tbl.StoreMeta()
}

func (tbl *Table) GetDocument(docid uint32) (map[string]string, bool) {

	for _, prt := range tbl.partitions {
		if docid >= prt.StartDocId && docid < prt.NextDocId {
			return prt.GetDocument(docid)
		}
	}
	return nil, false
}

func (tbl *Table) DeleteDocument(pk string) bool {

	docId, found := tbl.findPrimaryDockId(pk)
	if found {
		return tbl.bitMap.Set(uint64(docId.Docid))
	}
	return false
}

func (tbl *Table) MergePartitions() error {

	var startIdx int = -1
	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()

	if len(tbl.partitions) == 1 {
		return nil
	}

	//找到第一个待不符合规范，即要合并的partition
	for idx := range tbl.partitions {
		if tbl.partitions[idx].NextDocId - tbl.partitions[idx].StartDocId < partition.PARTITION_MAX_DOC_CNT {
			startIdx = idx
			break
		}
	}

	if startIdx == -1 {
		return nil
	}

	todoPartitions := tbl.partitions[startIdx:]


	segmentname := fmt.Sprintf("%v%v_%v", tbl.Path, tbl.TableName, tbl.Prefix)
	var summaries []field.BasicField
	for _, f := range tbl.BasicFields {
		if f.IndexType != index.IDX_TYPE_PK { //TODO why??
			summaries = append(summaries, f)
		}
	}

	tmpPartition := partition.NewEmptyPartitionWithBasicFields(segmentname, tbl.NextDocId, summaries)
	tbl.Prefix++ //TODO 要新增吗
	if err := tbl.StoreMeta(); err != nil {
		return err
	}
	err := tmpPartition.MergePersistPartitions(todoPartitions)
	if err != nil {
		log.Errf("MergePartitions Error: %s", err)
		return err
	}

	//tmpname:=tmpPartition.PartitionName
	tmpPartition.Close()
	tmpPartition = nil

	for _, prt := range todoPartitions {
		prt.Destroy()
	}

	//Load回来
	tmpPartition = partition.LoadPartition(segmentname)
	//截断后面的临时part
	tbl.partitions = tbl.partitions[:startIdx]
	tbl.PartitionNames = tbl.PartitionNames[:startIdx]
	//追加上
	tbl.partitions = append(tbl.partitions, tmpPartition)
	tbl.PartitionNames = append(tbl.PartitionNames, segmentname)
	return tbl.StoreMeta()
}

//关闭一张表
func (tbl *Table) Close() error {

	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()
	log.Infof("Close Table [%v]", tbl.TableName)

	if tbl.memPartition != nil {
		tbl.memPartition.Close()
	}

	for _, seg := range tbl.partitions {
		seg.Close()
	}

	if tbl.primaryBtdb != nil {
		tbl.primaryBtdb.Close()
	}

	if tbl.bitMap != nil {
		tbl.bitMap.Close()
	}

	log.Infof("Close Table [%v] Finish", tbl.TableName)
	return nil
}

//func (tbl *Table) SearchUnitDocIds(querys []basic.SearchQuery, filteds []basic.SearchFilted) ([]basic.DocNode, bool) {
//
//	docids := make([]basic.DocNode, 0)
//	for _, prt := range tbl.partitions {
//		docids, _ = prt.SearchUnitDocIds(querys, filteds, tbl.bitMap, docids)
//	}
//
//	if len(docids) > 0 {
//		return docids, true
//	}
//
//	return nil, false
//}

//TODO 有点奇怪, 再细化拆解
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

		if a[ia].Docid == b[ib].Docid {
			a[lenc] = a[ia]
			//uint32((float64(a[ia].Weight) / 10000 * idf ) * 10000)
			a[lenc].Weight += uint32(float64(a[ia].Weight) * idf)
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
				if tbl.bitMap.GetBit(uint64(doc.Docid)) == 1 {
					log.Infof("bitMap is 1 %v", doc.Docid)
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