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
	"github.com/hq-cml/spider-engine/core/partition"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/bitmap"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/core/field"
	"math"
)

type Table struct {
	Name           string                      `json:"name"`
	Path           string                      `json:"pathname"`
	FieldSummaries map[string]field.BasicField `json:"fields"`
	PrimaryKey     string                      `json:"primarykey"`
	StartDocId     uint32                      `json:"startdocid"`
	NextDocId      uint32                      `json:"nextdocid"`
	PrefixSegment  uint64                      `json:"prefixsegment"`
	PartitionNames []string                    `json:"partitionnames"`

	partitions    []*partition.Partition

	primaryBtdb    btree.Btree          //文件部分倒排索引（用于主键）
	primaryMap    map[string]string     //内存部分倒排索引（用于主键），主键不会重复，所以直接map[string]string

	memPartition  *partition.Partition

	bitmap        *bitmap.Bitmap
	mutex sync.Mutex //锁，当分区持久化到或者合并时使用或者新建分区时使用
}

//关闭
func (tbl *Table) Close() error {

	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()
	log.Infof("Close Table [%v]", tbl.Name)

	if tbl.memPartition != nil {
		tbl.memPartition.Close()
	}

	for _, seg := range tbl.partitions {
		seg.Close()
	}

	if tbl.primaryBtdb != nil {
		tbl.primaryBtdb.Close()
	}

	if tbl.bitmap != nil {
		tbl.bitmap.Close()
	}

	log.Infof("Close Table [%v] Finish", tbl.Name)
	return nil
}

//产生内存Partition
func (tbl *Table) generateMemPartition() {
	segmentname := fmt.Sprintf("%v%v_%v", tbl.Path, tbl.Name, tbl.PrefixSegment)
	var summaries []field.BasicField
	for _, f := range tbl.FieldSummaries {
		if f.IndexType != index.IDX_TYPE_PK { //TODO why??
			summaries = append(summaries, f)
		}
	}

	tbl.memPartition = partition.NewEmptyPartitionWithBasicFields(segmentname, tbl.NextDocId, summaries)
	tbl.PrefixSegment++
}

//新建空表
func NewEmptyTable(name, path string) *Table {
	var mu sync.Mutex
	table := Table{
		Name:           name,
		StartDocId:     0,
		NextDocId:      0,
		PrefixSegment:  1000,
		PartitionNames: make([]string, 0),
		PrimaryKey:     "",
		partitions:     make([]*partition.Partition, 0),
		memPartition:   nil,
		primaryBtdb:    nil,
		bitmap:         nil,
		Path:           path,
		FieldSummaries: make(map[string]field.BasicField),
		mutex:          mu,
		primaryMap:     make(map[string]string),
	}

	btmpName := path + name + basic.IDX_FILENAME_SUFFIX_BITMAP
	table.bitmap = bitmap.NewBitmap(btmpName, false)

	return &table
}

//从文件加载表
func LoadTable(name, path string) (*Table, error) {
	var mu sync.Mutex
	tbl := Table{
		mutex: 			mu,
	}

	metaFileName := path + name + basic.IDX_FILENAME_SUFFIX_META
	buffer, err := helper.ReadFile(metaFileName)
	if err != nil {
		return nil, err
	}
	//TODO ?? 是否会覆盖掉mutex
	err = json.Unmarshal(buffer, &tbl)
	if err != nil {
		return nil, err
	}

	for _, segmentname := range tbl.PartitionNames {
		segment := partition.LoadPartition(segmentname)
		tbl.partitions = append(tbl.partitions, segment)
	}

	//新建空的分区
	tbl.generateMemPartition()

	//读取bitmap
	btmpPath := path + name + basic.IDX_FILENAME_SUFFIX_BITMAP
	tbl.bitmap = bitmap.NewBitmap(btmpPath, true)

	if tbl.PrimaryKey != "" {
		primaryname := fmt.Sprintf("%v%v_primary%v", tbl.Path, tbl.Name, basic.IDX_FILENAME_SUFFIX_BTREE)
		tbl.primaryBtdb = btree.NewBtree("", primaryname)
	}

	log.Infof("Load Table %v success", tbl.Name)
	return &tbl, nil
}

//TODO 为什么这里添加列， 只有内存分区那一块有效，其他的分支只是增加一个分区？？
func (tbl *Table) AddField(summary field.BasicField) error {

	if _, ok := tbl.FieldSummaries[summary.FieldName]; ok {
		log.Warnf("Field %v have Exist ", summary.FieldName)
		return nil
	}

	tbl.FieldSummaries[summary.FieldName] = summary
	if summary.IndexType == index.IDX_TYPE_PK {
		tbl.PrimaryKey = summary.FieldName
		primaryname := fmt.Sprintf("%v%v_primary%v", tbl.Path, tbl.Name, basic.IDX_FILENAME_SUFFIX_BTREE)
		tbl.primaryBtdb = btree.NewBtree("", primaryname)
		tbl.primaryBtdb.AddTree(summary.FieldName)
	} else {
		tbl.mutex.Lock()
		defer tbl.mutex.Unlock()

		if tbl.memPartition == nil {

			tbl.generateMemPartition()

		} else if tbl.memPartition.IsEmpty() {
			err := tbl.memPartition.AddField(summary)
			if err != nil {
				log.Errf("Add Field Error  %v", err)
				return err
			}
		} else {
			tmpsegment := tbl.memPartition
			if err := tmpsegment.Persist(); err != nil {
				return err
			}
			tbl.partitions = append(tbl.partitions, tmpsegment)
			tbl.PartitionNames = make([]string, 0)
			for _, prt := range tbl.partitions {
				tbl.PartitionNames = append(tbl.PartitionNames, prt.PartitionName)
			}

			tbl.generateMemPartition()
		}
	}
	return tbl.StoreMeta()
}

func (tbl *Table) DeleteField(fieldname string) error {

	if _, ok := tbl.FieldSummaries[fieldname]; !ok {
		log.Warnf("Field %v not found ", fieldname)
		return nil
	}

	if fieldname == tbl.PrimaryKey {
		log.Warnf("Field %v is primaryBtdb key can not delete ", fieldname)
		return nil
	}

	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()

	delete(tbl.FieldSummaries, fieldname)

	if tbl.memPartition == nil {
		//tbl.memPartition.DeleteField(fieldname) //panic
		return tbl.StoreMeta()
	}

	if tbl.memPartition.IsEmpty() {
		tbl.memPartition.DeleteField(fieldname)
		return tbl.StoreMeta()
	}

	//TODO 这里为何不 tbl.memPartition.DeleteField(fieldname)
	tmpsegment := tbl.memPartition
	if err := tmpsegment.Persist(); err != nil {
		return err
	}
	tbl.partitions = append(tbl.partitions, tmpsegment)
	tbl.PartitionNames = make([]string, 0)
	for _, seg := range tbl.partitions {
		tbl.PartitionNames = append(tbl.PartitionNames, seg.PartitionName)
	}

	tbl.generateMemPartition()

	return tbl.StoreMeta()
}

func (tbl *Table) StoreMeta() error {
	metaFileName := fmt.Sprintf("%v%v%s", tbl.Path, tbl.Name, basic.IDX_FILENAME_SUFFIX_META)
	data := helper.JsonEncode(tbl)
	if data != "" {
		if err := helper.WriteToFile([]byte(data), metaFileName); err != nil {
			return err
		}
	} else {
		return errors.New("Json error")
	}

	startTime := time.Now()
	log.Debugf("Start muti set %v", startTime)
	tbl.primaryBtdb.MutiSet(tbl.PrimaryKey, tbl.primaryMap)
	endTime := time.Now()
	log.Debugf("Cost  muti set  %v", endTime.Sub(startTime))
	tbl.primaryMap = make(map[string]string)

	return nil
}

func (tbl *Table) AddOrUpdateDoc(content map[string]string, updateType uint8) (uint32, error) {

	if len(tbl.FieldSummaries) == 0 {
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
			tbl.bitmap.Set(uint64(oldDocid.Docid))
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
		return tbl.bitmap.Set(uint64(docId.Docid))
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


	segmentname := fmt.Sprintf("%v%v_%v", tbl.Path, tbl.Name, tbl.PrefixSegment)
	var summaries []field.BasicField
	for _, f := range tbl.FieldSummaries {
		if f.IndexType != index.IDX_TYPE_PK { //TODO why??
			summaries = append(summaries, f)
		}
	}

	tmpPartition := partition.NewEmptyPartitionWithBasicFields(segmentname, tbl.NextDocId, summaries)
	tbl.PrefixSegment++  //TODO 要新增吗
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

func (tbl *Table) SearchUnitDocIds(querys []basic.SearchQuery, filteds []basic.SearchFilted) ([]basic.DocNode, bool) {

	docids := make([]basic.DocNode, 0)
	for _, prt := range tbl.partitions {
		docids, _ = prt.SearchUnitDocIds(querys, filteds, tbl.bitmap, docids)
	}

	if len(docids) > 0 {
		return docids, true
	}

	return nil, false
}

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
			docids, _ = prt.SearchDocs(basic.SearchQuery{}, filteds, tbl.bitmap, docids)
		}
		if len(docids) > 0 {
			for _, doc := range docids {
				if tbl.bitmap.GetBit(uint64(doc.Docid)) == 1 {
					log.Infof("bitmap is 1 %v", doc.Docid)
				}
			}
			return docids, true
		}
		GiveDocIDsChan <- docids
		return nil, false
	}

	if len(querys) >= 1 {
		for _, prt := range tbl.partitions {
			docids, _ = prt.SearchDocs(querys[0], filteds, tbl.bitmap, docids)
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
			subdocids, _ = prt.SearchDocs(query, filteds, tbl.bitmap, subdocids)
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