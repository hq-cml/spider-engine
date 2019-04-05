package table

/**
 * 表的实现，类比于Mysql的表的概念
 * 一张表的构成：
 *   逻辑上由多个字段Field构成
 *   物理上，是由多个Partiton构成（各个Partition都拥有相同的字段）
 *
 * 一张表，拥有一套完整的索引系统
 * 其每个字段都会默认建立正排索引，并根据需要可选的建立倒排索引
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
)

type Table struct {
	Name           string                        `json:"name"`
	Path           string                        `json:"pathname"`
	FieldSummaries map[string]field.FieldSummary `json:"fields"`
	PrimaryKey     string                        `json:"primarykey"`
	StartDocId     uint32                        `json:"startdocid"`
	NextDocId      uint32                        `json:"nextdocid"`
	PrefixSegment  uint64                        `json:"prefixsegment"`
	PartitionNames []string                      `json:"partitionnames"`

	partitions   []*partition.Partition
	memPartition *partition.Partition
	btreeDb      btree.Btree
	bitmap       *bitmap.Bitmap

	pkmap map[string]string

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

	if tbl.btreeDb != nil {
		tbl.btreeDb.Close()
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
	var summaries []field.FieldSummary
	for _, f := range tbl.FieldSummaries {
		if f.FieldType != index.IDX_TYPE_PK { //TODO why??
			summaries = append(summaries, f)
		}
	}

	tbl.memPartition = partition.NewEmptyPartitionWithFieldsInfo(segmentname, tbl.NextDocId, summaries)
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
		btreeDb:        nil,
		bitmap:         nil,
		Path:           path,
		FieldSummaries: make(map[string]field.FieldSummary),
		mutex:          mu,
		pkmap:          make(map[string]string),
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
		tbl.btreeDb = btree.NewBtree("", primaryname)
	}

	log.Infof("Load Table %v success", tbl.Name)
	return &tbl, nil
}

//TODO 为什么这里添加列， 只有内存分区那一块有效，其他的分支只是增加一个分区？？
func (tbl *Table) AddField(summary field.FieldSummary) error {

	if _, ok := tbl.FieldSummaries[summary.FieldName]; ok {
		log.Warnf("Field %v have Exist ", summary.FieldName)
		return nil
	}

	tbl.FieldSummaries[summary.FieldName] = summary
	if summary.FieldType == index.IDX_TYPE_PK {
		tbl.PrimaryKey = summary.FieldName
		primaryname := fmt.Sprintf("%v%v_primary%v", tbl.Path, tbl.Name, basic.IDX_FILENAME_SUFFIX_BTREE)
		tbl.btreeDb = btree.NewBtree("", primaryname)
		tbl.btreeDb.AddTree(summary.FieldName)
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
				tbl.PartitionNames = append(tbl.PartitionNames, prt.SegmentName)
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
		log.Warnf("Field %v is btreeDb key can not delete ", fieldname)
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
		tbl.PartitionNames = append(tbl.PartitionNames, seg.SegmentName)
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
	tbl.btreeDb.MutiSet(tbl.PrimaryKey, tbl.pkmap)
	endTime := time.Now()
	log.Debugf("Cost  muti set  %v", endTime.Sub(startTime))
	tbl.pkmap = make(map[string]string)

	return nil
}
