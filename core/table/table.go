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
	"github.com/hq-cml/falconEngine/src/utils"
	"github.com/hq-cml/falconEngine/src/tree"
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
	Name           string                     		`json:"name"`
	Path           string                     		`json:"pathname"`
	Fields         map[string]field.FieldSummary 	`json:"fields"`
	PrimaryKey     string                    	    `json:"primarykey"`
	StartDocId     uint32                    		`json:"startdocid"`
	NextDocId      uint32                     		`json:"nextdocid"`
	PrefixSegment  uint64                     		`json:"prefixsegment"`
	PartitionNames []string                  		`json:"partitionnames"`

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
		Fields:         make(map[string]field.FieldSummary),
		mutex: 			mu,
		pkmap: make(map[string]string),
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
	segmentname := fmt.Sprintf("%v%v_%v", tbl.Path, tbl.Name, tbl.PrefixSegment)
	var fields []field.FieldSummary
	for _, f := range tbl.Fields {
		if f.FieldType != index.IDX_TYPE_PK {
			fields = append(fields, f)
		}
	}

	tbl.memPartition = partition.NewEmptyPartitionWithFieldsInfo(segmentname, tbl.NextDocId, fields)
	tbl.PrefixSegment++

	//读取bitmap
	btmpPath := path + name + basic.IDX_FILENAME_SUFFIX_BITMAP
	tbl.bitmap = bitmap.NewBitmap(btmpPath, true)

	if tbl.PrimaryKey != "" {
		primaryname := fmt.Sprintf("%v%v_primary.pk", tbl.Path, tbl.Name)
		tbl.btreeDb = btree.NewBtree("", primaryname)
	}

	log.Infof("Load Table %v success", tbl.Name)
	return &tbl, nil
}
