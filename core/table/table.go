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
	"github.com/hq-cml/spider-engine/core/partition"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/bitmap"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/basic"
)

// SimpleFieldInfo description: 字段的描述信息
type SimpleFieldInfo struct {
	FieldName string `json:"fieldname"`
	FieldType uint64 `json:"fieldtype"`
	PflOffset int64  `json:"pfloffset"` //正排索引的偏移量
	PflLen    int    `json:"pfllen"`    //正排索引长度
}

type Table struct {
	Name           string                     `json:"name"`
	Path           string                     `json:"pathname"`
	Fields         map[string]SimpleFieldInfo `json:"fields"`
	PrimaryKey     string                     `json:"primarykey"`
	StartDocId     uint32                     `json:"startdocid"`
	NextDocId      uint32                     `json:"nextdocid"`
	PrefixSegment  uint64                     `json:"prefixsegment"`
	PartitionNames []string                   `json:"partitionnames"`

	partitions   []*partition.Partition
	memPartition *partition.Partition
	btreeDb      btree.Btree
	bitmap       *bitmap.Bitmap

	pkmap map[string]string

	mutex sync.Mutex //锁，当分区持久化到或者合并时使用或者新建分区时使用
}

//关闭
func (table *Table) Close() error {

	table.mutex.Lock()
	defer table.mutex.Unlock()
	log.Infof("Close Table [%v]", table.Name)

	if table.memPartition != nil {
		table.memPartition.Close()
	}

	for _, seg := range table.partitions {
		seg.Close()
	}

	if table.btreeDb != nil {
		table.btreeDb.Close()
	}

	if table.bitmap != nil {
		table.bitmap.Close()
	}

	log.Infof("Close Table [%v] Finish", table.Name)
	return nil

}

//新建空表
func NewEmptyTable(name, path string) *Table {
	var mu sync.Mutex
	table := &Table{
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
		Fields:         make(map[string]SimpleFieldInfo),
		mutex: 			mu,
		pkmap: make(map[string]string),
	}

	btmpName := path + name + basic.IDX_FILENAME_SUFFIX_BITMAP
	table.bitmap = bitmap.NewBitmap(btmpName, false)

	return table
}
