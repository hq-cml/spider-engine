
package partition
/*
 * 分区, 类比于Mysql的分区的概念
 * 每个分区都拥有全量的filed, 但是数据是整体表数据的一部分,
 * 每个分区是独立的索引单元, 所有的分区合在一起, 就是一张完整的表
 */
import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"github.com/hq-cml/FalconEngine/src/tree"
	"github.com/hq-cml/FalconEngine/src/utils"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/google/btree"
)

// Segment description:段结构
type Partition struct {
	StartDocId  uint32 `json:"startdocid"`
	MaxDocId    uint32 `json:"maxdocid"`
	SegmentName string                           `json:"segmentname"`
	//FieldInfos  map[string]utils.SimpleFieldInfo `json:"fields"`
	Fields      map[string]*field.Field
	isMemory    bool
	ivtMmap     *mmap.Mmap
	btreeDb     btree.BTree
	baseMmap    *mmap.Mmap
	extMmap     *mmap.Mmap

}
