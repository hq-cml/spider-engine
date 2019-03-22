package index

/**
 * 文本类倒排索引实现
 *
 * 按照搜索引擎的原理, 每一个字段(列)都拥有一个倒排索引
 * 倒排索引Key用B+树实现, 便于搜索和范围过滤
 * 倒排索引val部分基于mmap, 便于快速存取并同步disk
 */
import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"unsafe"
	"reflect"
	"encoding/binary"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/log"
	//"github.com/hq-cml/FalconEngine/src/tree"
	//"github.com/hq-cml/FalconEngine/src/utils"
)

/************************************************************************

字符型倒排索引，操作文件

[fullname].dic 该字段的字典文件，格式 | termlen | term | termId(uint32) | DF(uint32) |  ......
[segmentname].pos 该段的位置信息
[segmentname].idx 该段的倒排文件

************************************************************************/
//倒排索引
//每个字段, 拥有一个倒排索引
type ReverseIndex struct {
	curDocId  uint32
	isMomery  bool
	fieldType uint64
	fieldName string
	idxMmap   *mmap.Mmap
	termMap   map[string][]basic.DocNode  //索引的临时索引
	btree     btree.Btree
}

type reverseMerge struct {
	idx   *ReverseIndex
	key   string
	nodes []basic.DocNode
	pgnum uint32
	index int
}

const NODE_CNT_BYTE int = 8

//新建空的字符型倒排索引
func NewReverseIndex(fieldType uint64, startDocId uint32, fieldname string) *ReverseIndex {
	this := &ReverseIndex{
		btree: nil,
		curDocId: startDocId,
		fieldName: fieldname,
		fieldType: fieldType,
		termMap: make(map[string][]basic.DocNode),
		isMomery: true,
	}
	return this
}

//TODO ??
//通过段的名称建立字符型倒排索引
func newInvertWithLocalFile(btdb btree.Btree, fieldType uint64, fieldname string, idxMmap *mmap.Mmap) *ReverseIndex {

	this := &ReverseIndex{
		btree: btdb,
		fieldType: fieldType,
		fieldName: fieldname,
		isMomery: false,
		idxMmap: idxMmap,
	}
	return this

}

//增加一个doc文档
func (rIdx *ReverseIndex) addDocument(docId uint32, content string) error {

	//docId校验
	if docId != rIdx.curDocId {
		return errors.New("invert --> AddDocument :: Wrong DocId Number")
	}

	//根据type进行分词
	var nodes map[string]basic.DocNode
	switch rIdx.fieldType {
	case IDX_TYPE_STRING, GATHER_TYPE: //全词匹配模式
		nodes = SplitWholeWords(docId, content)
	case IDX_TYPE_STRING_LIST: //分号切割模式
		nodes = SplitSemicolonWords(docId, content)
	case IDX_TYPE_STRING_SINGLE: //单个词模式
		nodes = SplitRuneWords(docId, content)
	case IDX_TYPE_STRING_SEG: //分词模式
		nodes = SplitTrueWords(docId, content)
	}

	//分词结果填入索引的临时存储
	for term, node := range nodes {
		if _, exist := rIdx.termMap[term]; !exist {
			rIdx.termMap[term] = []basic.DocNode{}
		}
		rIdx.termMap[term] = append(rIdx.termMap[term], node)
	}

	//docId自增
	rIdx.curDocId++

	log.Debugf("AddDocument --> Docid: %v ,Content: %v\n", docId, content)
	return nil
}

//持久化倒排索引
//落地 termMap落地到倒排文件; term进入B+tree
//倒排文件格式: 顺序的数据块, 每块数据长这个个样子 [{nodeCnt(8Byte)|node1|node2|....}, {}, {}]
//B+树: key是term, val则是term在倒排文件中的offset
func (rIdx *ReverseIndex) persist(segmentName string, tree btree.Btree) error {

	//打开倒排文件, 获取文件大小作为初始偏移
	//TODO 此处为何是直接写文件,而不是mmap??
	idxFileName := fmt.Sprintf("%v.idx", segmentName)
	idxFd, err := os.OpenFile(idxFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644) //追加打开file
	if err != nil {
		return err
	}
	defer idxFd.Close()
	fi, err := idxFd.Stat()
	if err != nil {
		return err
	}
	offset := int(fi.Size()) //当前偏移量, 即文件最后位置

	//开始文件写入
	rIdx.btree = tree //TODO 移出去
	for term, docNodeList := range rIdx.termMap {
		//先写入长度, 占8个字节
		nodeCnt := len(docNodeList)
		lenBufer := make([]byte, NODE_CNT_BYTE)
		binary.LittleEndian.PutUint64(lenBufer, uint64(nodeCnt))
		idxFd.Write(lenBufer)

		//在写入node list
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, docNodeList)
		if err != nil {
			log.Errf("Persist :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())

		//B+树录入
		rIdx.btree.Set(rIdx.fieldName, term, uint64(offset))
		offset = offset + NODE_CNT_BYTE + nodeCnt * basic.DOC_NODE_SIZE
	}

	rIdx.termMap = nil    //TODO ??直接置为 nil?
	rIdx.isMomery = false //TODO ??isMemry用法,用途

	log.Debugf("Persist :: Writing to File : [%v.idx] ", segmentName)
	return nil
}

// Query function description : 给定一个查询词query，找出doc的列表（标准操作）
// params : key string 查询的key值
// return : docid结构体列表  bool 是否找到相应结果
func (rIdx *ReverseIndex) queryTerm(term string) ([]basic.DocNode, bool) {

	//this.Logger.Info("[INFO] QueryTerm %v",keystr)
	if rIdx.isMomery == true {
		// this.Logger.Info("[INFO] ismemory is  %v",this.isMomery)
		docNodes, ok := rIdx.termMap[term]
		if ok {
			return docNodes, true
		}
	} else if rIdx.idxMmap != nil {
		offset, ok := rIdx.btree.GetInt(rIdx.fieldName, term)
		//this.Logger.Info("[INFO] found  %v this.FullName %v offset %v",keystr,this.fieldName,offset)
		if !ok {
			return nil, false
		}
		count := rIdx.idxMmap.ReadInt64(offset)
		docNodes := readDocNodes(uint64(offset) + uint64(NODE_CNT_BYTE), uint64(count), rIdx.idxMmap)
		return docNodes, true
	}

	return nil, false
}

//从mmap中读取出
func readDocNodes(start, count uint64, mmp *mmap.Mmap) []basic.DocNode {
	nodeList := *(*[]basic.DocNode)(unsafe.Pointer(&reflect.SliceHeader {
		Data: uintptr(unsafe.Pointer(&mmp.DataBytes[start])),
		Len:  int(count),
		Cap:  int(count),
	}))
	return nodeList
}

//索引销毁
func (rIdx *ReverseIndex) destroy() error {
	rIdx.termMap = nil
	return nil
}

//设置mmap
func (rIdx *ReverseIndex) setIdxMmap(mmap *mmap.Mmap) {
	rIdx.idxMmap = mmap
}

//设置btree
func (rIdx *ReverseIndex) setBtree(tree btree.Btree) {
	rIdx.btree = tree
}

//btree操作,
//TODO 返回值太多, 只有第1,2和最后一个返回值有用
func (rIdx *ReverseIndex) GetFristKV() (string, uint32, uint32, int, bool) {
	if rIdx.btree == nil {
		log.Err("Btree is null")
		return "", 0, 0, 0, false
	}
	return rIdx.btree.GetFristKV(rIdx.fieldName)
}

//btree操作
func (rIdx *ReverseIndex) GetNextKV(key string) (string, uint32, uint32, int, bool) {
	if rIdx.btree == nil {
		return "", 0, 0, 0, false
	}

	return rIdx.btree.GetNextKV(rIdx.fieldName, key)
}

//归并

func (this *ReverseIndex) mergeIndex(ivtlist []*ReverseIndex, fullsegmentname string, tree btree.Btree) error {

	idxFileName := fmt.Sprintf("%v.idx", fullsegmentname)
	idxFd, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer idxFd.Close()
	fi, _ := idxFd.Stat()
	totalOffset := int(fi.Size())

	this.btree = tree

	ivts := make([]reverseMerge, 0)

	for _, ivt := range ivtlist {

		if ivt.btree == nil {
			continue
		}

		key, _, pgnum, index, ok := ivt.GetFristKV()
		if !ok {
			continue
		}

		docids, _ := ivt.queryTerm(key)
		ivts = append(ivts, reverseMerge{idx: ivt, key: key, nodes: docids, pgnum: pgnum, index: index})
	}

	resflag := 0
	for i := range ivts {
		resflag = resflag | (1 << uint(i))
	}
	flag := 0
	for flag != resflag {
		maxkey := ""
		for idx, v := range ivts {
			if ((flag >> uint(idx)) & 0x1) == 0 {
				maxkey = v.key
			}
		}

		for idx, v := range ivts {
			if ((flag>>uint(idx))&0x1) == 0 && maxkey > v.key {
				maxkey = v.key
			}
		}

		//maxkey = ""
		meridxs := make([]int, 0)
		for idx, ivt := range ivts {

			if (flag>>uint(idx)&0x1) == 0 && maxkey == ivt.key {
				meridxs = append(meridxs, idx)
				continue
			}

		}

		value := make([]basic.DocNode, 0)

		for _, idx := range meridxs {
			value = append(value, ivts[idx].nodes...)
			key, _, pgnum, index, ok := ivts[idx].idx.GetNextKV( /*ivts[idx].pgnum,ivts[idx].index*/ ivts[idx].key)
			if !ok {
				flag = flag | (1 << uint(idx))
				continue
			}

			ivts[idx].key = key
			ivts[idx].pgnum = pgnum
			ivts[idx].index = index
			ivts[idx].nodes, ok = ivts[idx].idx.queryTerm(key)
		}

		lens := len(value)
		lenBufer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBufer, uint64(lens))
		idxFd.Write(lenBufer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			log.Err("[ERROR] invert --> Merge :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())
		this.btree.Set(this.fieldName, maxkey, uint64(totalOffset))
		totalOffset = totalOffset + 8 + lens*basic.DOC_NODE_SIZE
	}

	this.termMap = nil
	this.isMomery = false

	return nil
}