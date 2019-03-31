package index

/**
 * 倒排索引实现
 *
 * 根据搜索引擎的原理, 每一个字段(列)都拥有一个倒排索引
 * 倒排索引由一颗B+树和一个倒排文件搭配，宏观上可以看成一个Map
 * 其中Key的部分基于B+树实现, 便于搜索和范围过滤；val的部分是倒排文件，基于mmap, 便于快速存取并同步disk
 *
 * B+树（由bolt实现）: key是term, val则是term在倒排文件中的offset
 * 倒排文件: 由mmap实现，顺序的数据块, 每块数据长这个个样子
 * [nodeCnt(8Byte)|nodeStruct1|nodeStruct2|....][nodeCnt(8Byte)|nodeStruct1|nodeStruct3|....]....
 * nodeStuct:{docId: xx, weight: xx}
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
)

//倒排索引
//每个字段, 拥有一个倒排索引
type InvertedIndex struct {
	curDocId  uint32
	isMemory  bool
	fieldType uint8
	fieldName string
	idxMmap   *mmap.Mmap                  //倒排文件
	btreeDb   btree.Btree                 //B+树
	termMap   map[string][]basic.DocNode  //索引的内存容器
}

const DOCNODE_BYTE_CNT = 8

//新建空的字符型倒排索引
func NewInvertedIndex(fieldType uint8, startDocId uint32, fieldname string) *InvertedIndex {
	this := &InvertedIndex{
		btreeDb:   nil,
		curDocId:  startDocId,
		fieldName: fieldname,
		fieldType: fieldType,
		termMap:   make(map[string][]basic.DocNode),
		isMemory:  true,
	}
	return this
}

//TODO ??
//通过段的名称建立字符型倒排索引
func LoadInvertedIndex(btdb btree.Btree, fieldType uint8, fieldname string, idxMmap *mmap.Mmap) *InvertedIndex {
	this := &InvertedIndex{
		btreeDb:   btdb,
		fieldType: fieldType,
		fieldName: fieldname,
		isMemory:  false,
		idxMmap:   idxMmap,
	}
	return this
}

//增加一个doc文档
func (rIdx *InvertedIndex) AddDocument(docId uint32, content string) error {

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

	log.Debugf("InvertedIndex AddDocument --> DocId: %v ,Content: %v\n", docId, content)
	return nil
}

//持久化倒排索引
//落地 termMap落地到倒排文件; term进入B+tree
//倒排文件格式: 顺序的数据块, 每块数据长这个个样子 [{nodeCnt(8Byte)|node1|node2|....}, {}, {}]
//B+树: key是term, val则是term在倒排文件中的offsetduolv
func (rIdx *InvertedIndex) Persist(segmentName string, tree btree.Btree) error {

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
	rIdx.btreeDb = tree //TODO 移出去
	if !rIdx.btreeDb.HasTree(rIdx.fieldName) {
		rIdx.btreeDb.AddTree(rIdx.fieldName)
	}
	for term, docNodeList := range rIdx.termMap {
		//先写入长度, 占8个字节
		nodeCnt := len(docNodeList)
		lenBuffer := make([]byte, DOCNODE_BYTE_CNT)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(nodeCnt))
		idxFd.Write(lenBuffer)

		//在写入node list
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, docNodeList)
		if err != nil {
			log.Errf("Persist :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())

		//B+树录入
		err = rIdx.btreeDb.Set(rIdx.fieldName, term, uint64(offset))
		if err != nil {
			//TODO 造成不一致了哦
			return err
		}
		offset = offset + DOCNODE_BYTE_CNT + nodeCnt * basic.DOC_NODE_SIZE
	}

	rIdx.termMap = nil    //TODO ??直接置为 nil?
	rIdx.isMemory = false //TODO ??isMemry用法,用途

	log.Debugf("Persist :: Writing to File : [%v.idx] ", segmentName)
	return nil
}

//给定一个查询词query，找出doc的列表（标准操作）
func (rIdx *InvertedIndex) QueryTerm(term string) ([]basic.DocNode, bool) {
	if rIdx.isMemory == true {
		docNodes, ok := rIdx.termMap[term]
		if ok {
			return docNodes, true
		}
	} else if rIdx.idxMmap != nil {
		offset, ok := rIdx.btreeDb.GetInt(rIdx.fieldName, term)
		if !ok {
			return nil, false
		}
		count := rIdx.idxMmap.ReadUInt64(uint64(offset))
		docNodes := readDocNodes(uint64(offset) + DOCNODE_BYTE_CNT, count, rIdx.idxMmap)
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
//TODO 过于简单？？
func (rIdx *InvertedIndex) Destroy() error {
	rIdx.termMap = nil
	return nil
}

//设置倒排文件mmap
func (rIdx *InvertedIndex) SetIdxMmap(mmap *mmap.Mmap) {
	rIdx.idxMmap = mmap
}

//设置btree
func (rIdx *InvertedIndex) SetBtree(tree btree.Btree) {
	rIdx.btreeDb = tree
}

//btree操作,
//TODO 返回值太多, 只有第1,2和最后一个返回值有用
func (rIdx *InvertedIndex) GetFristKV() (string, uint32, uint32, int, bool) {
	if rIdx.btreeDb == nil {
		log.Err("Btree is null")
		return "", 0, 0, 0, false
	}
	return rIdx.btreeDb.GetFristKV(rIdx.fieldName)
}

//btree操作
func (rIdx *InvertedIndex) GetNextKV(key string) (string, uint32, uint32, int, bool) {
	if rIdx.btreeDb == nil {
		return "", 0, 0, 0, false
	}

	return rIdx.btreeDb.GetNextKV(rIdx.fieldName, key)
}

type tmpMerge struct {
	rIndex *InvertedIndex
	term   string
	nodes []basic.DocNode
}

//多路归并, 将多个反向索引进行合并成为一个大的反向索引
//比较烧脑，下面提供了一个简化版便于调试说明问题
//TODO 是否需要设置rIdx的curId???
func (rIdx *InvertedIndex) MergeIndex(rIndexes []*InvertedIndex, fullSetmentName string, tree btree.Btree) error {
	//打开文件，获取长度，作为offset
	idxFileName := fmt.Sprintf("%v.idx", fullSetmentName)
	fd, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644) //追加打开
	if err != nil {
		return err
	}
	defer fd.Close()
	fi, _ := fd.Stat()
	offset := int(fi.Size())

	//数据准备，开始多路归并
	rIdx.btreeDb = tree
	ivts := make([]tmpMerge, 0)
	for _, ivt := range rIndexes {
		if ivt.btreeDb == nil {
			continue
		}
		term, _, _, _, ok := ivt.GetFristKV()
		if !ok {
			continue
		}
		nodes, _ := ivt.QueryTerm(term)
		ivts = append(ivts, tmpMerge{
			rIndex: ivt,
			term:  term,
			nodes: nodes,
		})
	}

	//补齐树
	if !rIdx.btreeDb.HasTree(rIdx.fieldName) {
		rIdx.btreeDb.AddTree(rIdx.fieldName)
	}

	//开始进行归并
	//flag是子索引完毕标志，哪个子索引完毕，则对应位置1
	//quitFlag是结束标志，当所有的子索引都结束了，则flag==quitFlag
	quitFlag := 0
	for i := range ivts {
		quitFlag = quitFlag | (1 << uint(i))
	}
	flag := 0
	for flag != quitFlag {
		minTerm := ""
		for i, v := range ivts { //随便找一个还未完的索引的头term
			if ((flag >> uint(i)) & 0x1) == 0 {
				minTerm = v.term
				break
			}
		}

		for i, v := range ivts { //找到所有还未完结的索引中最小那个头term
			if ((flag>>uint(i))&0x1) == 0 && minTerm > v.term {
				minTerm = v.term
			}
		}

		minIds := make([]int, 0) //可能多个索引都包含这个最小头term，统统取出来
		for i, ivt := range ivts {
			if (flag>>uint(i)&0x1) == 0 && minTerm == ivt.term {
				minIds = append(minIds, i)
			}
		}

		value := make([]basic.DocNode, 0) //合并这这些个头term, 他们各自的后继者需要顶上来
		for _, i := range minIds {
			value = append(value, ivts[i].nodes...)

			//找到后继者，顶上去
			next, _, _, _, ok := ivts[i].rIndex.GetNextKV(ivts[i].term)
			if !ok {
				//如果没有后继者了，那么该索引位置标记为无效
				flag = flag | (1 << uint(i))
				continue
			}
			ivts[i].term = next
			ivts[i].nodes, ok = ivts[i].rIndex.QueryTerm(next)
			if !ok {
				panic("Index wrong!")
			}
		}

		//写倒排文件 & 写B+树
		nodeCnt := len(value)
		lenBuffer := make([]byte, DOCNODE_BYTE_CNT)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(nodeCnt))
		fd.Write(lenBuffer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			log.Err("[ERROR] invert --> Merge :: Error %v", err)
			return err
		}
		fd.Write(buffer.Bytes())
		rIdx.btreeDb.Set(rIdx.fieldName, minTerm, uint64(offset))
		offset = offset + DOCNODE_BYTE_CNT + nodeCnt * basic.DOC_NODE_SIZE
	}

	rIdx.termMap = nil     //TODO 存在和上面persist同样的疑问
	rIdx.isMemory = false

	return nil
}

//多路归并这块比较抽象， 一个对等简化版便于调试
/*
import (
	"fmt"
	"encoding/json"
)

//mock Btree
type myIndex struct {
	List []KV
}

type KV struct {
	Term  string
	Nodes []int
}

type tmpMerge struct {
	RIndex *InvertedIndex
	Term   string
	Nodes  []int
}

type InvertedIndex struct {
	//。。。省略
	Btree myIndex
}

func (rIdx *InvertedIndex)GetFristKV() (string, uint32, uint32, int, bool){
	tmp := rIdx.Btree.List[0]
	return tmp.Term, 0, 0, 0, true
}

func (rIdx *InvertedIndex) GetNextKV(key string) (string, uint32, uint32, int, bool) {
	length := len(rIdx.Btree.List)
	for i, v := range rIdx.Btree.List {
		if v.Term == key && i < (length-1) {
			return rIdx.Btree.List[i+1].Term, 0, 0, 0, true
		}
	}

	return "", 0, 0, 0, false
}

func (rIdx *InvertedIndex) QueryTerm(key string) ([]int, bool) {
	for i, v := range rIdx.Btree.List {
		if v.Term == key {
			return rIdx.Btree.List[i].Nodes, true
		}
	}

	return nil, false
}

var idx1 InvertedIndex
var idx2 InvertedIndex
var idx3 InvertedIndex

func init() {
	idx1 = InvertedIndex {
		Btree:myIndex{
			List: []KV {
				KV{Term:"a", Nodes:[]int{2,3}},
				KV{Term:"c", Nodes:[]int{1,2}},
				KV{Term:"f", Nodes:[]int{1,3}},
			},
		}}

	idx2 = InvertedIndex {
		Btree:myIndex{
			List: []KV {
				KV{Term:"b", Nodes:[]int{4,6}},
				KV{Term:"c", Nodes:[]int{5,6}},
				KV{Term:"d", Nodes:[]int{4,5}},
			},
		}}

	idx3 = InvertedIndex {
		Btree:myIndex{
			List: []KV {
				KV{Term:"a", Nodes:[]int{8,9}},
				KV{Term:"c", Nodes:[]int{7,9}},
				KV{Term:"e", Nodes:[]int{7,8}},
			},
		}}
}

func main() {
	rIndexes := []*InvertedIndex{&idx1, &idx2, &idx3}
	ivts := make([]tmpMerge, 0)
	for _, ivt := range rIndexes {
		//if ivt.Btree == nil {
		//	continue
		//}

		term, _, _, _, ok := ivt.GetFristKV()
		//fmt.Println(term)
		if !ok {
			continue
		}

		nodes, _ := ivt.QueryTerm(term)
		//fmt.Println(nodes)
		ivts = append(ivts, tmpMerge {
			RIndex: ivt,
			Term:   term,
			Nodes:  nodes,
		})
	}

	resflag := 0
	for i := range rIndexes {
		resflag = resflag | (1 << uint(i))
	}

	fmt.Println("Resflag: ", resflag) //7
	b, err := json.Marshal(ivts)
	fmt.Println(string(b), err)

	flag := 0
	TURN := 1
	for flag != resflag {
		fmt.Println("AAAAAAAA-----------[",  TURN ,"] Flag:", flag)
		minTerm := ""
		for i, v := range ivts {
			if ((flag >> uint(i)) & 0x1) == 0 {
				fmt.Println("XXXXXXXXXXX-----------", i)
				minTerm = v.Term
				break
			}
		}
		fmt.Println("XXXXXXXXXXX-----------", minTerm)

		for i, v := range ivts {
			if ((flag>>uint(i))&0x1) == 0 && minTerm > v.Term {
				fmt.Println("YYYYYYYYYYY-----------", i)
				minTerm = v.Term
			}
		}
		fmt.Println("YYYYYYYYYYY-----------", minTerm)

		meridxs := make([]int, 0)
		for i, ivt := range ivts {
			if (flag>>uint(i)&0x1) == 0 && minTerm == ivt.Term {
				fmt.Println("ZZZZZZZZZZZZ-----------", i)
				meridxs = append(meridxs, i)
			}
		}
		fmt.Println("ZZZZZZZZZZZZ-----------", meridxs, ",Term:", minTerm)

		value := make([]int, 0)
		for _, i := range meridxs {
			value = append(value, ivts[i].Nodes...)
			key, _, _, _, ok := ivts[i].RIndex.GetNextKV(ivts[i].Term)
			if !ok {
				fmt.Println("SSSSSSSSSSSSSS-------结束位：", i)
				flag = flag | (1 << uint(i))
				continue
			}

			ivts[i].Term = key
			ivts[i].Nodes, ok = ivts[i].RIndex.QueryTerm(key)
		}

		//写倒排文件 & 写B+树 省略
		//。。。。

		fmt.Println("AAAAAAAA-----------[",  TURN ,"], Value: ", value, "Flag:", flag, ",Term:", minTerm)

		//if TURN == 5 {
		//	return
		//}

		fmt.Println()
		fmt.Println()
		fmt.Println()
		TURN++
	}
}
*/