package index

/**
 * 倒排索引实现
 *
 * 根据搜索引擎的原理, 一个字段(列)需要支持搜索，那么其需要拥有一个倒排索引
 * 本倒排索引由一颗B+树和一个倒排文件搭配，宏观上可以看成一个Map（Key是分词后的各个Term，Val是Term对应的docId列表）
 * 其中Key的部分基于B+树实现, 便于搜索和范围过滤；val的部分存在倒排文件中，基于mmap, 便于快速存取并同步disk
 *
 * B+树（由bolt实现）: key是term, val则是term在倒排文件中的offset
 * 倒排文件: 由mmap实现，顺序的数据块, 每块数据长这个个样子
 * [nodeCnt(8Byte)|nodeStruct1|nodeStruct2|....][nodeCnt(8Byte)|nodeStruct1|nodeStruct3|....]....
 * nodeStuct:{docId: xx, weight: xx}
 *
 * Note：
 * 同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
 */
import (
	"bytes"
	"errors"
	"os"
	"unsafe"
	"reflect"
	"encoding/binary"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/log"
	"fmt"
)

//倒排索引
//每个字段, 拥有一个倒排索引
type InvertedIndex struct {
	nextDocId uint32                     //下一个加入本索引的docId（所以本索引最大docId是nextDocId-1）
	inMemory  bool                       //本索引是内存态还是磁盘态（不会同时并存）
	indexType uint16                     //本索引的类型
	fieldName string                     //本索引所属字段
	fake      bool                       //标记位, 用于占位，高层的分区缺少某个字段时候，用此占位
	termMap   map[string][]basic.DocNode //索引的内存容器
	ivtMmap   *mmap.Mmap                 //倒排文件(以mmap的形式)
	btdb      btree.Btree                //B+树
}

const DOCNODE_BYTE_CNT = 8

//新建空的倒排索引
func NewEmptyInvertedIndex(indexType uint16, nextDocId uint32, fieldName string) *InvertedIndex {
	rIdx := &InvertedIndex{
		nextDocId: nextDocId,
		fieldName: fieldName,
		indexType: indexType,
		fake:      false,
		termMap:   make(map[string][]basic.DocNode),
		inMemory:  true,                            	//新索引都是从内存态开始
		ivtMmap:   nil,
		btdb:      nil,
	}
	return rIdx
}

//新建空的倒排索引
func NewFakeInvertedIndex(indexType uint16, nextDocId uint32, fieldName string) *InvertedIndex {
	rIdx := &InvertedIndex{
		nextDocId: nextDocId,
		fieldName: fieldName,
		indexType: indexType,
		fake:      true,      //here is the point~~
	}
	return rIdx
}

//从磁盘加载倒排索引
//这里并未真的从磁盘加载，mmap和btdb都是从外部直接传入的，因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
//如果mmap自己创建的话，会造成多个mmap实例对应同一个磁盘文件，这样会造成不确定性(mmmap头部有隐藏信息字段)，也不易于维护
func LoadInvertedIndex(btdb btree.Btree, indexType uint16, fieldname string, ivtMmap *mmap.Mmap, nextDocId uint32) *InvertedIndex {
	rIdx := &InvertedIndex{
		indexType: indexType,
		fieldName: fieldname,
		nextDocId: nextDocId,
		fake:      false,
		inMemory:  false,       //加载的索引都是磁盘态的
		ivtMmap:   ivtMmap,
		btdb:      btdb,
	}
	return rIdx
}

//增加一个doc文档
//Note：
// 增加文档，只会出现在最新的一个分区（即都是内存态的），所以只会操作内存态的
// 也就是，一个索引一旦落盘之后，就不在支持增加Doc了（会有其他分区的内存态索引去负责新增）
//Note:
// 为了保证和正排以及其他字段的一致性，所以无论成功与否，nextDocId都会自增！！
func (rIdx *InvertedIndex) AddDocument(docId uint32, content string) error {
	var nodes map[string]basic.DocNode
	var err error
	//校验必须是内存态和DocId
	if !rIdx.inMemory || docId != rIdx.nextDocId {
		err = errors.New(fmt.Sprintf("Inverted-->AddDocument. Wrong DocId or MemStatus. DocId:%v, NextId:%v, Mem:%v",
			docId, rIdx.nextDocId, rIdx.inMemory))
		//return errors.New("Wrong DocId or MemStatus ")
		goto FAIL
	}

	//新增内容为空的内容，则直接成功返回
	if len(content) <= 0 {
		goto SUCC
	}

	//根据type进行分词
	switch rIdx.indexType {
	case IDX_TYPE_STR_WHOLE: 			            //全词匹配模式
		nodes = SplitWholeWords(docId, content)
	case IDX_TYPE_STR_LIST: 					    //分号切割模式
		nodes = SplitSemicolonWords(docId, content)
	case IDX_TYPE_STR_WORD: 				        //单个词模式
		nodes = SplitRuneWords(docId, content)
	case IDX_TYPE_STR_SPLITER:   			        //分词模式
		nodes = SplitTrueWords(docId, content)
	case IDX_TYPE_GOD:     						    //上帝模式--按分词处理
		nodes = SplitTrueWords(docId, content)
	default:
		err = errors.New(fmt.Sprintf("Inverted-->AddDocument: Type %v can't add invertIndex", rIdx.indexType))
		//return errors.New("Unsupport indexType")
		goto FAIL
	}

	//分词结果填入内存索引
	for term, node := range nodes {
		if _, exist := rIdx.termMap[term]; !exist {
			rIdx.termMap[term] = []basic.DocNode{}
		}
		rIdx.termMap[term] = append(rIdx.termMap[term], node)
	}

SUCC:
	rIdx.nextDocId++ //docId自增
	log.Debugf("InvertAddDoc--> DocId: %v, Field: %v, Content: %v, NodesNum: %v",
		docId, rIdx.fieldName, content, len(nodes))
	return nil

FAIL:
	rIdx.nextDocId++ //docId自增
	log.Warnf("InvertAddDoc Failed!. DocId: %v, Field: %v, Content: %v, Error: %v",
		docId, rIdx.fieldName, content, err.Error())
	return err
}

//给定一个查询词query，找出doc的list
//Note:
// 2019-5-6 发现一个隐藏的小坑，直接返回slice的函数，最好在底层copy一下
// 否则如果上层对slice进行了更改，那么底层就彻底乱了，而且不好排查。。。
func (rIdx *InvertedIndex) QueryTerm(term string) ([]basic.DocNode, bool) {
	if rIdx.inMemory {
		docNodes, ok := rIdx.termMap[term]
		if ok {
			//fmt.Println("Ivt Mem QueryTerm:", term, docNodes)
			retNodes := make([]basic.DocNode, len(docNodes))
			copy(retNodes, docNodes)
			return retNodes, true
		}
	} else if (rIdx.ivtMmap != nil && rIdx.btdb != nil) {
		offset, ok := rIdx.btdb.GetInt(rIdx.fieldName, term)
		if !ok {
			return nil, false
		}

		count := rIdx.ivtMmap.ReadUInt64(uint64(offset))
		docNodes := readDocNodes(uint64(offset) + DOCNODE_BYTE_CNT, count, rIdx.ivtMmap)
		//fmt.Println("Ivt Disk QueryTerm:", term, count, helper.JsonEncode(docNodes))
		retNodes := make([]basic.DocNode, len(docNodes))
		copy(retNodes, docNodes)
		return retNodes, true
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
// Note: 只销毁内存部分，mmap和btdb因为是公用，需要在高层统一销毁
func (rIdx *InvertedIndex) DoClose() {
	rIdx.termMap = nil
	return
}

//设置倒排文件mmap
func (rIdx *InvertedIndex) SetIvtMmap(mmap *mmap.Mmap) {
	if rIdx.ivtMmap != nil && rIdx.ivtMmap != mmap {
		rIdx.ivtMmap.Unmap()
	}
	rIdx.ivtMmap = mmap
}

//设置btree
func (rIdx *InvertedIndex) SetBtree(tree btree.Btree) {
	rIdx.btdb = tree
}

//设置btree
func (rIdx *InvertedIndex) SetInMemory(in bool) {
	rIdx.inMemory = in
}

//设置btree
func (rIdx *InvertedIndex) GetBtree() btree.Btree {
	return rIdx.btdb
}

//btree操作,
func (rIdx *InvertedIndex) GetFristKV() (string, uint32, bool) {
	if rIdx.btdb == nil {
		log.Err("Btree is nil")
		return "", 0, false
	}
	return rIdx.btdb.GetFristKV(rIdx.fieldName)
}

//btree操作
func (rIdx *InvertedIndex) GetNextKV(key string) (string, uint32, bool) {
	if rIdx.btdb == nil {
		log.Err("Btree is nil")
		return "", 0, false
	}

	return rIdx.btdb.GetNextKV(rIdx.fieldName, key)
}

//btree操作
func (rIdx *InvertedIndex) GetNextId() uint32 {
	return rIdx.nextDocId
}

//btree操作
func (rIdx *InvertedIndex) Walk() error {
	if rIdx.btdb != nil {
		//return rIdx.btdb.Display(rIdx.fieldName)
		term, _, ok := rIdx.GetFristKV()
		if !ok {
			return nil
		}

		for {
			tmpNodes, ok := rIdx.QueryTerm(term)
			if !ok {
				return nil
			}
			fmt.Println("磁盘中的ivt:", term, "----", tmpNodes)
			term, _, ok = rIdx.btdb.GetNextKV(rIdx.fieldName, term)
			if !ok {
				return nil
			}
		}
	} else if rIdx.termMap != nil {
		for k,v := range rIdx.termMap {
			fmt.Println("内存中的ivt:", k, "----", v)
		}
		return nil
	} else {
		return errors.New("IvtIndex Nil")
	}
}

//持久化倒排索引
//落地 termMap落地到倒排文件; term进入B+tree
//倒排文件格式:
//  顺序的数据块, 每块数据长这个个样子 [{nodeCnt(8Byte)|node1|node2|....}, {}, {}]
//B+树:
//  key是term, val则是term在倒排文件中的offset
//
//Note:
// 因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)，所以这里直接用分区的路径文件名做前缀
// 这里一个设计的问题，函数并未自动加载回mmap，但是设置了btdb
func (rIdx *InvertedIndex) Persist(partitionPathName string, btdb btree.Btree) error {

	//打开倒排文件, 获取文件大小作为初始偏移
	idxFileName := partitionPathName + basic.IDX_FILENAME_SUFFIX_INVERT
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
	if !btdb.HasTree(rIdx.fieldName) {
		btdb.AddTree(rIdx.fieldName)
	}
	for term, docNodeList := range rIdx.termMap {
		//fmt.Println(rIdx.fieldName, "落盘：", term, helper.JsonEncode(docNodeList))
		//先写入长度, 占8个字节
		nodeCnt := len(docNodeList)
		lenBuffer := make([]byte, DOCNODE_BYTE_CNT)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(nodeCnt))
		n, err := idxFd.Write(lenBuffer)
		if err != nil || n != DOCNODE_BYTE_CNT {
			log.Errf(fmt.Sprintf("Write err:%v, len:%v, len:%v", err, n, DOCNODE_BYTE_CNT))
			return errors.New("Write Error")
		}

		//再写入node list
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, docNodeList)
		if err != nil {
			log.Errf("Persist :: Error %v", err)
			return err
		}
		writeLength, err := idxFd.Write(buffer.Bytes())
		if err != nil || writeLength != nodeCnt * basic.DOC_NODE_SIZE{
			log.Errf("Write err, %v, %v, %v",err, writeLength, nodeCnt * basic.DOC_NODE_SIZE)
			return errors.New("Write Error")
		}

		//B+树录入
		err = btdb.Set(rIdx.fieldName, term, fmt.Sprintf("%v", offset))
		if err != nil {
			log.Errf("Persist :: Error %v", err)
			//造成不一致了哦，或者说写脏了一块数据，但是以后也不会被引用到，因为btree里面没落盘
			return err
		}
		offset = offset + DOCNODE_BYTE_CNT + writeLength
	}

	//绑定btdb，内存态 => 磁盘态
	rIdx.btdb = btdb
	rIdx.termMap = nil
	rIdx.inMemory = false
	//rIdx.nextDocId = 0

	log.Debugf("Persist :: Writing to File : [%v%v] ", partitionPathName, basic.IDX_FILENAME_SUFFIX_INVERT)
	return nil
}

//临时结构，辅助Merge
type tmpMerge struct {
	over   bool
	rIndex *InvertedIndex
	term   string
	nodes []basic.DocNode
}

//多路归并, 将多个反向索引进行合并成为一个大的反向索引， 将这一切作用在接收者上面
//Note:
// 一个设计的问题，因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
// 所以mmap并不会加载回来，但是btdb和inMemory参数被加载回来了
//
// 倒排索引只能合并磁盘态的，因为这里面涉及到term的顺序，要依赖B+树，所以内存态的索引不支持
func (rIdx *InvertedIndex)MergePersistIvtIndex(rIndexes []*InvertedIndex, partitionPathName string, btdb btree.Btree) error {
	//一些校验
	if rIndexes == nil || len(rIndexes) == 0 {
		return errors.New("Nil []*InvertedIndex")
	}
	indexType := rIdx.indexType
	fieldName := rIdx.fieldName
	for _, v := range rIndexes {
		if v.indexType != indexType || v.fieldName != fieldName || v.inMemory {
			return errors.New("Indexes not consistent or must be disk status")
		}
	}
	l := len(rIndexes)
	for i:=0; i<(l-1); i++ { //倒排索引合并顺序并不是必须的， 这里主要是为了获取最大的nextDocId
		if rIndexes[i].nextDocId > rIndexes[i+1].nextDocId {
			return errors.New("Indexes order wrong")
		}
	}

	//打开文件，获取长度，作为offset
	//因为同一个分区的所有字段，都公用同一套倒排文件，所以merge某个字段的index的时候，文件可能已经存在，需要追加打开
	idxFileName := partitionPathName + basic.IDX_FILENAME_SUFFIX_INVERT
	fd, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644) //追加打开
	if err != nil {
		return err
	}
	defer fd.Close()
	fi, _ := fd.Stat()
	offset := int(fi.Size())

	//数据准备，开始多路归并
	tmpIvts := make([]tmpMerge, 0)
	for _, ivt := range rIndexes {
		if ivt.fake {
			continue  //对于高层的占位假索引, 直接忽略即可
		}
		if ivt.btdb == nil {
			panic("Error")
		}
		term, _, ok := ivt.GetFristKV()
		if !ok {
			continue
		}
		nodes, _ := ivt.QueryTerm(term)
		tmpIvts = append(tmpIvts, tmpMerge{
			over: false,
			rIndex: ivt,
			term:  term,
			nodes: nodes,
		})
	}
	//补齐树
	if !btdb.HasTree(fieldName) {
		btdb.AddTree(fieldName)
	}

	//开始进行归并
	for {
		minTerm := ""
		for _, v := range tmpIvts { //随便找一个还未完的索引的首term
			if !v.over {
				minTerm = v.term
				break
			}
		}

		for _, v := range tmpIvts { //找到所有还未完结的索引中最小那个首term
			if !v.over && minTerm > v.term {
				minTerm = v.term
			}
		}

		minIds := make([]int, 0) //可能多个索引都包含这个最小首term，统统取出来
		for i, ivt := range tmpIvts {
			if !ivt.over && minTerm == ivt.term {
				minIds = append(minIds, i)
			}
		}

		value := make([]basic.DocNode, 0) //合并这这些个首term, 他们各自的后继者需要顶上来
		for _, i := range minIds {
			value = append(value, tmpIvts[i].nodes...)

			//找到后继者，顶上去
			nextTerm, _, ok := tmpIvts[i].rIndex.GetNextKV(tmpIvts[i].term)
			if !ok {
				//如果没有后继者了，那么该索引位置标记为无效
				tmpIvts[i].over = true
				continue
			}
			tmpIvts[i].term = nextTerm
			tmpIvts[i].nodes, ok = tmpIvts[i].rIndex.QueryTerm(nextTerm)
			if !ok {
				panic("Index wrong!")
			}
		}

		//写倒排文件 & 写B+树
		nodeCnt := len(value)
		lenBuffer := make([]byte, DOCNODE_BYTE_CNT)
		binary.LittleEndian.PutUint64(lenBuffer, uint64(nodeCnt))
		n, err := fd.Write(lenBuffer)
		if err != nil || n != DOCNODE_BYTE_CNT {
			log.Errf(fmt.Sprintf("Write err:%v, len:%v, len:%v", err, n, DOCNODE_BYTE_CNT))
			return errors.New("Write Error")
		}
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, value)
		if err != nil {
			log.Err("Invert--> Merge :: Error %v", err)
			return err
		}
		writeLength, err := fd.Write(buffer.Bytes())
		if err != nil || writeLength != nodeCnt * basic.DOC_NODE_SIZE{
			log.Errf("Write err, %v, %v, %v",err, writeLength, nodeCnt * basic.DOC_NODE_SIZE)
			return errors.New("Write Error")
		}

		err = btdb.Set(fieldName, minTerm, fmt.Sprintf("%v", offset))
		if err != nil {
			log.Errf("Invert--> Merge :: Error:%v, fieldName: %v, term: %v, len(term): %v", err, fieldName, minTerm, len(minTerm))
			return err
		}
		offset = offset + DOCNODE_BYTE_CNT + writeLength

		//如果所有的索引都合并完毕， 则退出
		quit := true
		for _, ivt := range tmpIvts {
			if !ivt.over {
				quit = false
				break
			}
		}
		if quit {
			break
		}
	}

	//绑定btdb，内存态 => 磁盘态
	rIdx.btdb = btdb
	rIdx.termMap = nil
	rIdx.inMemory = false
	rIdx.nextDocId = rIndexes[len(rIndexes)-1].nextDocId

	return nil
}