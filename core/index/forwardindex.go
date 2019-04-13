package index
/*
 * 正排索引类
 * 用一个数组（slice），来实现正排索引，数组的Id和docId正好一一对应
 *
 * 索引有两种状态：
 * 一种是内存态，此时索引存在于memoryNum或者memoryStr中，另一种是落盘态，此时分区分索引类型
 *   数字型：
 *          baseMmap当做一个[]int64数组来引用（每个元素占8个Byte）
 *   字符型：
 *          需要baseMmap和extMmap配合使用
 *          baseMmap当做一个[]int64数组来引用（每个元素占8个Byte），表示其在extMmap中的offset
 *          extMmap实际存string的内容，格式如： [len|content][len|content][len|content]...
 *
 * Note：
 * 正排索引巧妙应用了数组的下表，作为docId, 数组元素值作为实际的值，所以这也可看做一种map
 * 一个特殊的点是，每个分区只是一部分的文档，所以正排索引的startDocId和nextDocId很重要，这两个变量作为本索引内部的起始
 * 比如一个DocId需要获取：
 *    在索引内存态通过memoryNum[docId-startId]来引用
 *    若磁盘态则通过baseMmap.Get(fwdOffset + (docId-startId)*DATA_BYTE_CNT)
 **/
import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/utils/mmap"
	"strings"
	"github.com/hq-cml/spider-engine/utils/helper"
)

//profile 正排索引，detail也是保存在这里
type ForwardIndex struct {
	startDocId uint32 					//初始分区：从0开始；非初始分区：从本分区的第一篇DocId开始
	nextDocId  uint32 					//下一个加入本索引的docId（所以本索引最大docId是nextDocId-1）
	inMemory   bool   					//本索引是内存态还是磁盘态（不会同时并存）
	indexType  uint8  					//本索引的类型
	fwdOffset  uint64 					//本索引的数据，在base文件中的起始偏移
	docCnt     uint32 					//本索引文档数量
	fake       bool                     //假，用于占位，高层的分区缺少某个字段时候，用此占位
	memoryNum  []int64    `json:"-"`    //内存态本正排索引(数字)
	memoryStr  []string   `json:"-"`    //内存态本正排索引(字符串)
	baseMmap   *mmap.Mmap `json:"-"`    //底层mmap文件, 用于存储磁盘态正排索引
	extMmap    *mmap.Mmap `json:"-"`    //用于补充性的mmap, 主要存磁盘态正排索引string的实际内容
}

const DATA_BYTE_CNT = 8

//假索引，高层占位用
func NewEmptyFakeForwardIndex(indexType uint8, start uint32, docCnt uint32) *ForwardIndex {
	return &ForwardIndex{
		docCnt:     docCnt,
		indexType:  indexType,
		startDocId: start,
		nextDocId:  start,
		fake:       true,   //here is the point!
	}
}

//新建空正排索引
func NewEmptyForwardIndex(indexType uint8, start uint32) *ForwardIndex {
	return &ForwardIndex{
		fake:       false,
		fwdOffset:  0,
		inMemory:   true,
		indexType:  indexType,
		startDocId: start,
		nextDocId:  start,
		memoryNum:  make([]int64, 0),
		memoryStr:  make([]string, 0),
	}
}

//从磁盘加载正排索引
//这里并未真的从磁盘加载，mmap都是从外部直接传入的，因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)
//如果mmap自己创建的话，会造成多个mmap实例对应同一个磁盘文件，这样会造成不确定性(mmmap头部有隐藏信息字段)，也不易于维护
func LoadForwardIndex(indexType uint8, baseMmap, extMmap *mmap.Mmap,
		offset uint64, docLen, start, next uint32) *ForwardIndex {
	return &ForwardIndex{
		fake:      false,
		docCnt:    docLen,
		fwdOffset: offset,
		inMemory:  false,
		indexType: indexType,
		baseMmap:  baseMmap,
		extMmap:   extMmap,
		startDocId: start,
		nextDocId:  next,
	}
}

func (fwdIdx *ForwardIndex)String() string {
	return fmt.Sprintf("FwdIndex-- Start:%v, Next:%v, InMem:%v, Type:%v, Offset:%v, Cnt:%v, Fake:%v",
		fwdIdx.startDocId,
		fwdIdx.nextDocId,
		fwdIdx.inMemory,
		fwdIdx.indexType,
		fwdIdx.fwdOffset,
		fwdIdx.docCnt,
		fwdIdx.fake,
	)
}
//增加一个doc文档
//Note：
// 增加文档，只会出现在最新的一个分区（即都是内存态的），所以只会操作内存态的
// 也就是，一个索引一旦落盘之后，就不在支持增加Doc了（会有其他分区的内存态索引去负责新增）
func (fwdIdx *ForwardIndex) AddDocument(docId uint32, content interface{}) error {

	if docId != fwdIdx.nextDocId || fwdIdx.inMemory == false {
		return errors.New("ForwardIndex --> AddDocument :: Wrong DocId Number")
	}
	log.Debugf("ForwardIndex AddDocument --> DocId: %v ,Content: %v", docId, content)

	vtype := reflect.TypeOf(content)
	var value int64 = 0xFFFFFFFF
	var ok error
	switch vtype.Name() {
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		value, ok = strconv.ParseInt(fmt.Sprintf("%v", content), 0, 0)
		if ok != nil {
			value = 0xFFFFFFFF
		}
		fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
	case "float32":
		v, _ := content.(float32) 								//TODO why *100??
		value = int64(v * 100)
		fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
	case "float64":
		v, _ := content.(float64)
		value = int64(v * 100)
		fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
	case "string":
		if fwdIdx.indexType == IDX_TYPE_NUMBER {
			value, err := strconv.ParseInt(fmt.Sprintf("%v", content), 0, 0)
			if err != nil {
				value = 0xFFFFFFFF
			}
			fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
		} else if fwdIdx.indexType == IDX_TYPE_DATE {
			value, err := helper.String2Timestamp(fmt.Sprintf("%v", content))
			if err != nil {
				value = 0xFFFFFFFF
			}
			fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
		} else {
			fwdIdx.memoryStr = append(fwdIdx.memoryStr, fmt.Sprintf("%v", content))
		}
	default:
		fwdIdx.memoryStr = append(fwdIdx.memoryStr, fmt.Sprintf("%v", content))
	}
	fwdIdx.nextDocId++
	fwdIdx.docCnt ++
	return nil
}

//更新文档
//Note:
//只支持数字（包括时间）型的索引的更改，string类型的通过外层的bitmap来实现更改
func (fwdIdx *ForwardIndex) UpdateDocument(docId uint32, content interface{}) error {
	//范围校验
	if docId < fwdIdx.startDocId || docId >= fwdIdx.nextDocId {
		log.Errf("ForwardIndex --> UpdateDocument :: Wrong docid %v", docId)
		return errors.New("Wrong docId")
	}

	//只支持数字（包括时间）型的索引的更改，string类型的通过外层的bitmap来实现更改
	if fwdIdx.indexType != IDX_TYPE_NUMBER && fwdIdx.indexType != IDX_TYPE_DATE {
		return nil
	}

	vtype := reflect.TypeOf(content)
	var value int64 = 0xFFFFFFFF
	switch vtype.Name() {
	case "string", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		var err error
		if fwdIdx.indexType == IDX_TYPE_DATE {
			value, err = helper.String2Timestamp(fmt.Sprintf("%v", content))
			if err != nil {
				value = 0xFFFFFFFF
			}
		} else {
			value, err = strconv.ParseInt(fmt.Sprintf("%v", content), 0, 0)
			if err != nil {
				value = 0xFFFFFFFF
			}
		}
	case "float32":
		v, _ := content.(float32)
		value = int64(v * 100)    				//TODO why *100??
	case "float64":
		v, _ := content.(float64)
		value = int64(v * 100)
	default:
		value = 0xFFFFFFFF
	}

	//内存态直接更新map，否则更新底层mmap
	if fwdIdx.inMemory == true {
		fwdIdx.memoryNum[docId - fwdIdx.startDocId] = value    //下标:docId-fwdIdx.startDocId作为索引内部的引用值
	} else {
		offset := fwdIdx.fwdOffset + uint64(docId - fwdIdx.startDocId) * DATA_BYTE_CNT
		fwdIdx.baseMmap.WriteInt64(offset, value)
	}
	return nil
}

//以字符串形式获取
//参数Pos: 通常索引内引用元素，比如dockId - startId
func (fwdIdx *ForwardIndex) GetString(pos uint32) (string, bool) {
	if fwdIdx.fake {  //TODO why??
		return "", true
	}
	//先尝试从内存获取
	if fwdIdx.inMemory && (pos < uint32(len(fwdIdx.memoryNum)) || pos < uint32(len(fwdIdx.memoryStr))) {
		if fwdIdx.indexType == IDX_TYPE_NUMBER {
			return fmt.Sprintf("%v", fwdIdx.memoryNum[pos]), true
		} else if fwdIdx.indexType == IDX_TYPE_DATE {
			return helper.Timestamp2String(fwdIdx.memoryNum[pos]), true
		} else {
			return fwdIdx.memoryStr[pos], true
		}
	}

	//再从disk获取(其实是利用mmap, 速度会有所提升
	if fwdIdx.baseMmap == nil {
		return "", false
	}

	//数字或者日期类型, 直接从索引文件读取
	realOffset := fwdIdx.fwdOffset + uint64(pos) * DATA_BYTE_CNT
	if (int(realOffset) >= len(fwdIdx.baseMmap.DataBytes)) {
		return "", false
	}

	if fwdIdx.indexType == IDX_TYPE_NUMBER {
		return fmt.Sprintf("%v", fwdIdx.baseMmap.ReadInt64(realOffset)), true
	} else if fwdIdx.indexType == IDX_TYPE_DATE {
		return helper.Timestamp2String(fwdIdx.baseMmap.ReadInt64(realOffset)), true
	} else {
		//string类型则间接从文件获取
		if fwdIdx.extMmap == nil {
			return "", false
		}
		extOffset := fwdIdx.baseMmap.ReadUInt64(realOffset)
		if (int(extOffset) >= len(fwdIdx.extMmap.DataBytes)) {
			return "", false
		}
		strLen := fwdIdx.extMmap.ReadUInt64(extOffset)
		return fwdIdx.extMmap.ReadString(extOffset + DATA_BYTE_CNT, strLen), true
	}
}

//获取值 (以数值形式)
//参数Pos: 通常索引内引用元素，比如dockId - startId
//TODO 不支持从字符型中读取数字??
func (fwdIdx *ForwardIndex) GetInt(pos uint32) (int64, bool) {

	if fwdIdx.fake {  //TODO why??
		return 0xFFFFFFFF, true
	}

	//从内存读取
	if fwdIdx.inMemory {
		if (fwdIdx.indexType == IDX_TYPE_NUMBER || fwdIdx.indexType == IDX_TYPE_DATE) &&
			pos < uint32(len(fwdIdx.memoryNum)) {
			return fwdIdx.memoryNum[pos], true
		}
		return 0xFFFFFFFF, false
	}

	//从disk读取
	if fwdIdx.baseMmap == nil {
		return 0xFFFFFFFF, false
	}

	realOffset := fwdIdx.fwdOffset + uint64(pos) * DATA_BYTE_CNT
	if fwdIdx.indexType == IDX_TYPE_NUMBER || fwdIdx.indexType == IDX_TYPE_DATE {
		if (int(realOffset) >= len(fwdIdx.baseMmap.DataBytes)) {
			return 0xFFFFFFFF, false
		}
		return fwdIdx.baseMmap.ReadInt64(realOffset), true
	}

	return 0xFFFFFFFF, false
}

//销毁
//TODO 过于简单？？
func (fwdIdx *ForwardIndex) Destroy() error {
	fwdIdx.memoryNum = nil
	fwdIdx.memoryStr = nil
	return nil
}

func (fwdIdx *ForwardIndex) SetBaseMmap(mmap *mmap.Mmap) {
	fwdIdx.baseMmap = mmap
}

func (fwdIdx *ForwardIndex) SetExtMmap(mmap *mmap.Mmap) {
	fwdIdx.extMmap = mmap
}

func (fwdIdx *ForwardIndex) SetInMemory(in bool) {
	fwdIdx.inMemory = in
}

func (fwdIdx *ForwardIndex) SetFwdOffset(i uint64) {
	fwdIdx.fwdOffset = i
}

func (fwdIdx *ForwardIndex) SetDocCnt(i uint32) {
	fwdIdx.docCnt = i
}

//落地正排索引
//返回值: 本索引落地完成之后，在fwd文件中的偏移量和本索引一共存了Doc数量
//Note:
// 因为同一个分区的各个字段的正、倒排公用同一套文件(btdb, ivt, fwd, ext)，所以这里直接用分区的路径文件名做前缀
// 这里一个设计的问题，函数并未自动加载回mmap，但是设置了fwdOffset和docCnt
func (fwdIdx *ForwardIndex) Persist(partitionPathName string) (uint64, uint32, error) {

	//打开正排文件
	pflFileName := partitionPathName + basic.IDX_FILENAME_SUFFIX_FWD
	fwdFd, err := os.OpenFile(pflFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644) //append
	if err != nil {
		return 0, 0, err
	}
	defer fwdFd.Close()
	fi, err := fwdFd.Stat()
	if err != nil {
		return 0, 0, err
	}
	offset := fi.Size()

	var cnt int
	if fwdIdx.indexType == IDX_TYPE_NUMBER || fwdIdx.indexType == IDX_TYPE_DATE {
		buffer := make([]byte, DATA_BYTE_CNT)
		for _, num := range fwdIdx.memoryNum {
			binary.LittleEndian.PutUint64(buffer, uint64(num))
			n, err := fwdFd.Write(buffer)
			if err != nil || n != DATA_BYTE_CNT {
				log.Errf(fmt.Sprint("Write err:%v, len:%v, len:%v", err, n, DATA_BYTE_CNT))
				return 0, 0, errors.New("Write Error")
			}
		}

		cnt = len(fwdIdx.memoryNum)
	} else {
		//字符型,单开一个文件存string内容
		//打开dtl文件
		extFileName := partitionPathName + basic.IDX_FILENAME_SUFFIX_FWDEXT
		extFd, err := os.OpenFile(extFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return 0, 0, err
		}
		defer extFd.Close()
		fi, _ = extFd.Stat()
		extOffset := fi.Size()

		buffer := make([]byte, DATA_BYTE_CNT)
		for _, str := range fwdIdx.memoryStr {
			//ext写入内容
			strLen := len(str)
			binary.LittleEndian.PutUint64(buffer, uint64(strLen))
			n, err := extFd.Write(buffer)
			if err != nil || n != DATA_BYTE_CNT {
				log.Errf(fmt.Sprint("Write err:%v, len:%v, len:%v", err, n, DATA_BYTE_CNT))
				return 0, 0, errors.New("Write Error")
			}
			n, err = extFd.WriteString(str)
			if err != nil || n != strLen {
				log.Errf("StringForward --> Persist :: Write Error %v", err)
			}
			//fwd写入offset
			binary.LittleEndian.PutUint64(buffer, uint64(extOffset))
			n, err = fwdFd.Write(buffer)
			if err != nil || n != DATA_BYTE_CNT {
				log.Errf(fmt.Sprint("Write err:%v, len:%v, len:%v", err, n, DATA_BYTE_CNT))
				return 0, 0, errors.New("Write Error")
			}
			extOffset = extOffset + DATA_BYTE_CNT + int64(strLen)

		}
		cnt = len(fwdIdx.memoryStr)
	}

	//当前偏移量, 即文件最后位置
	fwdIdx.fwdOffset = uint64(offset) //TODO 此处到底要不要设置
	fwdIdx.docCnt = uint32(cnt)

	//内存态 => 磁盘态
	fwdIdx.inMemory = false
	fwdIdx.memoryStr = nil
	fwdIdx.memoryNum = nil
	return uint64(offset), uint32(cnt), nil
}

//归并索引
//正排索引的归并, 不存在倒排那种归并排序的问题, 因为每个索引内部按offset有序, 每个索引之间又是整体有序
func MergePersistFwdIndex(idxList []*ForwardIndex, partitionPathName string) (uint64, uint32, uint32, error) {
	//一些校验, index的类型，顺序必须完整正确
	if idxList == nil || len(idxList) == 0 {
		return 0, 0, 0, errors.New("Nil []*ForwardIndex")
	}
	indexType := idxList[0].indexType
	l := len(idxList)
	for i:=0; i<(l-1); i++ {
		if idxList[i].indexType != idxList[i+1].indexType {
			return 0, 0, 0, errors.New("Indexes not consistent")
		}

		if idxList[i].nextDocId != idxList[i+1].startDocId {
			return 0, 0, 0, errors.New("Indexes order wrong")
		}
	}
	nextId := idxList[l-1].nextDocId;

	//打开正排文件
	pflFileName := fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWD, partitionPathName)
	var fwdFd *os.File
	var err error
	fwdFd, err = os.OpenFile(pflFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return 0, 0, 0, err
	}
	defer fwdFd.Close()
	fi, _ := fwdFd.Stat()
	offset := fi.Size()
	//fwdIdx.fwdOffset = uint64(offset)

	cnt := 0
	if indexType == IDX_TYPE_NUMBER || indexType == IDX_TYPE_DATE {
		buffer := make([]byte, DATA_BYTE_CNT)
		for _, idx := range idxList {
			for i := uint32(0); i < uint32(idx.docCnt); i++ {
				val, _ := idx.GetInt(i)
				binary.LittleEndian.PutUint64(buffer, uint64(val))
				n, err := fwdFd.Write(buffer)
				if err != nil || n != DATA_BYTE_CNT {
					log.Errf(fmt.Sprint("Write err:%v, len:%v, len:%v", err, n, DATA_BYTE_CNT))
					return 0, 0, 0, errors.New("Write Error")
				}
				if err != nil {
					log.Errf("MergePersistFwdIndex :: Write Error %v", err)
					return 0, 0, 0, err
				}
				//fwdIdx.nextDocId++
				cnt ++
			}
		}
		//cnt = int(fwdIdx.nextDocId - fwdIdx.startDocId)
	} else {
		//打开dtl文件
		extFileName := fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWDEXT, partitionPathName)
		extFd, err := os.OpenFile(extFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return 0, 0, 0, err
		}
		defer extFd.Close()
		fi, _ = extFd.Stat()
		extOffset := fi.Size()

		buffer := make([]byte, DATA_BYTE_CNT)
		for _, idx := range idxList {
			for i := uint32(0); i < uint32(idx.docCnt); i++ {
				strContent, _ := idx.GetString(i)
				strLen := len(strContent)
				binary.LittleEndian.PutUint64(buffer, uint64(strLen))
				n, err := extFd.Write(buffer)
				if err != nil || n != DATA_BYTE_CNT {
					log.Errf(fmt.Sprint("Write err:%v, len:%v, len:%v", err, n, DATA_BYTE_CNT))
					return 0, 0, 0, errors.New("Write Error")
				}
				n, err = extFd.WriteString(strContent)
				if err != nil || n != strLen {
					log.Errf("MergePersistFwdIndex :: Write Error %v", err)
					return 0, 0, 0, err
				}
				//存储offset
				binary.LittleEndian.PutUint64(buffer, uint64(extOffset))
				n, err = fwdFd.Write(buffer)
				if err != nil || n != DATA_BYTE_CNT {
					log.Errf(fmt.Sprint("Write err:%v, len:%v, len:%v", err, n, DATA_BYTE_CNT))
					return 0, 0, 0, errors.New("Write Error")
				}
				extOffset = extOffset + DATA_BYTE_CNT + int64(strLen)
				//fwdIdx.nextDocId++
				cnt++
			}
		}
		//cnt = int(fwdIdx.nextDocId - fwdIdx.startDocId)
	}
	//fwdIdx.inMemory = false
	//fwdIdx.memoryStr = nil
	//fwdIdx.memoryNum = nil
	return uint64(offset), uint32(cnt), nextId, nil
}

//从numbers判断pos指定的数,如果
// type:EQ 只要有一个==, 就算ok
// type:NEQ 必须全部都是!=, 就算ok
func (fwdIdx *ForwardIndex) FilterNums(pos uint32, filterType uint8, numbers []int64) bool {
	var value int64
	if fwdIdx.fake {   //TODO ??
		return false
	}

	//仅支持数值型
	if fwdIdx.indexType != IDX_TYPE_NUMBER {
		return false
	}

	if fwdIdx.inMemory {
		value = fwdIdx.memoryNum[pos]
	} else {
		if fwdIdx.baseMmap == nil {
			return false
		}

		offset := fwdIdx.fwdOffset + uint64(pos) * DATA_BYTE_CNT
		value = fwdIdx.baseMmap.ReadInt64(offset)
	}

	switch filterType {
	case basic.FILT_EQ:
		for _, num := range numbers {
			if (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == num) {
				return true
			}
		}
		return false
	case basic.FILT_NEQ:
		for _, num := range numbers {
			if (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == num) {
				return false
			}
		}
		return true

	default:
		return false
	}
}

//过滤
func (fwdIdx *ForwardIndex) Filter(pos uint32, filterRype uint8, start, end int64, str string) bool {

	var value int64
	if fwdIdx.fake {
		return false
	}

	if fwdIdx.indexType == IDX_TYPE_NUMBER {
		if fwdIdx.inMemory {
			value = fwdIdx.memoryNum[pos]
		} else {
			if fwdIdx.baseMmap == nil {
				return false
			}
			offset := fwdIdx.fwdOffset + uint64(pos) * DATA_BYTE_CNT
			value = fwdIdx.baseMmap.ReadInt64(offset)
		}

		switch filterRype {
		case basic.FILT_EQ:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == start)
		case basic.FILT_OVER:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value >= start)
		case basic.FILT_LESS:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value <= start)
		case basic.FILT_RANGE:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value >= start && value <= end)
		case basic.FILT_NEQ:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value != start)
		default:
			return false
		}
	} else if fwdIdx.indexType == IDX_TYPE_STRING_SINGLE || fwdIdx.indexType == IDX_TYPE_STRING{
		vl := strings.Split(str, ",")
		switch filterRype {

		case basic.FILT_STR_PREFIX:
			if vstr, ok := fwdIdx.GetString(pos); ok {
				for _, v := range vl {
					if strings.HasPrefix(vstr, v) {
						return true
					}
				}
			}
			return false
		case basic.FILT_STR_SUFFIX:
			if vstr, ok := fwdIdx.GetString(pos); ok {
				for _, v := range vl {
					if strings.HasSuffix(vstr, v) {
						return true
					}
				}
			}
			return false
		case basic.FILT_STR_RANGE:
			if vstr, ok := fwdIdx.GetString(pos); ok {
				for _, v := range vl {
					if !strings.Contains(vstr, v) {
						return false
					}
				}
				return true
			}
			return false
		case basic.FILT_STR_ALL:
			if vstr, ok := fwdIdx.GetString(pos); ok {
				for _, v := range vl {
					if vstr == v {
						return true
					}
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}
