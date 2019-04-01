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
)

//profile 正排索引，detail也是保存在这里
type ForwardIndex struct {
	startDocId uint32                   //估计整体是从0开始 //TODO 疑问，如果是多个子索引，这里应该是多少？
	curDocId   uint32
	isMemory   bool                     //该索引是否在内存中
	fieldType  uint8
	fileOffset uint64                   //本索引的数据，在文件中的起始偏移
	docCnt     uint32                   //TODO 存疑, 为何不自增
	fake       bool
	memoryNum  []int64    `json:"-"`    //内存版本正排索引(数字)
	memoryStr  []string   `json:"-"`    //内存版本正排索引(字符串)
	baseMmap   *mmap.Mmap `json:"-"`    //底层mmap文件, 用于存储正排索引
	extMmap    *mmap.Mmap `json:"-"`    //用于补充性的mmap, 主要存string的实际内容 [len|content][][]...
}

const DATA_BYTE_CNT = 8

func NewEmptyFakeForwardIndex(fieldType uint8, start uint32, docCnt uint32) *ForwardIndex {
	return &ForwardIndex{
		docCnt:     docCnt,
		fileOffset: 0,
		isMemory:   true,
		fieldType:  fieldType,
		startDocId: start,
		curDocId:   start,
		memoryNum:  make([]int64, 0),
		memoryStr:  make([]string, 0),
		fake:       true,   //here is the point!
	}
}

//新建正排索引
func NewEmptyForwardIndex(fieldType uint8, start uint32) *ForwardIndex {
	return &ForwardIndex{
		fake:       false,
		fileOffset: 0,
		isMemory:   true,
		fieldType:  fieldType,
		startDocId: start,
		curDocId:   start,
		memoryNum:  make([]int64, 0),
		memoryStr:  make([]string, 0),
	}
}

//新建空的字符型正排索引
func LoadForwardIndex(fieldType uint8, baseMmap, extMmap *mmap.Mmap, offset uint64, docLen uint32, isMemory bool) *ForwardIndex {
	return &ForwardIndex{
		fake:       false,
		docCnt:     docLen,
		fileOffset: offset,
		isMemory:   isMemory,
		fieldType:  fieldType,
		baseMmap:   baseMmap,
		extMmap:    extMmap,
	}
}

//增加一个doc文档(仅增加在内存中)
//TODO 仅支持内存模式 ??
func (fwdIdx *ForwardIndex) AddDocument(docId uint32, content interface{}) error {

	if docId != fwdIdx.curDocId || fwdIdx.isMemory == false {
		return errors.New("profile --> AddDocument :: Wrong DocId Number")
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
		v, _ := content.(float32) //TODO 为毛*100??
		value = int64(v * 100)
		fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
	case "float64":
		v, _ := content.(float64)
		value = int64(v * 100)
		fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
	case "string":
		if fwdIdx.fieldType == IDX_TYPE_NUMBER {
			value, ok = strconv.ParseInt(fmt.Sprintf("%v", content), 0, 0)
			if ok != nil {
				value = 0xFFFFFFFF
			}
			fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
		} else if fwdIdx.fieldType == IDX_TYPE_DATE {
			value, _ = String2Timestamp(fmt.Sprintf("%v", content))
			fwdIdx.memoryNum = append(fwdIdx.memoryNum, value)
		} else {
			fwdIdx.memoryStr = append(fwdIdx.memoryStr, fmt.Sprintf("%v", content))
		}
	default:
		fwdIdx.memoryStr = append(fwdIdx.memoryStr, fmt.Sprintf("%v", content))
	}
	fwdIdx.curDocId++
	fwdIdx.docCnt ++ //TODO 原版么有,为什么能够运行
	return nil
}

//更新文档
//TODO 支持内存和文件两种模式
func (fwdIdx *ForwardIndex) UpdateDocument(docId uint32, content interface{}) error {
	//TODO 为什么add的时候没有这个验证
	//TODO 貌似没有string类型的，猜测是因为extMmap不好放置，因为string类型索引的特殊性

	if fwdIdx.fieldType != IDX_TYPE_NUMBER && fwdIdx.fieldType != IDX_TYPE_DATE {
		return errors.New("not support")
	}

	vtype := reflect.TypeOf(content)
	var value int64 = 0xFFFFFFFF
	switch vtype.Name() {
	case "string", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
		var ok error
		if fwdIdx.fieldType == IDX_TYPE_DATE {
			value, _ = String2Timestamp(fmt.Sprintf("%v", content))
		} else {
			value, ok = strconv.ParseInt(fmt.Sprintf("%v", content), 0, 0)
			if ok != nil {
				value = 0xFFFFFFFF
			}
		}
	case "float32":
		v, _ := content.(float32)
		value = int64(v * 100)    //TODO why *100??
	case "float64":
		v, _ := content.(float64)
		value = int64(v * 100)
	default:
		value = 0xFFFFFFFF
	}
	if fwdIdx.isMemory == true {
		fwdIdx.memoryNum[docId - fwdIdx.startDocId] = value
	} else {
		offset := fwdIdx.fileOffset + uint64(docId - fwdIdx.startDocId) * DATA_BYTE_CNT
		fwdIdx.baseMmap.WriteInt64(offset, value)
	}
	return nil
}

//落地正排索引
//返回值: ???
func (fwdIdx *ForwardIndex) Persist(fullsegmentname string) (uint64, uint32, error) {

	//打开正排文件
	pflFileName := fmt.Sprintf("%s" + basic.IDX_FILENAME_SUFFIX_FWD, fullsegmentname)
	idxFd, err := os.OpenFile(pflFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644) //append
	if err != nil {
		return 0, 0, err
	}
	defer idxFd.Close()
	fi, _ := idxFd.Stat()
	offset := fi.Size()
	fwdIdx.fileOffset = uint64(offset) //当前偏移量, 即文件最后位置

	var cnt int
	if fwdIdx.fieldType == IDX_TYPE_NUMBER || fwdIdx.fieldType == IDX_TYPE_DATE {
		buffer := make([]byte, DATA_BYTE_CNT)
		for _, info := range fwdIdx.memoryNum {
			binary.LittleEndian.PutUint64(buffer, uint64(info))
			_, err = idxFd.Write(buffer)
			if err != nil {
				log.Errf("NumberForward --> Persist :: Write Error %v", err)
			}
		}

		cnt = len(fwdIdx.memoryNum)
	} else {
		//字符型,单开一个文件存string内容
		//打开dtl文件
		dtlFileName := fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWDEXT, fullsegmentname)
		dtlFd, err := os.OpenFile(dtlFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return 0, 0, err
		}
		defer dtlFd.Close()
		fi, _ = dtlFd.Stat()
		dtloffset := fi.Size()

		buffer := make([]byte, DATA_BYTE_CNT)
		for _, str := range fwdIdx.memoryStr {
			strLen := len(str)
			binary.LittleEndian.PutUint64(buffer, uint64(strLen))
			_, err := dtlFd.Write(buffer)
			n, err := dtlFd.WriteString(str)
			if err != nil || n != strLen {
				log.Errf("StringForward --> Persist :: Write Error %v", err)
			}
			//存储offset
			binary.LittleEndian.PutUint64(buffer, uint64(dtloffset))
			_, err = idxFd.Write(buffer)
			if err != nil {
				log.Errf("StringForward --> Persist --> Serialization :: Write Error %v", err)
			}
			dtloffset = dtloffset + DATA_BYTE_CNT + int64(strLen)

		}
		cnt = len(fwdIdx.memoryStr)
	}

	//TODO ??
	fwdIdx.isMemory = false
	fwdIdx.memoryStr = nil
	fwdIdx.memoryNum = nil
	return uint64(offset), uint32(cnt), err

	//TODO ?? fwdIdx.pflOffset需要设置吗
}

//获取值 (以字符串形式)
func (fwdIdx *ForwardIndex) GetString(pos uint32) (string, bool) {
	if fwdIdx.fake {  //TODO 为毛??
		return "", true
	}
	//先尝试从内存获取
	if fwdIdx.isMemory && (pos < uint32(len(fwdIdx.memoryNum)) || pos < uint32(len(fwdIdx.memoryStr))) {
		if fwdIdx.fieldType == IDX_TYPE_NUMBER {
			return fmt.Sprintf("%v", fwdIdx.memoryNum[pos]), true
		} else if fwdIdx.fieldType == IDX_TYPE_DATE {
			return Timestamp2String(fwdIdx.memoryNum[pos])
		}
		return fwdIdx.memoryStr[pos], true

	}

	//再从disk获取(其实是利用mmap, 速度会有所提升)
	if fwdIdx.baseMmap == nil {
		return "", false
	}

	//数字或者日期类型, 直接从索引文件读取
	offset := fwdIdx.fileOffset + uint64(pos) * DATA_BYTE_CNT
	if fwdIdx.fieldType == IDX_TYPE_NUMBER {
		if (int(offset) >= len(fwdIdx.baseMmap.DataBytes)) {
			return "", false
		}
		return fmt.Sprintf("%v", fwdIdx.baseMmap.ReadInt64(offset)), true
	} else if fwdIdx.fieldType == IDX_TYPE_DATE {
		if (int(offset) >= len(fwdIdx.baseMmap.DataBytes)) {
			return "", false
		}
		return Timestamp2String(fwdIdx.baseMmap.ReadInt64(offset))
	}

	//string类型则间接从文件获取
	if fwdIdx.extMmap == nil || (int(offset) >= len(fwdIdx.baseMmap.DataBytes)) {
		return "", false
	}
	dtloffset := fwdIdx.baseMmap.ReadUInt64(offset)
	if (int(dtloffset) >= len(fwdIdx.extMmap.DataBytes)) {
		return "", false
	}
	strLen := fwdIdx.extMmap.ReadUInt64(dtloffset)
	return fwdIdx.extMmap.ReadString(dtloffset + DATA_BYTE_CNT, strLen), true

}

//获取值 (以数值形式)
//TODO 不支持从字符型中读取数字??
func (fwdIdx *ForwardIndex) GetInt(pos uint32) (int64, bool) {

	if fwdIdx.fake {//TODO 为毛??
		return 0xFFFFFFFF, true
	}

	//从内存读取
	if fwdIdx.isMemory {
		if (fwdIdx.fieldType == IDX_TYPE_NUMBER || fwdIdx.fieldType == IDX_TYPE_DATE) &&
			pos < uint32(len(fwdIdx.memoryNum)) {
			return fwdIdx.memoryNum[pos], true
		}
		return 0xFFFFFFFF, false
	}

	//从disk读取
	if fwdIdx.baseMmap == nil {
		return 0xFFFFFFFF, true //TODO false??
	}
	offset := fwdIdx.fileOffset + uint64(pos) * DATA_BYTE_CNT
	if fwdIdx.fieldType == IDX_TYPE_NUMBER || fwdIdx.fieldType == IDX_TYPE_DATE {
		if (int(offset) >= len(fwdIdx.baseMmap.DataBytes)) {
			return 0xFFFFFFFF, false
		}
		return fwdIdx.baseMmap.ReadInt64(offset), true
	}

	return 0xFFFFFFFF, false
}

//过滤从numbers切片中找出是否有=或!=于pos数
func (fwdIdx *ForwardIndex) FilterNums(pos uint32, filterType uint8, numbers []int64) bool {
	var value int64
	if fwdIdx.fake {
		//TODO ??
		return false
	}

	//仅支持数值型
	if fwdIdx.fieldType != IDX_TYPE_NUMBER {
		return false
	}

	if fwdIdx.isMemory {
		value = fwdIdx.memoryNum[pos]
	} else {
		if fwdIdx.baseMmap == nil {
			return false
		}

		offset := fwdIdx.fileOffset + uint64(pos) * DATA_BYTE_CNT
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
	case basic.FILT_NOT:
		for _, start := range numbers {
			if (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == start) {
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

	if fwdIdx.fieldType == IDX_TYPE_NUMBER {
		if fwdIdx.isMemory {
			value = fwdIdx.memoryNum[pos]
		} else {
			if fwdIdx.baseMmap == nil {
				return false
			}
			offset := fwdIdx.fileOffset + uint64(pos) * DATA_BYTE_CNT
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
		case basic.FILT_NOT:
			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value != start)
		default:
			return false
		}
	} else if fwdIdx.fieldType == IDX_TYPE_STRING_SINGLE || fwdIdx.fieldType == IDX_TYPE_STRING{
		vl := strings.Split(str, ",")  //TODO 为何是逗号 ??
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

//归并索引
//正排索引的归并, 不存在倒排那种归并排序的问题, 因为每个索引内部按offset有序, 每个索引之间又是整体有序
//TODO 这里面存在一个问题, 如果保证的多个index的顺序, 现在直接通过切片保证的, 如果切片顺序不对呢??
//TODO 按理说应该传进来的idxList不都是从docId=0开始, 应该能够自动拼上才对的
func (fwdIdx *ForwardIndex) MergeIndex(idxList []*ForwardIndex, fullSegmentName string) (uint64, uint32, error) {
	//打开正排文件
	pflFileName := fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWD, fullSegmentName)
	var fwdFd *os.File
	var err error
	fwdFd, err = os.OpenFile(pflFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer fwdFd.Close()
	fi, _ := fwdFd.Stat()
	offset := fi.Size()
	fwdIdx.fileOffset = uint64(offset)

	var cnt int
	if fwdIdx.fieldType == IDX_TYPE_NUMBER || fwdIdx.fieldType == IDX_TYPE_DATE {
		buffer := make([]byte, DATA_BYTE_CNT)
		for _, idx := range idxList {
			for i := uint32(0); i < uint32(idx.docCnt); i++ {
				//TODO 这里面存在一个问题, 如果保证的多个index的顺序, 现在直接通过切片保证的, 如果切片顺序不对呢??
				val, _ := idx.GetInt(i)
				binary.LittleEndian.PutUint64(buffer, uint64(val))
				_, err = fwdFd.Write(buffer)
				if err != nil {
					log.Errf("[ERROR] NumberProfile --> Serialization :: Write Error %v", err)
				}
				fwdIdx.curDocId++
			}
		}

		cnt = int(fwdIdx.curDocId - fwdIdx.startDocId)
	} else {
		//打开dtl文件
		dtlFileName := fmt.Sprintf("%v" + basic.IDX_FILENAME_SUFFIX_FWDEXT, fullSegmentName)
		dtlFd, err := os.OpenFile(dtlFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return 0, 0, err
		}
		defer dtlFd.Close()
		fi, _ = dtlFd.Stat()
		dtloffset := fi.Size()

		buffer := make([]byte, DATA_BYTE_CNT)
		for _, idx := range idxList {
			for i := uint32(0); i < uint32(idx.docCnt); i++ {
				strContent, _ := idx.GetString(i)
				strLen := len(strContent)
				binary.LittleEndian.PutUint64(buffer, uint64(strLen))
				_, err := dtlFd.Write(buffer)
				cnt, err := dtlFd.WriteString(strContent)
				if err != nil || cnt != strLen {
					log.Errf("[ERROR] StringProfile --> Serialization :: Write Error %v", err)
				}
				//存储offset
				//this.Logger.Info("[INFO] dtloffset %v,%v",dtloffset,infolen)
				binary.LittleEndian.PutUint64(buffer, uint64(dtloffset))
				_, err = fwdFd.Write(buffer)
				if err != nil {
					log.Errf("[ERROR] StringProfile --> Serialization :: Write Error %v", err)
				}
				dtloffset = dtloffset + DATA_BYTE_CNT + int64(strLen)
				fwdIdx.curDocId++
			}
		}
		cnt = int(fwdIdx.curDocId - fwdIdx.startDocId)
	}
	fwdIdx.isMemory = false
	fwdIdx.memoryStr = nil
	fwdIdx.memoryNum = nil
	return uint64(offset), uint32(cnt), nil
}
