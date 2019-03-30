package index
/*
 *  正排索引类
 *
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
	startDocId uint32                   //估计整体是从0开始
	curDocId   uint32
	isMomery   bool                     //是否在内存中
	fieldType  uint64
	fileOffset int64                    //在文件中的偏移
	docLen     uint64                   //TODO 存疑, 为何不自增
	fake       bool
	pflNumber  []int64       `json:"-"` //内存版本正排索引(数字)
	pflString  []string      `json:"-"` //内存版本正排索引(字符串)
	pflMmap    *mmap.Mmap    `json:"-"` //底层mmap文件, 用于存储正排索引
	extMmap    *mmap.Mmap    `json:"-"` //用于补充性的mmap, 主要存string的实际内容 [len|content][][]...
}

const DATA_LEN_BYTE int = 8

func newEmptyFakeProfile(fieldType uint64, start uint32, docLen uint64,) *ForwardIndex {
	return &ForwardIndex{
		docLen: docLen,
		fileOffset: 0,
		isMomery: true,
		fieldType: fieldType,
		startDocId: start,
		curDocId: start,
		pflNumber: make([]int64, 0),
		pflString: make([]string, 0),
		fake:true,   //here is the point!
	}
}

//新建正排索引
func newEmptyProfile(fieldType uint64, start uint32) *ForwardIndex {
	return &ForwardIndex{
		fake: false,
		fileOffset: 0,
		isMomery: true,
		fieldType: fieldType,
		startDocId: start,
		curDocId: start,
		pflNumber: make([]int64, 0),
		pflString: make([]string, 0),
	}
}

//新建空的字符型正排索引
func newProfileWithLocalFile(fieldType uint64, pflMmap, dtlMmap *mmap.Mmap, offset int64, docLen uint64, isMomery bool) *ForwardIndex {
	return &ForwardIndex{
		fake: false,
		docLen: docLen,
		fileOffset: offset,
		isMomery: isMomery,
		fieldType: fieldType,
		pflMmap: pflMmap,
		extMmap: dtlMmap,
	}
}

//增加一个doc文档(仅增加在内存中)
//TODO 仅支持内存模式 ??
func (fwdIdx *ForwardIndex) addDocument(docid uint32, content interface{}) error {

	if docid != fwdIdx.curDocId || fwdIdx.isMomery == false {
		return errors.New("profile --> AddDocument :: Wrong DocId Number")
	}
	log.Debugf("[TRACE] docid %v content %v", docid, content)

	vtype := reflect.TypeOf(content)
	var value int64 = 0xFFFFFFFF
	var ok error
	switch vtype.Name() {
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":

		value, ok = strconv.ParseInt(fmt.Sprintf("%v", content), 0, 0)
		if ok != nil {
			value = 0xFFFFFFFF
		}
		fwdIdx.pflNumber = append(fwdIdx.pflNumber, value)
	case "float32":
		v, _ := content.(float32) //TODO 为毛*100??
		value = int64(v * 100)
		fwdIdx.pflNumber = append(fwdIdx.pflNumber, value)
	case "float64":
		v, _ := content.(float64)
		value = int64(v * 100)
		fwdIdx.pflNumber = append(fwdIdx.pflNumber, value)
	case "string":
		if fwdIdx.fieldType == IDX_TYPE_NUMBER {
			value, ok = strconv.ParseInt(fmt.Sprintf("%v", content), 0, 0)
			if ok != nil {
				value = 0xFFFFFFFF
			}
			fwdIdx.pflNumber = append(fwdIdx.pflNumber, value)
		} else if fwdIdx.fieldType == IDX_TYPE_DATE {
			value, _ = String2Timestamp(fmt.Sprintf("%v", content))
			fwdIdx.pflNumber = append(fwdIdx.pflNumber, value)
		} else {
			fwdIdx.pflString = append(fwdIdx.pflString, fmt.Sprintf("%v", content))
		}
	default:
		fwdIdx.pflString = append(fwdIdx.pflString, fmt.Sprintf("%v", content))
	}
	fwdIdx.curDocId++
	fwdIdx.docLen ++ //TODO 原版么有,为什么能够运行
	return nil
}

//更新文档
//TODO 支持内存和文件两种模式
func (fwdIdx *ForwardIndex) updateDocument(docId uint32, content interface{}) error {
	//TODO 为什么add的时候没有这个验证
	//TODO 貌似没有string类型的

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
	if fwdIdx.isMomery == true {
		fwdIdx.pflNumber[docId - fwdIdx.startDocId] = value
	} else {
		offset := fwdIdx.fileOffset + int64(int64(docId - fwdIdx.startDocId) * int64(DATA_LEN_BYTE))
		fwdIdx.pflMmap.WriteInt64(offset, value)
	}
	return nil
}

//落地正排索引
//返回值: ???
func (fwdIdx *ForwardIndex) persist(fullsegmentname string) (int64, int, error) {

	//打开正排文件
	pflFileName := fmt.Sprintf("%s.pfl", fullsegmentname)
	idxFd, err := os.OpenFile(pflFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0644) //append
	if err != nil {
		return 0, 0, err
	}
	defer idxFd.Close()
	fi, _ := idxFd.Stat()
	offset := fi.Size()
	fwdIdx.fileOffset = offset //当前偏移量, 即文件最后位置

	var cnt int
	if fwdIdx.fieldType == IDX_TYPE_NUMBER || fwdIdx.fieldType == IDX_TYPE_DATE {
		buffer := make([]byte, DATA_LEN_BYTE)
		for _, info := range fwdIdx.pflNumber {
			binary.LittleEndian.PutUint64(buffer, uint64(info))
			_, err = idxFd.Write(buffer)
			if err != nil {
				log.Errf("NumberForward --> Persist :: Write Error %v", err)
			}
		}

		cnt = len(fwdIdx.pflNumber)
	} else {
		//字符型,单开一个文件存string内容
		//打开dtl文件
		dtlFileName := fmt.Sprintf("%v.dtl", fullsegmentname)
		dtlFd, err := os.OpenFile(dtlFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return 0, 0, err
		}
		defer dtlFd.Close()
		fi, _ = dtlFd.Stat()
		dtloffset := fi.Size()

		buffer := make([]byte, DATA_LEN_BYTE)
		for _, str := range fwdIdx.pflString {
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
			dtloffset = dtloffset + int64(DATA_LEN_BYTE) + int64(strLen)

		}
		cnt = len(fwdIdx.pflString)

	}

	//TODO ??
	fwdIdx.isMomery = false
	fwdIdx.pflString = nil
	fwdIdx.pflNumber = nil
	return offset, cnt, err

	//TODO ?? fwdIdx.pflOffset需要设置吗
}

//获取值 (以字符串形式)
func (fwdIdx *ForwardIndex) getString(pos uint32) (string, bool) {

	if fwdIdx.fake {  //TODO 为毛??
		return "", true
	}
	//先尝试从内存获取
	if fwdIdx.isMomery && (pos < uint32(len(fwdIdx.pflNumber)) || pos < uint32(len(fwdIdx.pflString))) {
		if fwdIdx.fieldType == IDX_TYPE_NUMBER {
			return fmt.Sprintf("%v", fwdIdx.pflNumber[pos]), true
		} else if fwdIdx.fieldType == IDX_TYPE_DATE {
			return Timestamp2String(fwdIdx.pflNumber[pos])
		}
		return fwdIdx.pflString[pos], true

	}

	//再从disk获取(其实是利用mmap, 速度会有所提升)
	if fwdIdx.pflMmap == nil {
		return "", false
	}

	//数字或者日期类型, 直接从索引文件读取
	offset := fwdIdx.fileOffset + int64(pos) * int64(DATA_LEN_BYTE)
	if fwdIdx.fieldType == IDX_TYPE_NUMBER {
		if (int(offset) >= len(fwdIdx.pflMmap.DataBytes)) {
			return "", false
		}
		return fmt.Sprintf("%v", fwdIdx.pflMmap.ReadInt64(offset)), true
	} else if fwdIdx.fieldType == IDX_TYPE_DATE {
		if (int(offset) >= len(fwdIdx.pflMmap.DataBytes)) {
			return "", false
		}
		return Timestamp2String(fwdIdx.pflMmap.ReadInt64(offset))
	}

	//string类型则间接从文件获取
	if fwdIdx.extMmap == nil || (int(offset) >= len(fwdIdx.pflMmap.DataBytes)) {
		return "", false
	}
	dtloffset := fwdIdx.pflMmap.ReadInt64(offset)
	if (int(dtloffset) >= len(fwdIdx.extMmap.DataBytes)) {
		return "", false
	}
	strLen := fwdIdx.extMmap.ReadInt64(dtloffset)
	return fwdIdx.extMmap.ReadString(dtloffset + int64(DATA_LEN_BYTE), strLen), true

}

//获取值 (以数值形式)
//TODO 不支持从字符型中读取数字??
func (fwdIdx *ForwardIndex) getInt(pos uint32) (int64, bool) {

	if fwdIdx.fake {//TODO 为毛??
		return 0xFFFFFFFF, true
	}

	//从内存读取
	if fwdIdx.isMomery {
		if (fwdIdx.fieldType == IDX_TYPE_NUMBER || fwdIdx.fieldType == IDX_TYPE_DATE) &&
			pos < uint32(len(fwdIdx.pflNumber)) {
			return fwdIdx.pflNumber[pos], true
		}
		return 0xFFFFFFFF, false
	}

	//从disk读取
	if fwdIdx.pflMmap == nil {
		return 0xFFFFFFFF, true //TODO false??
	}
	offset := fwdIdx.fileOffset + int64(pos) * int64(DATA_LEN_BYTE)
	if fwdIdx.fieldType == IDX_TYPE_NUMBER || fwdIdx.fieldType == IDX_TYPE_DATE {
		if (int(offset) >= len(fwdIdx.pflMmap.DataBytes)) {
			return 0xFFFFFFFF, false
		}
		return fwdIdx.pflMmap.ReadInt64(offset), true
	}

	return 0xFFFFFFFF, false
}

//过滤从numbers切片中找出是否有=或!=于pos数
func (fwdIdx *ForwardIndex) filterNums(pos uint32, filtertype uint64, numbers []int64) bool {
	var value int64
	if fwdIdx.fake {
		//TODO ??
		return false
	}

	//仅支持数值型
	if fwdIdx.fieldType != IDX_TYPE_NUMBER {
		return false
	}

	if fwdIdx.isMomery {
		value = fwdIdx.pflNumber[pos]
	} else {
		if fwdIdx.pflMmap == nil {
			return false
		}

		offset := fwdIdx.fileOffset + int64(pos) * int64(DATA_LEN_BYTE)
		value = fwdIdx.pflMmap.ReadInt64(offset)
	}

	switch filtertype {
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
func (fwdIdx *ForwardIndex) filter(pos uint32, filtertype uint64, start, end int64, str string) bool {

	var value int64
	if fwdIdx.fake {
		return false
	}

	if fwdIdx.fieldType == IDX_TYPE_NUMBER {
		if fwdIdx.isMomery {
			value = fwdIdx.pflNumber[pos]
		} else {
			if fwdIdx.pflMmap == nil {
				return false
			}
			offset := fwdIdx.fileOffset + int64(pos) * int64(DATA_LEN_BYTE)
			value = fwdIdx.pflMmap.ReadInt64(offset)
		}

		switch filtertype {
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
		switch filtertype {

		case basic.FILT_STR_PREFIX:
			if vstr, ok := fwdIdx.getString(pos); ok {
				for _, v := range vl {
					if strings.HasPrefix(vstr, v) {
						return true
					}
				}
			}
			return false
		case basic.FILT_STR_SUFFIX:
			if vstr, ok := fwdIdx.getString(pos); ok {
				for _, v := range vl {
					if strings.HasSuffix(vstr, v) {
						return true
					}
				}
			}
			return false
		case basic.FILT_STR_RANGE:
			if vstr, ok := fwdIdx.getString(pos); ok {
				for _, v := range vl {
					if !strings.Contains(vstr, v) {
						return false
					}
				}
				return true
			}
			return false
		case basic.FILT_STR_ALL:
			if vstr, ok := fwdIdx.getString(pos); ok {
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
func (fwdIdx *ForwardIndex) destroy() error {
	fwdIdx.pflNumber = nil
	fwdIdx.pflString = nil
	return nil
}

func (fwdIdx *ForwardIndex) setPflMmap(mmap *mmap.Mmap) {
	fwdIdx.pflMmap = mmap
}

func (fwdIdx *ForwardIndex) setDtlMmap(mmap *mmap.Mmap) {
	fwdIdx.extMmap = mmap
}

//归并索引
//正排索引的归并, 不存在倒排那种归并排序的问题, 因为每个索引内部按offset有序, 每个索引之间又是整体有序
//TODO 这里面存在一个问题, 如果保证的多个index的顺序, 现在直接通过切片保证的, 如果切片顺序不对呢??
func (fwdIdx *ForwardIndex) mergeIndex(idxList []*ForwardIndex, fullSegmentName string) (int64, int, error) {
	//打开正排文件
	pflFileName := fmt.Sprintf("%v.pfl", fullSegmentName)
	var fwdFd *os.File
	var err error
	fwdFd, err = os.OpenFile(pflFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer fwdFd.Close()
	fi, _ := fwdFd.Stat()
	offset := fi.Size()
	fwdIdx.fileOffset = offset


	var cnt int
	if fwdIdx.fieldType == IDX_TYPE_NUMBER || fwdIdx.fieldType == IDX_TYPE_DATE {
		buffer := make([]byte, DATA_LEN_BYTE)
		for _, idx := range idxList {
			for i := uint32(0); i < uint32(idx.docLen); i++ {
				//TODO 这里面存在一个问题, 如果保证的多个index的顺序, 现在直接通过切片保证的, 如果切片顺序不对呢??
				val, _ := idx.getInt(i)
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
		dtlFileName := fmt.Sprintf("%v.dtl", fullSegmentName)
		dtlFd, err := os.OpenFile(dtlFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return 0, 0, err
		}
		defer dtlFd.Close()
		fi, _ = dtlFd.Stat()
		dtloffset := fi.Size()

		buffer := make([]byte, DATA_LEN_BYTE)
		for _, idx := range idxList {
			for i := uint32(0); i < uint32(idx.docLen); i++ {
				strContent, _ := idx.getString(i)
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
				dtloffset = dtloffset + int64(DATA_LEN_BYTE) + int64(strLen)
				fwdIdx.curDocId++
			}
		}
		cnt = int(fwdIdx.curDocId - fwdIdx.startDocId)
	}
	fwdIdx.isMomery = false
	fwdIdx.pflString = nil
	fwdIdx.pflNumber = nil
	return offset, cnt, nil
}
