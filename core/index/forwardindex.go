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
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/utils/mmap"
)

//profile 正排索引，detail也是保存在这里
type ForwardIndex struct {
	startDocId uint32
	curDocId   uint32
	isMomery   bool
	fieldType  uint64
	pflOffset  int64
	docLen     uint64
	fieldName  string `json:"fullname"` //完整的名字，用来进行文件操作的
	fake       bool
	pflNumber  []int64       `json:"-"`
	pflString  []string      `json:"-"`
	pflMmap    *mmap.Mmap    `json:"-"`
	dtlMmap    *mmap.Mmap    `json:"-"`
}

const DATA_LEN_BYTE int = 8

func newEmptyFakeProfile(fieldType uint64, shift uint8, fieldName string, start uint32, docLen uint64,) *ForwardIndex {
	this := &ForwardIndex{docLen: docLen, pflOffset: 0, isMomery: true, fieldType: fieldType, fieldName: fieldName, startDocId: start, curDocId: start, pflNumber: nil, pflString: nil}
	this.pflString = make([]string, 0)
	this.pflNumber = make([]int64, 0)
	this.fake = true
	return this
}

// newEmptyProfile function description : 新建空的字符型正排索引
// params :
// return :
func newEmptyProfile(fieldType uint64, shift uint8, fieldName string, start uint32) *ForwardIndex {
	this := &ForwardIndex{fake: false, pflOffset: 0, isMomery: true, fieldType: fieldType, fieldName: fieldName, startDocId: start, curDocId: start, pflNumber: nil, pflString: nil}
	this.pflString = make([]string, 0)
	this.pflNumber = make([]int64, 0)

	return this
}

// newProfileWithLocalFile function description : 新建空的字符型正排索引
// params :
// return :
func newProfileWithLocalFile(fieldType uint64, shift uint8, fullsegmentname string, pflMmap, dtlMmap *mmap.Mmap, offset int64, docLen uint64, isMomery bool) *ForwardIndex {

	this := &ForwardIndex{fake: false, docLen: docLen, pflOffset: offset, isMomery: isMomery, fieldType: fieldType, pflMmap: pflMmap, dtlMmap: dtlMmap}

	/*
	   	//打开正排文件
	   	pflFileName := fmt.Sprintf("%v.pfl", fullsegmentname)
	   	this.Logger.Info("[INFO] NumberProfile --> NewNumberProfileWithLocalFile :: Load NumberProfile pflFileName: %v", pflFileName)
	   	pflFd, err := os.OpenFile(pflFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	   	if err != nil {
	   		return &NumberProfile{isMomery: false, pflType: idxType, Logger: logger, pflContent: make([]int64, 0)}
	   	}
	   	defer pflFd.Close()

	   	os, offseterr := pflFd.Seek(offset, 0)
	   	if offseterr != nil || os != offset {
	   		this.Logger.Error("[ERROR] NumberProfile --> NewNumberProfileWithLocalFile :: Seek Offset Error %v", offseterr)
	   		return &NumberProfile{isMomery: false, pflType: idxType, Logger: logger, pflContent: make([]int64, 0)}
	   	}

	   	for index := 0; index < docLen; index++ {
	           var value int64
	   		var pfl utils.DetailInfo
	   		pfl.Len = 8//int(lens)
	   		pfl.Offset = os
	           err := binary.Read(pflFd, binary.LittleEndian, &value)
	           if err != nil {
	               this.Logger.Error("[ERROR] NumberProfile --> NewNumberProfileWithLocalFile :: Read PosFile error %v", err)
	               this.pflPostion = append(this.pflPostion, utils.DetailInfo{0, 0})
	               this.pflContent= append(this.pflContent,0xFFFFFFFF)
	               continue
	           }
	           this.pflContent=append(this.pflContent,value)
	   		this.pflPostion = append(this.pflPostion, pfl)

	   		offset := os + 8
	   		os, offseterr = pflFd.Seek(offset, 0)
	   		if offseterr != nil || os != offset {
	   			this.Logger.Error("[ERROR] NumberProfile --> NewNumberProfileWithLocalFile :: Seek Offset Error %v", offseterr)
	   			this.pflPostion = append(this.pflPostion, utils.DetailInfo{0, 0})
	               this.pflContent=append(this.pflContent,0xFFFFFFFF)
	   			continue
	   		}
	   	}
	*/
	//this.Logger.Info("[INFO] Load  Profile : %v.pfl", fullsegmentname)
	return this

}

//增加一个doc文档(仅增加在内存中)
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
			//this.Logger.Info("[INFO] value %v", value)
			fwdIdx.pflNumber = append(fwdIdx.pflNumber, value)
			//this.pflString = append(this.pflString, fmt.Sprintf("%v", content))
		} else if fwdIdx.fieldType == IDX_TYPE_DATE {

			value, _ = IsDateTime(fmt.Sprintf("%v", content))
			fwdIdx.pflNumber = append(fwdIdx.pflNumber, value)

		} else {
			fwdIdx.pflString = append(fwdIdx.pflString, fmt.Sprintf("%v", content))
		}
	default:
		fwdIdx.pflString = append(fwdIdx.pflString, fmt.Sprintf("%v", content))
	}
	fwdIdx.curDocId++
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
	fwdIdx.pflOffset = offset //当前偏移量, 即文件最后位置

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
			dtloffset = dtloffset + DATA_LEN_BYTE + int64(strLen)

		}
		cnt = len(fwdIdx.pflString)

	}

	//TODO ??
	fwdIdx.isMomery = false
	fwdIdx.pflString = nil
	fwdIdx.pflNumber = nil
	return offset, cnt, err

	//TOOD ?? fwdIdx.pflOffset需要设置吗
}

//获取值 (以字符串形式)
func (this *ForwardIndex) getString(pos uint32) (string, bool) {

	if this.fake {  //TODO 为毛??
		return "", true
	}

	//先尝试从内存获取
	if this.isMomery && pos < uint32(len(this.pflNumber)) {
		if this.fieldType == IDX_TYPE_NUMBER {
			return fmt.Sprintf("%v", this.pflNumber[pos]), true
		} else if this.fieldType == IDX_TYPE_DATE {
			return FormatDateTime(this.pflNumber[pos])
		}
		return this.pflString[pos], true

	}

	//再从disk获取(其实是利用mmap, 速度会有所提升)
	if this.pflMmap == nil {
		return "", false
	}

	//数字或者日期类型, 直接从索引文件读取
	offset := this.pflOffset + int64(pos * DATA_LEN_BYTE)
	if this.fieldType == IDX_TYPE_NUMBER {
		return fmt.Sprintf("%v", this.pflMmap.ReadInt64(offset)), true
	} else if this.fieldType == IDX_TYPE_DATE {
		return FormatDateTime(this.pflMmap.ReadInt64(offset))

	}

	//string类型则间接从文件获取
	if this.dtlMmap == nil {
		return "", false
	}
	dtloffset := this.pflMmap.ReadInt64(offset)
	strLen := this.dtlMmap.ReadInt64(dtloffset)
	return this.dtlMmap.ReadString(dtloffset + DATA_LEN_BYTE, strLen), true

}

func (this *ForwardIndex) getInt(pos uint32) (int64, bool) {

	if this.fake {
		return 0xFFFFFFFF, true
	}

	if this.isMomery {
		if (this.fieldType == IDX_TYPE_NUMBER || this.fieldType == IDX_TYPE_DATE) &&
			pos < uint32(len(this.pflNumber)) {
			return this.pflNumber[pos], true
		}
		return 0xFFFFFFFF, false
	}
	if this.pflMmap == nil {
		return 0xFFFFFFFF, true
	}

	offset := this.pflOffset + int64(pos*8)
	if this.fieldType == IDX_TYPE_NUMBER || this.fieldType == IDX_TYPE_DATE {
		//ov:=this.pflMmap.ReadInt64(offset)
		//if this.shift>0{
		//    return fmt.Sprintf("%v",float64(ov)/(math.Pow10(int(this.shift))) ), true
		//}
		return this.pflMmap.ReadInt64(offset), true
	}

	return 0xFFFFFFFF, false
}

//func (this *ForwardIndex) filterNums(pos uint32, filtertype uint64, rangenum []int64) bool {
//	var value int64
//	if this.fake {
//		return false
//	}
//
//	if this.fieldType == IDX_TYPE_NUMBER {
//		if this.isMomery {
//			value = this.pflNumber[pos]
//		} else if this.pflMmap == nil {
//			return false
//		}
//
//		offset := this.pflOffset + int64(pos*8)
//		value = this.pflMmap.ReadInt64(offset)
//
//		switch filtertype {
//		case FILT_EQ:
//			for _, start := range rangenum {
//				if ok := (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == start); ok {
//					return true
//				}
//			}
//			return false
//		case FILT_NOT:
//			for _, start := range rangenum {
//				if ok := (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == start); ok {
//					return false
//				}
//			}
//			return true
//
//		default:
//			return false
//		}
//
//	}
//
//	return false
//}
//
//// Filter function description : 过滤
//// params :
//// return :
//func (this *ForwardIndex) filter(pos uint32, filtertype uint64, start, end int64, str string) bool {
//
//	var value int64
//	if /*(this.fieldType != utils.IDX_TYPE_NUMBER && this.fieldType != utils.IDX_TYPE_DATE) ||*/ this.fake {
//		return false
//	}
//
//	if this.fieldType == utils.IDX_TYPE_NUMBER {
//		if this.isMomery {
//			value = this.pflNumber[pos]
//		} else if this.pflMmap == nil {
//			return false
//		}
//
//		offset := this.pflOffset + int64(pos*8)
//		value = this.pflMmap.ReadInt64(offset)
//
//		switch filtertype {
//		case utils.FILT_EQ:
//
//			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value == start)
//		case utils.FILT_OVER:
//			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value >= start)
//		case utils.FILT_LESS:
//			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value <= start)
//		case utils.FILT_RANGE:
//			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value >= start && value <= end)
//		case utils.FILT_NOT:
//			return (0xFFFFFFFF&value != 0xFFFFFFFF) && (value != start)
//		default:
//			return false
//		}
//	}
//
//	if this.fieldType == utils.IDX_TYPE_STRING_SINGLE {
//		vl := strings.Split(str, ",")
//		switch filtertype {
//
//		case utils.FILT_STR_PREFIX:
//			if vstr, ok := this.getValue(pos); ok {
//
//				for _, v := range vl {
//					if strings.HasPrefix(vstr, v) {
//						return true
//					}
//				}
//
//			}
//			return false
//		case utils.FILT_STR_SUFFIX:
//			if vstr, ok := this.getValue(pos); ok {
//				for _, v := range vl {
//					if strings.HasSuffix(vstr, v) {
//						return true
//					}
//				}
//			}
//			return false
//		case utils.FILT_STR_RANGE:
//			if vstr, ok := this.getValue(pos); ok {
//				for _, v := range vl {
//					if !strings.Contains(vstr, v) {
//						return false
//					}
//				}
//				return true
//			}
//			return false
//		case utils.FILT_STR_ALL:
//
//			if vstr, ok := this.getValue(pos); ok {
//				for _, v := range vl {
//					if vstr == v {
//						return true
//					}
//				}
//			}
//			return false
//		default:
//			return false
//		}
//
//	}
//
//	return false
//
//}
//
//// destroy function description : 销毁
//// params :
//// return :
//func (this *ForwardIndex) destroy() error {
//	this.pflNumber = nil
//	this.pflString = nil
//	return nil
//}
//
//func (this *ForwardIndex) setPflMmap(mmap *utils.Mmap) {
//	this.pflMmap = mmap
//}
//
//func (this *ForwardIndex) setDtlMmap(mmap *utils.Mmap) {
//	this.dtlMmap = mmap
//}
//
//func (this *ForwardIndex) updateDocument(docid uint32, content interface{}) error {
//
//	if this.fieldType != utils.IDX_TYPE_NUMBER || this.fieldType != utils.IDX_TYPE_DATE {
//		return errors.New("not support")
//	}
//
//	vtype := reflect.TypeOf(content)
//	var value int64 = 0xFFFFFFFF
//	switch vtype.Name() {
//	case "string", "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64":
//		var ok error
//		if this.fieldType == utils.IDX_TYPE_DATE {
//			value, _ = utils.IsDateTime(fmt.Sprintf("%v", content))
//		}
//		value, ok = strconv.ParseInt(fmt.Sprintf("%v", content), 0, 0)
//		if ok != nil {
//			value = 0xFFFFFFFF
//		}
//
//	case "float32":
//		v, _ := content.(float32)
//		value = int64(v * 100)
//	case "float64":
//		v, _ := content.(float64)
//		value = int64(v * 100)
//	default:
//		value = 0xFFFFFFFF
//	}
//	if this.isMomery == true {
//		this.pflNumber[docid-this.startDocId] = value
//	} else {
//		offset := this.pflOffset + int64((docid-this.startDocId)*8)
//		this.pflMmap.WriteInt64(offset, value)
//	}
//	return nil
//}
//
//func (this *ForwardIndex) mergeProfiles(srclist []*ForwardIndex, fullsegmentname string) (int64, int, error) {
//
//	//this.Logger.Info("[INFO] mergeProfiles  %v",fullsegmentname )
//	//if this.startDocId != 0 {
//	//    this.Logger.Error("[ERROR] DocId Wrong %v",this.startDocId)
//	//    return 0,0,errors.New("DocId Wrong")
//	//}
//	//打开正排文件
//	pflFileName := fmt.Sprintf("%v.pfl", fullsegmentname)
//	var pflFd *os.File
//	var err error
//	//this.Logger.Info("[INFO] NumberProfile --> Serialization :: Load NumberProfile pflFileName: %v", pflFileName)
//	pflFd, err = os.OpenFile(pflFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
//	if err != nil {
//		return 0, 0, err
//	}
//	defer pflFd.Close()
//	fi, _ := pflFd.Stat()
//	offset := fi.Size()
//	this.pflOffset = offset
//	var lens int
//	if this.fieldType == utils.IDX_TYPE_NUMBER || this.fieldType == utils.IDX_TYPE_DATE {
//		valueBufer := make([]byte, 8)
//		for _, src := range srclist {
//			for i := uint32(0); i < uint32(src.docLen); i++ {
//				val, _ := src.getIntValue(i)
//				binary.LittleEndian.PutUint64(valueBufer, uint64(val))
//				_, err = pflFd.Write(valueBufer)
//				if err != nil {
//					log.Errf("[ERROR] NumberProfile --> Serialization :: Write Error %v", err)
//				}
//				this.curDocId++
//			}
//		}
//
//		lens = int(this.curDocId - this.startDocId)
//	} else {
//
//		//打开dtl文件
//		dtlFileName := fmt.Sprintf("%v.dtl", fullsegmentname)
//		//this.Logger.Info("[INFO] StringProfile --> Serialization :: Load StringProfile dtlFileName: %v", dtlFileName)
//		dtlFd, err := os.OpenFile(dtlFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
//		if err != nil {
//			return 0, 0, err
//		}
//		defer dtlFd.Close()
//		fi, _ = dtlFd.Stat()
//		dtloffset := fi.Size()
//
//		lenBufer := make([]byte, 8)
//		for _, src := range srclist {
//			for i := uint32(0); i < uint32(src.docLen); i++ {
//				info, _ := src.getValue(i)
//				infolen := len(info)
//				binary.LittleEndian.PutUint64(lenBufer, uint64(infolen))
//				_, err := dtlFd.Write(lenBufer)
//				cnt, err := dtlFd.WriteString(info)
//				if err != nil || cnt != infolen {
//					log.Errf("[ERROR] StringProfile --> Serialization :: Write Error %v", err)
//				}
//				//存储offset
//				//this.Logger.Info("[INFO] dtloffset %v,%v",dtloffset,infolen)
//				binary.LittleEndian.PutUint64(lenBufer, uint64(dtloffset))
//				_, err = pflFd.Write(lenBufer)
//				if err != nil {
//					log.Errf("[ERROR] StringProfile --> Serialization :: Write Error %v", err)
//				}
//				dtloffset = dtloffset + int64(infolen) + 8
//				this.curDocId++
//			}
//		}
//
//		lens = int(this.curDocId - this.startDocId)
//
//	}
//	this.isMomery = false
//	this.pflString = nil
//	this.pflNumber = nil
//	return offset, lens, nil
//
//}
