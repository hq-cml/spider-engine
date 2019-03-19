package mmap

/**
 * 封装syscall.Mmap
 */
import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

//MMap封装结构
//首部隐藏了8个字节, 保存了InternalIdx值, 用于二次加载映射
type Mmap struct {
	DataBytes   []byte
	Path        string
	Capacity    int64     //容量, 这个是算上了HEADER_LEN的. 所以真实容量是Capacity-HEADER_LEN
	InternalIdx int64     //内部操作指针, 从(0+HEADER_LEN)开始, 指向下一次要append的位置
	MapType     int64
	FilePtr     *os.File  //底层file句柄
}


//const APPEND_LEN int64 = 16  //test

const (
	APPEND_LEN int64 = 1024 * 1024  //1M
	HEADER_LEN int64 = 8   		    //头部, 保存InternalIdx便于加载
)

//创建文件, 并建立mmap映射
//load参数:
// true-加载已有文件(文件不存在则报错)
// false-创建新文件(如果存在旧文件会被清空)
func NewMmap(filePath string, load bool) (*Mmap, error) {

	mmp := &Mmap {
		DataBytes: make([]byte, 0),
		Path: filePath,
	}

	var err error
	if load {
		//尝试打开并加载文件
		//mmp.FilePtr, err = os.Open(filePath)
		mmp.FilePtr, err = os.OpenFile(filePath, os.O_RDWR, 0664)
		//defer mmp.FilePtr.Close()
		if err != nil {
			return nil, err
		}

		fi, err := mmp.FilePtr.Stat()
		if err != nil {
			return nil, err
		}
		mmp.Capacity = fi.Size()
	} else {
		//创建新文件
		mmp.FilePtr, err = os.Create(filePath)
		//defer mmp.FilePtr.Close()
		if err != nil {
			return nil, err
		}
		syscall.Ftruncate(int(mmp.FilePtr.Fd()), APPEND_LEN + HEADER_LEN) //申请空间需要算上头
		mmp.Capacity = APPEND_LEN + HEADER_LEN
		mmp.InternalIdx = HEADER_LEN //指针从0+HEADER_LEN开始
	}

	//建立mmap映射
	mmp.DataBytes, err = syscall.Mmap(int(mmp.FilePtr.Fd()), 0, int(mmp.Capacity), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	if load {
		mmp.InternalIdx = mmp.ReadInt64(0)  //从头部加载长度
	}

	return mmp, nil
}

//func (mmp *Mmap) SetFileEnd(fileLen int64) {
//	mmp.InternalIdx = fileLen
//}

//判断是否应该扩容, 如果应该, 则进一步确认扩多大
func (mmp *Mmap) checkNeedExpand(length int64) (int64, bool) {
	if mmp.InternalIdx + length > mmp.Capacity {
		var i int64 = 1

		for mmp.InternalIdx + length  > mmp.Capacity + i * APPEND_LEN {
			i ++
		}

		return i * APPEND_LEN, true
	} else {
		return 0, false
	}
}

//扩容
func (mmp *Mmap) doExpand(length int64) error {
	//trucate file, 扩容
	err := syscall.Ftruncate(int(mmp.FilePtr.Fd()), mmp.Capacity + length)
	if err != nil {
		return errors.New(fmt.Sprintf("Ftruncate error : %v\n", err))
	}
	mmp.Capacity += length
	syscall.Munmap(mmp.DataBytes)

	//重新建立mmap映射
	mmp.DataBytes, err = syscall.Mmap(
		int(mmp.FilePtr.Fd()), 0, int(mmp.Capacity), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return errors.New(fmt.Sprintf("MAPPING ERROR  %v \n", err))
	}

	return nil
}

//func (this *Mmap) isEndOfFile(start int64) bool {
//	if this.InternalIdx == start {
//		return true
//	}
//	return false
//}

func (mmp *Mmap) ReadInt64(start int64) int64 {
	return int64(binary.LittleEndian.Uint64(mmp.DataBytes[start : start+8]))
}

func (mmp *Mmap) ReadUInt64(start uint64) uint64 {
	return binary.LittleEndian.Uint64(mmp.DataBytes[start : start+8])
}

func (mmp *Mmap) ReadString(start, lens int64) string {
	return string(mmp.DataBytes[start : start+lens])
}

func (mmp *Mmap) Read(start, end int64) []byte {
	return mmp.DataBytes[start:end]
}

//指定位置写[]byte, 不考虑越界
func (mmp *Mmap) Write(start int64, buffer []byte) error {
	copy(mmp.DataBytes[start:int(start)+len(buffer)], buffer)
	return nil
}

//指定位置写int64, 不考虑越界
func (mmp *Mmap) WriteUInt64(start int64, value uint64) error {
	binary.LittleEndian.PutUint64(mmp.DataBytes[start:start+8], uint64(value))
	return nil
}

//指定位置写uint64, 不考虑越界
func (mmp *Mmap) WriteInt64(start, value int64) error {
	binary.LittleEndian.PutUint64(mmp.DataBytes[start:start+8], uint64(value))
	return nil
}

//从Idx向后追加int64, 考虑越界
func (mmp *Mmap) AppendInt64(value int64) error {
	expLen, b := mmp.checkNeedExpand(8)
	if b {
		if err := mmp.doExpand(expLen); err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint64(mmp.DataBytes[mmp.InternalIdx:mmp.InternalIdx +8], uint64(value))
	mmp.InternalIdx += 8
	return nil
}

//从Idx向后追加uint64, 考虑越界
func (mmp *Mmap) AppendUInt64(value uint64) error {
	expLen, b := mmp.checkNeedExpand(8)
	if b {
		if err := mmp.doExpand(expLen); err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint64(mmp.DataBytes[mmp.InternalIdx:mmp.InternalIdx +8], value)
	mmp.InternalIdx += 8
	return nil
}

//func (this *Mmap) AppendStringWithLen(value string) error {
//	this.AppendInt64(int64(len(value)))
//	this.AppendString(value)
//	return nil
//}

//从Idx向后追加[]byte, 考虑越界
func (mmp *Mmap) AppendBytes(value []byte) error {
	length := int64(len(value))
	expLen, b := mmp.checkNeedExpand(length)
	if b {
		if err := mmp.doExpand(expLen); err != nil {
			return err
		}
	}
	copy(mmp.DataBytes[mmp.InternalIdx : mmp.InternalIdx + length], value)
	mmp.InternalIdx += length
	return nil
}

//从Idx向后追加string, 考虑越界
func (mmp *Mmap) AppendString(value string) error {
	return mmp.AppendBytes([]byte(value))
}

//Unmmap
//根据linux规范, mmap会导致数据整体刷新到disk
func (mmp *Mmap) Unmap() error {
	mmp.WriteInt64(0, mmp.InternalIdx) //写回首部
	syscall.Munmap(mmp.DataBytes)
	mmp.FilePtr.Close()
	return nil
}

//
//func (this *Mmap) GetPointer() int64 {
//	return this.InternalIdx
//}

//func (mmp *Mmap) header() *reflect.SliceHeader {
//	return (*reflect.SliceHeader)(unsafe.Pointer(&mmp.DataBytes))
//}

//Sync
//在未调用Unmmap的情况下,手动刷新数据到disk
func (mmp *Mmap) Sync() error {
	//dh := mmp.header()
	//_, _, err := syscall.Syscall(syscall.SYS_MSYNC, dh.Data, uintptr(dh.Len), syscall.MS_SYNC)

	mmp.WriteInt64(0, mmp.InternalIdx) //写回首部
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&mmp.DataBytes[0])),
		uintptr(len(mmp.DataBytes)),
		syscall.MS_SYNC,
	)
	if err != 0 {
		fmt.Printf("Sync Error ")
		return errors.New("Sync Error")
	}
	return nil
}

//func (this *Mmap) ReadStringWith32Bytes(start int64) string {
//	lens := this.ReadInt64(start)
//	return this.ReadString(start+8, lens)
//}


