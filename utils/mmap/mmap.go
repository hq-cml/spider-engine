package mmap

/**
 * 封装syscall.Mmap
 */
import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

//MMap封装结构
type Mmap struct {
	DataBytes   []byte
	Path        string
	Capacity    int64     //容量
	InternalIdx int64     //内部操作指针, 从0开始, 指向下一个要append的位置
	MapType     int64
	FilePtr     *os.File  //底层file句柄
}

//const APPEND_LEN int64 = 1024 * 1024     //1M
const APPEND_LEN int64 = 16  //test

const (
	MODE_APPEND = iota
	MODE_CREATE
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
		syscall.Ftruncate(int(mmp.FilePtr.Fd()), APPEND_LEN)
		mmp.Capacity = APPEND_LEN
	}

	//建立mmap映射
	mmp.DataBytes, err = syscall.Mmap(int(mmp.FilePtr.Fd()), 0, int(mmp.Capacity), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return mmp, nil
}

//TODO ??
func (mmp *Mmap) SetFileEnd(fileLen int64) {
	mmp.InternalIdx = fileLen
}

//判断是否应该扩容, 如果应该, 则进一步确认扩多大
func (mmp *Mmap) checkNeedExpand(length int64) (int64, bool) {
	fmt.Println(mmp.InternalIdx)
	fmt.Println(length)
	fmt.Println(mmp.Capacity)
	fmt.Println()

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
	err := syscall.Ftruncate(int(mmp.FilePtr.Fd()), mmp.Capacity + APPEND_LEN)
	if err != nil {
		return errors.New(fmt.Sprintf("Ftruncate error : %v\n", err))
	}
	mmp.Capacity += APPEND_LEN
	syscall.Munmap(mmp.DataBytes)

	//重新建立mmap映射
	mmp.DataBytes, err = syscall.Mmap(
		int(mmp.FilePtr.Fd()), 0, int(mmp.Capacity), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return errors.New(fmt.Sprintf("MAPPING ERROR  %v \n", err))
	}

	return nil
}

func (this *Mmap) isEndOfFile(start int64) bool {
	if this.InternalIdx == start {
		return true
	}
	return false
}

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

//写[]byte, 不考虑越界
func (mmp *Mmap) Write(start int64, buffer []byte) error {
	copy(mmp.DataBytes[start:int(start)+len(buffer)], buffer)
	return nil
}

//写int64, 不考虑越界
func (mmp *Mmap) WriteUInt64(start int64, value uint64) error {
	binary.LittleEndian.PutUint64(mmp.DataBytes[start:start+8], uint64(value))
	return nil
}

//写uint64, 不考虑越界
func (mmp *Mmap) WriteInt64(start, value int64) error {
	binary.LittleEndian.PutUint64(mmp.DataBytes[start:start+8], uint64(value))
	return nil
}

//追加int64, 考虑越界
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

//追加uint64, 考虑越界
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

//追加[]byte, 考虑越界
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

//追加string, 考虑越界
func (mmp *Mmap) AppendString(value string) error {
	return mmp.AppendBytes([]byte(value))

}

//Unmmap
func (this *Mmap) Unmap() error {

	syscall.Munmap(this.DataBytes)
	this.FilePtr.Close()
	return nil
}

//
func (this *Mmap) GetPointer() int64 {
	return this.InternalIdx
}

func (this *Mmap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&this.DataBytes))
}

func (this *Mmap) Sync() error {
	dh := this.header()
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, dh.Data, uintptr(dh.Len), syscall.MS_SYNC)
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


