package mmap

/**
 * 封装 syscall.Mmap
 *
 * 参考:
 *   https://github.com/edsrzf/mmap-go/blob/master/mmap.go
 *   https://github.com/riobard/go-mmap/blob/master/mmap.go
 */
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

//MMap封装结构
//首部隐藏了8个字节, 保存了innerIdx值, 用于二次加载映射
type Mmap struct {
	DataBytes     []byte   //映射之后的内存实体
	Path          string
	TotalCapacity uint64   //容量, 这个是算上了HEADER_LEN的. 所以真实容量是Capacity-HEADER_LEN
	innerIdx      uint64   //内部操作指针, 从(0+HEADER_LEN)开始, 指向下一次要append操作的位置
	FilePtr       *os.File //底层file句柄
}

const (
	APPEND_LEN = 1024 * 1024 //1M, 默认的扩容长度
	HEADER_LEN = 8           //头部, 保存InnerIdx便于落盘后加载
)

//创建文件, 并建立mmap映射
//load参数:
// true-加载已有文件(文件不存在则报错), 此时size无效
// false-创建新文件(如果存在旧文件会被清空), 此时若size<0则会安排默认大小
func NewMmap(filePath string, load bool, size uint64) (*Mmap, error) {

	mmp := &Mmap{
		DataBytes: make([]byte, 0),
		Path:      filePath,
	}

	var err error
	if load {
		//尝试打开并加载文件
		mmp.FilePtr, err = os.OpenFile(filePath, os.O_RDWR, 0664)
		//defer mmp.FilePtr.Close()
		if err != nil {
			return nil, err
		}

		fi, err := mmp.FilePtr.Stat()
		if err != nil {
			return nil, err
		}
		mmp.TotalCapacity = uint64(fi.Size())
	} else {
		//创建新文件
		mmp.FilePtr, err = os.Create(filePath)
		//defer mmp.FilePtr.Close()
		if err != nil {
			return nil, err
		}

		if size < 0 {
			size = APPEND_LEN
		}
		syscall.Ftruncate(int(mmp.FilePtr.Fd()), int64(size+HEADER_LEN)) //申请空间需要算上头
		mmp.TotalCapacity = uint64(size) + HEADER_LEN
		mmp.innerIdx = HEADER_LEN //指针从0+HEADER_LEN开始
	}

	//建立mmap映射
	mmp.DataBytes, err = syscall.Mmap(int(mmp.FilePtr.Fd()), 0, int(mmp.TotalCapacity),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	if load {
		mmp.innerIdx = uint64(mmp.ReadInt64(0)) //从头部加载长度
	}
	return mmp, nil
}

//谨慎使用, 最好通过程序自动增加
func (mmp *Mmap) SetInnerIdx(idx uint64) {
	mmp.innerIdx = idx
}

//实际容量，不算隐藏HEADER
func (mmp *Mmap) RealCapcity() uint64 {
	return mmp.TotalCapacity - HEADER_LEN
}

//底层的边界, 超过这个值, 无论读写都将触发Panic
func (mmp *Mmap) Boundary() int {
	return len(mmp.DataBytes)
}

//判断当再次写入length的时候，是否应该扩容（如果应该, 则计算扩多大）
func (mmp *Mmap) checkNeedExpand(length uint64) (uint64, bool) {
	if mmp.innerIdx+length > mmp.TotalCapacity {
		var i uint64 = 1

		for mmp.innerIdx+length >= mmp.TotalCapacity+i*uint64(APPEND_LEN) {
			i++
		}

		return (i * APPEND_LEN), true
	} else {
		return 0, false
	}
}

//扩容
func (mmp *Mmap) DoExpand(length uint64) error {
	//trucate file, 扩容
	err := syscall.Ftruncate(int(mmp.FilePtr.Fd()), int64(mmp.TotalCapacity+length))
	if err != nil {
		return errors.New(fmt.Sprintf("Ftruncate error : %v\n", err))
	}
	mmp.TotalCapacity += uint64(length)
	syscall.Munmap(mmp.DataBytes)

	//重新建立mmap映射
	mmp.DataBytes, err = syscall.Mmap(
		int(mmp.FilePtr.Fd()), 0, int(mmp.TotalCapacity), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
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

//Read系列, 未忽略隐藏Header
func (mmp *Mmap) ReadInt64(start uint64) int64 {
	return int64(binary.LittleEndian.Uint64(mmp.DataBytes[start : start+8]))
}
func (mmp *Mmap) ReadUInt64(start uint64) uint64 {
	return binary.LittleEndian.Uint64(mmp.DataBytes[start : start+8])
}
func (mmp *Mmap) ReadInt64(start uint64) int64 {
	return int64(binary.LittleEndian.Uint64(mmp.DataBytes[start : start+8]))
}
func (mmp *Mmap) ReadUInt64(start uint64) uint64 {
	return binary.LittleEndian.Uint64(mmp.DataBytes[start : start+8])
}
func (mmp *Mmap) ReadString(start, length uint64) string {
	return string(mmp.DataBytes[start : start+length])
}
func (mmp *Mmap) ReadBytes(start, length uint64) []byte {
	return mmp.DataBytes[start : start+length]
}
func (mmp *Mmap) ReadByte(start uint64) byte {
	return mmp.DataBytes[start]
}

//Get系列, 屏蔽了隐藏Header的影响
func (mmp *Mmap) GetByte(start uint64) byte {
	return mmp.DataBytes[start+HEADER_LEN]
}
func (mmp *Mmap) GetBytes(start, length uint64) []byte {
	return mmp.DataBytes[start+HEADER_LEN : start+HEADER_LEN+length]
}
func (mmp *Mmap) GetString(start, length uint64) string {
	return string(mmp.DataBytes[start+HEADER_LEN : start+HEADER_LEN+length])
}
func (mmp *Mmap) GetInt64(start uint64) int64 {
	return int64(binary.LittleEndian.Uint64(mmp.DataBytes[start+HEADER_LEN : start+HEADER_LEN+8]))
}
func (mmp *Mmap) GetUInt64(start uint64) uint64 {
	return binary.LittleEndian.Uint64(mmp.DataBytes[start+HEADER_LEN : start+HEADER_LEN+8])
}

//Write系列, 指定位置写, 不考虑越界
func (mmp *Mmap) WriteByte(start uint64, b byte) {
	mmp.DataBytes[start] = b
}
func (mmp *Mmap) WriteBytes(start uint64, buffer []byte) {
	copy(mmp.DataBytes[start:int(start)+len(buffer)], buffer)
}
func (mmp *Mmap) WriteString(start uint64, s string) {
	mmp.WriteBytes(start, []byte(s))
}
func (mmp *Mmap) WriteUInt64(start uint64, value uint64) {
	binary.LittleEndian.PutUint64(mmp.DataBytes[start:start+8], value)
}
func (mmp *Mmap) WriteInt64(start uint64, value int64) {
	binary.LittleEndian.PutUint64(mmp.DataBytes[start:start+8], uint64(value))
}

//Set系列, 屏蔽了隐藏Header的影响, 不考虑越界
func (mmp *Mmap) SetByte(start uint64, b byte) {
	mmp.DataBytes[start+HEADER_LEN] = b
}
func (mmp *Mmap) SetBytes(start uint64, b []byte) {
	copy(mmp.DataBytes[start+HEADER_LEN:int(start)+HEADER_LEN+len(b)], b)
}
func (mmp *Mmap) SetString(start uint64, s string) {
	mmp.SetBytes(start, []byte(s))
}
func (mmp *Mmap) SetInt64(start uint64, value int64) {
	binary.LittleEndian.PutUint64(mmp.DataBytes[start+HEADER_LEN:start+HEADER_LEN+8], uint64(value))
}
func (mmp *Mmap) SetUInt64(start uint64, value uint64) {
	binary.LittleEndian.PutUint64(mmp.DataBytes[start+HEADER_LEN:start+HEADER_LEN+8], uint64(value))
}

//Append系列, 从Idx向后追加, 考虑越界，自动扩容
func (mmp *Mmap) AppendInt64(value int64) error {
	expLen, b := mmp.checkNeedExpand(8)
	if b {
		if err := mmp.DoExpand(expLen); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint64(mmp.DataBytes[mmp.innerIdx:mmp.innerIdx+8], uint64(value))
	mmp.innerIdx += 8
	return nil
}
func (mmp *Mmap) AppendUInt64(value uint64) error {
	expLen, b := mmp.checkNeedExpand(8)
	if b {
		if err := mmp.DoExpand(expLen); err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint64(mmp.DataBytes[mmp.innerIdx:mmp.innerIdx+8], value)
	mmp.innerIdx += 8
	return nil
}
func (mmp *Mmap) AppendBytes(value []byte) error {
	length := uint64(len(value))
	expLen, b := mmp.checkNeedExpand(length)
	if b {
		if err := mmp.DoExpand(expLen); err != nil {
			return err
		}
	}
	copy(mmp.DataBytes[mmp.innerIdx:mmp.innerIdx+length], value)
	mmp.innerIdx += length
	return nil
}
func (mmp *Mmap) AppendByte(b byte) error {
	return mmp.AppendBytes([]byte{b})
}
func (mmp *Mmap) AppendString(value string) error {
	return mmp.AppendBytes([]byte(value))
}

//Unmmap
//根据linux规范, mmap会导致数据整体刷新到disk
func (mmp *Mmap) Unmap() error {
	mmp.WriteInt64(0, int64(mmp.innerIdx)) //写回首部
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

	mmp.WriteInt64(0, int64(mmp.innerIdx)) //写回首部
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

func (mmp *Mmap) String() string {
	var buf bytes.Buffer
	buf.WriteString("The Mmap => \n")
	buf.WriteString(fmt.Sprintf("  Capcity: %d\n", mmp.TotalCapacity))
	buf.WriteString(fmt.Sprintf("  InnerIdx: %d\n", mmp.innerIdx))
	buf.WriteString(fmt.Sprintf("  Length of DataBytes: %d\n", len(mmp.DataBytes)))
	return buf.String()
}
