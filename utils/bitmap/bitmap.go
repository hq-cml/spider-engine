package bitmap

/*
 * bitmap
 */
import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"log"
)

//2^32个bit位, 能够表示2^32个槽位, 即 0 - 2^32-1 的数字范围
const BitmapMaxNum = 0x01 << 32

//一个字节占bit数
const BYTE_SIZE = 8

// Bitmap
type Bitmap struct {
	FilePath    string
	Data        []byte //保存实际的 bit 数据
	MaxNum      uint64 //指示该Bitmap能表示的最大的数
	FirstOneIdx uint64 //Bitmap被设置为1的最大位置（方便遍历）
}

// NewBitmap 使用默认容量实例化一个 Bitmap
func NewBitmap(indexname string, loadFile bool) *Bitmap {
	return NewBitmapSize(BitmapMaxNum, indexname, loadFile)
}

//根据指定的 size 实例化一个 Bitmap
//如果size非8的整数倍, 则会进行修正
func NewBitmapSize(size int, fileName string, loadFile bool) *Bitmap {
	if size == 0 || size > BitmapMaxNum {
		size = BitmapMaxNum
	} else if remainder := size % BYTE_SIZE; remainder != 0 {
		size += BYTE_SIZE - remainder
	}
	bm := &Bitmap {
		FilePath: fileName,
		Data: make([]byte, size / BYTE_SIZE), //size >> 3
		MaxNum: uint64(size - 1),
	}

	if loadFile {
		err := bm.File2Map(fileName)
		if err != nil {
			log.Fatal("Map2File Error: ", err)
			return nil
		}
	}

	return bm
}

//将Bitmap和磁盘文件建立mmap映射, 将文件载入bitmap
func (bm *Bitmap) File2Map(indexName string) error {
	f, err := os.OpenFile(indexName, os.O_RDWR, 0664)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("ERR:%v", err)
		return err
	}

	if fi.Size() != int64(len(bm.Data)) {
		return errors.New("Wront length file")
	}

	//建立mmap映射到磁盘文件
	bm.Data, err = syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return err
	}

	//找到最大的1, 有点烧脑
	bm.ChoseMaxOne()

	return nil
}

//找到最大的1, 有点烧脑
//从最高位开始, 逐个Byte探测
func (bm *Bitmap)ChoseMaxOne() {
	OUT:
	for i := len(bm.Data) - 1; i >= 0 ; i-- {
		v := bm.Data[i]
		if v == 0 {
			continue //跳过全0的字节
		}
		for j:=7; j>=0; j-- {
			if (v & (0x01<<uint(j))) == 0x01<<uint(j) {
				bm.FirstOneIdx = uint64(i * BYTE_SIZE + j)
				break OUT
			}
		}
	}
}

func (bm *Bitmap)Map2File(indexName string) error {
	fout, err := os.Create(indexName)
	defer fout.Close()
	if err != nil {
		return err
	}
	err = syscall.Ftruncate(int(fout.Fd()), int64(len(bm.Data)))
	if err != nil {
		fmt.Printf("ftruncate error : %v\n", err)
		return err
	}

	//建立mmap映射到磁盘文件
	data, err := syscall.Mmap(int(fout.Fd()), 0, len(bm.Data), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return err
	}
	copy(data, bm.Data)

	//err = syscall.Munmap(data)
	//if err != nil {
	//	return err
	//}
	return nil

}

//func (bm *Bitmap)MakeBitmapFile(indexname string) error {
//	size := BitmapMaxNum
//	if size == 0 || size > BitmapMaxNum {
//		size = BitmapMaxNum
//	} else if remainder := size % BYTE_SIZE; remainder != 0 {
//		size += BYTE_SIZE - remainder
//	}
//
//	fout, err := os.Create(indexname)
//	defer fout.Close()
//	if err != nil {
//		return err
//	}
//	err = syscall.Ftruncate(int(fout.Fd()), int64(size / BYTE_SIZE))
//	if err != nil {
//		fmt.Printf("ftruncate error : %v\n", err)
//		return err
//	}
//
//	return nil
//
//}

func (bm *Bitmap) Set(idx uint64) bool {
	return bm.setBit(idx, 1)
}

func (bm *Bitmap) Clear(idx uint64) bool {
	return bm.setBit(idx, 0)
}

//SetBit将idx位置的 bit 置为 value (0/1)
//idx取值是[0, MaxNum]
func (bm *Bitmap) setBit(idx uint64, value uint8) bool {
	index, pos := idx / BYTE_SIZE, idx % BYTE_SIZE

	if bm.MaxNum < idx {
		return false
	}

	if value == 0 {
		//&^ 清位操作符
		bm.Data[index] &^= 0x01 << pos

		//如果idx==FirstOneIdx, 则需要重新找到最大的1
		if bm.FirstOneIdx == idx {
			bm.ChoseMaxOne()
		}
	} else {
		bm.Data[index] |= 0x01 << pos

		//记录曾经设置为 1 的最大位置
		if bm.FirstOneIdx < idx {
			bm.FirstOneIdx = idx
		}
	}

	return true
}

//GetBit 获得 idx 位置处的 value
//返回0 或者 1
func (bm *Bitmap) GetBit(idx uint64) uint8 {
	index, pos := idx / BYTE_SIZE, idx % BYTE_SIZE

	if bm.MaxNum < idx {
		return 0
	}

	return (bm.Data[index] >> pos) & 0x01
}

//Maxpos 获的置为 1 的最大位置
func (bm *Bitmap) Maxpos() uint64 {
	return bm.FirstOneIdx
}

//实现Stringer接口（输出所有的1的索引位置）
func (bm *Bitmap) String() string {
	var max uint64 = bm.FirstOneIdx + 1

	numSlice := make([]uint64, 0)
	var offset uint64
	for offset =0; offset < max; offset++ {
		if bm.GetBit(offset) == 1 {
			numSlice = append(numSlice, offset)
		}
	}

	return fmt.Sprintf("MaxIdx: %d, %v", bm.FirstOneIdx, numSlice)
}

func (bm *Bitmap) Destroy(indexName string) error {
	syscall.Munmap(bm.Data)
	os.Remove(indexName)
	return nil
}

func (bm *Bitmap) Close() error {
	//err := this.Sync()
	//if err != nil {
	//	return err
	//}

	err := syscall.Munmap(bm.Data)
	if err != nil {
		return err
	}
	return nil
}


//TODO 暂时没必要
//func (this *Bitmap) Sync() error {
//	dh := (*reflect.SliceHeader)(unsafe.Pointer(&this.Data))
//	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, dh.Data, uintptr(dh.Len), syscall.MS_SYNC)
//	if err != 0 {
//		return errors.New("Sync Error:" + err.Error())
//	}
//	return nil
//}

