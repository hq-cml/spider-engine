package bitmap

/*
 * bitmap
 */
import (
	"fmt"
	"os"
	"log"
	"github.com/hq-cml/spider-engine/utils/mmap"
)

const (
	BitmapMaxMax = (0x01 << 63) -1 //最大容忍值, 超过这个直接报错( 因为int64, 符号位占用1位, 所以63. 其实也不可能有这么大的文件啊...
	BitmapMaxNum = 0x01 << 32      //2^32个bit位, 能够表示2^32个槽位, 即 0 - 2^32-1 的数字范围
	BYTE_SIZE = 8				   //一个字节占bit数
)

// Bitmap
type Bitmap struct {
	DataMap     *mmap.Mmap
	MaxNum      int64 //指示该Bitmap能表示的最大的数
	FirstOneIdx int64 //Bitmap被设置为1的最大值（方便遍历），初始值是负数！！
}

// NewBitmap 使用默认容量实例化一个Bitmap
func NewBitmap(indexname string, loadFile bool) *Bitmap {
	return NewBitmapSize(BitmapMaxNum, indexname, loadFile)
}

//根据指定的 size 实例化一个 Bitmap
//如果size非8的整数倍, 则会进行修正
func NewBitmapSize(size int, fileName string, loadFile bool) *Bitmap {
	if size > BitmapMaxMax {
		panic("No suport bitmap size!!!")
	}
	//参数修正
	if size == 0 || size > BitmapMaxNum {
		size = BitmapMaxNum
	} else if remainder := size % BYTE_SIZE; remainder != 0 {
		size += BYTE_SIZE - remainder
	}

	//新建实例
	bm := &Bitmap {}

	if loadFile {
		err := bm.loadFile(fileName)
		if err != nil {
			log.Fatal("Map2File Error: ", err)
			return nil
		}
	} else {
		bm.MaxNum = int64(size - 1)
		bm.FirstOneIdx = -1
		bm.newFile(fileName)
	}

	return bm
}

//将Bitmap和磁盘文件建立mmap映射, 将文件载入bitmap
func (bm *Bitmap) loadFile(indexName string) error {
	var err error
	bm.DataMap, err = mmap.NewMmap(indexName, true, 0)
	if err != nil {
		return err
	}
	bm.MaxNum = int64(bm.DataMap.RealCapcity()) * BYTE_SIZE - 1

	//找到最大的1
	bm.FindMaxOne()

	return nil
}

//将Bitmap和磁盘文件建立mmap映射
func (bm *Bitmap)newFile(indexName string) error {
	var err error

	//按理说, 应该先创建0长度的mmap, 然后append一个对应长度的bitmap
	//bm.DataMap, err = mmap.NewMmap(indexName, false, 0)
	//if err != nil {
	//	return err
	//}
	//err = bm.DataMap.AppendBytes(make([]byte, int64((bm.MaxNum+1)/BYTE_SIZE)))
	//if err != nil {
	//	return err
	//}

	//但是, 因为bitmap没有append一说, 设置好了直接使用
	//所以这里直接生成合适的大小,并且直接设置InternalIdx指向最后
	bm.DataMap, err = mmap.NewMmap(indexName, false, uint64((bm.MaxNum+1)/BYTE_SIZE))
	if err != nil {
		return err
	}
	bm.DataMap.SetInnerIdx(uint64((bm.MaxNum+1)/BYTE_SIZE) + mmap.HEADER_LEN)
	return nil
}

//找到最大的1, 有点烧脑
//从最高位开始, 逐个Byte探测
func (bm *Bitmap)FindMaxOne() {
	for i := bm.DataMap.RealCapcity() - 1; i >= 0 ; i-- {
		v := bm.DataMap.GetByte(i)
		if v == 0 {
			continue //跳过全0的字节
		}
		for j:=7; j>=0; j-- {
			if (v & (0x01<<uint(j))) == 0x01<<uint(j) {
				bm.FirstOneIdx = int64(i) * int64(BYTE_SIZE) + int64(j)
				return
			}
		}
	}
	bm.FirstOneIdx = -1
}

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

	if int64(idx) > bm.MaxNum {
		return false
	}

	if value == 0 {
		tmp := bm.DataMap.GetByte(index)
		tmp &^= 0x01 << pos   //&^ 清位操作符
		bm.DataMap.SetByte(index, tmp)

		//如果idx==FirstOneIdx, 则需要重新找到最大的1
		if bm.FirstOneIdx == int64(idx) {
			bm.FindMaxOne()
		}
	} else {
		tmp := bm.DataMap.GetByte(index)
		tmp |= 0x01 << pos
		bm.DataMap.SetByte(index, tmp)

		//记录曾经设置为 1 的最大位置
		if bm.FirstOneIdx < int64(idx) {
			bm.FirstOneIdx = int64(idx)
		}
	}

	return true
}

//GetBit 获得 idx 位置处的 value
//返回0 或者 1
func (bm *Bitmap) GetBit(idx uint64) uint8 {
	index, pos := idx / BYTE_SIZE, idx % BYTE_SIZE

	if bm.MaxNum < int64(idx) {
		return 0
	}

	return (bm.DataMap.GetByte(index) >> pos) & 0x01
}

//Maxpos 获的置为 1 的最大位置
func (bm *Bitmap) Maxpos() int64 {
	return bm.FirstOneIdx
}

//实现Stringer接口（输出所有的1的索引位置）
func (bm *Bitmap) String() string {
	var max int64 = bm.FirstOneIdx + 1

	numSlice := make([]int64, 0)
	var offset int64
	for offset =0; offset < max; offset++ {
		if bm.GetBit(uint64(offset)) == 1 {
			numSlice = append(numSlice, offset)
		}
	}

	return fmt.Sprintf("The BitMap => \n  MaxIdx: %d, %v", bm.FirstOneIdx, numSlice)
}

func (bm *Bitmap) Destroy(indexName string) error {
	bm.DataMap.Unmap()
	os.Remove(indexName)
	return nil
}

func (bm *Bitmap) Sync() error {
	err := bm.DataMap.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (bm *Bitmap) Close() error {
	err := bm.DataMap.Unmap()
	if err != nil {
		return err
	}
	return nil
}
