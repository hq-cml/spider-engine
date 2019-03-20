package bitmap

import (
	"testing"
	//"os"
)

//func TestA(t *testing.T) {
//	v := 132
//	j := 7
//	fmt.Println(v, (0x01<<uint(j)), (v & (0x01<<uint(j))))
//}

func TestLeftShift(t *testing.T) {
	a := 0x01<<8
	if a != 256 {
		t.Log("Error 0x01<<8")
	}
}
//
//func TestNewBitmap(t *testing.T) {
//	bm := NewBitmapSize(16, "/tmp/bitmap.dat", false)
//
//	fmt.Println(bm)
//	fmt.Println(bm.DataMap.DataBytes)
//}
//
//func TestSetGet(t *testing.T) {
//	bm := NewBitmapSize(32, "/tmp/bitmap.dat", false)
//	defer bm.Close()
//
//	fmt.Println(bm)
//	fmt.Println(bm.DataMap)
//
//	bm.Set(3)
//	//fmt.Println()
//	//fmt.Println(bm)
//	//fmt.Println(bm.DataMap)
//	//fmt.Println(bm.DataMap.ReadBytes(8, 2))
//
//	bm.Set(7)
//	//fmt.Println(bm)
//
//	bm.Set(10)
//	//fmt.Println(bm)
//
//	bm.Set(15)
//	//fmt.Println(bm)
//
//	bm.Set(21)
//	fmt.Println(bm)
//}
//
//func TestLoadBitmap(t *testing.T) {
//	bm := NewBitmapSize(16, "/tmp/bitmap.dat", true)
//
//	fmt.Println(bm)
//	fmt.Println(bm.DataMap)
//}

//func TestFindMax1(t *testing.T) {
//	bm := NewBitmapSize(32, "/tmp/bitmap.dat", false)
//	fmt.Println(bm)
//
//	bm.Set(3)
//	fmt.Println(bm)
//
//	bm.Set(7)
//	fmt.Println(bm)
//
//	bm.Set(10)
//	fmt.Println(bm)
//
//	//bm.Set(11)
//	//fmt.Println(bm)
//
//	bm.Set(15)
//	fmt.Println(bm)
//
//	fmt.Println(bm.Data)
//
//	OUT:
//	for i := len(bm.Data) - 1; i >= 0 ; i-- {
//		fmt.Println("A-------------", i)
//		v := bm.Data[i]
//		if v == 0 {
//			continue
//		}
//		for j:=7; j>=0; j-- {
//			fmt.Println("B-------------", j, v, (0x01<<uint(j)), (v & (0x01<<uint(j))))
//			if (v & (0x01<<uint(j))) == 0x01<<uint(j) {
//				fmt.Println("C-------------", i, j)
//				bm.FirstOneIdx = uint64(i * BYTE_SIZE + j)
//				break OUT
//			}
//		}
//	}
//
//	fmt.Println("Max: ", bm.FirstOneIdx)
//}

//func TestClose(t *testing.T) {
//	bm := NewBitmapSize(32, "/tmp/bitmap.dat", true)
//
//	bm.Set(16)
//
//	err := bm.Close()
//	if err != nil {
//		fmt.Println("Error: ", err)
//	} else {
//		fmt.Println("ok~")
//	}
//}


