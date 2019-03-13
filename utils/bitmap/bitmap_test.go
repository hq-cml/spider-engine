package bitmap

import (
	"testing"
	"fmt"
	"os"
)

func TestLeftShift(t *testing.T) {
	a := 0x01<<8
	fmt.Println(a)
}

func TestNewBitmap(t *testing.T) {
	bm := NewBitmapSize(16, "/tmp/test.bitmap", true)

	fmt.Println(bm)
	fmt.Println(bm.Data)
}

func TestSetGet(t *testing.T) {
	bm := NewBitmapSize(32, "", false)
	fmt.Println(bm)
	fmt.Println(bm.Data)

	bm.Set(3)
	fmt.Println(bm)

	bm.Set(7)
	fmt.Println(bm)

	bm.Set(10)
	fmt.Println(bm)

	bm.Set(15)
	fmt.Println(bm)

	bm.Set(21)
	fmt.Println(bm)

	fmt.Println(bm.Data)

	err := bm.Map2File("/tmp/test.bitmap")
	if err != nil {
		t.Fatal(err)
	}
}


func TestDumpFile(t *testing.T) {
	f, _ := os.Open("/tmp/test.bitmap")
	b := make([]byte, 10)
	n, err := f.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Len:", n)
	fmt.Println(b)
}

func TestFindMax1(t *testing.T) {
	bm := NewBitmapSize(32, "", false)
	fmt.Println(bm)

	bm.Set(3)
	fmt.Println(bm)

	bm.Set(7)
	fmt.Println(bm)

	bm.Set(10)
	fmt.Println(bm)

	//bm.Set(11)
	//fmt.Println(bm)

	bm.Set(15)
	fmt.Println(bm)

	fmt.Println(bm.Data)

	OUT:
	for i := len(bm.Data) - 1; i >= 0 ; i-- {
		fmt.Println("A-------------", i)
		v := bm.Data[i]
		if v == 0 {
			continue
		}
		for j:=7; j>=0; j-- {
			fmt.Println("B-------------", j, v, (0x01<<uint(j)), (v & (0x01<<uint(j))))
			if (v & (0x01<<uint(j))) == 0x01<<uint(j) {
				fmt.Println("C-------------", i, j)
				bm.FirstOneIdx = uint64(i * BYTE_SIZE + j)
				break OUT
			}
		}
	}

	fmt.Println("Max: ", bm.FirstOneIdx)
}

func TestClose(t *testing.T) {
	bm := NewBitmapSize(32, "/tmp/test.bitmap", true)

	fmt.Println(bm)
	fmt.Println(bm.Data)

	bm.Set(16)

	fmt.Println(bm)
	fmt.Println(bm.Data)

	err := bm.Close()
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("ok~")
	}

}

func TestA(t *testing.T) {
	v := 132
	j := 7
	fmt.Println(v, (0x01<<uint(j)), (v & (0x01<<uint(j))))
}
