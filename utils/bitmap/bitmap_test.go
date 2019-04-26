package bitmap

import (
	"testing"
)

func TestLeftShift(t *testing.T) {
	a := 0x01<<8
	if a != 256 {
		t.Log("Error 0x01<<8")
	}
	t.Log("\n")
}

func TestNewBitmap(t *testing.T) {
	bm := NewBitmapSize(16, "/tmp/bitmap.dat", false)

	t.Log("Create bitmap:", bm)
	t.Log("Create bitmap:", bm.DataMap)

	//if bm.FirstOneIdx != -1 {
	//	panic("wrong")
	//}

	if bm.MaxNum != 15 {
		panic("wrong")
	}

	if bm.DataMap.RealCapcity() != 2 {
		panic("wrong")
	}

	if bm.DataMap.Capacity != 10 {
		panic("wrong")
	}
	t.Log("\n")
}

func TestSetGet(t *testing.T) {
	bm := NewBitmapSize(32, "/tmp/bitmap.dat", false)
	defer bm.Close()

	t.Log("Create bitmap:", bm)
	t.Log("Create bitmap:", bm.DataMap)

	t.Log("Before Foreach: ", bm)

	bm.Set(3)
	bm.Set(7)
	bm.Set(10)
	bm.Set(15)
	bm.Set(21)
	t.Log("After Foreach: ", bm)
	if(bm.Get(21) != 1) {
		panic("wrong")
	}

	bm.Clear(2)
	bm.Clear(7)
	bm.Clear(15)
	bm.Clear(21)
	t.Log("Clear Foreach: ", bm)
	if(bm.Get(21) == 1) {
		panic("wrong")
	}
	if(bm.Get(3) != 1) {
		panic("wrong")
	}

	bm.Set(31)
	bm.Set(19)
	bm.Set(27)
	t.Log("Final Foreach: ", bm)
	if(bm.Get(31) != 1) {
		panic("wrong")
	}

	bm.Clear(19)
	t.Log("Clear Foreach: ", bm)
	if(bm.Maxpos() != 31) {
		panic(bm.Maxpos())
	}

	t.Log("\n")
}

func TestLoadBitmap(t *testing.T) {
	bm := NewBitmapSize(0, "/tmp/bitmap.dat", true)

	t.Log("Load bitmap:", bm)
	t.Log("Load bitmap:", bm.DataMap)

	if bm.Get(3) != 1 ||
		bm.Get(10) != 1 ||
		bm.Get(27) != 1 ||
		bm.Get(31) != 1 ||
		bm.Get(2) == 1 ||
		bm.Get(4) == 1 ||
		bm.Get(5) == 1 ||
		bm.Get(11) == 1 ||
		bm.Get(17) == 1 ||
		bm.Get(23) == 1 ||
		bm.Get(24) == 1 {
		panic("wrong data")
	}
	t.Log("\n")
}

//and do expand
func TestClose(t *testing.T) {
	bm := NewBitmapSize(0, "/tmp/bitmap.dat", true)

	err := bm.DoExpand()
	if err != nil {
		panic(err)
	}

	err = bm.Close()
	if err != nil {
		panic(err)
	}
	t.Log("\n")
}

func TestLoadAfterExpand(t *testing.T) {
	bm := NewBitmapSize(0, "/tmp/bitmap.dat", true)

	t.Log("Load bitmap:", bm)
	t.Log("Load bitmap:", bm.DataMap)

	if bm.Get(3) != 1 ||
		bm.Get(10) != 1 ||
		bm.Get(27) != 1 ||
		bm.Get(31) != 1 ||
		bm.Get(2) == 1 ||
		bm.Get(4) == 1 ||
		bm.Get(5) == 1 ||
		bm.Get(11) == 1 ||
		bm.Get(17) == 1 ||
		bm.Get(23) == 1 ||
		bm.Get(24) == 1 {
		panic("wrong data")
	}
	t.Log("\n")
}

