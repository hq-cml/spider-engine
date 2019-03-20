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

	if bm.FirstOneIdx != -1 {
		t.Fatal("wrong")
	}

	if bm.MaxNum != 15 {
		t.Fatal("wrong")
	}

	if bm.DataMap.RealCapcity() != 2 {
		t.Fatal("wrong")
	}

	if bm.DataMap.Capacity != 10 {
		t.Fatal("wrong")
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

	bm.Clear(2)
	bm.Clear(7)
	bm.Clear(15)
	bm.Clear(21)
	t.Log("Clear Foreach: ", bm)

	bm.Set(31)
	bm.Set(19)
	bm.Set(27)
	t.Log("Final Foreach: ", bm)

	bm.Clear(19)
	t.Log("Clear Foreach: ", bm)

	t.Log("\n")
}

func TestLoadBitmap(t *testing.T) {
	bm := NewBitmapSize(0, "/tmp/bitmap.dat", true)

	t.Log("Load bitmap:", bm)
	t.Log("Load bitmap:", bm.DataMap)

	if bm.GetBit(3) != 1 ||
		bm.GetBit(10) != 1 ||
		bm.GetBit(27) != 1 ||
		bm.GetBit(31) != 1 ||
		bm.GetBit(2) == 1 ||
		bm.GetBit(4) == 1 ||
		bm.GetBit(5) == 1 ||
		bm.GetBit(11) == 1 ||
		bm.GetBit(17) == 1 ||
		bm.GetBit(23) == 1 ||
		bm.GetBit(24) == 1 {
		t.Fatal("wrong data")
	}
	t.Log("\n")
}

func TestClose(t *testing.T) {
	bm := NewBitmapSize(0, "/tmp/bitmap.dat", true)

	bm.Set(16)

	err := bm.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n")
}


