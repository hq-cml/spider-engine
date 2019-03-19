package mmap

import (
	"testing"
	"os"
	"time"
	"fmt"
)

func TestOpenNoexistFile(t *testing.T) {
	_, err := os.Open("/tmp/noexist")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateFile(t *testing.T) {
	f, err := os.Create("/tmp/cc")
	if err != nil {
		t.Fatal(err)
	}

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(fi.Size(), fi.Mode())
}

func TestNewMmap(t *testing.T) {
	m, err := NewMmap("/tmp/ee", false)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Unmap()

	m.DataBytes [8] = 'a'
}

func TestLoadNewMmap(t *testing.T) {
	mmp, err := NewMmap("/tmp/ee", true)
	if err != nil {
		t.Fatal(err)
	}
	defer mmp.Unmap()

	t.Log(mmp.ReadString(8, 1))
}

func TestCheckNeedExpand(t *testing.T) {
	mmp, err := NewMmap("/tmp/cc", true)
	if err != nil {
		t.Fatal(err)
	}
	defer mmp.Unmap()

	mmp.Capacity = 12 //实际容量只有4
	t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	mmp.InternalIdx = 8
	tt, b := mmp.checkNeedExpand(3) //不扩
	t.Log(b, tt)

	mmp.InternalIdx = 8
	tt, b = mmp.checkNeedExpand(4)  //不扩
	t.Log(b, tt)

	mmp.InternalIdx = 8
	tt, b = mmp.checkNeedExpand(5)  //扩一次
	t.Log(b, tt)


	mmp.InternalIdx = 11
	tt, b = mmp.checkNeedExpand(16)  //扩一次
	t.Log(b, tt)

	mmp.InternalIdx = 12
	tt, b = mmp.checkNeedExpand(16)  //扩一次
	t.Log(b, tt)

	mmp.InternalIdx = 11
	tt, b = mmp.checkNeedExpand(17)  //扩一次
	t.Log(b, tt)

	mmp.InternalIdx = 11
	tt, b = mmp.checkNeedExpand(18)  //扩2次
	t.Log(b, tt)

	mmp.InternalIdx = 12
	tt, b = mmp.checkNeedExpand(17)  //扩2次
	t.Log(b, tt)
}

func TestExpand(t *testing.T) {
	mmp, err := NewMmap("/tmp/cc", false)
	if err != nil {
		t.Fatal(err)
	}
	defer mmp.Unmap()
	t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	mmp.AppendString("abcdefghijklmno") //15
	t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	tt, b := mmp.checkNeedExpand(1)  //不扩
	t.Log(b, tt)

	tt, b = mmp.checkNeedExpand(2)   //扩
	t.Log(b, tt)
	if b {
		mmp.doExpand(tt)
		t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)
	}
}

func TestWriteString(t *testing.T) {
	mmp, err := NewMmap("/tmp/cc", false)
	if err != nil {
		t.Fatal(err)
	}
	defer mmp.Unmap()
	t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	//mmp.AppendString("abcdefghijklmno") //15
	//t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	//mmp.AppendString("abcdefghijklmnop") //16
	//t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	mmp.AppendString("abcdefghijklmnopr") //17
	t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)
}

//感觉没啥效果, 在执行Sync之前, 文件也是同步更改的...
func TestSync(t *testing.T) {
	mmp, err := NewMmap("/tmp/cc", true)
	if err != nil {
		t.Fatal(err)
	}
	defer mmp.Unmap()
	fmt.Println("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	//t.Log("C: ", mmp.DataBytes, len(mmp.DataBytes))
	fmt.Println("Before:", mmp.ReadString(8, 16))

	mmp.Write(8, []byte("123"))

	fmt.Println("After:", mmp.ReadString(8, 16))

	time.Sleep(30 * time.Second)
	fmt.Println("Begin Sync")
	mmp.Sync()
	fmt.Println("End Sync")
	time.Sleep(30 * time.Second)
}
