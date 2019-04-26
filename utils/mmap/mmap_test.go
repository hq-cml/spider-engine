package mmap

import (
	"testing"
	"os"
)

func TestOpenNoexistFile(t *testing.T) {
	_, err := os.Open("/tmp/noexist")
	if err == nil {
		panic(err)
	}
	t.Log("\n")
}

func TestCreateFile(t *testing.T) {
	f, err := os.Create("/tmp/xx")
	if err != nil {
		panic(err)
	}

	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}

	t.Log("Create /tmp/xx success: ", fi.Size(), fi.Mode())
	t.Log("\n")
	t.Log("\n")
}

func TestNewMmap(t *testing.T) {
	m, err := NewMmap("/tmp/ee", false, 16)
	if err != nil {
		panic(err)
	}
	defer m.Unmap()
	t.Log("Create mmap: ", m)
	m.AppendByte('a')
	m.AppendString("bcd")
	t.Log("After append: ", m)
	t.Log("\n")
	t.Log("\n")
}

func TestLoadMmap(t *testing.T) {
	m, err := NewMmap("/tmp/ee", true, 0)
	if err != nil {
		panic(err)
	}
	defer m.Unmap()
	t.Log("Load mmap: ", m)

	if m.GetByte(0) != 'a' {
		panic(m.GetByte(0))
	}

	if m.GetString(0, 4) != "abcd" {
		panic(m.GetString(0, 4))
	}
	t.Log("\n")
}

////测试越界Panic
//func TestPanic(t *testing.T) {
//	m, err := NewMmap("/tmp/ee", true, 0)
//	if err != nil {
//		panic(err)
//	}
//	defer m.Unmap()
//	t.Log("Load mmap: ", m)
//
//	m.DataBytes[23] = 'x'
//	t.Log("Ok")
//
//	m.DataBytes[24] = 'y'  //write panic
//	t.Log("Ok")
//
//	_ = m.DataBytes[24]    //read panic
//	t.Log("Ok")
//
//	t.Log("\n")
//	t.Log("\n")
//}

//临时测试函数
func (mmp *Mmap) tempCheckNeedExpand(length uint64) (int64, bool) {
	if mmp.innerIdx+ length > mmp.Capacity {
		var i uint64 = 1

		for mmp.innerIdx+ length  > mmp.Capacity + i * 16 {
			i ++
		}

		return int64(i) * 16, true
	} else {
		return 0, false
	}
}

func TestCheckNeedExpand(t *testing.T) {
	mmp, err := NewMmap("/tmp/cc", false, 4)
	if err != nil {
		panic(err)
	}
	defer mmp.Unmap()

	t.Log("Org mmap:", mmp)

	tt, b := mmp.tempCheckNeedExpand(3) //不扩
	if b == false {
		t.Log("yes, no expand: ", tt)
	} else {
		panic("wrong")
	}

	tt, b = mmp.tempCheckNeedExpand(4)  //不扩
	if b == false {
		t.Log("yes, no expand: ", tt)
	} else {
		panic("wrong")
	}

	tt, b = mmp.tempCheckNeedExpand(5)  //扩一次
	if b == true {
		t.Log("yes, should expand: ", tt)
	} else {
		panic("wrong")
	}

	mmp.innerIdx = 11
	tt, b = mmp.tempCheckNeedExpand(16)  //扩一次
	if b == true && tt == 16{
		t.Log("yes, should expand: ", tt)
	} else {
		panic("wrong")
	}

	mmp.innerIdx = 12
	tt, b = mmp.tempCheckNeedExpand(16)  //扩一次
	if b == true && tt == 16{
		t.Log("yes, should expand: ", tt)
	} else {
		panic("wrong")
	}

	mmp.innerIdx = 11
	tt, b = mmp.tempCheckNeedExpand(17)  //扩一次
	if b == true && tt == 16{
		t.Log("yes, should expand: ", tt)
	} else {
		panic("wrong")
	}

	mmp.innerIdx = 11
	tt, b = mmp.tempCheckNeedExpand(18)  //扩2次
	if b == true && tt == 32{
		t.Log("yes, should expand: ", tt)
	} else {
		panic("wrong")
	}

	mmp.innerIdx = 12
	tt, b = mmp.tempCheckNeedExpand(17)  //扩2次
	if b == true && tt == 32{
		t.Log("yes, should expand: ", tt)
	} else {
		panic("wrong")
	}
	t.Log("\n")
}

func TestExpand(t *testing.T) {
	mmp, err := NewMmap("/tmp/aa", false, 16)
	if err != nil {
		panic(err)
	}
	t.Log("Before expand:" , mmp)
	mmp.AppendString("abcdefghijklmnop") //16
	if mmp.Capacity != 24 || mmp.innerIdx != 24 {
		panic("Wrong expand")
	}
	mmp.Unmap()

	mmp, err = NewMmap("/tmp/aa", false, 16)
	if err != nil {
		panic(err)
	}
	t.Log("Before expand:" , mmp)
	mmp.AppendString("abcdefghijklmnopq") //17
	if mmp.Capacity != (24 + uint64(APPEND_LEN)) || mmp.innerIdx != 25 {
		panic("Wrong expand")
	}
	mmp.Unmap()

	t.Log("\n")
}

/*
//这个需要指定 --run=TestSync 单独测试
//感觉没啥效果, 在执行Sync之前, 文件也是同步更改的...
func TestSync(t *testing.T) {
	//t.Skip("Skip fuck")
	mmp, err := NewMmap("/tmp/cc", true, 0)
	if err != nil {
		panic(err)
	}
	defer mmp.Unmap()
	fmt.Println("Cap:", mmp.Capacity, "Idx:", mmp.innerIdx)

	//t.Log("C: ", mmp.DataBytes, len(mmp.DataBytes))
	fmt.Println("Before:", mmp.ReadString(8, 16))

	mmp.WriteBytes(8, []byte("123"))

	fmt.Println("After:", mmp.ReadString(8, 16))

	time.Sleep(30 * time.Second)
	fmt.Println("Begin Sync")
	mmp.Sync()
	fmt.Println("End Sync")
	time.Sleep(30 * time.Second)
}
*/
