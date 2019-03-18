package mmap

import (
	"testing"
	"os"
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

	m.DataBytes [0] = 'a'
}

func TestLoadNewMmap(t *testing.T) {
	mmp, err := NewMmap("/tmp/ee", true)
	if err != nil {
		t.Fatal(err)
	}
	defer mmp.Unmap()
}

func TestCheckNeedExpand(t *testing.T) {
	mmp, err := NewMmap("/tmp/cc", true)
	if err != nil {
		t.Fatal(err)
	}
	defer mmp.Unmap()

	mmp.Capacity = 4
	t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	mmp.InternalIdx = 0
	tt, b := mmp.checkNeedExpand(3)
	t.Log(b, tt)

	mmp.InternalIdx = 0
	tt, b = mmp.checkNeedExpand(4)
	t.Log(b, tt)

	mmp.InternalIdx = 0
	tt, b = mmp.checkNeedExpand(5)
	t.Log(b, tt)


	mmp.InternalIdx = 3
	tt, b = mmp.checkNeedExpand(16)
	t.Log(b, tt)

	mmp.InternalIdx = 4
	tt, b = mmp.checkNeedExpand(16)
	t.Log(b, tt)

	mmp.InternalIdx = 3
	tt, b = mmp.checkNeedExpand(17)
	t.Log(b, tt)

	mmp.InternalIdx = 3
	tt, b = mmp.checkNeedExpand(18)
	t.Log(b, tt)

	mmp.InternalIdx = 4
	tt, b = mmp.checkNeedExpand(17)
	t.Log(b, tt)
}

func TestExpand(t *testing.T) {
	mmp, err := NewMmap("/tmp/cc", true)
	if err != nil {
		t.Fatal(err)
	}
	defer mmp.Unmap()
	t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	tt, b := mmp.checkNeedExpand(3)
	t.Log(b, tt)

	tt, b = mmp.checkNeedExpand(4)
	t.Log(b, tt)
	if b {
		mmp.doExpand(4)
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

	mmp.AppendString("abcdefghijklmno") //15
	t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)

	//mmp.AppendString("abcdefghijklmnop") //16
	//t.Log("Cap:", mmp.Capacity, "Idx:", mmp.InternalIdx)
}

