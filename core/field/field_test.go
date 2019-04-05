package field

import (
	"testing"
	"os/exec"
	"os"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/mmap"
)

const TEST_FIELD = "user_name"

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -f /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

func TestAddDocAndQueryAndGetAndPersist(t *testing.T) {
	field := NewEmptyField(TEST_FIELD, 0, index.IDX_TYPE_STRING_SEG)

	//add doc
	field.AddDocument(0, "我爱北京天安门")
	field.AddDocument(1, "天安门上太阳升")
	field.AddDocument(2, "火红的太阳")
	field.AddDocument(3, "火红的萨日朗")

	//测试query
	tmp, b := field.Query("天安门")
	if !b {
		t.Fatal("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		t.Fatal("Wrong")
	}
	tmp, b = field.Query("火红")
	if !b {
		t.Fatal("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		t.Fatal("Wrong")
	}

	//测试get
	s, b := field.GetString(2)
	if !b {
		t.Fatal("Wrong")
	}
	if s != "火红的太阳" {
		t.Fatal("Wrong")
	}

	_, b = field.GetString(4)
	if b {
		t.Fatal("Wrong")
	}

	//准备落地
	t.Logf("FiledOffset: %v, DocCnt: %v", field.FwdOffset, field.FwdDocCnt)
	treedb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb.Close()
	if err := field.Persist("/tmp/spider/Segment0", treedb); err != nil {
		t.Fatal("Wrong:", err)
	}

	t.Logf("FiledOffset: %v, DocCnt: %v", field.FwdOffset, field.FwdDocCnt)
	t.Log("\n\n")
}

func TestLoad(t *testing.T) {
	//从磁盘加载btree
	btdb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer btdb.Close()
	//从磁盘加载mmap
	ivtMmap, err := mmap.NewMmap("/tmp/spider/Segment0" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	mmp1, err := mmap.NewMmap("/tmp/spider/Segment0" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	mmp2, err := mmap.NewMmap("/tmp/spider/Segment0" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}

	field := LoadField(TEST_FIELD, 0, 3, index.IDX_TYPE_STRING_SEG, 0, 3, ivtMmap, mmp1, mmp2, false, btdb)
	//测试query
	tmp, b := field.Query("天安门")
	if !b {
		t.Fatal("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		t.Fatal("Wrong")
	}
	tmp, b = field.Query("火红")
	if !b {
		t.Fatal("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		t.Fatal("Wrong")
	}

	//测试get
	s, b := field.GetString(2)
	if !b {
		t.Fatal("Wrong")
	}
	if s != "火红的太阳" {
		t.Fatal("Wrong")
	}

	_, b = field.GetString(4)
	if b {
		t.Fatal("Wrong")
	}
	t.Log("\n\n")
}

//为merge做准备, 建立两个独立的field
func TestPrepareMerge(t *testing.T) {
	//清空
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -f /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}

	field1 := NewEmptyField(TEST_FIELD, 0, index.IDX_TYPE_STRING_SEG)
	field1.AddDocument(0, "我爱北京天安门")
	field1.AddDocument(1, "天安门上太阳升")

	field2 := NewEmptyField(TEST_FIELD, 0, index.IDX_TYPE_STRING_SEG)
	field2.AddDocument(0, "火红的太阳")
	field2.AddDocument(1, "火红的萨日朗")

	//准备落地
	treedb1 := btree.NewBtree("xx", "/tmp/spider/spider1" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb1.Close()
	if err := field1.Persist("/tmp/spider/Segment1", treedb1); err != nil {
		t.Fatal("Wrong:", err)
	}
	treedb2 := btree.NewBtree("xx", "/tmp/spider/spider2" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb2.Close()
	if err := field2.Persist("/tmp/spider/Segment2", treedb2); err != nil {
		t.Fatal("Wrong:", err)
	}
}

//将两个filed合并
func TestMerge(t *testing.T) {
	//加载Field1
	btdb1 := btree.NewBtree("xx", "/tmp/spider/spider1" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer btdb1.Close()
	ivtMmap1, err := mmap.NewMmap("/tmp/spider/Segment1" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	mmp11, err := mmap.NewMmap("/tmp/spider/Segment1" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	mmp21, err := mmap.NewMmap("/tmp/spider/Segment1" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}

	field1 := LoadField(TEST_FIELD, 0, 2, index.IDX_TYPE_STRING_SEG, 0, 2, ivtMmap1, mmp11, mmp21, false, btdb1)

	//加载field2
	btdb2 := btree.NewBtree("xx", "/tmp/spider/spider2" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer btdb2.Close()
	ivtMmap2, err := mmap.NewMmap("/tmp/spider/Segment2" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	mmp12, err := mmap.NewMmap("/tmp/spider/Segment2" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	mmp22, err := mmap.NewMmap("/tmp/spider/Segment2" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	field2 := LoadField(TEST_FIELD, 2, 4, index.IDX_TYPE_STRING_SEG, 0, 2, ivtMmap2, mmp12, mmp22, false, btdb2)

	//准备合并
	treedb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb.Close()
	field := NewEmptyField(TEST_FIELD, 0, index.IDX_TYPE_STRING_SEG)

	offset, cnt, err := field.MergeField([]*Field{field1, field2}, "/tmp/spider/segment", treedb)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Offset:", offset, "Cnt:", cnt)
	field.btree.Display(TEST_FIELD)

	t.Log("\n\n")

	//TODO 这些操作, 完全不闭合, 而且还依赖顺序, 后续要大改
	//TODO 目前只能合并出完整的磁盘版本, 但是filed并不能直接用
	//TODO 这里必须要重新加载一次...
	////测试query
	//tmp, b := field.Query("天安门")
	//if !b {
	//	t.Fatal("Wrong")
	//}
	//t.Log(json.JsonEncode(tmp))
	//if len(tmp) != 2 {
	//	t.Fatal("Wrong")
	//}
	//tmp, b = field.Query("火红")
	//if !b {
	//	t.Fatal("Wrong")
	//}
	//t.Log(json.JsonEncode(tmp))
	//if len(tmp) != 2 {
	//	t.Fatal("Wrong")
	//}
	//
	////测试get
	//s, b := field.GetString(2)
	//if !b {
	//	t.Fatal("Wrong")
	//}
	//if s != "火红的太阳" {
	//	t.Fatal("Wrong")
	//}
	//
	//_, b = field.GetString(4)
	//if b {
	//	t.Fatal("Wrong")
	//}
}

//将合并的filed加载回来测试
//TODO 这些例子都太宽厚了, 刻意成分太重, 后面要返工
func TestLoadMerge(t *testing.T) {
	//从磁盘加载btree
	btdb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer btdb.Close()
	//从磁盘加载mmap
	ivtMmap, err := mmap.NewMmap("/tmp/spider/segment" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	mmp1, err := mmap.NewMmap("/tmp/spider/segment" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}
	mmp2, err := mmap.NewMmap("/tmp/spider/segment" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		t.Fatal("Load Error:", err)
	}

	field := LoadField(TEST_FIELD, 0, 3, index.IDX_TYPE_STRING_SEG, 0, 3, ivtMmap, mmp1, mmp2, false, btdb)

	field.btree.Display(TEST_FIELD)

	//测试query
	tmp, b := field.Query("天安门")
	if !b {
		t.Fatal("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		t.Fatal("Wrong")
	}
	tmp, b = field.Query("火红")
	if !b {
		t.Fatal("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		t.Fatal("Wrong")
	}

	//测试get
	s, b := field.GetString(2)
	if !b {
		t.Fatal("Wrong")
	}
	if s != "火红的太阳" {
		t.Fatal("Wrong")
	}

	_, b = field.GetString(4)
	if b {
		t.Fatal("Wrong")
	}
	t.Log("\n\n")
}
