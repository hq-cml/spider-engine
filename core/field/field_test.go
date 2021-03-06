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
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

func TestAddDocAndQueryAndGetAndPersist(t *testing.T) {
	field := NewEmptyField(TEST_FIELD, 0, index.IDX_TYPE_STR_SPLITER)

	//add doc
	err := field.AddDocument(0, "我爱北京天安门"); if err != nil {panic(err.Error())}
	err = field.AddDocument(1, "天安门上太阳升"); if err != nil {panic(err.Error())}
	err = field.AddDocument(2, "火红的太阳"); if err != nil {panic(err.Error())}
	err = field.AddDocument(3, "火红的萨日朗"); if err != nil {panic(err.Error())}


	//测试query
	tmp, b := field.Query("天安门")
	if !b {
		panic("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		panic("Wrong")
	}
	tmp, b = field.Query("火红")
	if !b {
		panic("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		panic("Wrong")
	}

	//测试get
	s, b := field.GetString(2)
	if !b {
		panic("Wrong")
	}
	if s != "火红的太阳" {
		panic("Wrong")
	}

	_, b = field.GetString(4)
	if b {
		panic("Wrong")
	}

	//准备落地
	treedb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb.Close()
	t.Log("Before Persist. NextId:", field.NextDocId)
	if _, _, err := field.Persist("/tmp/spider/Partition0", treedb); err != nil {
		panic(err)
	}
	t.Log("After Persist. NextId:", field.NextDocId)

	t.Log("\n\n")
}

func TestLoad(t *testing.T) {
	//从磁盘加载btree
	btdb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer btdb.Close()
	//从磁盘加载mmap
	ivtMmap, err := mmap.NewMmap("/tmp/spider/Partition0" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		panic(err)
	}
	mmp1, err := mmap.NewMmap("/tmp/spider/Partition0" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	mmp2, err := mmap.NewMmap("/tmp/spider/Partition0" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		panic(err)
	}

	field := LoadField(TEST_FIELD, 0, 3, index.IDX_TYPE_STR_SPLITER, 0, 3, mmp1, mmp2, ivtMmap, btdb)
	//测试query
	tmp, b := field.Query("天安门")
	if !b {
		panic("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		panic("Wrong")
	}
	tmp, b = field.Query("火红")
	if !b {
		panic("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		panic("Wrong")
	}

	//测试get
	s, b := field.GetString(2)
	if !b {
		panic("Wrong")
	}
	if s != "火红的太阳" {
		panic("Wrong")
	}

	_, b = field.GetString(4)
	if b {
		panic("Wrong")
	}
	t.Log("\n\n")
}

//为merge做准备, 建立两个独立的field
func TestPrepareMerge(t *testing.T) {
	//清空
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}

	field1 := NewEmptyField(TEST_FIELD, 0, index.IDX_TYPE_STR_SPLITER)
	field1.AddDocument(0, "我爱北京天安门")
	field1.AddDocument(1, "天安门上太阳升")

	field2 := NewEmptyField(TEST_FIELD, 2, index.IDX_TYPE_STR_SPLITER)
	field2.AddDocument(2, "火红的太阳")
	field2.AddDocument(3, "火红的萨日朗")

	//准备落地
	treedb1 := btree.NewBtree("xx", "/tmp/spider/spider1" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb1.Close()
	if _, _, err := field1.Persist("/tmp/spider/Partition1", treedb1); err != nil {
		panic(err)
	}
	treedb2 := btree.NewBtree("xx", "/tmp/spider/spider2" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb2.Close()
	if _, _, err := field2.Persist("/tmp/spider/Partition2", treedb2); err != nil {
		panic(err)
	}
	t.Log("\n\n")
}

//将两个filed合并
func TestMerge(t *testing.T) {
	//加载Field1
	btdb1 := btree.NewBtree("xx", "/tmp/spider/spider1" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer btdb1.Close()
	ivtMmap1, err := mmap.NewMmap("/tmp/spider/Partition1" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		panic(err)
	}
	mmp11, err := mmap.NewMmap("/tmp/spider/Partition1" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	mmp21, err := mmap.NewMmap("/tmp/spider/Partition1" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		panic(err)
	}

	field1 := LoadField(TEST_FIELD, 0, 2, index.IDX_TYPE_STR_SPLITER, 0, 2, mmp11, mmp21, ivtMmap1, btdb1)

	//加载field2
	btdb2 := btree.NewBtree("xx", "/tmp/spider/spider2" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer btdb2.Close()
	ivtMmap2, err := mmap.NewMmap("/tmp/spider/Partition2" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		panic(err)
	}
	mmp12, err := mmap.NewMmap("/tmp/spider/Partition2" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	mmp22, err := mmap.NewMmap("/tmp/spider/Partition2" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		panic(err)
	}
	field2 := LoadField(TEST_FIELD, 2, 4, index.IDX_TYPE_STR_SPLITER, 0, 2, mmp12, mmp22, ivtMmap2, btdb2)

	//准备合并
	treedb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer treedb.Close()
	field := NewEmptyField(TEST_FIELD, 0, index.IDX_TYPE_STR_SPLITER)
	_, _, err = field.MergePersistField([]*Field{field1, field2}, "/tmp/spider/Partition", treedb)
	if err != nil {
		panic(err)
	}
	t.Log( "StartId:", field.StartDocId, ". NextId:", field.NextDocId)

	//合并完毕后进行测试
	//从磁盘加载mmap
	ivtMmap, err := mmap.NewMmap("/tmp/spider/Partition" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		panic(err)
	}
	mmp1, err := mmap.NewMmap("/tmp/spider/Partition" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	mmp2, err := mmap.NewMmap("/tmp/spider/Partition" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		panic(err)
	}
	field.SetMmap(mmp1, mmp2, ivtMmap)

	field.btdb.Display(TEST_FIELD)

	//测试query
	tmp, b := field.Query("天安门")
	if !b {
		panic("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		panic("Wrong")
	}
	tmp, b = field.Query("火红")
	if !b {
		panic("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		panic("Wrong")
	}

	//测试get
	s, b := field.GetString(2)
	if !b {
		panic("Wrong")
	}
	if s != "火红的太阳" {
		panic("Wrong")
	}

	_, b = field.GetString(4)
	if b {
		panic("Wrong")
	}
	t.Log("\n\n")
}

//将合并的filed加载回来测试
func TestLoadMerge(t *testing.T) {
	//从磁盘加载btree
	btdb := btree.NewBtree("xx", "/tmp/spider/spider" + basic.IDX_FILENAME_SUFFIX_BTREE)
	defer btdb.Close()
	//从磁盘加载mmap
	ivtMmap, err := mmap.NewMmap("/tmp/spider/Partition" + basic.IDX_FILENAME_SUFFIX_INVERT, true, 0)
	if err != nil {
		panic(err)
	}
	mmp1, err := mmap.NewMmap("/tmp/spider/Partition" + basic.IDX_FILENAME_SUFFIX_FWD, true, 0)
	if err != nil {
		panic(err)
	}
	mmp2, err := mmap.NewMmap("/tmp/spider/Partition" + basic.IDX_FILENAME_SUFFIX_FWDEXT, true, 0)
	if err != nil {
		panic(err)
	}

	field := LoadField(TEST_FIELD, 0, 3, index.IDX_TYPE_STR_SPLITER, 0, 3, mmp1, mmp2, ivtMmap, btdb)

	field.btdb.Display(TEST_FIELD)

	//测试query
	tmp, b := field.Query("天安门")
	if !b {
		panic("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		panic("Wrong")
	}
	tmp, b = field.Query("火红")
	if !b {
		panic("Wrong")
	}
	t.Log(helper.JsonEncode(tmp))
	if len(tmp) != 2 {
		panic("Wrong")
	}

	//测试get
	s, b := field.GetString(2)
	if !b {
		panic("Wrong")
	}
	if s != "火红的太阳" {
		panic("Wrong")
	}

	_, b = field.GetString(4)
	if b {
		panic("Wrong")
	}
	t.Log("\n\n")
}
