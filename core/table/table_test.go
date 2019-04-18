package table

import (
	"testing"
	"os"
	"os/exec"

	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
	//"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/helper"
)

const TEST_TABLE = "user"         //用户
const TEST_FIELD0 = "user_id"
const TEST_FIELD1 = "user_name"
const TEST_FIELD2 = "user_age"
const TEST_FIELD3 = "user_desc"
const TEST_FIELD4 = "tobe_del"

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -f /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

func TestNewTableAndPersistAndDelfield(t *testing.T) {
	table := NewEmptyTable("/tmp/spider", TEST_TABLE)

	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD0,
		IndexType: index.IDX_TYPE_PK,
	}); err != nil {
		t.Fatal(err)
	}

	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD1,
		IndexType: index.IDX_TYPE_STRING,
	}); err != nil {
		t.Fatal(err)
	}
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD2,
		IndexType: index.IDX_TYPE_NUMBER,
	}); err != nil {
		t.Fatal(err)
	}
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD3,
		IndexType: index.IDX_TYPE_STRING_SEG,
	}); err != nil {
		t.Fatal(err)
	}
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD4,
		IndexType: index.IDX_TYPE_NUMBER,
	}); err != nil {
		t.Fatal(err)
	}

	docId, err := table.AddDoc(map[string]string{TEST_FIELD0: "10001", TEST_FIELD1: "张三",TEST_FIELD2: "20",TEST_FIELD3: "喜欢美食,也喜欢旅游", TEST_FIELD4: "77"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add DocId:", docId)

	docId, err = table.AddDoc(map[string]string{TEST_FIELD0: "10002", TEST_FIELD1: "李四", TEST_FIELD2: "18", TEST_FIELD3: "喜欢电影,也喜欢美食", TEST_FIELD4: "88"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add DocId:", docId)

	docId, err = table.AddDoc(map[string]string{TEST_FIELD0: "10003",TEST_FIELD1: "王二麻",	TEST_FIELD2: "30",TEST_FIELD3: "喜欢养生", TEST_FIELD4: "99"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add DocId:", docId)

	//测试倒排搜索(内存)
	t.Log("Before Persist")
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		t.Fatal("Should exist")
	}
	if docNode.DocId != 1 {
		t.Fatal("Should is 1")
	}

	ids, ok := table.SearchDocs(TEST_FIELD3, "美食")
	if !ok {
		t.Fatal("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	//测试正排获取(内存)
	docId = docNode.DocId
	t.Log("Get doc ", docId)
	content,exist := table.getDoc(docId)
	if !exist {
		t.Fatal("Should exist")
		table.Close()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	//测试落地
	err = table.Persist()
	if err != nil {
		t.Fatal("Persist Error:", err)
		table.Close()
	}

	//测试落地后能否直接从磁盘读取
	t.Log("After Persist")
	docNode, exist = table.findDocIdByPrimaryKey("10002")
	if !exist {
		t.Fatal("Should exist")
	}
	if docNode.DocId != 1 {
		t.Fatal("Should is 1")
	}

	ids, ok = table.SearchDocs(TEST_FIELD3, "美食")
	if !ok {
		t.Fatal("Can't find")
	}
	t.Log(helper.JsonEncode(ids))
	docId = docNode.DocId
	t.Log("Get doc ", docId)
	content,exist = table.getDoc(docId)
	if !exist {
		t.Fatal("Should exist")
		table.Close()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))


	//再次新增一个文档
	docId, err = table.AddDoc(map[string]string{TEST_FIELD0: "10004",TEST_FIELD1: "爱新觉罗",	TEST_FIELD2: "30",TEST_FIELD3: "喜欢打仗", TEST_FIELD4: "99"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add DocId:", docId)

	//删除一个分区
	err = table.DeleteField(TEST_FIELD4)
	if err != nil {
		t.Fatal("DeleteField Error:", err)
	}
	content,exist = table.getDoc(docId)
	if !exist {
		t.Fatal("Should exist")
		table.Close()
	}
	t.Log("User[10004]:", helper.JsonEncode(content))

	//再次新增一个文档, 应该随着Close落盘固化
	docId, err = table.AddDoc(map[string]string{TEST_FIELD0: "10005",TEST_FIELD1: "唐伯虎",	TEST_FIELD2: "31",TEST_FIELD3: "喜欢书法"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add DocId:", docId)
	ids, ok = table.SearchDocs(TEST_FIELD3, "书法")
	if !ok {
		t.Fatal("Can't find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids))

	//关闭, 应该会落地最后一个文档的新增变化, 下一个函数测试
	table.Close()

	t.Log("\n\n")
}

func TestLoad(t *testing.T) {
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		t.Fatal("LoadTable Error:", err)
	}
	//测试倒排搜索(磁盘)
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		t.Fatal("Should exist")
	}
	if docNode.DocId != 1 {
		t.Fatal("Should is 1")
	}

	ids, ok := table.SearchDocs(TEST_FIELD3, "美食")
	if !ok {
		t.Fatal("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	ids, ok = table.SearchDocs(TEST_FIELD3, "书法")
	if !ok {
		t.Fatal("Can't find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids)) //测试最后一个由Persist落地的文档

	//测试正排搜索(磁盘)
	docId := docNode.DocId
	t.Log("Get doc ", docId)
	content,exist := table.getDoc(docId)
	if !exist {
		t.Fatal("Should exist")
		table.Close()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	docId = 4
	t.Log("Get doc ", docId)
	content,exist = table.getDoc(docId)
	if !exist {
		t.Fatal("Should exist")
		table.Close()
	}
	t.Log("User[10005]:", helper.JsonEncode(content))

	//测试编辑
	content = map[string]string{TEST_FIELD0: "10005",TEST_FIELD1: "唐伯虎",	TEST_FIELD2: "33",TEST_FIELD3: "喜欢秋香"}
	docId, err = table.UpdateDoc(content)
	if err != nil {
		t.Fatal("UpdateDoc error:", err)
	}
	if docId != 5 {
		t.Fatal("Error")
	}
	docNode, exist = table.findDocIdByPrimaryKey("10005") //找回来试试
	if !exist {
		t.Fatal("Should exist")
	}
	t.Log(helper.JsonEncode(docNode))
	content,exist = table.getDoc(docNode.DocId)
	if !exist {
		t.Fatal("Should exist")
		table.Close()
	}
	t.Log(helper.JsonEncode(content))

	//测试删除
	b := table.DeleteDoc("10005")
	if !b {
		t.Fatal("DeleteDoc Err")
	}
	docNode, exist = table.findDocIdByPrimaryKey("10005") //找回来试试
	if exist {
		t.Fatal("Should not exist")
	} else {
		t.Log("10005 is delete")
	}

	//关闭
	table.Close()
	t.Log("\n\n")
}

//再次Load回来测试, 看看上面的编辑和删除是否生效
func TestLoadAgain(t *testing.T) {
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		t.Fatal("LoadTable Error:", err)
	}

	//测试倒排搜索(磁盘)
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		t.Fatal("Should exist")
	}
	if docNode.DocId != 1 {
		t.Fatal("Should is 1")
	}

	ids, ok := table.SearchDocs(TEST_FIELD3, "美食")
	if !ok {
		t.Fatal("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	ids, ok = table.SearchDocs(TEST_FIELD3, "书法")
	if ok {
		t.Fatal("should not find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids)) //测试最后一个由Persist落地的文档

	//测试正排搜索(磁盘)
	docId := docNode.DocId
	t.Log("Get doc ", docId)
	content,exist := table.getDoc(docId)
	if !exist {
		t.Fatal("Should exist")
		table.Close()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	_, exist = table.GetDoc("10005") //找回来试试
	if exist {
		t.Fatal("Should not exist")
	} else {
		t.Log("10005 is delete")
	}

	//关闭
	table.Close()
	t.Log("\n\n")
}

func TestMerge(t *testing.T) {
	//加载回来
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		t.Fatal("LoadTable Error:", err)
	}

	//TODO
	//合并没考虑到内存分区啊
	content := map[string]string{TEST_FIELD0: "10005",TEST_FIELD1: "祝枝山",	TEST_FIELD2: "33",TEST_FIELD3: "喜欢石榴"}
	docId, err := table.AddDoc(content)
	if err != nil {
		t.Fatal("UpdateDoc error:", err)
	}

	//合并!!
	err = table.MergePartitions()
	if err != nil {
		t.Fatal("MergePartitions Error:", err)
	}

	//测试倒排搜索(磁盘)
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		t.Fatal("Should exist")
	}
	if docNode.DocId != 1 {
		t.Fatal("Should is 1")
	}

	ids, ok := table.SearchDocs(TEST_FIELD3, "美食")
	if !ok {
		t.Fatal("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	ids, ok = table.SearchDocs(TEST_FIELD3, "书法")
	if ok {
		t.Fatal("should not find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids)) //测试最后一个由Persist落地的文档

	//测试正排搜索(磁盘)
	docId = docNode.DocId
	t.Log("Get doc ", docId)
	content,exist = table.getDoc(docId)
	if !exist {
		t.Fatal("Should exist")
		table.Close()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	content, exist = table.GetDoc("10005") //找回来试试
	if exist {
		t.Fatal("Should not exist", helper.JsonEncode(content))
	} else {
		t.Log("10005 is delete")
	}
}