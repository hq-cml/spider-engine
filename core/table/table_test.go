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

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -f /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

func TestNewTableAndPersist(t *testing.T) {
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
		IndexType: index.IDX_TYPE_STRING_SINGLE,
	}); err != nil {
		t.Fatal(err)
	}

	docId, err := table.AddDoc(map[string]string{TEST_FIELD0: "10001", TEST_FIELD1: "张三",TEST_FIELD2: "20",TEST_FIELD3: "喜欢登山,也喜欢旅游"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add DocId:", docId)

	docId, err = table.AddDoc(map[string]string{TEST_FIELD0: "10002", TEST_FIELD1: "李四", TEST_FIELD2: "18", TEST_FIELD3: "喜欢电影,也喜欢美食"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add DocId:", docId)

	docId, err = table.AddDoc(map[string]string{TEST_FIELD0: "10003",TEST_FIELD1: "王二麻",	TEST_FIELD2: "30",TEST_FIELD3: "喜欢养生"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add DocId:", docId)


	//测试倒排搜索(内存)
	docNode, exist := table.findPrimaryDockId("10002")
	if !exist {
		t.Fatal("Should exist")
	}
	if docNode.DocId != 1 {
		t.Fatal("Should is 1")
	}

	//测试正排获取(内存)
	docId = docNode.DocId
	t.Log("Get doc ", docId)
	content,exist := table.GetDoc(docId)
	if !exist {
		t.Fatal("Should exist")
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	//测试落地
	err = table.Persist()
	if err != nil {
		t.Fatal("Persist Error:", err)
	}
	table.Close()

	t.Log("\n\n")
}

func TestLoad(t *testing.T) {
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		t.Fatal("LoadTable Error:", err)
	}
	//测试倒排搜索
	docNode, exist := table.findPrimaryDockId("10002")
	if !exist {
		t.Fatal("Should exist")
	}
	if docNode.DocId != 1 {
		t.Fatal("Should is 1")
	}

	//测试正排获取
	docId := docNode.DocId
	t.Log("Get doc ", docId)
	content,exist := table.GetDoc(docId)
	if !exist {
		t.Fatal("Should exist")
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	t.Log("\n\n")
}