package partition

import (
	"testing"
	"os"
	"os/exec"
	"fmt"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/utils/helper"
)

const TEST_TABLE = "user"
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

func TestNewPartitionAndQueryAndPersist(t *testing.T) {
	patitionName := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 0)
	//var basicFields = []field.BasicField{
	//}

	//创建空的分区
	memPartition := NewEmptyPartitionWithBasicFields(patitionName, 0, nil)
	if memPartition.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}

	//新增字段
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING, FwdOffset:0, DocCnt:0})
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD2, IndexType:index.IDX_TYPE_NUMBER, FwdOffset:0, DocCnt:0})
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD3, IndexType:index.IDX_TYPE_STRING_SEG, FwdOffset:0, DocCnt:0})
	if memPartition.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}

	user0 := map[string]string {TEST_FIELD1:"张三", TEST_FIELD2:"20", TEST_FIELD3:"张三喜欢游泳,也喜欢美食"}
	user1 := map[string]string {TEST_FIELD1:"李四", TEST_FIELD2:"30", TEST_FIELD3:"李四喜欢美食,也喜欢文艺"}
	user2 := map[string]string {TEST_FIELD1:"王二", TEST_FIELD2:"25", TEST_FIELD3:"王二喜欢装逼"}

	memPartition.AddDocument(0, user0)
	memPartition.AddDocument(1, user1)
	memPartition.AddDocument(2, user2)

	list, exist := memPartition.Query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 2 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = memPartition.Query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = memPartition.Query(TEST_FIELD3, "游泳")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	c, _ := memPartition.GetDocument(2)
	t.Log(helper.JsonEncode(c))

	s1, _ := memPartition.GetFieldValue(1, TEST_FIELD3)
	t.Log(s1)

	//持久化落地
	memPartition.Persist()

	list, exist = memPartition.Query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 2 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = memPartition.Query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = memPartition.Query(TEST_FIELD3, "游泳")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	d, _ := memPartition.GetDocument(2)
	t.Log(helper.JsonEncode(d))

	s2, _ := memPartition.GetFieldValue(1, TEST_FIELD3)
	t.Log(s2)

	if s1 != s2 {
		t.Fatal("Should ==")
	}
	memPartition.Close()

	t.Log("\n\n")
}

func TestLoad(t *testing.T) {
	patitionName := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 0)
	part, err := LoadPartition(patitionName)
	if err != nil {
		t.Fatal(err)
	}
	list, exist := part.Query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 2 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = part.Query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = part.Query(TEST_FIELD3, "游泳")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	d, _ := part.GetDocument(2)
	t.Log(helper.JsonEncode(d))

	s2, _ := part.GetFieldValue(1, TEST_FIELD3)
	t.Log(s2)

	t.Log("\n\n")
}