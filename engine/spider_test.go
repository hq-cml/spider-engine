package engine

import (
	"os/exec"
	"os"
	"testing"
)

const TEST_TABLE = "user"         //用户
const TEST_FIELD0 = "user_id"
const TEST_FIELD1 = "user_name"
const TEST_FIELD2 = "user_age"
const TEST_FIELD3 = "user_desc"

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

//测试新建库，新建表，增加文档，关闭库表等
func TestInitSpider(t *testing.T) {
	spider, err := InitSpider("/tmp/spider/", "0.1")
	if err != nil {
		t.Fatal(err)
	}

	//_, err = db.CreateTable(TEST_TABLE, []field.BasicField {
	//	{
	//		FieldName: TEST_FIELD0,
	//		IndexType: index.IDX_TYPE_PK,
	//	},{
	//		FieldName: TEST_FIELD1,
	//		IndexType: index.IDX_TYPE_STRING,
	//	},{
	//		FieldName: TEST_FIELD2,
	//		IndexType: index.IDX_TYPE_NUMBER,
	//	},{
	//		FieldName: TEST_FIELD3,
	//		IndexType: index.IDX_TYPE_STRING_SEG,
	//	},
	//})
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//docId, err := db.AddDoc(TEST_TABLE, map[string]string{TEST_FIELD0: "10001", TEST_FIELD1: "张三",TEST_FIELD2: "20",TEST_FIELD3: "喜欢美食,也喜欢旅游"})
	//if err != nil {
	//	t.Fatal("AddDoc Error:", err)
	//}
	//t.Log("Add doc:", docId)
	//
	//docId, err = db.AddDoc(TEST_TABLE, map[string]string{TEST_FIELD0: "10002", TEST_FIELD1: "李四", TEST_FIELD2: "18", TEST_FIELD3: "喜欢电影,也喜欢美食"})
	//if err != nil {
	//	t.Fatal("AddDoc Error:", err)
	//}
	//t.Log("Add doc:", docId)

	err = spider.DoClose()
	if err != nil {
		t.Fatal(err)
	}
}