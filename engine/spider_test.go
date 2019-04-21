package engine

import (
	"os/exec"
	"os"
	"testing"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/utils/helper"
)

const TEST_DATABASE = "db1"
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

//测试初始化Spider，新建库，新建表，增加文档，关闭库表等
func TestInitSpider(t *testing.T) {
	spider, err := InitSpider("/tmp/spider/", "0.1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = spider.CreateDatabase(TEST_DATABASE)
	if err != nil {
		t.Fatal(err)
	}

	err = spider.CreateTable(TEST_DATABASE, TEST_TABLE, []field.BasicField {
		{
			FieldName: TEST_FIELD0,
			IndexType: index.IDX_TYPE_PK,
		},{
			FieldName: TEST_FIELD1,
			IndexType: index.IDX_TYPE_STRING,
		},{
			FieldName: TEST_FIELD2,
			IndexType: index.IDX_TYPE_NUMBER,
		},{
			FieldName: TEST_FIELD3,
			IndexType: index.IDX_TYPE_STRING_SEG,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	docId, err := spider.AddDoc(TEST_DATABASE, TEST_TABLE, map[string]string{TEST_FIELD0: "10001", TEST_FIELD1: "张三",TEST_FIELD2: "20",TEST_FIELD3: "喜欢美食,也喜欢旅游"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add doc:", docId)

	docId, err = spider.AddDoc(TEST_DATABASE, TEST_TABLE, map[string]string{TEST_FIELD0: "10002", TEST_FIELD1: "李四", TEST_FIELD2: "18", TEST_FIELD3: "喜欢电影,也喜欢美食"})
	if err != nil {
		t.Fatal("AddDoc Error:", err)
	}
	t.Log("Add doc:", docId)

	err = spider.DoClose()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n\n")
}

//测试启动spider，加载库表，查询库表、删除库表等
func TestLoadDatabase(t *testing.T) {
	spider, err := InitSpider("/tmp/spider/", "0.1")
	if err != nil {
		t.Fatal(err)
	}

	user, ok := spider.GetDoc(TEST_DATABASE, TEST_TABLE, "10002")
	if !ok {
		t.Fatal("Should exist!")
	}
	t.Log("Got the user[10002]:", helper.JsonEncode(user))

	tmp, ok := spider.SearchDocs(TEST_DATABASE, TEST_TABLE, TEST_FIELD3, "游泳")
	if ok {
		t.Fatal("Should not exist!")
	}
	t.Log("Got the doc[游泳]:", helper.JsonEncode(tmp))

	tmp, ok = spider.SearchDocs(TEST_DATABASE, TEST_TABLE, TEST_FIELD3, "")
	if !ok {
		t.Fatal("Should exist!")
	}
	t.Log("Got the doc[美食]:", helper.JsonEncode(tmp))

	tmp, ok = spider.SearchDocs(TEST_DATABASE, TEST_TABLE, TEST_FIELD3, "电影")
	if !ok {
		t.Fatal("Should exist!")
	}
	t.Log("Got the doc[电影]:", helper.JsonEncode(tmp))

	err = spider.DropTable(TEST_DATABASE, TEST_TABLE)
	if err != nil {
		t.Fatal(err)
	}

	err = spider.DropDatabase(TEST_DATABASE)
	if err != nil {
		t.Fatal(err)
	}
}