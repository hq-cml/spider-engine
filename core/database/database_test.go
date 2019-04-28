package database

import (
	"testing"
	"github.com/hq-cml/spider-engine/utils/helper"
	"os/exec"
	"os"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
	"fmt"
	"github.com/hq-cml/spider-engine/core/table"
)

const TEST_TABLE = "user"         //用户
const TEST_FIELD0 = "user_id"
const TEST_FIELD1 = "user_name"
const TEST_FIELD2 = "user_age"
const TEST_FIELD3 = "user_desc"

var temp_pk string

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

//测试新建库，新建表，增加文档，关闭库表等
//不提供主键，那么就依赖
func TestNewDatabase(t *testing.T) {
	db, err := NewDatabase("/tmp/spider/db1", "db1")
	if err != nil {
		panic(err)
	}

	//测试不提供主键
	_, err = db.CreateTable(TEST_TABLE, []field.BasicField {
		//{
		//	FieldName: TEST_FIELD0,
		//	IndexType: index.IDX_TYPE_PK,
		//},
		{
			FieldName: TEST_FIELD1,
			IndexType: index.IDX_TYPE_STR_WHOLE,
		},{
			FieldName: TEST_FIELD2,
			IndexType: index.IDX_TYPE_INTEGER,
		},{
			FieldName: TEST_FIELD3,
			IndexType: index.IDX_TYPE_STR_SPLITER,
		},
	})
	if err != nil {
		panic(err)
	}

	docId, key, err := db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD1: "张三",TEST_FIELD2: 20,TEST_FIELD3: "喜欢美食,也喜欢旅游"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)

	docId, temp_pk, err = db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD1: "李四", TEST_FIELD2: 18, TEST_FIELD3: "喜欢电影,也喜欢美食"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, temp_pk)

	err = db.DoClose()
	if err != nil {
		panic(err)
	}

	t.Log("\n\n")
}

//测试加载库表，查询库表、删除库表等
func TestLoadDatabase(t *testing.T) {
	db, err := LoadDatabase("/tmp/spider/db1", "db1")
	if err != nil {
		panic(err)
	}

	//!!!!这里从临时变量里面拿到了主键
	user, ok := db.GetDoc(TEST_TABLE, temp_pk)
	if !ok {
		panic("Should exist!")
	}
	t.Log("Got the user[",temp_pk,"]:", helper.JsonEncode(user))

	tmp, ok := db.SearchDocs(TEST_TABLE, TEST_FIELD3, "游泳", nil)
	if ok {
		panic("Should not exist!")
	}
	t.Log("Got the doc[游泳]:", helper.JsonEncode(tmp))

	tmp, ok = db.SearchDocs(TEST_TABLE, TEST_FIELD3, "", nil)
	if !ok {
		panic("Should exist!")
	}
	t.Log("Got the doc[美食]:", helper.JsonEncode(tmp))

	tmp, ok = db.SearchDocs(TEST_TABLE, TEST_FIELD3, "电影", nil)
	if !ok {
		panic("Should exist!")
	}
	t.Log("Got the doc[电影]:", helper.JsonEncode(tmp))

	err = db.DropTable(TEST_TABLE)
	if err != nil {
		panic(err)
	}

	err = db.Destory()
	if err != nil {
		panic(err)
	}

	t.Log("\n\n")
}


//测试一下增加两个相同主键, 逐渐重复校验的效果
func TestDuplicatePrimaryKey(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}

	db, err := NewDatabase("/tmp/spider/db1", "db1")
	if err != nil {
		panic(err)
	}

	_, err = db.CreateTable(TEST_TABLE, []field.BasicField{
		{
			FieldName: TEST_FIELD0,
			IndexType: index.IDX_TYPE_PK,
		},
		{
			FieldName: TEST_FIELD1,
			IndexType: index.IDX_TYPE_STR_WHOLE,
		}, {
			FieldName: TEST_FIELD2,
			IndexType: index.IDX_TYPE_INTEGER,
		}, {
			FieldName: TEST_FIELD3,
			IndexType: index.IDX_TYPE_STR_SPLITER,
		},
	})
	if err != nil {
		panic(err)
	}

	docId, key, err := db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10001", TEST_FIELD1: "张三", TEST_FIELD2: 20, TEST_FIELD3: "喜欢美食,也喜欢旅游"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)

	docId, key, err = db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10001", TEST_FIELD1: "李四", TEST_FIELD2: 18, TEST_FIELD3: "喜欢电影,也喜欢美食"})
	if err == nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)

	//销毁
	err = db.Destory()
	if err != nil {
		panic(err)
	}

	t.Log("\n\n")
}

//重点测试一下文档的编辑和删除
func TestDocUpdateAndDel(t *testing.T) {
	db, err := NewDatabase("/tmp/spider/db1", "db1")
	if err != nil {
		panic(err)
	}

	_, err = db.CreateTable(TEST_TABLE, []field.BasicField {
		{
			FieldName: TEST_FIELD0,
			IndexType: index.IDX_TYPE_PK,
		},
		{
			FieldName: TEST_FIELD1,
			IndexType: index.IDX_TYPE_STR_WHOLE,
		},{
			FieldName: TEST_FIELD2,
			IndexType: index.IDX_TYPE_INTEGER,
		},{
			FieldName: TEST_FIELD3,
			IndexType: index.IDX_TYPE_STR_SPLITER,
		},
	})
	if err != nil {
		panic(err)
	}

	docId, key, err := db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10001", TEST_FIELD1: "张三",TEST_FIELD2: 20,TEST_FIELD3: "喜欢美食,也喜欢旅游"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)

	docId, key, err = db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10002", TEST_FIELD1: "李四", TEST_FIELD2: 18, TEST_FIELD3: "喜欢电影,也喜欢美食"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)

	docId, key, err = db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10003", TEST_FIELD1: "王二", TEST_FIELD2: 13, TEST_FIELD3: "喜欢电脑，也喜欢看书"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)

	docId, key, err = db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10004", TEST_FIELD1: "哈哈", TEST_FIELD2: 11, TEST_FIELD3: "喜欢电脑，也喜欢看书"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)

	docId, key, err = db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10005", TEST_FIELD1: "asdfa", TEST_FIELD2: 13, TEST_FIELD3: "日乐购"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)

	fmt.Println("\nOrgin !!")
	fmt.Println("A------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("A------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	fmt.Println("A------")
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 5 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 5 {
		panic("Err")
	}

	//内存中删一个doc
	db.DeleteDoc(TEST_TABLE, "10005")
	fmt.Println("\nDel doc in mem !!")
	fmt.Println("B------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("B------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	fmt.Println("B------")
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 4 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 4 {
		panic("Err")
	}


	//落盘再加载
	db.DoClose()
	db, err = LoadDatabase("/tmp/spider/db1", "db1")
	if err != nil {
		panic(err)
	}
	fmt.Println("\nAfter reload !!")
	fmt.Println("C------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("C------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	fmt.Println("C------")
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 0 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 0 {
		panic("Err")
	}
	v,ok := db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_IVT_BTREE_NAME, "10004")
	if !ok || v != "3" {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_FWD_BTREE_NAME, "3")
	if !ok || v != "10004" {
		panic("Err")
	}


	//磁盘中删一个doc
	db.DeleteDoc(TEST_TABLE, "10004")
	fmt.Println("\nDel doc in disk !!")
	fmt.Println("D------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("D------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	fmt.Println("D------")
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 0 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 0 {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_IVT_BTREE_NAME, "10004")
	if !ok || v != "3" {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_FWD_BTREE_NAME, "3")
	if !ok || v != "10004" {
		panic("Err")
	}


	//再增加一个Doc，并且还是主键的10004
	docId, key, err = db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10004", TEST_FIELD1: "李冰", TEST_FIELD2: 2000, TEST_FIELD3: "喜欢工程"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)
	//磁盘中删一个doc
	fmt.Println("\nAdd another doc !!")
	fmt.Println("E------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("E------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	fmt.Println("E------")
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 1 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 1 {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_IVT_BTREE_NAME, "10004")
	if !ok || v != "3" {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_FWD_BTREE_NAME, "3")
	if !ok || v != "10004" {
		panic("Err")
	}


	//落盘再加载
	db.DoClose()
	db, err = LoadDatabase("/tmp/spider/db1", "db1")
	if err != nil {
		panic(err)
	}
	fmt.Println("\nAfter reload !!")
	fmt.Println("F------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("F------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	d, ok := db.GetDoc(TEST_TABLE, "10004")
	if !ok {
		panic("Err")
	}
	fmt.Println("F------", helper.JsonEncode(d))
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 0 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 0 {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_IVT_BTREE_NAME, "10004")
	if !ok || v != "5" {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_FWD_BTREE_NAME, "5")
	if !ok || v != "10004" {
		panic("Err")
	}


	//编辑
	db.UpdateDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10004", TEST_FIELD1: "牛顿", TEST_FIELD2: 200, TEST_FIELD3: "喜欢物理"})
	fmt.Println("\nAfter update !!")
	fmt.Println("G------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("G------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	d, ok = db.GetDoc(TEST_TABLE, "10004")
	if !ok {
		panic("Err")
	}
	fmt.Println("G------", helper.JsonEncode(d))
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 0 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 0 {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_IVT_BTREE_NAME, "10004")
	if !ok || v != "6" {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_FWD_BTREE_NAME, "6")
	if !ok || v != "10004" {
		panic("Err")
	}

	//再增加一个Doc，并且还是主键的10005
	docId, key, err = db.AddDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10005", TEST_FIELD1: "爱婴斯坦", TEST_FIELD2: 100, TEST_FIELD3: "喜欢电子"})
	if err != nil {
		panic(err)
	}
	t.Log("Add doc:", docId, key)
	//磁盘中删一个doc
	fmt.Println("\nAdd another doc !!")
	fmt.Println("H------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("H------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	fmt.Println("H------")
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 1 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 1 {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_IVT_BTREE_NAME, "10004")
	if !ok || v != "6" {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_FWD_BTREE_NAME, "6")
	if !ok || v != "10004" {
		panic("Err")
	}


	//编辑内存中的doc
	db.UpdateDoc(TEST_TABLE,
		map[string]interface{}{TEST_FIELD0:"10005", TEST_FIELD1: "莱布尼茨", TEST_FIELD2: 200, TEST_FIELD3: "微积分"})
	fmt.Println("\nAfter update !!")
	fmt.Println("I------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("I------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	d, ok = db.GetDoc(TEST_TABLE, "10005")
	if !ok {
		panic("Err")
	}
	fmt.Println("I------", helper.JsonEncode(d))
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 1 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 1 {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_IVT_BTREE_NAME, "10004")
	if !ok || v != "6" {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_FWD_BTREE_NAME, "6")
	if !ok || v != "10004" {
		panic("Err")
	}


	//落盘再加载
	db.DoClose()
	db, err = LoadDatabase("/tmp/spider/db1", "db1")
	if err != nil {
		panic(err)
	}
	fmt.Println("\nAfter reload !!")
	fmt.Println("J------", db.TableMap[TEST_TABLE].GetIvtMap())
	fmt.Println("J------", db.TableMap[TEST_TABLE].GetFwdMap())
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_IVT_BTREE_NAME)
	db.TableMap[TEST_TABLE].GetBtdb().Display(table.PRI_FWD_BTREE_NAME)
	d, ok = db.GetDoc(TEST_TABLE, "10005")
	if !ok {
		panic("Err")
	}
	fmt.Println("J------", helper.JsonEncode(d))
	if len(db.TableMap[TEST_TABLE].GetIvtMap()) != 0 ||
		len(db.TableMap[TEST_TABLE].GetFwdMap()) != 0 {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_IVT_BTREE_NAME, "10005")
	if !ok || v != "8" {
		panic("Err")
	}
	v,ok = db.TableMap[TEST_TABLE].GetBtdb().GetStr(table.PRI_FWD_BTREE_NAME, "8")
	if !ok || v != "10005" {
		panic("Err")
	}


	//测试搜索
	tmp, ok := db.SearchDocs(TEST_TABLE, TEST_FIELD3, "微积分", nil)
	if !ok {
		panic("Err")
	}
	fmt.Println(helper.JsonEncode(tmp))
	tmp, ok = db.SearchDocs(TEST_TABLE, TEST_FIELD3, "电子", nil)
	if ok {
		panic("Err")
	}
	fmt.Println(helper.JsonEncode(tmp))

	//关闭
	db.DoClose()

	t.Log("\n\n")
}
