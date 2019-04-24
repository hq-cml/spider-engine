package table

import (
	"os"
	"os/exec"

	"testing"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/utils/helper"
	"fmt"
)

const TEST_TABLE = "user"         //用户
const TEST_FIELD0 = "user_id"
const TEST_FIELD1 = "user_name"
const TEST_FIELD2 = "user_age"
const TEST_FIELD3 = "user_desc"
const TEST_FIELD4 = "tobe_del"

func init() {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
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
		panic(err)
	}

	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD1,
		IndexType: index.IDX_TYPE_STRING,
	}); err != nil {
		panic(err)
	}
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD2,
		IndexType: index.IDX_TYPE_INTEGER,
	}); err != nil {
		panic(err)
	}
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD3,
		IndexType: index.IDX_TYPE_STRING_SEG,
	}); err != nil {
		panic(err)
	}
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD4,
		IndexType: index.IDX_TYPE_INTEGER,
	}); err != nil {
		panic(err)
	}

	docId, err := table.AddDoc(map[string]interface{}{TEST_FIELD0: "10001", TEST_FIELD1: "张三",TEST_FIELD2: 20,TEST_FIELD3: "喜欢美食,也喜欢旅游", TEST_FIELD4: 77})

	if err != nil {
		panic(fmt.Sprintf("AddDoc Error:%s", err))
	}
	t.Log("Add DocId:", docId)

	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10002", TEST_FIELD1: "李四", TEST_FIELD2: 28, TEST_FIELD3: "喜欢电影,也喜欢美食", TEST_FIELD4: 88})
	if err != nil {
		panic(fmt.Sprintf("AddDoc Error:%s", err))
	}
	t.Log("Add DocId:", docId)

	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10003",TEST_FIELD1: "王二麻",	TEST_FIELD2: 30,TEST_FIELD3: "喜欢养生", TEST_FIELD4: 99})
	if err != nil {
		panic(fmt.Sprintf("AddDoc Error:%s", err))
	}
	t.Log("Add DocId:", docId)

	//测试倒排搜索(内存)
	t.Log("Before Persist")
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		panic("Should exist")
	}
	if docNode.DocId != 1 {
		panic("Should is 1")
	}

	ids, ok := table.SearchDocs(TEST_FIELD3, "美食", nil)
	if !ok {
		panic("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	//测试正排获取(内存)
	docId = docNode.DocId
	t.Log("Get doc ", docId)
	content,exist := table.getDocByDocId(docId)
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	//测试落地
	err = table.Persist()
	if err != nil {
		panic(fmt.Sprintf("Persist Error:%s", err))
		table.DoClose()
	}

	//测试落地后能否直接从磁盘读取
	t.Log("After Persist")
	docNode, exist = table.findDocIdByPrimaryKey("10002")
	if !exist {
		panic("Should exist")
	}
	if docNode.DocId != 1 {
		panic("Should is 1")
	}

	ids, ok = table.SearchDocs(TEST_FIELD3, "美食", nil)
	if !ok {
		panic("Can't find")
	}
	t.Log(helper.JsonEncode(ids))
	docId = docNode.DocId
	t.Log("Get doc ", docId)
	content,exist = table.getDocByDocId(docId)
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))


	//再次新增一个文档
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10004",TEST_FIELD1: "爱新觉罗", TEST_FIELD2: 30,TEST_FIELD3: "喜欢打仗", TEST_FIELD4: 99})
	if err != nil {
		panic(fmt.Sprintf("AddDoc Error:%s", err))
	}
	t.Log("Add DocId:", docId)

	//删除一个分区
	err = table.DeleteField(TEST_FIELD4)
	if err != nil {
		panic(fmt.Sprintf("Del field Error:%s", err))
	}
	content,exist = table.getDocByDocId(docId)
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log("User[10004]:", helper.JsonEncode(content))

	//再次新增一个文档, 应该随着Close落盘固化
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10005",TEST_FIELD1: "唐伯虎",	TEST_FIELD2: 31,TEST_FIELD3: "喜欢书法"})
	if err != nil {
		panic(fmt.Sprintf("AddDoc Error:%s", err))
	}
	t.Log("Add DocId:", docId)
	ids, ok = table.SearchDocs(TEST_FIELD3, "书法", nil)
	if !ok {
		panic("Can't find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids))

	//关闭, 应该会落地最后一个文档的新增变化, 下一个函数测试
	table.DoClose()

	t.Log("\n\n")
}

func TestLoad(t *testing.T) {
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		panic(fmt.Sprintf("Load table Error:%s", err))
	}
	//测试倒排搜索(磁盘)
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		panic("Should exist")
	}
	if docNode.DocId != 1 {
		panic("Should is 1")
	}

	ids, ok := table.SearchDocs(TEST_FIELD3, "美食", nil)
	if !ok {
		panic("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	ids, ok = table.SearchDocs(TEST_FIELD3, "书法", nil)
	if !ok {
		panic("Can't find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids)) //测试最后一个由Persist落地的文档

	//测试正排搜索(磁盘)
	docId := docNode.DocId
	t.Log("Get doc ", docId)
	content,exist := table.getDocByDocId(docId)
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	docId = 4
	t.Log("Get doc ", docId)
	content,exist = table.getDocByDocId(docId)
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log("User[10005]:", helper.JsonEncode(content))

	//测试编辑
	content = map[string]interface{}{TEST_FIELD0: "10005",TEST_FIELD1: "唐伯虎",	TEST_FIELD2: 33,TEST_FIELD3: "喜欢秋香"}
	docId, err = table.UpdateDoc(content)
	if err != nil {
		panic(fmt.Sprintf("Update doc Error:%s", err))
	}
	if docId != 5 {
		panic("Error")
	}
	content, exist = table.GetDoc("10005") //找回来试试
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log(helper.JsonEncode(content))

	//测试删除
	b := table.DeleteDoc("10005")
	if !b {
		panic("DeleteDoc Err")
	}
	docNode, exist = table.findDocIdByPrimaryKey("10005") //找回来试试
	if exist {
		panic("Should not exist")
	} else {
		t.Log("10005 is delete")
	}

	//关闭
	table.DoClose()
	t.Log("\n\n")
}

//再次Load回来测试, 看看上面的编辑和删除是否生效
func TestLoadAgain(t *testing.T) {
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		panic(fmt.Sprintf("Load table Error:%s", err))
	}

	//测试倒排搜索(磁盘)
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		panic("Should exist")
	}
	if docNode.DocId != 1 {
		panic("Should is 1")
	}

	ids, ok := table.SearchDocs(TEST_FIELD3, "美食", nil)
	if !ok {
		panic("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	ids, ok = table.SearchDocs(TEST_FIELD3, "书法", nil)
	if ok {
		panic("should not find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids)) //测试最后一个由Persist落地的文档

	//测试正排搜索(磁盘)
	docId := docNode.DocId
	t.Log("Get doc ", docId)
	content,exist := table.getDocByDocId(docId)
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))

	_, exist = table.GetDoc("10005") //找回来试试
	if exist {
		panic("Should not exist")
	} else {
		t.Log("10005 is delete")
	}

	//关闭
	table.DoClose()
	t.Log("\n\n")
}

func TestMerge(t *testing.T) {
	//加载回来
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		panic(fmt.Sprintf("Load table Error:%s", err))
	}

	//找一个已经删除的来试试
	content, exist := table.GetDoc("10005")
	if exist {
		panic("Should not exist")
	} else {
		t.Log("10005 is delete")
	}

	//新增一个试试看
	content = map[string]interface{}{TEST_FIELD0: "10005",TEST_FIELD1: "祝枝山",	TEST_FIELD2: 33,TEST_FIELD3: "喜欢石榴"}
	docId, err := table.AddDoc(content)
	if err != nil {
		panic(fmt.Sprintf("Update doc Error:%s", err))
	}
	t.Log("Add new docId:", docId)

	//合并!!
	err = table.MergePartitions()
	if err != nil {
		panic(fmt.Sprintf("Merge partition Error:%s", err))
	}

	//测试倒排搜索(磁盘)
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		panic("Should exist")
	}
	if docNode.DocId != 1 {
		panic("Should is 1")
	}
	docId = docNode.DocId
	t.Log("Get doc ", docId)
	content,exist = table.getDocByDocId(docId)
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))


	ids, ok := table.SearchDocs(TEST_FIELD3, "美食", nil)
	if !ok {
		panic("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	ids, ok = table.SearchDocs(TEST_FIELD3, "书法", nil)
	if ok {
		panic("should not find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids)) //测试最后一个由Persist落地的文档

	//搜索一个重新增加的doc
	content, exist = table.GetDoc("10005") //找回来试试
	if !exist {
		panic("Should exist")
	}
	t.Log(helper.JsonEncode(content))

	//关闭
	table.DoClose()
	t.Log("\n\n")
}

//测试被合并之后,再load回来
func TestMergeThenLoad(t *testing.T) {
	//加载回来
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		panic(fmt.Sprintf("Load table Error:%s", err))
	}

	//找一个曾经删除过,后来又加回来的试试看
	content, exist := table.GetDoc("10005")
	if !exist {
		panic("Should exist")
	}
	t.Log(helper.JsonEncode(content))

	//测试倒排搜索(磁盘)
	docNode, exist := table.findDocIdByPrimaryKey("10002")
	if !exist {
		panic("Should exist")
	}
	if docNode.DocId != 1 {
		panic("Should is 1")
	}
	docId := docNode.DocId
	t.Log("Get doc ", docId)
	content,exist = table.getDocByDocId(docId)
	if !exist {
		panic("Should exist")
		table.DoClose()
	}
	t.Log("User[10002]:", helper.JsonEncode(content))


	ids, ok := table.SearchDocs(TEST_FIELD3, "美食", nil)
	if !ok {
		panic("Can't find")
	}
	t.Log(helper.JsonEncode(ids))

	ids, ok = table.SearchDocs(TEST_FIELD3, "书法", nil)
	if ok {
		panic("should not find")
	}
	t.Log("唐伯虎", helper.JsonEncode(ids))

	table.DoClose()
	t.Log("\n\n")
}

//测试碎片化的merge
func TestMultiMerge(t *testing.T) {
	//清理目录
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}

	//新建表
	table := NewEmptyTable("/tmp/spider", TEST_TABLE)

	//新建字段
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD0,
		IndexType: index.IDX_TYPE_PK,
	}); err != nil {
		panic(err)
	}
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD1,
		IndexType: index.IDX_TYPE_STRING,
	}); err != nil {
		panic(err)
	}
	if err := table.AddField(field.BasicField{
		FieldName: TEST_FIELD2,
		IndexType: index.IDX_TYPE_INTEGER,
	}); err != nil {
		panic(err)
	}

	//增加doc, 表落地
	docId, _ := table.AddDoc(map[string]interface{}{TEST_FIELD0: "10001", TEST_FIELD1: "张0",TEST_FIELD2: 20})
	docId, _ = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10002", TEST_FIELD1: "李一", TEST_FIELD2: 18})
	table.Persist()

	//增加doc, 表落地
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10003",TEST_FIELD1: "王二", TEST_FIELD2: 30})
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10004",TEST_FIELD1: "陈三", TEST_FIELD2: 35})
	table.Persist()

	//增加doc, 表落地
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10005",TEST_FIELD1: "黄四", TEST_FIELD2: 30})
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10006",TEST_FIELD1: "何五", TEST_FIELD2: 35})
	table.Persist()

	//增加doc, 表落地
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10007",TEST_FIELD1: "宋六", TEST_FIELD2: 35})
	table.Persist()

	//增加doc, 表落地
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10008",TEST_FIELD1: "刘七", TEST_FIELD2: 35})
	table.Persist()

	//增加doc, 表落地
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10009",TEST_FIELD1: "任八", TEST_FIELD2: 35})
	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10010",TEST_FIELD1: "化九", TEST_FIELD2: 35})
	table.Persist()

	docId, err = table.AddDoc(map[string]interface{}{TEST_FIELD0: "10011",TEST_FIELD1: "钟十", TEST_FIELD2: 35})
	_ = docId
	t.Log(table.displayInner())

	//测试一下搜索
	docs, ok := table.SearchDocs(TEST_FIELD1, "刘七", nil)
	if !ok {
		panic("shuoud exist")
	}
	t.Log(helper.JsonEncode(docs))

	user, ok := table.GetDoc("10003")
	if !ok {
		panic("shuoud exist")
	}
	t.Log("User[10003]:", helper.JsonEncode(user))

	//启动合并
	err = table.MergePartitions()
	if err != nil {
		panic(fmt.Sprintf("Merge parition Error:%s", err))
	}

	//再次测试一下搜索
	docs, ok = table.SearchDocs(TEST_FIELD1, "刘七", nil)
	if !ok {
		panic("shuoud exist")
	}
	t.Log(helper.JsonEncode(docs))

	user, ok = table.GetDoc("10003")
	if !ok {
		panic("shuoud exist")
	}
	t.Log("User[10003]:", helper.JsonEncode(user))
	t.Log(table.displayInner())

	//关闭
	table.DoClose()
	t.Log("\n\n")
}

func TestLoadAgainAgain(t *testing.T) {
	//加载回来
	table, err := LoadTable("/tmp/spider", TEST_TABLE)
	if err != nil {
		panic(fmt.Sprintf("Load table Error:%s", err))
	}

	docs, ok := table.SearchDocs(TEST_FIELD1, "刘七", nil)
	if !ok {
		panic("shuoud exist")
	}
	t.Log(helper.JsonEncode(docs))

	user, ok := table.GetDoc("10003")
	if !ok {
		panic("shuoud exist")
	}
	t.Log("User[10003]:", helper.JsonEncode(user))
	t.Log(table.displayInner())

	//关闭
	table.DoClose()
	t.Log("\n\n")
}
