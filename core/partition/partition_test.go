package partition

import (
	"os"
	"os/exec"
	"testing"
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
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
}

func TestNewPartitionAndQueryAndPersist(t *testing.T) {
	patitionName := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 0)
	//var coreFields = []field.CoreField{
	//}

	//创建空的分区
	memPartition := NewEmptyPartitionWithBasicFields(patitionName, 0, nil)
	if memPartition.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}

	//新增字段
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING})
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD2, IndexType:index.IDX_TYPE_INTEGER})
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD3, IndexType:index.IDX_TYPE_STRING_SEG})
	if memPartition.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}

	user0 := map[string]interface{} {TEST_FIELD1:"张三", TEST_FIELD2:20, TEST_FIELD3:"张三喜欢游泳,也喜欢美食"}
	user1 := map[string]interface{} {TEST_FIELD1:"李四", TEST_FIELD2:30, TEST_FIELD3:"李四喜欢美食,也喜欢文艺"}
	user2 := map[string]interface{} {TEST_FIELD1:"王二", TEST_FIELD2:25, TEST_FIELD3:"王二喜欢装逼"}

	err := memPartition.AddDocument(0, user0); if err != nil { panic(err) }
	err = memPartition.AddDocument(1, user1); if err != nil { panic(err) }
	err = memPartition.AddDocument(2, user2); if err != nil { panic(err) }

	if memPartition.Fields[TEST_FIELD1].DocCnt != 3 {
		t.Fatal("wrong number")
	}

	list, exist := memPartition.query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 2 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = memPartition.query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = memPartition.query(TEST_FIELD3, "游泳")
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

	list, exist = memPartition.query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 2 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = memPartition.query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = memPartition.query(TEST_FIELD3, "游泳")
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
	memPartition.DoClose()

	t.Log("\n\n")
}

func TestLoad(t *testing.T) {
	patitionName := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 0)
	part, err := LoadPartition(patitionName)
	if err != nil {
		t.Fatal(err)
	}
	list, exist := part.query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 2 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = part.query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 2!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = part.query(TEST_FIELD3, "游泳")
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

func TestPartitionMerge(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
	patitionName0 := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 0)
	//创建分区1
	part0 := NewEmptyPartitionWithBasicFields(patitionName0, 0, []field.BasicField{
		{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING},
		{FieldName:TEST_FIELD2, IndexType:index.IDX_TYPE_INTEGER},
		{FieldName:TEST_FIELD3, IndexType:index.IDX_TYPE_STRING_SEG},
	})
	if part0.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}
	if part0.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}
	user0 := map[string]interface{} {TEST_FIELD1:"张三", TEST_FIELD2:20, TEST_FIELD3:"喜欢游泳,也喜欢美食"}
	user1 := map[string]interface{} {TEST_FIELD1:"李四", TEST_FIELD2:30, TEST_FIELD3:"喜欢美食,也喜欢文艺"}
	user2 := map[string]interface{} {TEST_FIELD1:"王二", TEST_FIELD2:25, TEST_FIELD3:"喜欢装逼"}
	err = part0.AddDocument(0, user0); if err != nil { panic(err) }
	err = part0.AddDocument(1, user1); if err != nil { panic(err) }
	err = part0.AddDocument(2, user2); if err != nil { panic(err) }

	//创建分区2
	patitionName1 := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 1)
	part1 := NewEmptyPartitionWithBasicFields(patitionName1, 3, []field.BasicField{
		{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING},
		{FieldName:TEST_FIELD2, IndexType:index.IDX_TYPE_INTEGER},
		{FieldName:TEST_FIELD3, IndexType:index.IDX_TYPE_STRING_SEG},
	})
	if part1.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}
	if part1.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}
	user3 := map[string]interface{} {TEST_FIELD1:"赵六", TEST_FIELD2:22, TEST_FIELD3:"喜欢打牌,也喜欢美食"}
	user4 := map[string]interface{} {TEST_FIELD1:"钱七", TEST_FIELD2:29, TEST_FIELD3:"喜欢旅游,也喜欢音乐"}
	user5 := map[string]interface{} {TEST_FIELD1:"李八", TEST_FIELD2:24, TEST_FIELD3:"喜欢睡觉"}
	err = part1.AddDocument(3, user3); if err != nil { panic(err) }
	err = part1.AddDocument(4, user4); if err != nil { panic(err) }
	err = part1.AddDocument(5, user5); if err != nil { panic(err) }

	//新建的两个分区落地
	//一定要落地先一次，两个分区的btdb才能被正确设置，否则无法进行合并
	part0.Persist()
	part1.Persist()
	part0.btdb.Display(TEST_FIELD1)
	part1.btdb.Display(TEST_FIELD1)

	//外插花一个分区, 准备合并
	patitionName2 := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 2)
	part2 := NewEmptyPartitionWithBasicFields(patitionName2, 6, []field.BasicField{
		{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING},
		{FieldName:TEST_FIELD2, IndexType:index.IDX_TYPE_INTEGER},
		{FieldName:TEST_FIELD3, IndexType:index.IDX_TYPE_STRING_SEG},
	})
	defer part2.DoClose()

	//合并
	err = part2.MergePersistPartitions([]*Partition{part0, part1})
	if err != nil {
		t.Fatal(err)
	}

	//part2.Fields[TEST_FIELD3].IvtIdx.GetBtree().Display(TEST_FIELD3)


	//合并完毕, 测试合并效果
	list, exist := part2.query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 3!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 6 {
		t.Fatal("Should 6!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD3, "游泳")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 1!!")
	}

	t.Log(helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD1, "李八")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 1!!")
	}
	t.Log(helper.JsonEncode(list))

	d, ok := part2.GetDocument(2)
	if !ok {
		t.Fatal("Shuold exist")
	}
	v, ok := d[TEST_FIELD2].(int64)
	if !ok || v != 25 {
		t.Fatal("Error", d[TEST_FIELD2])
	}
	t.Log(helper.JsonEncode(d))

	s2, ok := part2.GetFieldValue(1, TEST_FIELD3)
	if !ok {
		t.Fatal("Shuold exist")
	}
	if s2 != "喜欢美食,也喜欢文艺" {
		t.Fatal("Error")
	}
	t.Log(s2)

	t.Log(helper.JsonEncode(part2.CoreFields))

	t.Log("\n\n")

}

func TestLoadMerge(t *testing.T) {
	//t.Skip()
	patitionName := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 2)
	part2, err := LoadPartition(patitionName)
	if err != nil {
		t.Fatal(err)
	}

	//测试Load回来的结果
	list, exist := part2.query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 3!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 6 {
		t.Fatal("Should 6!!")
	}
	t.Log(helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD3, "游泳")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 1!!")
	}

	t.Log(helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD1, "李八")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 1!!")
	}
	t.Log(helper.JsonEncode(list))

	d, ok := part2.GetDocument(2)
	if !ok {
		t.Fatal("Shuold exist")
	}
	v, ok := d[TEST_FIELD2].(int64)
	if !ok || v != 25 {
		t.Fatal("Error", d[TEST_FIELD2])
	}
	t.Log(helper.JsonEncode(d))

	s2, ok := part2.GetFieldValue(1, TEST_FIELD3)
	if !ok {
		t.Fatal("Shuold exist")
	}
	if s2 != "喜欢美食,也喜欢文艺" {
		t.Fatal("Error")
	}
	t.Log(s2)
	t.Log(helper.JsonEncode(part2.CoreFields))
	t.Log("\n\n")
}

//测试在字段出现新增前后，分区的合并情况
//这个用例主要在测试底层索引的fake字段的使用
func TestPartitionMergeAfterFiledChange(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
	patitionName0 := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 0)
	//创建分区1
	part0 := NewEmptyPartitionWithBasicFields(patitionName0, 0, []field.BasicField{
		{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING},
	})
	if part0.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}
	if part0.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}
	user0 := map[string]interface{} {TEST_FIELD1:"张三"}
	user1 := map[string]interface{} {TEST_FIELD1:"李四"}
	user2 := map[string]interface{} {TEST_FIELD1:"王二"}
	err = part0.AddDocument(0, user0); if err != nil { panic(err) }
	err = part0.AddDocument(1, user1); if err != nil { panic(err) }
	err = part0.AddDocument(2, user2); if err != nil { panic(err) }


	//创建分区2, 多了两个字段
	patitionName1 := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 1)
	part1 := NewEmptyPartitionWithBasicFields(patitionName1, 3, []field.BasicField{
		{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING},
		{FieldName:TEST_FIELD2, IndexType:index.IDX_TYPE_INTEGER},
		{FieldName:TEST_FIELD3, IndexType:index.IDX_TYPE_STRING_SEG},
	})
	if part1.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}
	if part1.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}
	user3 := map[string]interface{} {TEST_FIELD1:"赵六", TEST_FIELD2:22, TEST_FIELD3:"喜欢打牌,也喜欢美食"}
	user4 := map[string]interface{} {TEST_FIELD1:"钱七", TEST_FIELD2:29, TEST_FIELD3:"喜欢旅游,也喜欢音乐"}
	user5 := map[string]interface{} {TEST_FIELD1:"李八", TEST_FIELD2:24, TEST_FIELD3:"喜欢睡觉"}
	err = part1.AddDocument(3, user3); if err != nil { panic(err) }
	err = part1.AddDocument(4, user4); if err != nil { panic(err) }
	err = part1.AddDocument(5, user5); if err != nil { panic(err) }

	//新建的两个分区落地
	//一定要落地先一次，两个分区的btdb才能被正确设置，否则无法进行合并
	part0.Persist()
	part1.Persist()
	part0.btdb.Display(TEST_FIELD1)
	part1.btdb.Display(TEST_FIELD1)

	//外插花一个分区, 准备合并
	patitionName2 := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 2)
	part2 := NewEmptyPartitionWithBasicFields(patitionName2, 6, []field.BasicField{
		{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING},
		{FieldName:TEST_FIELD2, IndexType:index.IDX_TYPE_INTEGER},
		{FieldName:TEST_FIELD3, IndexType:index.IDX_TYPE_STRING_SEG},
	})
	defer part2.DoClose()

	//合并
	err = part2.MergePersistPartitions([]*Partition{part0, part1})
	if err != nil {
		panic(err)
	}

	//part2.Fields[TEST_FIELD3].IvtIdx.GetBtree().Display(TEST_FIELD3)


	//合并完毕, 测试合并效果, 测试倒排
	list, exist := part2.query(TEST_FIELD3, "美食")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 3!!")
	}
	t.Log("美食:", helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD3, "喜欢")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 3 {
		t.Fatal("Should 3!!")
	}
	t.Log("喜欢:", helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD3, "游泳")
	if exist {
		t.Fatal("Should not exist!!")
	}
	t.Log("游泳:", helper.JsonEncode(list))

	list, exist = part2.query(TEST_FIELD1, "李八")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 1 {
		t.Fatal("Should 1!!")
	}
	t.Log("李八:", helper.JsonEncode(list))

	//测试正排索引
	d, ok := part2.GetDocument(2)
	if !ok {
		t.Fatal("Shuold exist")
	}
	t.Log("Got doc 2:", helper.JsonEncode(d))

	s2, ok := part2.GetFieldValue(3, TEST_FIELD3)
	if !ok {
		t.Fatal("Shuold exist")
	}
	if s2 != "喜欢打牌,也喜欢美食" {
		t.Fatal("Error")
	}
	t.Log(s2)

	t.Log(helper.JsonEncode(part2.CoreFields))

	t.Log("\n\n")

}

//测试跨字段搜索, 利用上帝视角
func TestQueryByGodField(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", `/bin/rm -rf /tmp/spider/*`)
	_, err := cmd.Output()
	if err != nil {
		os.Exit(1)
	}
	patitionName := fmt.Sprintf("%v%v_%v", "/tmp/spider/", TEST_TABLE, 0)

	//创建空的分区
	memPartition := NewEmptyPartitionWithBasicFields(patitionName, 0, nil)
	if memPartition.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}

	//新增字段
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD1, IndexType:index.IDX_TYPE_STRING})
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD2, IndexType:index.IDX_TYPE_INTEGER})
	memPartition.AddField(field.BasicField{FieldName:TEST_FIELD3, IndexType:index.IDX_TYPE_STRING_SEG})
	if memPartition.IsEmpty() != true {
		t.Fatal("Should empty!!")
	}

	user0 := map[string]interface{}{TEST_FIELD1:"张三", TEST_FIELD2:20, TEST_FIELD3:"喜欢游泳,也喜欢美食"}
	user1 := map[string]interface{}{TEST_FIELD1:"李四", TEST_FIELD2:30, TEST_FIELD3:"李四喜欢美食,也喜欢文艺"}
	user2 := map[string]interface{}{TEST_FIELD1:"王二", TEST_FIELD2:25, TEST_FIELD3:"喜欢张三"}

	err = memPartition.AddDocument(0, user0); if err != nil {
		panic(err)
	}
	err = memPartition.AddDocument(1, user1); if err != nil {
		panic(err)
	}
	err = memPartition.AddDocument(2, user2); if err != nil {
		panic(err)
	}

	list, exist := memPartition.query(GOD_FIELD_NAME, "张三")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 2 {
		t.Fatal("Should 2!!")
	}
	t.Log("上帝视角:张三: ", helper.JsonEncode(list))

	c, _ := memPartition.GetDocument(2)
	t.Log(helper.JsonEncode(c))

	s1, _ := memPartition.GetFieldValue(1, TEST_FIELD3)
	t.Log(s1)

	//持久化落地
	memPartition.Persist()

	list, exist = memPartition.query(GOD_FIELD_NAME, "张三")
	if !exist {
		t.Fatal("Should exist!!")
	}
	if len(list) != 2 {
		t.Fatal("Should 2!!")
	}
	t.Log("上帝视角:张三: ", helper.JsonEncode(list))

	d, _ := memPartition.GetDocument(2)
	t.Log(helper.JsonEncode(d))

	s2, _ := memPartition.GetFieldValue(1, TEST_FIELD3)
	t.Log(s2)

	if s1 != s2 {
		t.Fatal("Should ==")
	}

	memPartition.DoClose()

	t.Log("\n\n")
}