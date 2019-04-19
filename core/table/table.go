package table

/**
 * 表的实现，类比于Mysql的表的概念
 * 一张表的构成：
 *   逻辑上由多个字段Field构成
 *   物理上，是由多个Partiton构成（各个Partition都拥有相同的字段）
 *
 * 一张表，拥有一套完整的索引系统
 * 其每个字段都会默认建立正排索引，并根据需要可选的建立倒排索引
 *
 * 一张表，必须拥有自己的主键，主键是锁定文档的唯一Key，而不是docId
 * 一个文档如果编辑，那么底层docId可能发生变化，但是Key是不变的
 */
import (
	"sync"
	"fmt"
	"encoding/json"
	"errors"
	//"math"
	"github.com/hq-cml/spider-engine/core/partition"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/bitmap"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/core/field"
	"strconv"
)

//表的原则：
// 主键和其他字段不同，单独用一个btree实例存储，不分区
// 最新的文档增加只会操作内存态分区，到达阈值后回落地或者和其他分区合并落地
// 文档的删除采用假删除，通过bitmap标记
type Table struct {
	TableName      string                      `json:"tableName"`
	Path           string                      `json:"pathName"`
	BasicFields    map[string]field.BasicField `json:"fields"`      //包括主键
	PrimaryKey     string                      `json:"primaryKey"`
	StartDocId     uint32                      `json:"startDocId"`
	NextDocId      uint32                      `json:"nextDocId"`
	Prefix         uint64                      `json:"prefix"`
	PartitionNames []string                    `json:"partitionNames"` //磁盘态的分区列表名--这些分区均不包括主键！！！

	memPartition   *partition.Partition   //内存态的分区,分区不包括逐渐
	partitions     []*partition.Partition //磁盘态的分区列表--这些分区均不包括主键！！！
	primaryBtdb    btree.Btree            //主键专用倒排索引（内存态）
	primaryMap     map[string]string      //主键专用倒排索引（磁盘态），主键不会重复，直接map[string]string
	bitMap         *bitmap.Bitmap         //用于文档删除标记
	mutex          sync.Mutex             //锁，当分区持久化到或者合并时使用或者新建分区时使用
}

const (
	PRIMARY_KEY_MEM_MAXCNT = 50000   //TODO 放小了测试
)

//新建空表
func NewEmptyTable(path, name string) *Table {
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}
	table := Table{
		TableName:      name,
		PartitionNames: make([]string, 0),
		partitions:     make([]*partition.Partition, 0),
		Path:           path,
		BasicFields:    make(map[string]field.BasicField),
		primaryMap:     make(map[string]string),
	}

	//bitmap文件新建
	btmpName := fmt.Sprintf("%v%v%v", path, name, basic.IDX_FILENAME_SUFFIX_BITMAP)
	table.bitMap = bitmap.NewBitmap(btmpName, false)

	return &table
}

//产出一块空的内存分区
func (tbl *Table) generateMemPartition() {
	partitionName := fmt.Sprintf("%v%v_%010v", tbl.Path, tbl.TableName, tbl.Prefix) //10位数补零
	var basicFields []field.BasicField
	for _, f := range tbl.BasicFields {
		if f.IndexType != index.IDX_TYPE_PK { //剔除主键，其他字段建立架子
			basicFields = append(basicFields, f)
		}
	}

	tbl.memPartition = partition.NewEmptyPartitionWithBasicFields(partitionName, tbl.NextDocId, basicFields)
	tbl.Prefix++
}

//从文件加载一张表
func LoadTable(path, name string) (*Table, error) {
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}
	tbl := Table{}
	metaFileName := fmt.Sprintf("%v%v%v",path, name, basic.IDX_FILENAME_SUFFIX_META)
	buffer, err := helper.ReadFile(metaFileName)
	if err != nil {
		return nil, err
	}
	//json.Unmarshal仅会覆盖大写的公开字段，小写字段包括锁都不受影响
	err = json.Unmarshal(buffer, &tbl)
	if err != nil {
		return nil, err
	}

	//分别加载各个分区
	for _, partitionName := range tbl.PartitionNames {
		segment, err := partition.LoadPartition(partitionName)
		if err != nil {
			log.Errf("partition.LoadPartition Error:%v", err)
			return nil, err
		}
		tbl.partitions = append(tbl.partitions, segment)
	}

	//产出一块空的内存分区
	tbl.generateMemPartition()

	//加载bitmap
	btmpPath := path + name + basic.IDX_FILENAME_SUFFIX_BITMAP
	tbl.bitMap = bitmap.NewBitmap(btmpPath, true)

	//如果表存在主键，则直接加载主键专用btree和内存map
	if tbl.PrimaryKey != "" {
		primaryname := fmt.Sprintf("%v%v_primary%v", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_BTREE)
		tbl.primaryBtdb = btree.NewBtree("", primaryname)
		tbl.primaryMap = make(map[string]string)
	}

	log.Infof("Load Table %v success", tbl.TableName)
	return &tbl, nil
}

//落地表的元信息
func (tbl *Table) StoreMeta() error {
	metaFileName := fmt.Sprintf("%v%v%s", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_META)
	data := helper.JsonEncode(tbl)
	if data != "" {
		if err := helper.WriteToFile([]byte(data), metaFileName); err != nil {
			return err
		}
	} else {
		return errors.New("Json error")
	}

	//将内存态的主键，全部落盘到btree
	if tbl.PrimaryKey != "" {
		tbl.primaryBtdb.MutiSet(tbl.PrimaryKey, tbl.primaryMap)
		tbl.primaryMap = make(map[string]string)
	}
	return nil
}

//新增字段
//Note:
// 新增的字段只会在最新的空分区生效，如果新增的时候有非空的分区，会先落地，然后产出新分区
// 分区的变动，会锁表
func (tbl *Table) AddField(basicField field.BasicField) error {
	//校验
	if _, exist := tbl.BasicFields[basicField.FieldName]; exist {
		log.Warnf("Field %v have Exist ", basicField.FieldName)
		return errors.New(fmt.Sprintf("Field %v have Exist ", basicField.FieldName))
	}

	//实施新增
	tbl.BasicFields[basicField.FieldName] = basicField
	if basicField.IndexType == index.IDX_TYPE_PK {
		//主键新增单独处理
		if tbl.PrimaryKey != "" {
			return errors.New("Primary key has exist!")
		}
		tbl.PrimaryKey = basicField.FieldName
		primaryname := fmt.Sprintf("%v%v_primary%v", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_BTREE)
		tbl.primaryBtdb = btree.NewBtree("", primaryname)
		tbl.primaryBtdb.AddTree(basicField.FieldName)
	} else {
		//锁表
		tbl.mutex.Lock()
		defer tbl.mutex.Unlock()

		if tbl.memPartition == nil {
			//如果内存分区为nil，则直接新增一个内存分区，新增出来的分区已经包含了新的新增字段
			tbl.generateMemPartition()

		} else if tbl.memPartition.IsEmpty() {
			//如果内存分区为空架子，则直接在内存分区新增字段
			err := tbl.memPartition.AddField(basicField)
			if err != nil {
				log.Errf("Add Field Error  %v", err)
				return err
			}
		} else {
			//将当前的内存分区落地，然后新建内存分区，新增出来的分区已经包含了新的新增字段
			tmpPartition := tbl.memPartition
			//分区落地
			if err := tmpPartition.Persist(); err != nil {
				return err
			}
			//归档分区
			tbl.partitions = append(tbl.partitions, tmpPartition)
			tbl.PartitionNames = append(tbl.PartitionNames, tmpPartition.PartitionName)
			//新分区（包含新字段）生成
			tbl.generateMemPartition()
		}
	}

	//元信息落地
	err := tbl.StoreMeta()
	if err != nil {
		return errors.New("StoreMeta Error:" + err.Error())
	}

	return nil
}

//删除分区
//本质上也是一种假删除, 只是把tbl.BasicFields对应项删除, 使得上层查询隐藏该字段
//如果内存分区非空, 则会先落地内存分区
func (tbl *Table) DeleteField(fieldname string) error {
	//校验
	if _, exist := tbl.BasicFields[fieldname]; !exist {
		log.Warnf("Field %v not found ", fieldname)
		return nil
	}
	if fieldname == tbl.PrimaryKey {
		log.Warnf("Field %v is primaryBtdb key can not delete ", fieldname)
		return nil
	}

	//锁表
	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()

	//假删除
	delete(tbl.BasicFields, fieldname)

	if tbl.memPartition == nil {
		//啥也不需要干
		log.Info("Delete field. memPartition is nil. do nothing~")
	} else if tbl.memPartition.IsEmpty() {
		//删除内存分区的字段
		tbl.memPartition.DeleteField(fieldname)
	} else {
		//当前内存分区先落地
		tmpPartition := tbl.memPartition
		if err := tmpPartition.Persist(); err != nil {
			return err
		}
		tbl.partitions = append(tbl.partitions, tmpPartition)
		tbl.PartitionNames = append(tbl.PartitionNames, tmpPartition.PartitionName)
		//产出新的分区(字段已删除）
		tbl.generateMemPartition()
	}

	//元信息落地
	err := tbl.StoreMeta()
	if err != nil {
		return errors.New("StoreMeta Error:" + err.Error())
	}
	return nil
}

//获取文档
func (tbl *Table) GetDoc(primaryKey string) (map[string]string, bool) {
	docNode, exist := tbl.findDocIdByPrimaryKey(primaryKey)
	if !exist {
		return nil, false
	}
	tmp, ok := tbl.getDocByDocId(docNode.DocId)
	if !ok {
		return nil, false
	}
	tmp[tbl.PrimaryKey] = primaryKey
	return tmp, true
}

//删除
//标记假删除
func (tbl *Table) DeleteDoc(primaryKey string) bool {
	docId, found := tbl.findDocIdByPrimaryKey(primaryKey)
	if found {
		return tbl.bitMap.Set(uint64(docId.DocId))
	}
	return true
}

//新增
func (tbl *Table) AddDoc(content map[string]string) (uint32, error) {
	//校验
	if len(tbl.BasicFields) == 0 {
		log.Errf("Field or Partiton is nil")
		return 0, errors.New("field or partition is nil")
	}

	//如果内存分区为空，则新建
	if tbl.memPartition == nil {
		tbl.mutex.Lock()
		tbl.generateMemPartition()
		//意义何在？
		//if err := tbl.StoreMeta(); err != nil {
		//	tbl.mutex.Unlock()
		//	return 0, err
		//}
		tbl.mutex.Unlock()
	}

	newDocId := tbl.NextDocId
	tbl.NextDocId++

	//如果存在主键先添加
	if tbl.PrimaryKey != "" {
		tbl.primaryMap[content[tbl.PrimaryKey]] = fmt.Sprintf("%v", newDocId)

		if tbl.NextDocId % PRIMARY_KEY_MEM_MAXCNT == 0 {
			tbl.primaryBtdb.MutiSet(tbl.PrimaryKey, tbl.primaryMap)
			tbl.primaryMap = make(map[string]string)
		}
	}
	//其他字段新增Doc
	err := tbl.memPartition.AddDocument(newDocId, content)
	if err != nil {
		log.Errf("tbl.memPartition.AddDocument Error:%v", err)
		return 0, err
	}
	return newDocId, nil
}

//变更文档
// Note:
// 如果表没有主键，则不支持变更
// 本质上还是新增，主键不变，但是doc变了
func (tbl *Table) UpdateDoc(content map[string]string) (uint32, error) {
	//校验
	if len(tbl.BasicFields) == 0 {
		log.Errf("Field or Partiton is nil")
		return 0, errors.New("field or partition is nil")
	}
	//如果表没有主键，则不支持变更
	if tbl.PrimaryKey == "" {
		return 0, errors.New("No Primary Key")
	}
	if _, exist := content[tbl.PrimaryKey]; !exist {
		log.Errf("Primary Key Not Found %v", tbl.PrimaryKey)
		return 0, errors.New(fmt.Sprintf("Primary Key Not Found %v", tbl.PrimaryKey))
	}

	//如果内存分区为空，则新建
	if tbl.memPartition == nil {
		tbl.mutex.Lock()
		tbl.generateMemPartition()
		//意义何在？
		//if err := tbl.StoreMeta(); err != nil {
		//	tbl.mutex.Unlock()
		//	return 0, err
		//}
		tbl.mutex.Unlock()
	}

	//本质上仍然是新增文档
	newDocId := tbl.NextDocId
	tbl.NextDocId++

	//先标记删除oldDocId
	oldDocid, found := tbl.findDocIdByPrimaryKey(content[tbl.PrimaryKey])
	if found {
		tbl.bitMap.Set(uint64(oldDocid.DocId))
	}

	//再新增docId
	if err := tbl.changeDocIdByPrimaryKey(content[tbl.PrimaryKey], newDocId); err != nil {
		return 0, err
	}

	//实质内容本质上还是新增，主键不变，但是doc变了
	err := tbl.memPartition.AddDocument(newDocId, content)
	if err != nil {
		log.Errf("tbl.memPartition.AddDocument Error:%v", err)
		return 0, err
	}
	return newDocId, nil
}

//内部获取
//Note:
// 不包括主键
func (tbl *Table) getDocByDocId(docId uint32) (map[string]string, bool) {
	//校验
	if docId < tbl.StartDocId || docId >= tbl.NextDocId {
		return nil, false
	}
	fieldNames := []string{}
	for _, v := range tbl.BasicFields {
		fieldNames = append(fieldNames, v.FieldName)
	}

	//如果在内存分区, 则从内存分区获取
	if tbl.memPartition != nil &&
		(docId >= tbl.memPartition.StartDocId && docId <  tbl.memPartition.NextDocId) {
		return  tbl.memPartition.GetValueWithFields(docId, fieldNames)
	}

	//否则尝试从磁盘分区获取
	for _, prt := range tbl.partitions {
		if docId >= prt.StartDocId && docId < prt.NextDocId {
			return prt.GetValueWithFields(docId, fieldNames)
		}
	}
	return nil, false
}

//内部变更篡改了docId
func (tbl *Table) changeDocIdByPrimaryKey(key string, docId uint32) error {
	if _, exist := tbl.primaryMap[key]; exist {
		val := strconv.Itoa(int(docId))
		tbl.primaryMap[key] = val
		return nil
	}

	err := tbl.primaryBtdb.Set(tbl.PrimaryKey, key, uint64(docId))
	if err != nil {
		log.Errf("Update Put key error  %v", err)
		return err
	}
	return nil
}

//根据主键找到内部的docId
func (tbl *Table) findDocIdByPrimaryKey(key string) (basic.DocNode, bool) {
	//现在内存map中找
	var val int
	var err error

	if v, exist := tbl.primaryMap[key]; exist {
		val , err = strconv.Atoi(v)
		if err != nil {
			return basic.DocNode{}, false
		}
	} else {
		//再在磁盘btree中找
		vv, ok := tbl.primaryBtdb.GetInt(tbl.PrimaryKey, key)
		if !ok {
			return basic.DocNode{}, false
		}
		val = int(vv)
	}

	if tbl.bitMap.GetBit(uint64(val)) == 1 {
		return basic.DocNode{}, false
	}

	return basic.DocNode{DocId: uint32(val)}, true
}

//表落地
func (tbl *Table) Persist() error {

	if tbl.memPartition == nil {
		return nil
	}
	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()

	return tbl.persistMemPartition()
}

//将内存分区落盘
func (tbl *Table) persistMemPartition() error {

	if tbl.memPartition == nil || tbl.memPartition.IsEmpty() {
		return nil
	}
	//分区落地
	tmpPartition := tbl.memPartition
	if err := tmpPartition.Persist(); err != nil {
		return err
	}
	//归档分区
	tbl.partitions = append(tbl.partitions, tmpPartition)
	tbl.PartitionNames = append(tbl.PartitionNames, tmpPartition.PartitionName)
	tbl.memPartition = nil

	return tbl.StoreMeta()
}

//关闭一张表
func (tbl *Table) Close() error {
	//锁表
	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()
	log.Infof("Close Table [%v] Begin", tbl.TableName)

	//关闭内存分区(非空则需要先落地)
	if tbl.memPartition != nil {
		tbl.persistMemPartition() //内存分区落地
	}

	//逐个关闭磁盘分区
	for _, prt := range tbl.partitions {
		prt.Close()
	}

	//关闭主键btdb
	if tbl.primaryBtdb != nil {
		tbl.primaryBtdb.MutiSet(tbl.PrimaryKey, tbl.primaryMap)
		tbl.primaryBtdb.Close()
	}

	//关闭btmp
	if tbl.bitMap != nil {
		tbl.bitMap.Close()
	}

	log.Infof("Close Table [%v] Finish", tbl.TableName)
	return nil
}

//合并表内分区
//TODO 合并逻辑还不够智能
//TODO 合并时机需要自动化
func (tbl *Table) MergePartitions() error {
	//校验
	if len(tbl.partitions) == 1 {
		return nil
	}

	var startIdx int = -1

	//锁表
	tbl.mutex.Lock()
	defer tbl.mutex.Unlock()

	//内存分区非空则先落地
	if tbl.memPartition != nil {
		tbl.persistMemPartition()
	}

	//找到第一个个数不符合规范，即要合并的partition
	for idx := range tbl.partitions {
		if tbl.partitions[idx].NextDocId - tbl.partitions[idx].StartDocId < partition.PARTITION_MAX_DOC_CNT {
			startIdx = idx
			break
		}
	}

	if startIdx == -1 {
		return nil
	}
	todoPartitions := tbl.partitions[startIdx:]
	if len(todoPartitions) == 1 {
		log.Infof("No nessary to merge!")
		return nil
	}

	//开始合并
	partitionName := fmt.Sprintf("%v%v_%010v", tbl.Path, tbl.TableName, tbl.Prefix) //10位数补零
	tbl.Prefix++
	var basicFields []field.BasicField
	for _, f := range tbl.BasicFields {
		//主键分区是独立的存在，没必要参与到分区合并中
		if f.IndexType != index.IDX_TYPE_PK {
			basicFields = append(basicFields, f)
		}
	}

	//生成内存分区骨架，开始合并
	tmpPartition := partition.NewEmptyPartitionWithBasicFields(partitionName, tbl.NextDocId, basicFields)
	//if err := tbl.StoreMeta(); err != nil {
	//	return err
	//}
	err := tmpPartition.MergePersistPartitions(todoPartitions)
	if err != nil {
		log.Errf("MergePartitions Error: %s", err)
		return err
	}

	//没必要
	//tmpPartition.Close()
	//tmpPartition = nil
	////Load回来
	//tmpPartition = partition.LoadPartition(partitionName)

	//清理旧的分区
	for _, prt := range todoPartitions {
		prt.Destroy()
	}
	//截断后面的没用的分区
	tbl.partitions = tbl.partitions[:startIdx]
	tbl.PartitionNames = tbl.PartitionNames[:startIdx]

	//追加上有用的分区
	tbl.partitions = append(tbl.partitions, tmpPartition)
	tbl.PartitionNames = append(tbl.PartitionNames, partitionName)
	return tbl.StoreMeta()
}

//表内搜索
func (tbl *Table) SearchDocs(fieldName, keyWord string) ([]basic.DocNode, bool) {

	retDocs := []basic.DocNode{}
	exist := false

	//各个磁盘分区执行搜索
	for _, prt := range tbl.partitions {
		ids, ok := prt.SearchDocs(fieldName, keyWord, tbl.bitMap)
		if ok {
			exist = true
			retDocs = append(retDocs, ids...)
		}
	}

	//内存分区执行搜索
	if tbl.memPartition != nil {
		ids, ok := tbl.memPartition.SearchDocs(fieldName, keyWord, tbl.bitMap)
		if ok {
			exist = true
			retDocs = append(retDocs, ids...)
		}
	}

	return retDocs, exist
}