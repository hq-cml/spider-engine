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
	"strconv"
	"github.com/hq-cml/spider-engine/core/partition"
	"github.com/hq-cml/spider-engine/utils/btree"
	"github.com/hq-cml/spider-engine/utils/bitmap"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/core/index"
	"github.com/hq-cml/spider-engine/core/field"
)

//表的原则：
// 主键和其他字段不同，单独用一个btree实例存储，不分区
// 最新的文档增加只会操作内存态分区，到达阈值后回落地或者和其他分区合并落地
// 文档的删除采用假删除，通过bitmap标记
//
// 为了性能的尽量提升和逻辑简单，并且便于追溯和排查，priFwdMap将会留存全部的docId
// 由于bitmap和priIvtMap为主查询，priFwdMapz只是辅助，所以不会影响正确性
type Table struct {
	TableName    string                      `json:"tableName"`
	Path         string                      `json:"pathName"`
	BasicFields  map[string]field.BasicField `json:"fields"`       //不包括主键！！
	PrimaryKey   string                      `json:"primaryKey"`
	StartDocId   uint32                      `json:"startDocId"`
	NextDocId    uint32                      `json:"nextDocId"`
	RealDocNum   uint32                      `json:"realDocNum"` //表总文档数，和底层的docCnt不同，这个docNum表示实际有多少有效文档
	MaxDocNum    uint32                      `json:"maxDocNum"`
	PartSuffix   uint64                      `json:"prefix"`
	PrtPathNames []string                    `json:"prtPathNames"` //磁盘态的分区列表名--这些分区均不包括主键！！！

	status         uint8
	memPartition   *partition.Partition   //内存态的分区,分区不包括逐渐
	partitions     []*partition.Partition //磁盘态的分区列表--这些分区均不包括主键字段！！！
	priBtdb        btree.Btree            //主键专用正排 & 倒排索引（磁盘态）
	priIvtMap      map[string]string      //主键专用倒排索引（内存态），primaryKey => docId
	priFwdMap      map[string]string      //主键专正排排索引（内存态），docId => primaryKey
	delFlagBitMap  *bitmap.Bitmap         //用于文档删除标记
	rwMutex        sync.RWMutex           //读写锁
}

type TableStatus struct {
	TableName  string                       `json:"tableName"`
	Path       string                       `json:"pathName"`
	Fields     []*field.BasicStatus         `json:"fields"`       //不包括主键！！
	PrimaryKey string                       `json:"primaryKey"`
	RealDocNum uint32                       `json:"realDocNum"`
	StartDocId uint32                       `json:"startDocId"`
	NextDocId  uint32                       `json:"nextDocId"`
	DiskParts  []*partition.PartitionStatus `json:"partitions"`  //磁盘态的分区列表名--这些分区均不包括主键！！！
	MemPart    *partition.PartitionStatus   `json:"memPartition"`
}

const (
	DEFAULT_PRIMARY_FIELD_NAME = "#Def%Pri$Key@" //系统默认主键名称
	PRI_FWD_BTREE_NAME 		   = "pri_fwd_tree"
	PRI_IVT_BTREE_NAME         = "pri_ivt_tree"
	BitmapOrgNum               = 0x01 << 27      //16M 文件, 表示最大1.3亿的文档
	//BitmapOrgNum             = 8 			     //test
)

const (
	TABLE_STATUS_INIT uint8 = iota
	TABLE_STATUS_LOADING
	TABLE_STATUS_RUNNING
	TABLE_STATUS_MERGEING
	TABLE_STATUS_CLOSING
	TABLE_STATUS_CLOSED
)

//新建空表
func newEmptyTable(path, name string) *Table {
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}
	tab := Table {
		TableName:    name,
		PrtPathNames: make([]string, 0),
		partitions:   make([]*partition.Partition, 0),
		Path:         path,
		BasicFields:  make(map[string]field.BasicField),
		priIvtMap:    make(map[string]string),
		priFwdMap:    make(map[string]string),
		status:       TABLE_STATUS_INIT,
	}

	//bitmap文件新建
	btmpName := tab.getBitMapName()
	tab.delFlagBitMap = bitmap.NewBitmap(btmpName, BitmapOrgNum)
	tab.MaxDocNum = BitmapOrgNum

	return &tab
}

//产出一块空的内存分区
func (tbl *Table) generateMemPartition() error {
	if tbl.status != TABLE_STATUS_RUNNING && tbl.status != TABLE_STATUS_INIT &&
		tbl.status != TABLE_STATUS_LOADING {
		return errors.New("Table status must be running/init/loading")
	}
	prtPathName := tbl.getPrtPathName()
	var basicFields []field.BasicField
	for _, f := range tbl.BasicFields {
		basicFields = append(basicFields, f)
	}

	tbl.memPartition = partition.NewEmptyPartitionWithBasicFields(prtPathName, tbl.NextDocId, basicFields)
	tbl.PartSuffix++ //自增

	return nil
}

//创建表
//如果用户没有传主键，则系统自动补充一个主键
func CreateTable(path, tableName string, fields []field.BasicField) (*Table, error) {
	tab := newEmptyTable(path, tableName)

	hasKey := false
	for _, bf := range fields {
		err := tab.AddField(bf);
		if err != nil {
			return nil, err
		}

		if bf.IndexType == index.IDX_TYPE_PK {
			hasKey = true
		}
	}

	//如果没有主键，则自动补充主键
	if !hasKey {
		tab.AddField(field.BasicField {
			IndexType: index.IDX_TYPE_PK,
			FieldName: DEFAULT_PRIMARY_FIELD_NAME,
		})
	}
	tab.status = TABLE_STATUS_RUNNING

	return tab, nil
}

//从文件加载一张表
func LoadTable(path, name string) (*Table, error) {
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}
	tbl := Table{Path:path, TableName:name, status: TABLE_STATUS_LOADING}
	metaFileName := tbl.getMetaName()
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
	for _, prtPathName := range tbl.PrtPathNames {
		prt, err := partition.LoadPartition(prtPathName)
		if err != nil {
			log.Errf("partition.LoadPartition Error:%v", err)
			return nil, err
		}
		tbl.partitions = append(tbl.partitions, prt)
	}

	//产出一块空的内存分区
	err = tbl.generateMemPartition()
	if err != nil {
		return nil, err
	}

	//加载bitmap
	btmpPath := path + name + basic.IDX_FILENAME_SUFFIX_BITMAP
	tbl.delFlagBitMap = bitmap.LoadBitmap(btmpPath)

	//如果存在主键，则加载主键专用btree并初始化内存map
	if tbl.PrimaryKey != "" {
		primaryName := tbl.getPrimaryBtName()
		tbl.priBtdb = btree.NewBtree("", primaryName)
		tbl.priIvtMap = make(map[string]string)
		tbl.priFwdMap = make(map[string]string)
	}

	log.Infof("Load Table %v success", tbl.TableName)
	tbl.status = TABLE_STATUS_RUNNING
	return &tbl, nil
}

//落地表的元信息
func (tbl *Table) storeMetaAndBtdb() error {
	metaFileName := tbl.getMetaName()
	data := helper.JsonEncodeIndent(tbl)
	if data != "" {
		if err := helper.OverWriteToFile([]byte(data), metaFileName); err != nil {
			log.Errf("OverWriteToFile Error:%v", err.Error())
			return err
		}
	} else {
		return errors.New("Json error")
	}

	//将内存态的主键，全部落盘到btree
	if tbl.PrimaryKey != "" {
		err := tbl.priBtdb.MutiSet(PRI_IVT_BTREE_NAME, tbl.priIvtMap); if err != nil {
			log.Errf("tbl.priBtdb.MutiSet Error:%v", err.Error())
			return err
		}
		err = tbl.priBtdb.MutiSet(PRI_FWD_BTREE_NAME, tbl.priFwdMap); if err != nil {
			log.Errf("tbl.priBtdb.MutiSet Error:%v", err.Error())
			return err
		}
		tbl.priIvtMap = make(map[string]string)
		tbl.priFwdMap = make(map[string]string)
	}
	return nil
}

//新增字段
//Note:
// 新增的字段只会在最新的空分区生效，如果新增的时候有非空的分区，会先落地，然后产出新分区
// 分区的变动，会锁表
func (tbl *Table) AddField(basicField field.BasicField) error {
	//锁整张表
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	//校验
	if tbl.status != TABLE_STATUS_RUNNING && tbl.status != TABLE_STATUS_INIT {
		return errors.New("Table status must be running or init")
	}
	if basicField.IndexType == index.IDX_TYPE_PK && tbl.PrimaryKey != "" {
		return errors.New("Primary key has exist!")
	}
	if _, exist := tbl.BasicFields[basicField.FieldName]; exist {
		log.Warnf("Field %v have Exist ", basicField.FieldName)
		return errors.New(fmt.Sprintf("Field %v have Exist ", basicField.FieldName))
	}

	//实施新增
	if basicField.IndexType == index.IDX_TYPE_PK {
		//主键独立操作
		tbl.PrimaryKey = basicField.FieldName
		primaryName := tbl.getPrimaryBtName()
		tbl.priBtdb = btree.NewBtree("", primaryName)
		tbl.priBtdb.AddTree(PRI_IVT_BTREE_NAME)
		tbl.priBtdb.AddTree(PRI_FWD_BTREE_NAME)
	} else {

		//基础信息注册
		tbl.BasicFields[basicField.FieldName] = basicField

		//新增字段生效到后续的新分区中
		if tbl.memPartition == nil {
			//如果内存分区为nil，则直接新增一个内存分区，新增出来的分区已经包含了新的新增字段
			err := tbl.generateMemPartition()
			if err != nil {
				return err
			}
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
			tbl.PrtPathNames = append(tbl.PrtPathNames, tmpPartition.PrtPathName)
			//新分区（包含新字段）生成
			err := tbl.generateMemPartition()
			if err != nil {
				return err
			}
		}
	}

	//元信息落地
	err := tbl.storeMetaAndBtdb()
	if err != nil {
		return errors.New("storeMetaAndBtdb Error:" + err.Error())
	}

	return nil
}

//删除分区
//本质上也是一种假删除, 只是把tbl.BasicFields对应项删除, 使得上层查询隐藏该字段
//如果内存分区非空, 则会先落地内存分区
func (tbl *Table) DeleteField(fieldname string) error {
	//锁整张表
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	//校验
	if tbl.status != TABLE_STATUS_RUNNING && tbl.status != TABLE_STATUS_INIT {
		return errors.New("Table status must be running or init")
	}
	if fieldname == tbl.PrimaryKey {
		return errors.New("Can not del primary key!")
	}
	if _, exist := tbl.BasicFields[fieldname]; !exist {
		return errors.New(fmt.Sprintf("Field %v not found ", fieldname))
	}

	//假删除
	delete(tbl.BasicFields, fieldname)

	if tbl.memPartition == nil {
		//啥也不需要干
		log.Info("Delete field. memPartition is nil. do nothing~")
	} else if tbl.memPartition.IsEmpty() {
		//删除内存分区的字段
		err := tbl.memPartition.DeleteField(fieldname)
		if err != nil {
			return err
		}
	} else {
		//当前内存分区先落地
		tmpPartition := tbl.memPartition
		if err := tmpPartition.Persist(); err != nil {
			return err
		}
		tbl.partitions = append(tbl.partitions, tmpPartition)
		tbl.PrtPathNames = append(tbl.PrtPathNames, tmpPartition.PrtPathName)
		//产出新的分区(字段已删除）
		err := tbl.generateMemPartition()
		if err != nil {
			return err
		}
	}

	//元信息落地
	err := tbl.storeMetaAndBtdb()
	if err != nil {
		return errors.New("storeMetaAndBtdb Error:" + err.Error())
	}
	return nil
}

//BitMap扩大
func (tbl *Table) doExpandBitMap() error{

	err := tbl.delFlagBitMap.DoExpand()
	if err != nil {
		log.Errf("Expand error: %v", err)
		return err
	}
	tbl.MaxDocNum = uint32(tbl.delFlagBitMap.MaxNum + 1)

	return nil
}

//获取文档
func (tbl *Table) GetDoc(primaryKey string) (*basic.DocInfo, bool) {
	//读取
	tbl.rwMutex.RLock()
	defer tbl.rwMutex.RUnlock()

	if tbl.status != TABLE_STATUS_RUNNING {
		return nil, false
	}
	docNode, exist := tbl.findDocIdByPrimaryKey(primaryKey)
	if !exist {
		return nil, false
	}
	tmp, ok := tbl.getDocByDocId(docNode.DocId)
	if !ok {
		return nil, false
	}

	//如果表主键是系统自动生成的，则在详情中隐藏不体现
	//否则，如果是用户自己提供的主键，则体现在详情中
	if tbl.PrimaryKey != DEFAULT_PRIMARY_FIELD_NAME {
		tmp[tbl.PrimaryKey] = primaryKey
	}

	detail := basic.DocInfo{
		Key: primaryKey,
		Detail:tmp,
	}

	return &detail, true
}

//新增文档
//Note:
// 底层为了保证一致性（各个字段之间、正排和倒排之间），所以出错也会继续执行，此处捕获错误，将docId废弃掉
func (tbl *Table) AddDoc(content map[string]interface{}) (uint32, string, error) {
	//写锁
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	//校验
	if tbl.status != TABLE_STATUS_RUNNING {
		return 0, "", errors.New("Table status must be running!")
	}
	if len(tbl.BasicFields) == 0 {
		return 0, "", errors.New("field is nil")
	}

	//获取主键
	var key string
	if tbl.PrimaryKey != "" {
		var ok bool
		if v, exist := content[tbl.PrimaryKey]; exist {
			key, ok = v.(string)
			if !ok {
				return 0, "", errors.New("Primary key must be string")
			}
		} else {
			//用户没传主键，则系统自动生成一个主键
			key = helper.GenUuid()
		}
	}

	//校验主键不能重复
	_, exist := tbl.findDocIdByPrimaryKey(key)
	if exist {
		return 0, "", errors.New("Duplicate Primary Key! " + key)
	}

	//如果内存分区为空，则新建内存分区
	if tbl.memPartition == nil {
		err := tbl.generateMemPartition()
		if err != nil {
			return 0, "", err
		}
	}

	//bitmap判断是否需要自动扩容
	if tbl.NextDocId == tbl.MaxDocNum {
		err := tbl.doExpandBitMap()
		if err != nil {
			log.Errf("doExpandBitMap Error: %v", err.Error())
			return 0, "", err
		}
	}

	newDocId := tbl.NextDocId

	//实质新增流程开始：一致性考虑，此刻开始必须执行到底
	// 处理主键新增
	if tbl.PrimaryKey != "" {
		tbl.priIvtMap[key] = fmt.Sprintf("%v", newDocId)
		tbl.priFwdMap[fmt.Sprintf("%v", newDocId)] = key
	}

	//其他字段新增Doc
	var err error
	err = tbl.memPartition.AddDocument(newDocId, content)
	if err != nil {
		delete(tbl.priIvtMap, key)
		//delete(tbl.priFwdMap, fmt.Sprintf("%v", newDocId))

		//标记删除
		tbl.delFlagBitMap.Set(uint64(newDocId))

		//失败，但是仍然会新增NextDocId，保证一致性
		tbl.NextDocId++
		log.Errf("Table AddDoc Error:%v. PrimaryKey:%v", err, key)
	} else {
		//成功，NextDocId自增
		tbl.NextDocId++
		tbl.RealDocNum++
		log.Infof("Table AddDoc Success. PrimaryKey: %v", key)
	}

	//最后，如果满足了落地的阈值, 需要先落地分区
	if tbl.NextDocId % partition.PART_PERSIST_MIN_DOC_CNT == 0 {
		err := tbl.Persist()
		if err != nil {
			log.Fatalf("MergePartitions Error: %v. Table: %v", err.Error(), tbl.TableName)
		}
	}

	//最后，如果满足了合并归档的阈值, 需要合并归档
	if tbl.NextDocId % partition.PART_MERGE_MIN_DOC_CNT == 0 {
		err := tbl.MergePartitions()
		if err != nil {
			log.Fatalf("MergePartitions Error: %v. Table: %v", err.Error(), tbl.TableName)
		}
	}

	return newDocId, key, err
}

//删除
//标记假删除
func (tbl *Table) DelDoc(primaryKey string) bool {
	//写锁
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	//校验
	if tbl.status != TABLE_STATUS_RUNNING {
		return false
	}
	docId, found := tbl.findDocIdByPrimaryKey(primaryKey)
	if found {
		//Table的realDocNum--
		tbl.RealDocNum--

		//文档所属分区的realDocNum--
		if tbl.memPartition != nil &&
			(docId.DocId >= tbl.memPartition.StartDocId && docId.DocId < tbl.memPartition.NextDocId) {
			//如果主键此刻还在内存中，则捎带手删掉，如果已经落了btdb，那就算了
			//不过不会影响到正确性，因为以bitmap的删除标记为准
			delete(tbl.priIvtMap, primaryKey)
			//delete(tbl.priFwdMap, fmt.Sprintf("%v", docId.DocId))
			tbl.memPartition.RealDocNum --
		} else {
			//尝试从磁盘分区获取
			for _, prt := range tbl.partitions {
				if docId.DocId >= prt.StartDocId && docId.DocId < prt.NextDocId {
					prt.RealDocNum --
					break
				}
			}
		}
		//核心标记删除
		return tbl.delFlagBitMap.Set(uint64(docId.DocId))
	}
	return true
}

//变更文档
//Note:
// 如果表没有主键，则不支持变更
// 本质上还是新增，主键不变，但是docId变了
//Note:
// 底层为了保证一致性（各个字段之间、正排和倒排之间），所以出错也会继续执行，此处捕获错误，将docId废弃掉
func (tbl *Table) UpdateDoc(content map[string]interface{}) (uint32, error) {
	//写锁
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	//校验
	if tbl.status != TABLE_STATUS_RUNNING {
		return 0, errors.New("Table status must be running!")
	}
	if len(tbl.BasicFields) == 0 {
		return 0, errors.New("field is nil")
	}
	//如果表没有主键，则不支持变更
	if tbl.PrimaryKey == "" {
		return 0, errors.New("No Primary Key")
	}
	if _, exist := content[tbl.PrimaryKey]; !exist {
		log.Errf("Primary Key Not Found %v", tbl.PrimaryKey)
		return 0, errors.New(fmt.Sprintf("Primary Key Not Found %v", tbl.PrimaryKey))
	}
	key, ok := content[tbl.PrimaryKey].(string)
	if !ok {
		return 0, errors.New("Primary must be string!")
	}

	//如果内存分区为空，则新建
	if tbl.memPartition == nil {
		tbl.rwMutex.Lock()
		err := tbl.generateMemPartition()
		if err != nil {
			tbl.rwMutex.Unlock()
			return 0, err
		}
		tbl.rwMutex.Unlock()
	}

	//bitmap自动扩容
	if tbl.NextDocId == tbl.MaxDocNum {
		err := tbl.doExpandBitMap()
		if err != nil {
			log.Errf("doExpandBitMap Error: %v", err.Error())
			return 0, err
		}
	}

	//找到原来的docId
	oldDocid, found := tbl.findDocIdByPrimaryKey(key)
	if !found {
		return 0, errors.New(fmt.Sprintf("Can not find the doc %v. Update faield!", key))
	}

	//本质上仍然是新增文档
	//实质流程开始：一致性考虑，此刻开始必须执行到底
	newDocId := tbl.NextDocId

	//实质内容本质上还是新增，主键不变，但是docId变了
	var err error
	err = tbl.memPartition.AddDocument(newDocId, content)
	if err != nil {
		//在顶层将新的docId标记删除
		tbl.delFlagBitMap.Set(uint64(newDocId))
		log.Errf("MemPartition.UpdateDocument Error:%v. PrimaryKey:%v", err, key)
	} else {
		//成功增加了主体文档，则开始底层篡改
		//先标记删除oldDocId
		tbl.delFlagBitMap.Set(uint64(oldDocid.DocId))

		//变更指向 key=>docId
		if _, exist := tbl.priIvtMap[key]; exist {
			tbl.priIvtMap[key] = strconv.Itoa(int(newDocId))           //直接覆盖
			//delete(tbl.priFwdMap, strconv.Itoa(int(oldDocid.DocId))) //不主动删除，留存下来便于排查追溯等
			tbl.priFwdMap[strconv.Itoa(int(newDocId))] = key           //设置辅助映射
		} else {
			//对于已经落盘的主键，则修改btdb，（会在PRI_FWD_BTREE_NAME留下一点脏数据，不影响是正确性）
			tmpErr := tbl.priBtdb.Set(PRI_IVT_BTREE_NAME, key, fmt.Sprintf("%v", newDocId))
			if tmpErr != nil { //这里出错的概率基本没有
				log.Errf("Set key error  %v", tmpErr)
				//return 0, tmpErr
				err = tmpErr
			}
			tmpErr = tbl.priBtdb.Set(PRI_FWD_BTREE_NAME, fmt.Sprintf("%v", newDocId), key)
			if tmpErr != nil {
				log.Errf("Set key error  %v", tmpErr)
				//return 0, tmpErr
				err = tmpErr
			}
		}

		//被删除文档所属分区的realDocNum--
		if tbl.memPartition != nil &&
			(oldDocid.DocId >= tbl.memPartition.StartDocId && oldDocid.DocId < tbl.memPartition.NextDocId) {
			tbl.memPartition.RealDocNum --
		} else {
			//尝试从磁盘分区获取
			for _, prt := range tbl.partitions {
				if oldDocid.DocId >= prt.StartDocId && oldDocid.DocId < prt.NextDocId {
					prt.RealDocNum --
					break
				}
			}
		}
	}
	//无论成功与否，兼容一致性，均nextDocId均自增
	tbl.NextDocId++

	//最后，如果满足了落地的阈值, 需要先落地分区
	if tbl.NextDocId % partition.PART_PERSIST_MIN_DOC_CNT == 0 {
		err := tbl.Persist()
		if err != nil {
			log.Fatalf("MergePartitions Error: %v. Table: %v", err.Error(), tbl.TableName)
		}
	}

	//最后，如果满足了合并归档的阈值, 需要合并归档
	if tbl.NextDocId % partition.PART_MERGE_MIN_DOC_CNT == 0 {
		err := tbl.MergePartitions()
		if err != nil {
			log.Fatalf("MergePartitions Error: %v. Table: %v", err.Error(), tbl.TableName)
		}
	}

	return newDocId, err
}

//内部获取
//Note:
// 不包括主键!
func (tbl *Table) getDocByDocId(docId uint32) (map[string]interface{}, bool) {
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

//根据主键找到内部的docId
func (tbl *Table) findDocIdByPrimaryKey(key string) (*basic.DocNode, bool) {
	var docId int
	var err error

	//先尝试在内存map中找，没有则再去磁盘btree中找
	v, exist := tbl.priIvtMap[key]
	if exist {
		docId, err = strconv.Atoi(v)
		if err != nil {
			return nil, false
		}
	} else {
		vv, ok := tbl.priBtdb.GetInt(PRI_IVT_BTREE_NAME, key)
		if !ok {
			return nil, false
		}
		docId = int(vv)
	}

	//校验是否已经删除
	if tbl.delFlagBitMap.IsSet(uint64(docId)) {
		return nil, false
	}

	return &basic.DocNode{DocId: uint32(docId)}, true
}

//根据主键找到内部的docId
func (tbl *Table) findPrimaryKeyByDocId(docId uint32) (string, bool) {
	//校验是否已经删除
	if tbl.delFlagBitMap.IsSet(uint64(docId)) {
		return "", false
	}

	docIdStr := fmt.Sprintf("%v", docId)
	//先尝试在内存map中找，没有则再去磁盘btree中找
	if v, exist := tbl.priFwdMap[docIdStr]; exist {
		return v, true
	} else {
		vv, ok := tbl.priBtdb.GetStr(PRI_FWD_BTREE_NAME, docIdStr)
		if !ok {
			return "", false
		}
		return vv, true
	}
}

//表落地
//Note:
// 本质上就是内存分区落地
func (tbl *Table) Persist() error {
	if tbl.status != TABLE_STATUS_RUNNING {
		return errors.New("Table status must be running!")
	}

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
	tbl.PrtPathNames = append(tbl.PrtPathNames, tmpPartition.PrtPathName)
	tbl.memPartition = nil

	return tbl.storeMetaAndBtdb()
}

//关闭一张表
func (tbl *Table) DoClose() error {
	//写锁
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	if tbl.status != TABLE_STATUS_RUNNING {
		return errors.New("Table status must be running!")
	}
	tbl.status = TABLE_STATUS_CLOSING

	log.Infof("DoClose Table [%v] Begin", tbl.TableName)

	//关闭内存分区(非空则需要先落地)
	if tbl.memPartition != nil {
		tbl.persistMemPartition() //内存分区落地
	}

	//逐个关闭磁盘分区
	for _, prt := range tbl.partitions {
		prt.DoClose()
	}

	//关闭主键btdb，如果有
	if tbl.priBtdb != nil {
		tbl.priBtdb.Close()
	}

	//关闭btmp
	if tbl.delFlagBitMap != nil {
		tbl.delFlagBitMap.Close()
	}
	log.Infof("DoClose Table [%v] Finish", tbl.TableName)
	tbl.status = TABLE_STATUS_CLOSED
	return nil
}

//销毁一张表在磁盘的文件
func (tbl *Table) Destroy() error {
	if tbl.status != TABLE_STATUS_CLOSED && tbl.status != TABLE_STATUS_CLOSING {
		err := tbl.DoClose()
		if err != nil {
			return err
		}
	}

	//写锁
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()
	log.Infof("Destroy Table [%v] Begin", tbl.TableName)

	//因为刚刚Close，所以不应该存在内存分区
	if tbl.memPartition != nil && !tbl.memPartition.IsEmpty() {
		log.Err("Should not exist mem partition!")
		return errors.New("Should not exist mem partition!")
	}
	//逐个删除磁盘分区
	for _, prt := range tbl.partitions {
		prt.Remove()
	}

	//删除残留的文件和目录
	metaFile := tbl.getMetaName()
	primaryFile := tbl.getPrimaryBtName()
	bitmapFile := tbl.getBitMapName()

	if err := helper.Remove(metaFile); err != nil {	log.Err(err.Error()); return err }
	if err := helper.Remove(primaryFile); err != nil { log.Err(err.Error()); return err }
	if err := helper.Remove(bitmapFile); err != nil { log.Err(err.Error()); return err }
	if err := helper.Remove(tbl.Path); err != nil {	log.Err(err.Error()); return err }

	log.Infof("Destroy Table [%v] Finish", tbl.TableName)
	return nil
}

//合并表内分区
func (tbl *Table) MergePartitions() error {

	//锁整表
	tbl.rwMutex.Lock()
	defer tbl.rwMutex.Unlock()

	if tbl.status != TABLE_STATUS_RUNNING {
		return errors.New("Table status must be running!")
	}

	//表状态变更
	tbl.status = TABLE_STATUS_MERGEING
	defer func() {
		tbl.status = TABLE_STATUS_RUNNING
	}()

	var startIdx int = -1


	//内存分区非空则先落地
	if tbl.memPartition != nil && !tbl.memPartition.IsEmpty(){
		tbl.persistMemPartition()
	}

	//找到第一个个数不符合规范，即要合并的partition
	for idx := range tbl.partitions {
		if tbl.partitions[idx].NextDocId - tbl.partitions[idx].StartDocId < partition.PART_MERGE_MIN_DOC_CNT {
			startIdx = idx
			break
		}
	}
	if startIdx == -1 {
		log.Infof("No nessary to merge!")
		return nil
	}
	if len(tbl.partitions[startIdx:]) == 1 {
		log.Infof("No nessary to merge!")
		return nil
	}

	//准备好非主键字段信息备用
	var basicFields []field.BasicField
	for _, f := range tbl.BasicFields {
		basicFields = append(basicFields, f)
	}

	//从startIdx开始, 一点点尝试出最佳的分区合并方式
	todoPartitions := [][]*partition.Partition{}
	start := tbl.partitions[startIdx].StartDocId
	tmpPrts := []*partition.Partition{}
	for i := startIdx; i < len(tbl.partitions); i++ {
		tmpPrts = append(tmpPrts, tbl.partitions[i])
		if tbl.partitions[i].NextDocId - start >= partition.PART_MERGE_MIN_DOC_CNT {
			todoPartitions = append(todoPartitions, tmpPrts)
			tmpPrts = []*partition.Partition{}
			start = tbl.partitions[i].NextDocId
		}
	}
	if len(tmpPrts) > 0{
		todoPartitions = append(todoPartitions, tmpPrts)
	}

	//截断后面的没用的分区
	tbl.partitions = tbl.partitions[:startIdx]
	tbl.PrtPathNames = tbl.PrtPathNames[:startIdx]
	//开始合并
	for _, todoParts := range todoPartitions {
		//生成内存分区骨架，开始合并
		prtPathName := tbl.getPrtPathName()
		tbl.PartSuffix++
		tmpPartition := partition.NewEmptyPartitionWithBasicFields(prtPathName, todoParts[0].StartDocId, basicFields)

		err := tmpPartition.MergePersistPartitions(todoParts)
		if err != nil {
			log.Errf("MergePartitions Error: %s", err)
			return err
		}

		//追加上有用的分区
		tbl.partitions = append(tbl.partitions, tmpPartition)
		tbl.PrtPathNames = append(tbl.PrtPathNames, prtPathName)

		//清理旧的分区
		for _, prt := range todoParts {
			prt.Destroy()
		}
	}

	//存储meta
	err := tbl.storeMetaAndBtdb()
	if err != nil {
		return err
	}
	return nil
}

//表内搜索
func (tbl *Table) SearchDocs(fieldName, keyWord string, filters []basic.SearchFilter) ([]basic.DocInfo, bool) {
	//读锁
	tbl.rwMutex.RLock()
	defer tbl.rwMutex.RUnlock()

	if tbl.status != TABLE_STATUS_RUNNING {
		return nil, false
	}
	docIds := []basic.DocNode{}
	exist := false

	//fmt.Println("-----------Table search -------------")
	//fmt.Println("BitMap: ", tbl.delFlagBitMap.String())

	//各个磁盘分区执行搜索
	for _, prt := range tbl.partitions {
		ids, ok := prt.SearchDocs(fieldName, keyWord, tbl.delFlagBitMap, filters)
		if ok {
			exist = true
			docIds = append(docIds, ids...)
		}
	}

	//内存分区执行搜索
	if tbl.memPartition != nil && !tbl.memPartition.IsEmpty(){
		ids, ok := tbl.memPartition.SearchDocs(fieldName, keyWord, tbl.delFlagBitMap, filters)
		if ok {
			exist = true
			docIds = append(docIds, ids...)
		}
	}

	//结果组装
	retDocs := []basic.DocInfo{}
	for _, doc := range docIds {
		tmp, ok := tbl.getDocByDocId(doc.DocId)
		if !ok {
			log.Errf("Can't find doc[%v] !!!!", doc.DocId)
			//return nil, false
			continue
		}

		primaryKey, ok := tbl.findPrimaryKeyByDocId(doc.DocId)
		if !ok {
			log.Errf("Can't find doc[%v]' primary key !!!!", doc.DocId)
			//return nil, false
			continue
		}

		//如果表主键是系统自动生成的，则在详情中隐藏不体现
		//否则，如果是用户自己提供的主键，则体现在详情中
		if tbl.PrimaryKey != DEFAULT_PRIMARY_FIELD_NAME {
			tmp[tbl.PrimaryKey] = primaryKey
		}

		detail := basic.DocInfo{
			Key: primaryKey,
			Detail:tmp,
		}

		retDocs = append(retDocs, detail)
	}
	return retDocs, exist
}

func (tbl *Table) displayInner() string {
	str := "\n"
	for _, idx := range tbl.partitions {
		str += fmt.Sprintln("Disk--> Start:", idx.StartDocId, ". Next:", idx.NextDocId)
	}

	if tbl.memPartition != nil && !tbl.memPartition.IsEmpty(){
		str += fmt.Sprintln("Mem--> Start:", tbl.memPartition.StartDocId, ". Next:", tbl.memPartition.NextDocId)
	}

	return str
}

func (tbl *Table) getBitMapName() string {
	btmpName := fmt.Sprintf("%v%v%v", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_BITMAP)
	return btmpName
}

func (tbl *Table) getMetaName() string {
	metaFileName := fmt.Sprintf("%v%v%v", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_META)
	return metaFileName
}

func (tbl *Table) getPrimaryBtName() string {
	primaryName := fmt.Sprintf("%v%v_primary%v", tbl.Path, tbl.TableName, basic.IDX_FILENAME_SUFFIX_BTREE)
	return primaryName
}

func (tbl *Table) getPrtPathName() string {
	prtPathName := fmt.Sprintf("%v%v_%010v", tbl.Path, tbl.TableName, tbl.PartSuffix) //10位补零
	return prtPathName
}

func (tbl *Table) GetIvtMap() map[string]string {
	return tbl.priIvtMap
}

func (tbl *Table) GetFwdMap() map[string]string {
	return tbl.priFwdMap
}

func (tbl *Table) GetBtdb() btree.Btree {
	return tbl.priBtdb
}

func (tbl *Table) GetStatus() *TableStatus {
	m := []*field.BasicStatus{}
	p := []*partition.PartitionStatus{}
	var memPart *partition.PartitionStatus
	for _, v := range tbl.BasicFields {
		m = append(m, v.GetBasicStatus())
	}
	for _, prt := range tbl.partitions {
		//prt.Fields["user_desc"].IvtIdx.Walk()
		p = append(p, prt.GetStatus())
	}
	if tbl.memPartition != nil && !tbl.memPartition.IsEmpty() {
		//tbl.memPartition.Fields["user_desc"].IvtIdx.Walk()
		memPart = tbl.memPartition.GetStatus()
	}

	fmt.Println("-----------Table Status -------------")
	fmt.Println("BitMap: ", tbl.delFlagBitMap.String())
	fmt.Println("PRIMARY BTDB:")
	tbl.priBtdb.Display(PRI_IVT_BTREE_NAME)
	tbl.priBtdb.Display(PRI_FWD_BTREE_NAME)

	return &TableStatus {
		TableName      : tbl.TableName,
		Path           : tbl.Path,
		Fields         : m,
		PrimaryKey     : tbl.PrimaryKey,
		RealDocNum:      tbl.RealDocNum,
		StartDocId     : tbl.StartDocId,
		NextDocId      : tbl.NextDocId,
		DiskParts      : p,
		MemPart        : memPart,
	}
}