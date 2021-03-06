package database

import (
	"github.com/hq-cml/spider-engine/core/table"
	"github.com/hq-cml/spider-engine/utils/helper"
	"errors"
	"fmt"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/basic"
	"encoding/json"
	"github.com/hq-cml/spider-engine/utils/log"
)

/**
 * Database，对等于Mysql的database
 * 一个DB有多个Table构成
 *
 * 主要就起到一个整体的组织和管理的功能
 */

type Database struct {
	DbName      string                      `json:"dbName"`
	Path        string                      `json:"path"`
	TableList   []string                    `json:"tables"`
	TableMap    map[string]*table.Table     `json:"-"`
}

type DatabaseStatus struct {
	DbName      string                            `json:"dbName"`
	Path        string                            `json:"path"`
	TableMap    map[string]*table.TableStatus     `json:"tables"`
}

func NewDatabase(path, name string) (*Database, error) {
	//修正
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}

	//路径校验
	if string(path[0]) == "." {
		return nil, errors.New("The path must be absolute path!")
	}
	if helper.Exist(path) {
		return nil, errors.New("The database already exist!")
	}

	//创建目录, 每一个db都有独立的目录
	if ok := helper.Mkdir(path); !ok {
		return nil, errors.New("Failed create dir!")
	}

	db := &Database {
		Path:path,
		DbName:name,
		TableList:[]string{},
		TableMap:map[string]*table.Table{},
	}

	db.storeMeta()
	return db, nil
}

//加载db
func LoadDatabase(path, name string) (*Database, error) {
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}
	db := Database{Path:path, DbName:name}
	metaFileName := db.genMetaName()
	buffer, err := helper.ReadFile(metaFileName)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(buffer, &db)
	if err != nil {
		return nil, err
	}

	db.TableMap = map[string]*table.Table{}
	for _, tableName := range db.TableList {
		tablePath := db.Path + tableName
		tab, err := table.LoadTable(tablePath, tableName)
		if err != nil {
			return nil, err
		}
		db.TableMap[tableName] = tab
	}

	return &db, nil
}

//meta落地
func (db *Database) storeMeta() error {

	metaFileName := db.genMetaName()
	data := helper.JsonEncodeIndent(db)
	if data != "" {
		if err := helper.OverWriteToFile([]byte(data), metaFileName); err != nil {
			return err
		}
	} else {
		return errors.New("Json error")
	}

	return nil
}

//关闭
func (db *Database) DoClose() error {
	//逐个关闭表
	for _, tab := range db.TableMap {
		if err := tab.DoClose(); err != nil {
			return err
		}
	}

	//meta落地
	err := db.storeMeta()
	if err != nil {
		return err
	}
	return nil
}

//建表
func (db *Database) CreateTable(tableName string, fields []field.BasicField) (*table.Table, error) {
	path := fmt.Sprintf("%s%s", db.Path, tableName)

	//路径校验
	_, exist := db.TableMap[tableName]
	if exist || helper.Exist(path) {
		return nil, errors.New("The table already exist!")
	}

	//创建目录, 每一个Table都有独立的目录
	if ok := helper.Mkdir(path); !ok {
		return nil, errors.New("Failed create dir!")
	}

	//创建表和字段
	tab, err := table.CreateTable(path, tableName, fields)
	if err != nil {
		return nil, err
	}

	//关联进入db
	db.TableMap[tableName] = tab
	db.TableList = append(db.TableList, tableName)

	//存meta
	db.storeMeta()
	return tab, nil
}

//删除表
func (db *Database) DropTable(tableName string) (error) {
	//路径校验
	_, exist := db.TableMap[tableName]
	if !exist {
		return errors.New("The table not exist!")
	}

	//删除表
	err := db.TableMap[tableName].Destroy()
	if err != nil {
		log.Errf("Table Destroy Failed. Err:%v, Table:%v", err.Error(), tableName)
		return err
	}

	//删map和slice
	delete(db.TableMap, tableName)
	for i := 0; i < len(db.TableList); i++ {
		if db.TableList[i] == tableName {
			db.TableList = append(db.TableList[:i], db.TableList[i+1:]...)
		}
	}

	//更新meta
	db.storeMeta()

	return nil
}

//新增Doc
func (db *Database) AddDoc(tableName string, content map[string]interface{}, primaryVal string) (uint32, string, error) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return 0, "", errors.New("Table not exist!")
	}

	//主键值的一致性校验 get参数和post参数需一致
	primaryKey := tab.PrimaryKey
	if v, exist := content[primaryKey]; !exist {
		content[primaryKey] = primaryVal
	} else {
		if v != primaryVal {
			return 0, "", errors.New("PrimaryKey val is not consitent!")
		}
	}

	//如果主键值给了_auto, 那么清除之, 底层会自动创建主键值
	_, exist = content[primaryKey]
	if primaryVal == "_auto" && exist {
		delete(content, primaryKey)
	}
	return tab.AddDoc(content)
}

//获取Doc
func (db *Database) GetDoc(tableName string, primaryKey string) (*basic.DocInfo, uint32, bool, error) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return nil, 0, false, errors.New("Table not exist!")
	}

	return tab.GetDoc(primaryKey)
}

//改变doc
func (db *Database) UpdateDoc(tableName string, content map[string]interface{}, primaryVal string) (uint32, error) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return 0, errors.New("Table not exist!")
	}

	//主键值的一致性校验 get参数和post参数需一致
	primaryKey := tab.PrimaryKey
	if v, exist := content[primaryKey]; !exist {
		content[primaryKey] = primaryVal
	} else {
		if v != primaryVal {
			return 0, errors.New("PrimaryKey val is not consitent!")
		}
	}

	return tab.UpdateDoc(content)
}

//删除Doc
func (db *Database) DeleteDoc(tableName string, primaryKey string) (bool) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return false
	}

	return tab.DelDoc(primaryKey)
}

//搜索
func (db *Database) SearchDocs(tableName, fieldName, keyWord string,
		filters []basic.SearchFilter, offset, size int32) ([]basic.DocInfo, int, bool, error) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return nil, 0, false, errors.New("The Table Not Exist!")
	}

	return tab.SearchDocs(fieldName, keyWord, filters, offset, size)
}

//增减字段
func (db *Database) AddField(tableName string, basicField field.BasicField) error {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return errors.New("Table not exist!")
	}

	return tab.AddField(basicField)
}

func (db *Database) DeleteField(tableName string, fieldName string) error {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return errors.New("Table not exist!")
	}

	return tab.DeleteField(fieldName)
}

func (db *Database) genMetaName() string {
	return fmt.Sprintf("%v%v%v", db.Path, db.DbName, basic.IDX_FILENAME_SUFFIX_META)
}

//删除库
func (db *Database) Destory() error {
	//关闭库
	err := db.DoClose()
	if err != nil {
		return err
	}

	//表逐个销毁
	for _, tab := range db.TableMap {
		if err := tab.Destroy(); err != nil {
			log.Errf("Destroy Table Failed. Err:%v, Table:%v", err.Error(), tab.TableName)
			return err
		}
	}

	//删除残留的文件和目录
	metaPath := db.genMetaName()
	if err := helper.Remove(metaPath); err != nil {	return err }
	if err := helper.Remove(db.Path); err != nil {	return err }

	return nil
}

func (db *Database) GetStatus() *DatabaseStatus {
	mp := map[string]*table.TableStatus{}
	for k, v := range db.TableMap {
		mp[k] = v.GetStatus()
	}

	return &DatabaseStatus {
		DbName:   db.DbName,
		Path:     db.Path,
		TableMap: mp,
	}
}