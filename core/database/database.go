package database

import (
	"github.com/hq-cml/spider-engine/core/table"
	"github.com/hq-cml/spider-engine/utils/helper"
	"errors"
	"fmt"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/basic"
	"encoding/json"
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

	db := &Database{
		Path:path,
		DbName:name,
		TableList:[]string{},
		TableMap:map[string]*table.Table{},
	}
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
		if err := helper.WriteToFile([]byte(data), metaFileName); err != nil {
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
	tab := table.NewEmptyTable(path, tableName)
	for _, bf := range fields {
		if err := tab.AddField(bf); err != nil {
			return nil, err
		}
	}

	//关联进入db
	db.TableMap[tableName] = tab
	db.TableList = append(db.TableList, tableName)

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
		return err
	}

	//删slice
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
func (db *Database) AddDoc(tableName string, content map[string]interface{}) (uint32, error) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return 0, errors.New("Table not exist!")
	}

	return tab.AddDoc(content)
}

//获取Doc
func (db *Database) GetDoc(tableName string, primaryKey string) (map[string]interface{}, bool) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return nil, false
	}

	return tab.GetDoc(primaryKey)
}

//改变doc
func (db *Database) UpdateDoc(tableName string, content map[string]interface{}) (uint32, error) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return 0, errors.New("Table not exist!")
	}

	return tab.UpdateDoc(content)
}

//删除Doc
func (db *Database) DeleteDoc(tableName string, primaryKey string) (bool) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return false
	}

	return tab.DeleteDoc(primaryKey)
}

//搜索
func (db *Database) SearchDocs(tableName, fieldName, keyWord string, filters []basic.SearchFilter) ([]basic.DocNode, bool) {
	tab, exist := db.TableMap[tableName]
	if !exist {
		return nil, false
	}

	return tab.SearchDocs(fieldName, keyWord, filters)
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
	//表逐个销毁
	for _, tab := range db.TableMap {
		if err := tab.Destroy(); err != nil {
			return err
		}
	}

	//删除残留的文件和目录
	metaPath := db.genMetaName()
	if err := helper.Remove(metaPath); err != nil {	return err }
	if err := helper.Remove(db.Path); err != nil {	return err }

	return nil
}