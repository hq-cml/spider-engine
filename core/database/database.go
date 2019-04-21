package database

import (
	"github.com/hq-cml/spider-engine/core/table"
	"github.com/hq-cml/spider-engine/utils/helper"
	"errors"
	"fmt"
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/basic"
)

/**
 * Database，对等于Mysql的database
 * 一个DB有多个Table构成
 *
 * 主要就起到了一个整体组织管理的功能
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

//func LoadDatabase() (*Database, error) {
//
//}

func (db *Database) DoClose() error {
	for _, tab := range db.TableMap {
		if err := tab.DoClose(); err != nil {
			return err
		}
	}

	metaFileName := fmt.Sprintf("%v%v%s", db.Path, db.DbName, basic.IDX_FILENAME_SUFFIX_META)
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

func (db *Database) DropTable(tableName string) (error) {
	//路径校验
	_, exist := db.TableMap[tableName]
	if !exist {
		return errors.New("The table not exist!")
	}

	//删除表
	db.TableMap[tableName].Destroy()

	//删
	delete(db.TableMap, tableName)
	for i := 0; i < len(db.TableList); i++ {
		if db.TableList[i] == tableName {
			db.TableList = append(db.TableList[:i], db.TableList[i+1:]...)
		}
	}

	return nil
}

