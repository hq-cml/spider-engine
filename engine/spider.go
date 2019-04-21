package engine

import (
	"github.com/hq-cml/spider-engine/core/database"
	"fmt"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
	"errors"
	"encoding/json"
	"github.com/hq-cml/spider-engine/core/field"
)

type SpiderEngine struct {
	Path     string                         `json:"path"`
	Version  string                         `json:"version"`
	DbList   []string                       `json:"databases"`
	DbMap    map[string]*database.Database  `json:"-"`
}

func InitSpider(path string, ver string) (*SpiderEngine, error) {
	//修正
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}

	//路径校验
	if !helper.Exist(path) {
		return nil, errors.New("Path not exist! Detail:" + path)
	}

	se := SpiderEngine{
		Path: path,
	}
	metaPath := se.genMetaName()

	if helper.Exist(metaPath) {
		//加载现有的引擎数据
		buffer, err := helper.ReadFile(metaPath)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(buffer, &se)
		if err != nil {
			return nil, err
		}
		se.DbMap = map[string]*database.Database{}
		for _, dbName := range se.DbList {
			dbPath := fmt.Sprintf("%s%s", path, dbName)
			tmpDb, err := database.LoadDatabase(dbPath, dbName)
			if err != nil {
				return nil, err
			}
			se.DbMap[dbName] = tmpDb
		}
	} else {
		//全新的启动
		se.DbList = []string{}
		se.DbMap = map[string]*database.Database{}
	}

	se.Version = ver
	return &se, nil
}

func (se *SpiderEngine) genMetaName() string {
	return fmt.Sprintf("%v%v%v", se.Path, "spider", basic.IDX_FILENAME_SUFFIX_META)
}

func (se *SpiderEngine) Start() {

}

func (se *SpiderEngine) storeMeta() error {
	metaFileName := se.genMetaName()
	data := helper.JsonEncodeIndent(se)
	if data != "" {
		if err := helper.WriteToFile([]byte(data), metaFileName); err != nil {
			return err
		}
	} else {
		return errors.New("Json error")
	}
	return nil
}

func (se *SpiderEngine) DoClose() error {
	//逐个关闭表
	for _, db := range se.DbMap {
		if err := db.DoClose(); err != nil {
			return err
		}
	}
	//meta落地
	err := se.storeMeta()
	if err != nil {
		return err
	}
	return nil
}

//建库
func (se *SpiderEngine) CreateDatabase(dbName string) (*database.Database, error) {
	_, exist := se.DbMap[dbName]
	if exist {
		return nil, errors.New("The db already exist!")
	}

	//创建表和字段
	path := fmt.Sprintf("%s%s", se.Path, dbName)
	db, err := database.NewDatabase(path, dbName)
	if err != nil {
		return nil, err
	}

	//关联进入db
	se.DbMap[dbName] = db
	se.DbList = append(se.DbList, dbName)

	return db, nil
}

//删除库
func (se *SpiderEngine) DropDatabase(dbName string) (error) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return errors.New("The db not exist!")
	}

	//删除库
	err := db.Destory()
	if err != nil {
		return err
	}

	//删slice
	delete(se.DbMap, dbName)
	for i := 0; i < len(se.DbList); i++ {
		if se.DbList[i] == dbName {
			se.DbList = append(se.DbList[:i], se.DbList[i+1:]...)
		}
	}

	//更新meta
	se.storeMeta()
	return nil
}

//建表
func (se *SpiderEngine) CreateTable(dbName, tableName string, fields []field.BasicField) (error) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return errors.New("The db not exist!")
	}
	_, err := db.CreateTable(tableName, fields)
	if err != nil {
		return err
	}
	return nil

}

//删除表
func (se *SpiderEngine) DropTable(dbName, tableName string) (error) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return errors.New("The db not exist!")
	}
	err := db.DropTable(tableName)
	if err != nil {
		return err
	}
	return nil
}

//增减字段
func (se *SpiderEngine) AddField(dbName, tableName string, basicField field.BasicField) error {
	db, exist := se.DbMap[dbName]
	if !exist {
		return errors.New("The db not exist!")
	}
	return db.AddField(tableName, basicField)
}

func (se *SpiderEngine) DeleteField(dbName, tableName string, fieldName string) error {
	db, exist := se.DbMap[dbName]
	if !exist {
		return errors.New("The db not exist!")
	}
	return db.DeleteField(tableName, fieldName)
}


//新增Doc
func (se *SpiderEngine) AddDoc(dbName, tableName string, content map[string]string) (uint32, error) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return 0, errors.New("The db not exist!")
	}
	return db.AddDoc(tableName, content)
}

//获取Doc
func (se *SpiderEngine) GetDoc(dbName, tableName string, primaryKey string) (map[string]string, bool) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return nil, false
	}
	return db.GetDoc(tableName, primaryKey)
}

//改变doc
func (se *SpiderEngine) UpdateDoc(dbName, tableName string, content map[string]string) (uint32, error) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return 0, errors.New("The db not exist!")
	}
	return db.UpdateDoc(tableName, content)
}

//删除Doc
func (se *SpiderEngine) DeleteDoc(dbName, tableName string, primaryKey string) (bool) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return false
	}
	return db.DeleteDoc(tableName, primaryKey)
}

//搜索
func (se *SpiderEngine) SearchDocs(dbName, tableName, fieldName, keyWord string) ([]basic.DocNode, bool) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return nil, false
	}
	return db.SearchDocs(tableName, fieldName, keyWord)
}

