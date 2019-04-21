package engine

import (
	"github.com/hq-cml/spider-engine/core/database"
	"fmt"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
	"errors"
	"encoding/json"
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
	path := fmt.Sprintf("%s%s", se.Path, dbName)

	//创建表和字段
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
	//路径校验
	_, exist := se.DbMap[dbName]
	if !exist {
		return errors.New("The db not exist!")
	}

	//删除库
	err := se.DbMap[dbName].Destory()
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