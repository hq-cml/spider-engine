package engine

import (
	"fmt"
	"errors"
	"net/http"
	"encoding/json"
	_ "net/http/pprof"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/core/database"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/log"
)

type SpiderEngine struct {
	Path     string                         `json:"path"`
	Version  string                         `json:"version"`
	DbList   []string                       `json:"databases"`
	DbMap    map[string]*database.Database  `json:"-"`
}

func InitSpider(path string, ver string) (*SpiderEngine, error) {
	//路径修正
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

func (se *SpiderEngine) storeMeta() error {
	metaFileName := se.genMetaName()
	data := helper.JsonEncodeIndent(se)
	if data != "" {
		if err := helper.OverWriteToFile([]byte(data), metaFileName); err != nil {
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

//TODO
func (se *SpiderEngine) Start() {
	go func() {
		//注册路由
		se.RegisterRouter()

		//启动http服务
		addr := fmt.Sprintf("%s:%s", basic.GlobalConf.BindIp, basic.GlobalConf.Port)
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatal("ListenAndServe: ", err)
		}
	}()

	log.Infof("The Spider Engin Start To Work! Version: %s\n", se.Version)
}

//TODO
func (se *SpiderEngine) Stop() string {
	se.DoClose()
	return "see you again"
}