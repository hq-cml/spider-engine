package database

import (
	"github.com/hq-cml/spider-engine/core/table"
	"github.com/hq-cml/spider-engine/utils/helper"
	"errors"
)

/**
 * Database，对等于Mysql的database
 * 一个DB有多个Table构成
 *
 * 主要就起到了一个组织管理的功能
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
	if !helper.Exist(path) {
		return nil, errors.New("The path not exist!")
	}
	if helper.Exist(path + name) {
		return nil, errors.New("The database already exist!")
	}

	//创建目录

	db := &Database{
		Path:path,
		DbName:name,
		TableList:[]string{},
		TableMap:map[string]*table.Table{},
	}

	return db, nil
}

