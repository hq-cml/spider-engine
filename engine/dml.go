package engine

/*
 * 封装了全部的dml操作
 */

import (
	"github.com/hq-cml/spider-engine/utils/log"
	"errors"
	"github.com/hq-cml/spider-engine/basic"
	"fmt"
	"github.com/hq-cml/spider-engine/utils/helper"
)

//增加文档
func (se *SpiderEngine) AddDoc(p *AddDocParam) (string, error) {
	//校验
	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return "", errors.New("The db already exist!")
	}

	//操作
	docId, primaryKey, err :=  db.AddDoc(p.Table, p.Content)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		return "", err
	}

	log.Infof("Add Doc: %v, %v, %v, %v", p.Database, p.Table, primaryKey, docId)
	return primaryKey, nil
}

//删除文档
func (se *SpiderEngine) DeleteDoc(p *DocParam) error {
	//校验
	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db already exist!")
	}

	ok := db.DeleteDoc(p.Table, p.PrimaryKey)
	if !ok {
		log.Errf("DeleteDoc get null: %v", p.PrimaryKey)
		return errors.New(fmt.Sprintf("DeleteDoc get null: %v", p.PrimaryKey))
	}

	log.Infof("DeleteDoc: %v", p.Database + "." + p.Table + "." + p.PrimaryKey)
	return nil
}

//改文档
func (se *SpiderEngine) UpdateDoc(p *AddDocParam) error {
	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db already exist!")
	}

	docId, err :=  db.UpdateDoc(p.Table, p.Content)
	if err != nil {
		log.Errf("UpdateDoc Error: %v", err)
		return err
	}

	log.Infof("UpdateDoc Doc: %v, %v, %v", p.Database, p.Table, docId)
	return nil
}

//获取文档
func (se *SpiderEngine) GetDoc(dbName, tableName, key string) (*basic.DocInfo, error) {
	//校验
	db, exist := se.DbMap[dbName]
	if !exist {
		log.Errf("The db not exist!")
		return nil, errors.New("The db already exist!")
	}

	//获取
	doc, ok := db.GetDoc(tableName, key)
	if !ok {
		log.Warnf("GetDoc get null: %v", key)
		return nil, nil
	}

	log.Infof("GetDoc: %v", dbName + "." + tableName + "." + key)
	return doc, nil
}

//搜索文档
func (se *SpiderEngine) SearchDocs(p *SearchParam) ([]basic.DocInfo, error) {
	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return nil, errors.New("The db already exist!")
	}
	docs, ok := db.SearchDocs(p.Table, p.FieldName, p.Value, p.Filters)
	if !ok {
		log.Warnf("SearchDocs get null:%v", helper.JsonEncode(p))
		return nil, nil
	}

	log.Infof("SearchDocs: %v, %v, %v, %v, %v", p.Database ,p.Table ,p.FieldName ,p.Value, len(docs))
	return docs, nil
}