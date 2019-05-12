package engine

/*
 * 封装了全部的dml操作
 */

import (
	"fmt"
	"errors"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
)

func (se *SpiderEngine) ProcessDMLRequest(req *basic.SpiderRequest) {
	if req.Type == basic.REQ_TYPE_DML_ADD_DOC {
		p := req.Req.(*AddDocParam)
		//新增文档
		db, _ := se.DbMap[p.Database]
		docId, primaryKey, err := db.AddDoc(p.Table, p.Content)
		if err != nil {
			log.Errf("AddDoc Error: %v", err)
			req.Resp <- basic.NewResponse(err, nil)
			return
		}

		log.Infof("Add Doc Success: %v, %v, %v, %v", p.Database, p.Table, primaryKey, docId)
		req.Resp <- basic.NewResponse(nil, primaryKey)
		return
	} else if req.Type == basic.REQ_TYPE_DML_DEL_DOC {
		p := req.Req.(*DocParam)
		//删除文档
		db, _ := se.DbMap[p.Database]
		ok := db.DeleteDoc(p.Table, p.PrimaryKey)
		if !ok {
			log.Errf("DeleteDoc get null: %v", p.PrimaryKey)
			req.Resp <- basic.NewResponse(
				errors.New(fmt.Sprintf("DeleteDoc get null: %v", p.PrimaryKey)), nil)
			return
		}

		log.Infof("DeleteDoc Success: %v", p.Database + "." + p.Table + "." + p.PrimaryKey)
		req.Resp <- basic.NewResponse(nil, nil)
		return
	} else if req.Type == basic.REQ_TYPE_DML_EDIT_DOC {
		p := req.Req.(*AddDocParam)
		//编辑文档
		db, _ := se.DbMap[p.Database]
		docId, err :=  db.UpdateDoc(p.Table, p.Content)
		if err != nil {
			log.Errf("UpdateDoc Error: %v", err)
			req.Resp <- basic.NewResponse(err, nil)
			return
		}

		log.Infof("UpdateDoc Doc Success: %v, %v, %v", p.Database, p.Table, docId)
		req.Resp <- basic.NewResponse(nil, nil)
		return
	} else {
		log.Fatal("Unsupport req.Type:%v", req.Type)
		req.Resp <- basic.NewResponse(errors.New(fmt.Sprintf("Unsupport req.Type:%v", req.Type)), nil)
	}

	return
}

//增加文档（串行化）
func (se *SpiderEngine) AddDoc(p *AddDocParam) (string, error) {
	if se.Closed {
		return "", errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()
	//校验
	_, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return "", errors.New("The db already exist!")
	}

	//生成请求放入cache
	req := basic.NewRequest(basic.REQ_TYPE_DML_ADD_DOC, p)
	se.CacheMap[p.Database + "." + p.Table].Put(req)
	log.Debug("Put AddDoc request: ", p.Database + "." + p.Table)

	//等待结果
	resp := <- req.Resp
	if resp.Err != nil {
		return "", resp.Err
	}

	return resp.Data.(string), nil
}

//删除文档
func (se *SpiderEngine) DeleteDoc(p *DocParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()

	//校验
	_, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db already exist!")
	}

	//生成请求放入cache
	req := basic.NewRequest(basic.REQ_TYPE_DML_DEL_DOC, p)
	se.CacheMap[p.Database + "." + p.Table].Put(req)
	log.Debug("Put DeleteDoc request: ", p.Database + "." + p.Table)

	//等待结果
	resp := <- req.Resp
	if resp.Err != nil {
		return resp.Err
	}
	return nil
}

//改文档
func (se *SpiderEngine) UpdateDoc(p *AddDocParam) error {
	if se.Closed {
		return errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()

	//校验
	_, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return errors.New("The db already exist!")
	}

	//生成请求放入cache
	req := basic.NewRequest(basic.REQ_TYPE_DML_EDIT_DOC, p)
	se.CacheMap[p.Database + "." + p.Table].Put(req)
	log.Debug("Put UpdateDoc request: ", p.Database + "." + p.Table)

	//等待结果
	resp := <- req.Resp
	if resp.Err != nil {
		return resp.Err
	}
	return nil
}

//获取文档
func (se *SpiderEngine) GetDoc(dbName, tableName, key string) (*basic.DocInfo, error) {
	if se.Closed {
		return nil, errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()

	//校验
	db, exist := se.DbMap[dbName]
	if !exist {
		log.Errf("The db not exist!")
		return nil, errors.New("The db already exist!")
	}

	//获取
	doc, docId, ok, err := db.GetDoc(tableName, key)
	if err != nil {
		log.Warnf("GetDoc Error: %v", err.Error())
		return nil, err
	}
	if !ok {
		log.Warnf("GetDoc get null: %v", key)
		return nil, nil
	}

	log.Infof("GetDoc: %v. Db:%v. Table:%v. DocId:%v", key, dbName, tableName, docId)
	return doc, nil
}

//搜索文档
func (se *SpiderEngine) SearchDocs(p *SearchParam) ([]basic.DocInfo, error) {
	if se.Closed {
		return nil, errors.New("Spider Engine is closed!")
	}
	se.RwMutex.RLock()          //读锁
	defer se.RwMutex.RUnlock()

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		return nil, errors.New("The db already exist!")
	}
	docs, ok, err := db.SearchDocs(p.Table, p.FieldName, p.Value, p.Filters)
	if err != nil {
		log.Errf("SearchDocs Error: %v", err.Error())
		return nil, err
	}
	if !ok {
		log.Warnf("SearchDocs get null:%v", helper.JsonEncode(p))
		return nil, nil
	}

	log.Infof("SearchDocs: %v, %v, %v, %v, %v", p.Database ,p.Table ,p.FieldName ,p.Value, len(docs))
	return docs, nil
}