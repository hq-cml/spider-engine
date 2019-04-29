package engine

import (
	"net/http"
	"io"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/core/database"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"errors"
	"github.com/hq-cml/spider-engine/core/field"
)

// hello world, the web server
func HelloServer(w http.ResponseWriter, req *http.Request) {
	ret := basic.NewOkResult("hello world!")
	io.WriteString(w, helper.JsonEncode(ret))
}

func (se *SpiderEngine) RegisterRouter() {
	http.HandleFunc("/hello", HelloServer)
	http.HandleFunc("/_createDb", se.CreateDatabase)
	http.HandleFunc("/_dropDb", se.DropDatabase)
}

//建库
func (se *SpiderEngine) CreateDatabase(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}
	p := DatabaseParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}

	_, exist := se.DbMap[p.Database]
	if exist {
		log.Errf("The db already exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db already exist!")))
		return
	}

	//创建表和字段
	path := fmt.Sprintf("%s%s", se.Path, p.Database)
	db, err := database.NewDatabase(path, p.Database)
	if err != nil {
		log.Errf("CreateDatabase Error: %v, %v", err, path)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}

	//关联进入db
	se.DbMap[p.Database] = db
	se.DbList = append(se.DbList, p.Database)
	log.Infof("Create database: %v", p.Database)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}


//删除库
func (se *SpiderEngine) DropDatabase(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("DropDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}
	p := DatabaseParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DropDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}

	//删除库
	err = db.Destory()
	if err != nil {
		log.Errf("DropDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}

	//删slice
	delete(se.DbMap, p.Database)
	for i := 0; i < len(se.DbList); i++ {
		if se.DbList[i] == p.Database {
			se.DbList = append(se.DbList[:i], se.DbList[i+1:]...)
		}
	}

	//更新meta
	err = se.storeMeta()
	if err != nil {
		log.Errf("DropDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}

	log.Infof("DropDatabase database: %v", p.Database)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//建表
func (se *SpiderEngine) CreateTable(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}
	p := DatabaseParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err)))
		return
	}

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
func (se *SpiderEngine) AddDoc(dbName, tableName string, content map[string]interface{}) (uint32, string, error) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return 0, "", errors.New("The db not exist!")
	}
	return db.AddDoc(tableName, content)
}

//获取Doc
func (se *SpiderEngine) GetDoc(dbName, tableName string, primaryKey string) (*basic.DocInfo, bool) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return nil, false
	}
	return db.GetDoc(tableName, primaryKey)
}

//改变doc
func (se *SpiderEngine) UpdateDoc(dbName, tableName string, content map[string]interface{}) (uint32, error) {
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
func (se *SpiderEngine) SearchDocs(dbName, tableName, fieldName,
keyWord string, filters []basic.SearchFilter) ([]basic.DocInfo, bool) {
	db, exist := se.DbMap[dbName]
	if !exist {
		return nil, false
	}
	return db.SearchDocs(tableName, fieldName, keyWord, filters)
}

