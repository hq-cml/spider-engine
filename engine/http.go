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
	"github.com/hq-cml/spider-engine/core/field"
	"github.com/hq-cml/spider-engine/core/index"
)

//注册路由
func (se *SpiderEngine) RegisterRouter() {
	http.HandleFunc("/_status", se.Status)
	http.HandleFunc("/_createDb", se.CreateDatabase)
	http.HandleFunc("/_dropDb", se.DropDatabase)
	http.HandleFunc("/_createTable", se.CreateTable)
	http.HandleFunc("/_dropTable", se.DropTable)
	http.HandleFunc("/_addField", se.AddField)
	http.HandleFunc("/_deleteField", se.DeleteField)
	http.HandleFunc("/_addDoc", se.AddDoc)
	http.HandleFunc("/_getDoc", se.GetDoc)
	http.HandleFunc("/_deleteDoc", se.DeleteDoc)
	http.HandleFunc("/_updateDoc", se.UpdateDoc)
	http.HandleFunc("/_search", se.SearchDocs)
}

// hello world, the web server
func (se *SpiderEngine)Status(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, helper.JsonEncodeIndent(se.GetStatus()))
}

//建库
func (se *SpiderEngine) CreateDatabase(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := DatabaseParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
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
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//关联进入db
	se.DbMap[p.Database] = db
	se.DbList = append(se.DbList, p.Database)

	//meta落地
	se.storeMeta()

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
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := DatabaseParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DropDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
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
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
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
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
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
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := CreateTableParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}
	fields := []field.BasicField{}
	for _, f := range p.Fileds {
		t, ok := index.IDX_MAP[f.Type]
		if !ok {
			log.Errf("Unsuport index type: %v", f.Type)
			io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Unsuport index type: " + f.Type)))
			return
		}
		fields = append(fields, field.BasicField{
			FieldName:  f.Name,
			IndexType:  t,
		})
	}
	_, err = db.CreateTable(p.Table, fields)
	if err != nil {
		log.Errf("CreateTable Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	log.Infof("Create Table: %v", p.Database + "." + p.Table)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//删除表
func (se *SpiderEngine) DropTable(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := CreateTableParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}

	err = db.DropTable(p.Table)
	if err != nil {
		log.Errf("Drop Table Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	log.Infof("Drop Table: %v", p.Database + "." + p.Table)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//增减字段
func (se *SpiderEngine) AddField(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("AddField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := AddFieldParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("AddField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}

	t, ok := index.IDX_MAP[p.Filed.Type]
	if !ok {
		log.Errf("Unsuport index type: %v", p.Filed.Type)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Unsuport index type: " + p.Filed.Type)))
		return
	}

	fld := field.BasicField{
		FieldName: p.Filed.Name,
		IndexType: t,
	}

	err = db.AddField(p.Table, fld)
	if err != nil {
		log.Errf("AddField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	log.Infof("Add Field: %v", p.Database + "." + p.Table + "." + p.Filed.Name + "." + p.Filed.Type)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

func (se *SpiderEngine) DeleteField(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("DeleteField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := AddFieldParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DeleteField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}

	err = db.DeleteField(p.Table, p.Filed.Name)
	if err != nil {
		log.Errf("DeleteField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	log.Infof("Delete Field: %v", p.Database + "." + p.Table + "." + p.Filed.Name)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//新增Doc
func (se *SpiderEngine) AddDoc(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := AddDocParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}
	docId, primaryKey, err :=  db.AddDoc(p.Table, p.Content)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	log.Infof("Add Doc: %v, %v, %v, %v", p.Database, p.Table, primaryKey, docId)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult(primaryKey)))
	return
}

//获取Doc
func (se *SpiderEngine) GetDoc(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	query := req.URL.Query()
	dbName := query["db"][0]
	table := query["table"][0]
	primaryKey := query["primary_key"][0]

	db, exist := se.DbMap[dbName]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}

	doc, ok := db.GetDoc(table, primaryKey)
	if !ok {
		log.Errf("GetDoc get null: %v", primaryKey)
		io.WriteString(w, helper.JsonEncode(basic.NewFailedResult("Can't find " + primaryKey)))
		return
	}

	log.Infof("GetDoc: %v", dbName + "." + table + "." + primaryKey)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult(doc)))
	return
}

//改变doc
func (se *SpiderEngine) UpdateDoc(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("UpdateDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := AddDocParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("UpdateDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}

	docId, err :=  db.UpdateDoc(p.Table, p.Content)
	if err != nil {
		log.Errf("UpdateDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	log.Infof("UpdateDoc Doc: %v, %v, %v", p.Database, p.Table, docId)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
}

//删除Doc
func (se *SpiderEngine) DeleteDoc(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("DeleteDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := DocParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DeleteDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}

	ok := db.DeleteDoc(p.Table, p.PrimaryKey)
	if !ok {
		log.Errf("DeleteDoc get null: %v", p.PrimaryKey)
		io.WriteString(w, helper.JsonEncode(basic.NewFailedResult("Can't find " + p.PrimaryKey)))
		return
	}

	log.Infof("DeleteDoc: %v", p.Database + "." + p.Table + "." + p.PrimaryKey)
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
}

//搜索
func (se *SpiderEngine) SearchDocs(w http.ResponseWriter, req *http.Request) {
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("DeleteDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := SearchParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DeleteDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	db, exist := se.DbMap[p.Database]
	if !exist {
		log.Errf("The db not exist!")
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("The db not exist!")))
		return
	}

	docs, ok := db.SearchDocs(p.Table, p.FieldName, p.Value, p.Filters)
	if !ok {
		log.Info("SearchDocs get null")
		io.WriteString(w, helper.JsonEncode(basic.NewFailedResult("SearchDocs null")))
		return
	}

	log.Infof("SearchDocs: %v, %v, %v, %v, %v", p.Database ,p.Table ,p.FieldName ,p.Value, len(docs))
	io.WriteString(w, helper.JsonEncode(basic.NewOkResult(docs)))

	return
}

