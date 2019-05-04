package engine

/*
 * 接口层封装，相当于controller层
 */
import (
	"net/http"
	"io"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/log"
	"io/ioutil"
	"encoding/json"
)

//注册路由
func (se *SpiderEngine) RegisterRouter() {
	http.HandleFunc("/_status", Status)
	http.HandleFunc("/_createDb", CreateDatabase)
	http.HandleFunc("/_dropDb", DropDatabase)
	http.HandleFunc("/_createTable", CreateTable)
	http.HandleFunc("/_dropTable", DropTable)
	http.HandleFunc("/_addField", AddField)
	http.HandleFunc("/_deleteField", DeleteField)
	http.HandleFunc("/_addDoc", AddDoc)
	http.HandleFunc("/_getDoc", GetDoc)
	http.HandleFunc("/_deleteDoc", DeleteDoc)
	http.HandleFunc("/_updateDoc", UpdateDoc)
	http.HandleFunc("/_search", SearchDocs)
}

// hello world, the web server
func Status(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, helper.JsonEncodeIndent(g_spider_ins.GetStatus()))
}

//建库
func CreateDatabase(w http.ResponseWriter, req *http.Request) {
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

	//操作
	err = g_spider_ins.CreateDatabase(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//删除库
func DropDatabase(w http.ResponseWriter, req *http.Request) {
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

	//操作
	err = g_spider_ins.DropDatabase(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//建表
func CreateTable(w http.ResponseWriter, req *http.Request) {
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

	//操作
	err = g_spider_ins.CreateTable(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//删除表
func DropTable(w http.ResponseWriter, req *http.Request) {
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

	//操作
	err = g_spider_ins.DropTable(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//增减字段
func AddField(w http.ResponseWriter, req *http.Request) {
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

	//操作
	err = g_spider_ins.AddField(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

func DeleteField(w http.ResponseWriter, req *http.Request) {
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

	//操作
	err = g_spider_ins.DeleteField(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//新增Doc
func AddDoc(w http.ResponseWriter, req *http.Request) {
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

	db, exist := g_spider_ins.DbMap[p.Database]
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
func GetDoc(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	query := req.URL.Query()
	dbName := query["db"][0]
	table := query["table"][0]
	primaryKey := query["primary_key"][0]

	db, exist := g_spider_ins.DbMap[dbName]
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
func UpdateDoc(w http.ResponseWriter, req *http.Request) {
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

	db, exist := g_spider_ins.DbMap[p.Database]
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
func DeleteDoc(w http.ResponseWriter, req *http.Request) {
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

	db, exist := g_spider_ins.DbMap[p.Database]
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
func SearchDocs(w http.ResponseWriter, req *http.Request) {
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

	db, exist := g_spider_ins.DbMap[p.Database]
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

