package controller

import (
	"io"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/engine"
	"github.com/hq-cml/spider-engine/utils/log"
)

//新增Doc
func AddDoc(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.AddDocParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	primaryKey, err := engine.SpdInstance().AddDoc(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

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

	doc, err := engine.SpdInstance().GetDoc(dbName, table, primaryKey)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

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
	p := engine.AddDocParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("UpdateDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	err = engine.SpdInstance().UpdateDoc(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
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
	p := engine.DocParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DeleteDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	err = engine.SpdInstance().DeleteDoc(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//搜索
func SearchDocs(w http.ResponseWriter, req *http.Request) {
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("DeleteDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.SearchParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DeleteDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	docs, err := engine.SpdInstance().SearchDocs(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult(docs)))
	return
}
