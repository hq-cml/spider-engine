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
	"strings"
)

//新增Doc
func AddDoc(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 3 {
		log.Errf("AlterTable Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}
	db := parts[0]
	table := parts[1]
	primaryKey := parts[2]
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.DocContent{}
	err = json.Unmarshal(body, &p)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	primaryKey, err = engine.SpdInstance().AddDoc(&engine.DocParam{
		Database: db,
		Table:  table,
		Primary: primaryKey,
		Content: p,
	})
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
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 3 {
		log.Errf("AlterTable Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}
	db := parts[0]
	table := parts[1]
	primaryKey := parts[2]

	doc, err := engine.SpdInstance().GetDoc(db, table, primaryKey)
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
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 3 {
		log.Errf("AlterTable Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}
	db := parts[0]
	table := parts[1]
	primaryKey := parts[2]
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("AddDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.DocContent{}
	err = json.Unmarshal(body, &p)
	if err != nil {
		log.Errf("UpdateDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	err = engine.SpdInstance().UpdateDoc(&engine.DocParam{
		Database: db,
		Table:  table,
		Primary: primaryKey,
		Content: p,
	})
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
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 3 {
		log.Errf("AlterTable Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}
	p := engine.DelDocParam{
		Database: parts[0],
		Table: parts[1],
		PrimaryKey:parts[2],
	}

	err := engine.SpdInstance().DeleteDoc(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//搜索
func SearchDocs(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("DeleteDoc Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.SearchParam{}
	err = json.Unmarshal(body, &p)
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
