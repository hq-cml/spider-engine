package controller

import (
	"io"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/engine"
	"strings"
	"fmt"
)

//建库
func CreateDatabase(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 1 {
		log.Errf("CreateDatabase Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}

	p := engine.DatabaseParam{
		Database: parts[0],
	}
	//操作
	err := engine.SpdInstance().CreateDatabase(&p)
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
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 1 {
		log.Errf("DropDatabase Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}

	p := engine.DatabaseParam{
		Database: parts[0],
	}

	//操作
	err := engine.SpdInstance().DropDatabase(&p)
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
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 2 {
		log.Errf("CreateTable Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}
	db := parts[0]
	table := parts[1]
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.FieldsParam{}
	err = json.Unmarshal(body, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	err = engine.SpdInstance().CreateTable(&engine.CreateTableParam{
		Database: db,
		Table: table,
		Fileds: p,
	})
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
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 2 {
		log.Errf("CreateTable Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}

	//操作
	err := engine.SpdInstance().DropTable(&engine.CreateTableParam{
		Database: parts[0],
		Table: parts[1],
	})
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//删除表
func AlterTable(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	url := strings.Trim(req.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	if partLen != 2 {
		log.Errf("AlterTable Param Error: %v", url)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult("Param Error")))
		return
	}
	db := parts[0]
	table := parts[1]
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("AddField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.AlterTableParam{}
	err = json.Unmarshal(body, &p)
	if err != nil {
		log.Errf("AlterTableParam Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	if p.Type != "addField" && p.Type != "delField" {
		log.Errf("No support opType: %v", p.Type)
		io.WriteString(w, helper.JsonEncode(fmt.Sprintf("No support opType: %v", p.Type)))
		return
	}

	ap := engine.AlterFieldParam{
		Table: table,
		Database: db,
		Filed: p.Filed,
	}
	if p.Type == "addField" {
		addField(w, &ap)
	} else {
		deleteField(w, &ap)
	}
}

//增减字段
func addField(w http.ResponseWriter, ap *engine.AlterFieldParam) {
	//操作
	err := engine.SpdInstance().AddField(ap)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//删字段
func deleteField(w http.ResponseWriter, ap *engine.AlterFieldParam) {
	//操作
	err := engine.SpdInstance().DeleteField(ap)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}
