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
)

//建库
func CreateDatabase(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.DatabaseParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	err = engine.SpdInstance().CreateDatabase(&p)
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
	p := engine.DatabaseParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DropDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	err = engine.SpdInstance().DropDatabase(&p)
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
	p := engine.CreateTableParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	err = engine.SpdInstance().CreateTable(&p)
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
	p := engine.CreateTableParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("CreateDatabase Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	err = engine.SpdInstance().DropTable(&p)
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
	p := engine.AddFieldParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("AddField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	err = engine.SpdInstance().AddField(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}

//删字段
func DeleteField(w http.ResponseWriter, req *http.Request) {
	//参数读取与解析
	result, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errf("DeleteField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}
	p := engine.AddFieldParam{}
	err = json.Unmarshal(result, &p)
	if err != nil {
		log.Errf("DeleteField Error: %v", err)
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	//操作
	err = engine.SpdInstance().DeleteField(&p)
	if err != nil {
		io.WriteString(w, helper.JsonEncode(basic.NewErrorResult(err.Error())))
		return
	}

	io.WriteString(w, helper.JsonEncode(basic.NewOkResult("")))
	return
}
