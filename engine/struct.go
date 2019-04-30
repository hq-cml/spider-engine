package engine

import (
	"github.com/hq-cml/spider-engine/core/index"
)

const (
	IDX_TYPE_NAME_PRIME = "primary"
	IDX_TYPE_NAME_WHOLE = "whole"
	IDX_TYPE_NAME_WORDS = "words"
	IDX_TYPE_NAME_PURE  = "pure"
	IDX_TYPE_NAME_TIME  = "time"
	IDX_TYPE_NAME_INT   = "number"
)

var IDX_MAP = map[string]uint16 {
	IDX_TYPE_NAME_PRIME : index.IDX_TYPE_PK,
	IDX_TYPE_NAME_WHOLE : index.IDX_TYPE_STR_WHOLE,
	IDX_TYPE_NAME_WORDS : index.IDX_TYPE_STR_SPLITER,
	IDX_TYPE_NAME_PURE  : index.IDX_TYPE_PURE_TEXT,
	IDX_TYPE_NAME_TIME  : index.IDX_TYPE_DATE,
	IDX_TYPE_NAME_INT   : index.IDX_TYPE_INTEGER,
}

//增/删库参数
type DatabaseParam struct {
	Database string `json:"database"`
}

//字段参数
type FieldParam struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

//建/删表参数
type CreateTableParam struct {
	Database string 	   `json:"database"`
	Table 	 string        `json:"table"`
	Fileds   []FieldParam  `json:"fields"`
}

//增/删段参数
type AddFieldParam struct {
	Database string 	  `json:"database"`
	Table    string       `json:"table"`
	Filed    FieldParam   `json:"field"`
}

//增/改文档参数
type AddDocParam struct {
	Database string 				  `json:"database"`
	Table    string 			      `json:"table"`
	Content  map[string]interface{}   `json:"content"`
}

//获取/删除文档参数
type GetDocParam struct {
	Database   string 	 `json:"database"`
	Table	   string 	 `json:"table"`
	PrimaryKey string	 `json:"primaryKey"`
}