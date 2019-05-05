package engine

import (
	"github.com/hq-cml/spider-engine/basic"
)

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
type DocParam struct {
	Database   string 	 `json:"database"`
	Table	   string 	 `json:"table"`
	PrimaryKey string	 `json:"primaryKey"`
}

type SearchParam struct {
	Database   string 	 			`json:"database"`
	Table	   string 			    `json:"table"`
	FieldName  string				`json:"fieldName"`
	Value      string				`json:"value"`
	Filters    []basic.SearchFilter `json:"filters"`
}

