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
type FieldsParam []FieldParam
type CreateTableParam struct {
	Database string 	   `json:"database"`
	Table 	 string        `json:"table"`
	Fileds   FieldsParam   `json:"fields"`
}

//增/删段参数
type AlterTableParam struct {
	Type     string       `json:"type"`
	Filed    FieldParam   `json:"field"`
}
type AlterFieldParam struct {
	Database string 	  `json:"database"`
	Table    string       `json:"table"`
	Filed    FieldParam   `json:"field"`
}

//增/改文档参数
type DocContent map[string]interface{}
type DocParam struct {
	Database string 	  `json:"database"`
	Table    string 	  `json:"table"`
	Primary  string       `json:"parimary"`
	Content  DocContent   `json:"content"`
}

//获取/删除文档参数
type DelDocParam struct {
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
	Offset     int32                `json:"offset"`
	Size       int32                `json:"size"`
}

