package engine

//库参数
type DatabaseParam struct {
	Database string `json:"database"`
}

//建表参数
type CreateTableParam struct {
	Database string `json:"database"`
	Table string `json:"table"`
	Fileds []
}