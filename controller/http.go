package controller

/*
 * 接口层封装，相当于controller层
 */
import (
	"net/http"
	"io"
	"github.com/hq-cml/spider-engine/engine"
	"github.com/hq-cml/spider-engine/utils/helper"
)

//注册路由
func RegisterRouter() {
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
	//http.HandleFunc("/_test", TestDocs)
}

// hello world, the web server
func Status(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, helper.JsonEncodeIndent(engine.SpdInstance().GetStatus()))
}
