package controller

/*
 * 接口层封装，相当于controller层
 */
import (
	"io"
	"fmt"
	"net/http"
	"github.com/hq-cml/spider-engine/engine"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/basic"
	"strings"
)

//Spider路由复用器
type spiderHttpMux struct {

}

//spiderHttpMux实现http.Handler接口
func (spdMux *spiderHttpMux)ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		url := strings.Trim(r.URL.String(), "/")
		parts := strings.Split(url, "/")
		lenPart := len(parts)
		if lenPart == 1 && parts[0] == "_status" {
			Status(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "404 Not Found")
		}

	default:
		w.WriteHeader(http.StatusNotImplemented)
		fmt.Fprintln(w, "Not supoort method: ", r.Method)
	}
}

func InitHttpServer() http.Server {
	//注册路由
	RegisterRouter()

	//初始化server, 使用spider复用器替换掉默认的handler
	server := http.Server {
		Addr: fmt.Sprintf("%s:%s", basic.GlobalConf.BindIp, basic.GlobalConf.Port),
		Handler: &spiderHttpMux{},
	}

	return server
}

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
}

// hello world, the web server
func Status(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, helper.JsonEncodeIndent(engine.SpdInstance().GetStatus()))
}
