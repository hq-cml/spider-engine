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
	fmt.Println(r.Method, r.URL.String())
	url := strings.Trim(r.URL.String(), "/")
	parts := strings.Split(url, "/")
	partLen := len(parts)
	switch r.Method {
	case "GET":
		if partLen == 1 && parts[0] == "_status" {
			Status(w, r)
		} else if partLen == 1 && parts[0] == "_search" {
			SearchDocs(w, r)
		} else if partLen == 3 {
			GetDoc(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "404 Not Found")
		}
	case "POST":
		if partLen == 1 {
			CreateDatabase(w, r)
		} else if partLen == 2 {
			CreateTable(w, r)
		} else if partLen == 3 {
			AddDoc(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "404 Not Found")
		}
	case "PUT":
		if partLen == 3 {
			UpdateDoc(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "404 Not Found")
		}
	case "DELETE":
		if partLen == 1 {
			DropDatabase(w, r)
		} else if partLen == 2 {
			DropTable(w, r)
		} else if partLen == 3 {
			DeleteDoc(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "404 Not Found")
		}
	case "PATCH":
		if partLen == 2 {
			AlterTable(w, r)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "404 Not Found")
		}
	default:
		w.WriteHeader(http.StatusNotImplemented)
		fmt.Fprintln(w, "No support method: ", r.Method)
	}
}

func InitHttpServer() http.Server {
	//注册路由
	//RegisterRouter()

	//初始化server, 使用spider复用器替换掉默认的handler
	server := http.Server {
		Addr: fmt.Sprintf("%s:%s", basic.GlobalConf.BindIp, basic.GlobalConf.Port),
		Handler: &spiderHttpMux{},
	}

	return server
}

// hello world, the web server
func Status(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, helper.JsonEncodeIndent(engine.SpdInstance().GetStatus()))
}
