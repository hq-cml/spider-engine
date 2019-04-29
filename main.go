package main

import (
	"os"
	"flag"
	"time"
	"runtime"
	"net/http"
	"syscall"
	"os/signal"
	_ "net/http/pprof"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/engine"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/engine/config"
)

//全局配置
var confPath *string = flag.String("c", "conf/spider.conf", "config file")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	//解析参数
	flag.Parse()

	//配置解析
	conf, err := config.ParseConfig(*confPath)
	if err != nil {
		panic("parse conf err:" + err.Error())
	}
	basic.GlobalConf = conf

	//创建日志文件并初始化日志句柄
	log.InitLog(conf.LogPath, conf.LogLevel)
	log.Infof("------------Spider Begin To Run------------")

	//启动调试器
	if conf.Pprof {
		go func() {
			http.ListenAndServe(":" + conf.PprofPort, nil)
		}()
	}

	//启动引擎主体
	se := engine.SpiderEngine{}
	se.Start()

	//阻塞等待程序结束
	loopWait(&se)
	log.Infof("The Engine stop!")
}


//检查状态，并在满足条件时采取必要退出措施。
//1. 达到了持续空闲时间
//2. 接收到了结束的信号
func loopWait(eng *engine.SpiderEngine) uint64 {
	var checkCount uint64

	//创建监听退出chan, 这里遇到一个坑
	//通过查阅Notify源码注释看到, 如果chan长度是0, 则select不能有defalut分支
	//因为这里有default分支,则长度不能为0, 否则会丢失signal
	c := make(chan os.Signal, 10)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT) //监听指定信号

	QUIT:
	for {
		//检查信号, 如果收到结束信号, 则退出
		select {
		case s := <-c:
			switch s {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				log.Infoln("Recv signal:", s, ". Begin To Stop")
				result := eng.Stop()
				log.Infoln("Stop scheduler...", result)
				break QUIT
			default:
				log.Infoln("Recv signal: ", s)
			}
		default:
		//do nothing
		//因为存在default分支, 保证程序不会阻塞在此, 但是也要求chan os.Signal长度不能为0
		}

		checkCount++
		time.Sleep(1 * time.Second)
	}

	return checkCount
}