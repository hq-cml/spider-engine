package main

import (
	"os"
	"flag"
	"time"
	"runtime"
	"syscall"
	"os/signal"
	_ "net/http/pprof"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/engine"
	"github.com/hq-cml/spider-engine/utils/log"
	"fmt"
	"github.com/hq-cml/spider-engine/controller"
)

//全局配置
var confPath *string = flag.String("c", "conf/spider.conf", "config file")

func main() {
	//TODO recover兜底panic
	runtime.GOMAXPROCS(runtime.NumCPU())

	//解析参数
	flag.Parse()

	//配置解析
	conf, err := basic.ParseConfig(*confPath)
	if err != nil {
		panic("parse conf err:" + err.Error())
	}
	basic.GlobalConf = conf
	basic.PART_PERSIST_MIN_DOC_CNT = uint32(conf.PartPersistMinCnt)
	basic.PART_MERGE_MIN_DOC_CNT = uint32(conf.PartMergeMinCnt)

	//创建日志文件并初始化日志句柄
	log.InitLog(conf.LogPath, conf.LogLevel)
	log.Infof("Begin to start")

	//初始化并启动引擎主体
	se, err := engine.InitSpider(conf.DataDir, basic.SPIDER_VERSION)
	if err != nil {
		log.Fatalf("Init spider Error:%v", err)
		return
	}
	//注册单例
	engine.RegisterInstance(se)
	//注册路由
	httpServer := controller.InitHttpServer()
	//启动
	se.Start(httpServer)
	fmt.Println("The spider is running...")
	//阻塞等待程序结束
	loopWait(se)
	log.Infof("The Engine stop!")
}


//检查状态，并在满足条件时采取必要退出措施。
//1. 达到了持续空闲时间
//2. 接收到了结束的信号
func loopWait(se *engine.SpiderEngine) uint64 {
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
				result := se.Stop()
				log.Infoln("Stop loopWait...", result)
				break QUIT
			default:
				log.Infoln("Recv signal: ", s)
			}
		case <-se.CloseChan:
			log.Infof("One Table Is Delete! The Spider Will Go On！")
		default:
		//do nothing
		//因为存在default分支, 保证程序不会阻塞在此, 但是也要求chan os.Signal长度不能为0
		}

		checkCount++
		time.Sleep(1 * time.Second)
	}

	return checkCount
}