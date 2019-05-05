package engine

import (
	"fmt"
	"errors"
	"net/http"
	"encoding/json"
	_ "net/http/pprof"
	"github.com/hq-cml/spider-engine/basic"
	"github.com/hq-cml/spider-engine/core/database"
	"github.com/hq-cml/spider-engine/utils/helper"
	"github.com/hq-cml/spider-engine/utils/log"
	"github.com/hq-cml/spider-engine/engine/middleware"
	"sync"
	"time"
)

var (
	g_spider_ins *SpiderEngine
)

type SpiderEngine struct {
	Path     string                               `json:"path"`
	Version  string                               `json:"version"`
	DbList   []string                             `json:"databases"`
	DbMap    map[string]*database.Database        `json:"-"`
	CacheMap map[string]*middleware.RequestCache  `json:"-"`
	Closed   bool								  `json:"-"`
	RwMutex  sync.RWMutex                         `json:"-"`
}

type SpiderStatus struct {
	Path     string                               `json:"path"`
	Version  string                         	  `json:"version"`
	DbMap    map[string]*database.DatabaseStatus  `json:"databases"`
}

//注册实例句柄
func RegisterInstance(ins *SpiderEngine) {
	g_spider_ins = ins
}

//句柄初始化
func InitSpider(path string, ver string) (*SpiderEngine, error) {
	//路径修正
	if string(path[len(path)-1]) != "/" {
		path = path + "/"
	}

	//路径校验
	if !helper.Exist(path) {
		return nil, errors.New("Path not exist! Detail:" + path)
	}

	se := SpiderEngine{
		Path: path,
	}
	metaPath := se.genMetaName()

	if helper.Exist(metaPath) {
		//加载现有的引擎数据
		buffer, err := helper.ReadFile(metaPath)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(buffer, &se)
		if err != nil {
			return nil, err
		}
		se.DbMap = map[string]*database.Database{}
		for _, dbName := range se.DbList {
			dbPath := fmt.Sprintf("%s%s", path, dbName)
			tmpDb, err := database.LoadDatabase(dbPath, dbName)
			if err != nil {
				return nil, err
			}
			se.DbMap[dbName] = tmpDb
		}
	} else {
		//全新的启动
		se.DbList = []string{}
		se.DbMap = map[string]*database.Database{}
	}

	//每一张表，启动独立的任务调度，负责处理dml和ddl中的写入任务
	se.CacheMap = map[string]*middleware.RequestCache{}
	for dbName, db := range se.DbMap {
		for tbName, _ := range db.TableMap {
			dbTable := dbName + "." + tbName
			se.CacheMap[dbTable] = se.doSchedule(dbTable)
		}
	}
	se.Version = ver
	return &se, nil
}

func (se *SpiderEngine) genMetaName() string {
	return fmt.Sprintf("%v%v%v", se.Path, "spider", basic.IDX_FILENAME_SUFFIX_META)
}

func (se *SpiderEngine) storeMeta() error {
	metaFileName := se.genMetaName()
	data := helper.JsonEncodeIndent(se)
	if data != "" {
		if err := helper.OverWriteToFile([]byte(data), metaFileName); err != nil {
			return err
		}
	} else {
		return errors.New("Json error")
	}
	return nil
}

func (se *SpiderEngine) Start() {
	go func() {
		//启动前,看看分区是否合并必要
		for _, db := range se.DbMap {
			for _, tab := range db.TableMap {
				err := tab.MergePartitions()
				if err != nil {
					log.Fatalf("Table MergePartitions failed! db:%v, table:%v", db.DbName, tab.TableName)
					return
				}
			}
		}

		//注册路由
		se.RegisterRouter()

		//启动http服务
		addr := fmt.Sprintf("%s:%s", basic.GlobalConf.BindIp, basic.GlobalConf.Port)
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatal("ListenAndServe: ", err)
		}
	}()

	log.Infof("The Spider Engin Start To Work! Version: %s\n", se.Version)
}

func (se *SpiderEngine) Stop() string {
	se.Closed = true

	//TODO 等待所有调度器结束


	//逐个关闭表
	se.RwMutex.RLock()
	defer se.RwMutex.RUnlock()
	for _, db := range se.DbMap {
		if err := db.DoClose(); err != nil {
			log.Errf("Db Close Error:%v, Db:%v", err.Error(), db.DbName)
		}
	}

	//meta落地
	err := se.storeMeta()
	if err != nil {
		log.Errf("StoreMeta Error:v", err.Error())
	}
	return "See you again"
}

func (se *SpiderEngine) GetStatus() *SpiderStatus {

	se.RwMutex.RLock()
	defer se.RwMutex.RUnlock()

	mp := map[string]*database.DatabaseStatus{}
	for k, v := range se.DbMap {
		mp[k] = v.GetStatus()
	}

	return &SpiderStatus{
		Path:    se.Path,
		Version: se.Version,
		DbMap:   mp,
	}
}

/*
 * 调度，每一张表有一对独立的goroutine负责：
 *
 * 搬运goroutine，无限Loop，适当的搬运请求缓存中的请求到请求通道, 以防止request通道的阻塞
 * 每一轮都会先计算出request通道的剩余容量，然后从缓冲中取出相同的数量的请求放入通道
 *
 * 工作goroutine，无限Loop，从chan中拿出任务，实际执行ddl和dml
 */
func (se *SpiderEngine)doSchedule(dbTable string) *middleware.RequestCache {
	reqCache := middleware.NewRequestCache()
	reqChannel := middleware.NewCommonChannel(1000, dbTable)

	//搬运工
	go func(reqCache *middleware.RequestCache, reqChan *middleware.CommonChannel) {
		log.Debug("mover: ", dbTable)
		for {
			//请求通道的空闲数量（请求通道的容量 - 长度）
			remainder := reqChan.Cap() - reqChan.Len()
			log.Debug("mover!", remainder,  reqCache.Capacity() , reqCache.Length())
			if remainder == 0 && se.Closed {
				reqChan.Close()
				return
			}
			var temp *basic.SpiderRequest
			for remainder > 0 {
				temp = reqCache.Get()
				if temp == nil {
					break
				}

				reqChan.Put(temp)
				remainder--
			}

			//time.Sleep(10 * time.Millisecond)
			time.Sleep(5 * time.Second)

		}
	}(reqCache, reqChannel)

	//实际worker
	go func(reqChan *middleware.CommonChannel, se *SpiderEngine) {
		log.Debug("worker: ", dbTable)
		for {
			tmp, ok := reqChan.Get()
			if !ok {
				return
			}
			req := tmp.(*basic.SpiderRequest)
			log.Debug("Got request: ", req.Type)
			//处理请求
			se.ProcessDMLRequest(req)
		}
	}(reqChannel, se)

	log.Infof("Init Schedule: %v", dbTable)
	return reqCache
}