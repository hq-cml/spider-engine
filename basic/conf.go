package basic

import "github.com/Unknwon/goconfig"

/************************************** 配置相关 *************************************/
type SpiderEngineConf struct {
	DataDir       	    string    //数据目录(存放数据文件)
	PartPersistMinCnt   int
	PartMergeMinCnt     int

	LogPath             string    //日志路径
	LogLevel            string    //日志级别

	BindIp				string    //是否启动pprof
	Port		    	string    //pprof端口

	Step                bool      //调试用, 一步步的走
}

/************************************ 全局Conf变量 **********************************/
var	GlobalConf *SpiderEngineConf

//解析配置文件
func ParseConfig(confPath string) (*SpiderEngineConf, error) {
	cfg, err := goconfig.LoadConfigFile(confPath)
	if err != nil {
		panic("Load conf file failed!")
	}

	c := &SpiderEngineConf{}
	if c.DataDir, err = cfg.GetValue("spider", "dataDir"); err != nil {
		panic("Load conf dataDir failed!")
	}

	if c.PartPersistMinCnt, err = cfg.Int("spider", "partitionPersistMinDocCnt"); err != nil {
		panic("Load conf PartPersistMinCnt failed!")
	}

	if c.PartMergeMinCnt, err = cfg.Int("spider", "partitionMergeMinDocCnt"); err != nil {
		panic("Load conf PartPersistMinCnt failed!")
	}

	if c.LogPath, err = cfg.GetValue("log", "logPath"); err != nil {
		panic("Load conf logPath failed!")
	}

	if c.LogLevel, err = cfg.GetValue("log", "logLevel"); err != nil {
		panic("Load conf logLevel failed!")
	}

	if c.BindIp, err = cfg.GetValue("http", "bindIp"); err != nil {
		panic("Load conf pprof failed!" + err.Error())
	}

	if c.Port, err = cfg.GetValue("http", "port"); err != nil {
		panic("Load conf maxIdleCount failed!")
	}

	if c.Step, err = cfg.Bool("debug", "step"); err != nil {
		panic("Load conf step failed!" + err.Error())
	}

	return c, nil
}
