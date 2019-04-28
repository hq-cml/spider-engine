package config

import (
	"github.com/Unknwon/goconfig"
	"github.com/hq-cml/spider-engine/basic"
)

//解析配置文件
func ParseConfig(confPath string) (*basic.SpiderEngineConf, error) {
	cfg, err := goconfig.LoadConfigFile(confPath)
	if err != nil {
		panic("Load conf file failed!")
	}

	c := &basic.SpiderEngineConf{}
	if c.DataDir, err = cfg.GetValue("spider", "dataDir"); err != nil {
		panic("Load conf pluginKey failed!")
	}

	if c.LogPath, err = cfg.GetValue("log", "logPath"); err != nil {
		panic("Load conf logPath failed!")
	}

	if c.LogLevel, err = cfg.GetValue("log", "logLevel"); err != nil {
		panic("Load conf logLevel failed!")
	}

	if c.Pprof, err = cfg.Bool("pprof", "pprof"); err != nil {
		panic("Load conf pprof failed!" + err.Error())
	}

	if c.PprofPort, err = cfg.GetValue("pprof", "pprofPort"); err != nil {
		panic("Load conf maxIdleCount failed!")
	}

	if c.Step, err = cfg.Bool("debug", "step"); err != nil {
		panic("Load conf step failed!" + err.Error())
	}

	return c, nil
}
