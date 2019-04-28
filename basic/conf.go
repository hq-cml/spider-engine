package basic

/************************************** 配置相关 *************************************/
type SpiderEngineConf struct {
	DataDir       	    string    //数据目录(存放数据文件)

	LogPath             string    //日志路径
	LogLevel            string    //日志级别

	Pprof				bool      //是否启动pprof
	PprofPort			string    //pprof端口

	Step                bool      //调试用, 一步步的走
}

/************************************ 全局Conf变量 **********************************/
var	GlobalConf *SpiderEngineConf