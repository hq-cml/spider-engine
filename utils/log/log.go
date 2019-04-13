package log

/*
 * 日志相关
 */
import (
	"log"
	"os"
)

type SpiderLog struct {
	*log.Logger
	level   int
}

const (
	SPIDER_LOG_LEVEL_DEBUG = iota
	SPIDER_LOG_LEVEL_INFO
	SPIDER_LOG_LEVEL_WARN
	SPIDER_LOG_LEVEL_ERR
	SPIDER_LOG_LEVEL_FATAL
)

var spiderLog SpiderLog

func init() {
	InitLog("", "debug")
}

func InitLog(path string, level string) {
	var f *os.File
	var err error
	if path == "" { //如果没有文件路径，则用标准错误Stderr
		f = os.Stderr
	} else {
		f, err = os.OpenFile(path, os.O_RDWR | os.O_CREATE | os.O_APPEND , 0755)
		if err != nil {
			log.Fatal(err)
		}
	}

	levelInt := SPIDER_LOG_LEVEL_DEBUG
	switch level {
	case "debug", "Debug", "DEBUG":
		levelInt = SPIDER_LOG_LEVEL_DEBUG
	case "info", "Info", "INFO":
		levelInt = SPIDER_LOG_LEVEL_INFO
	case "warn", "Warn", "WARN":
		levelInt = SPIDER_LOG_LEVEL_WARN
	case "err", "Err", "ERR":
		levelInt = SPIDER_LOG_LEVEL_ERR
	case "fatal", "Fatal", "FATAL":
		levelInt = SPIDER_LOG_LEVEL_FATAL
	}
	spiderLog = SpiderLog {
		Logger:log.New(f, "", log.LstdFlags),
		level: levelInt,
	}
	//spiderLog.Logger.SetFlags(log.Ldate | log.Lshortfile)
}

func Debugf(format string, v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_DEBUG {return}
	spiderLog.Printf("[DEBUG] "+format, v...)
}

func Debugln(v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_DEBUG {return}
	v1 := []interface{}{"[DEBUG]"}
	v1 = append(v1, v...)
	spiderLog.Println(v1...)
}

func Debug(v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_DEBUG {return}
	v1 := []interface{}{"[DEBUG]"}
	v1 = append(v1, v...)
	spiderLog.Print(v1...)
}

func Infof(format string, v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_INFO {return}
	spiderLog.Printf("[INFO] "+format, v...)
}

func Infoln(v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_INFO {return}
	v1 := []interface{}{"[INFO]"}
	v1 = append(v1, v...)
	spiderLog.Println(v1...)
}

func Info(v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_INFO {return}
	v1 := []interface{}{"[INFO]"}
	v1 = append(v1, v...)
	spiderLog.Print(v1...)
}

func Warnf(format string, v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_WARN {return}
	spiderLog.Printf("[WARN] "+format, v...)
}

func Warnln(v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_WARN {return}
	v1 := []interface{}{"[WARN]"}
	v1 = append(v1, v...)
	spiderLog.Println(v1...)
}

func Warn(v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_WARN {return}
	v1 := []interface{}{"[WARN]"}
	v1 = append(v1, v...)
	spiderLog.Print(v1...)
}


func Errf(format string, v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_ERR {return}
	spiderLog.Printf("[ERROR] "+format, v...)
}

func Errln(v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_ERR {return}
	v1 := []interface{}{"[ERROR]"}
	v1 = append(v1, v...)
	spiderLog.Println(v1...)
}

func Err(v ...interface{}) {
	if spiderLog.level > SPIDER_LOG_LEVEL_ERR {return}
	v1 := []interface{}{"[ERROR]"}
	v1 = append(v1, v...)
	spiderLog.Print(v1...)
}

func Fatalf(format string, v ...interface{}) {
	spiderLog.Fatalf("[FATAL] "+format, v...)
}

func Fatalln(v ...interface{}) {
	v1 := []interface{}{"[FATAL]"}
	v1 = append(v1, v...)
	spiderLog.Fatalln(v1...)
}

func Fatal(v ...interface{}) {
	v1 := []interface{}{"[FATAL]"}
	v1 = append(v1, v...)
	spiderLog.Fatal(v1...)
}
