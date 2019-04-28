package log

import (
	"testing"
)

func TestLogDebug(t *testing.T) {
	InitLog("s.log", "debug")
	Debugln("Hello world")
}

func TestLogDebugV(t *testing.T) {
	InitLog("s.log", "debug")
	Debug("Hello world")
}

func TestLogInfo(t *testing.T) {
	InitLog("s.log", "info")
	Infoln("Hello world")
}

func TestLogWarn(t *testing.T) {
	InitLog("s.log", "warn")
	Warnln("Hello world")
}

func TestLogFatal(t *testing.T) {
	InitLog("s.log", "fatal")
	Fatalln("Hello world")
}

func TestLogWarnNoLog(t *testing.T) {
	InitLog("s.log", "warn")
	Debugln("Not log")
}
