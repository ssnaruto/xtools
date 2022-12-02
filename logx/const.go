package logx

const (
	LOG_LEVEL_DEBUG = 0
	LOG_LEVEL_INFO  = 1
	LOG_LEVEL_WARN  = 2
	LOG_LEVEL_ERROR = 3
	LOG_LEVEL_FATAL = 4
)

var OFF = LogLevel{Name: "off", Scores: -1}
var DEBUG = LogLevel{Name: "debug", Scores: 0}
var INFO = LogLevel{Name: "info", Scores: 1}
var WARN = LogLevel{Name: "warn", Scores: 2}
var ERROR = LogLevel{Name: "error", Scores: 3}
var FATAL = LogLevel{Name: "fatal", Scores: 4}

var logLevelList = map[string]LogLevel{"off": OFF, "debug": DEBUG, "info": INFO, "warn": WARN, "error": ERROR, "fatal": FATAL}
var LogLevelRuntime = INFO
var logCtx LogX

type LogLevel struct {
	Name   string
	Scores int
}

type Config struct {
	EsCloudId      string
	EsUserName     string
	EsUserPassword string
	IndexName      string
	Fields         Fields
	OnFail         func(IndexError)
	LogLevel       string
}

type Fields map[string]interface{}
