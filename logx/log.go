package logx

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/ssnaruto/xtools/notify"
)

func SetConfig(cfg Config) error {
	if cfg.EsCloudId != "" && cfg.EsUserName != "" && cfg.EsUserPassword != "" && cfg.IndexName != "" {
		eLog, err := newEsLog(EsConfig{
			EsCfg: elasticsearch.Config{
				CloudID:  cfg.EsCloudId,
				Username: cfg.EsUserName,
				Password: cfg.EsUserPassword,
				Transport: &http.Transport{
					MaxIdleConnsPerHost:   10,
					ResponseHeaderTimeout: time.Second,
					DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
					TLSClientConfig: &tls.Config{
						MinVersion: tls.VersionTLS11,
					},
				},
			},
			Index:         cfg.IndexName,
			FlushBytes:    int(1024 * 1024),
			FlushInterval: 10 * time.Second,
			NumWorkers:    2,
			OnFail:        cfg.OnFail,
		})
		if err != nil {
			return err
		}

		logCtx.esLog = eLog
	}
	if cfg.Fields["hostName"] == "" && os.Getenv("HOSTNAME") != "" {
		cfg.Fields["hostName"] = os.Getenv("HOSTNAME")
	}

	logCtx.SetFieldsDefault(cfg.Fields)
	if level, ok := logLevelList[strings.ToLower(cfg.LogLevel)]; ok {
		LogLevelRuntime = level
	}
	return nil
}

type LogX struct {
	esLog
}

func (l *LogX) InfoP(mss ...interface{}) {
	l.Info(mss...)
	notify.PushMessage("<b>INFO</b> / %s", l.sprint(INFO, mss...))
}
func (l *LogX) InfoPf(format string, mss ...interface{}) {
	l.Infof(format, mss...)
	notify.PushMessage("<b>INFO</b> / %s", l.sprint(INFO, fmt.Sprintf(format, mss...)))
}

func (l *LogX) ErrorP(mss ...interface{}) {
	l.Error(mss...)
	notify.PushMessage("<b>ERROR</b> / %s", l.sprint(ERROR, mss...))
}
func (l *LogX) ErrorPf(format string, mss ...interface{}) {
	l.Errorf(format, mss...)
	notify.PushMessage("<b>ERROR</b> / %s", l.sprint(ERROR, fmt.Sprintf(format, mss...)))
}

func (l *LogX) WarnP(mss ...interface{}) {
	l.Warn(mss...)
	notify.PushMessage("<b>WARNING</b> / %s", l.sprint(WARN, mss...))
}
func (l *LogX) WarnPf(format string, mss ...interface{}) {
	l.Warnf(format, mss...)
	notify.PushMessage("<b>WARNING</b> / %s", l.sprint(WARN, fmt.Sprintf(format, mss...)))
}

func (l *LogX) FatalP(mss ...interface{}) {
	notify.PushMessage("<b>FATAL</b> / %s", l.sprint(FATAL, mss...))
	l.Fatal(mss...)
}
func (l *LogX) FatalPf(format string, mss ...interface{}) {
	notify.PushMessage("<b>FATAL</b> / %s", l.sprint(FATAL, fmt.Sprintf(format, mss...)))
	l.Fatalf(format, mss...)
}

func WithFields(fields Fields) LogX {
	return LogX{
		esLog: logCtx.WithFields(fields),
	}
}

func Debug(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_DEBUG {
		return
	}
	logCtx.Debug(mss...)
}
func Debugf(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_DEBUG {
		return
	}
	logCtx.Debugf(format, mss...)
}

func Info(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_INFO {
		return
	}
	logCtx.Info(mss...)
}
func Infof(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_INFO {
		return
	}
	logCtx.Infof(format, mss...)
}
func InfoP(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_INFO {
		return
	}
	logCtx.InfoP(mss...)
}
func InfoPf(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_INFO {
		return
	}
	logCtx.InfoPf(format, mss...)
}

func Error(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_ERROR {
		return
	}
	logCtx.Error(mss...)
}
func Errorf(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_ERROR {
		return
	}
	logCtx.Errorf(format, mss...)
}
func ErrorP(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_ERROR {
		return
	}
	logCtx.ErrorP(mss...)
}
func ErrorPf(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_ERROR {
		return
	}
	logCtx.ErrorPf(format, mss...)
}

func Warn(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_WARN {
		return
	}
	logCtx.Warn(mss...)
}
func Warnf(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_WARN {
		return
	}
	logCtx.Warnf(format, mss...)
}
func WarnP(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_WARN {
		return
	}
	logCtx.WarnP(mss...)
}
func WarnPf(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_WARN {
		return
	}
	logCtx.WarnPf(format, mss...)
}

func Fatal(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_FATAL {
		return
	}
	logCtx.Fatal(mss...)
}
func Fatalf(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_FATAL {
		return
	}
	logCtx.Fatalf(format, mss...)
}
func FatalP(mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_FATAL {
		return
	}
	logCtx.FatalP(mss...)
}
func FatalPf(format string, mss ...interface{}) {
	if LogLevelRuntime.Scores > LOG_LEVEL_FATAL {
		return
	}
	logCtx.FatalPf(format, mss...)
}

func Close() {
	logCtx.Close()
}
