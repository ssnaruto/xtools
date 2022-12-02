package logx

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	uuid "github.com/gofrs/uuid"
	"github.com/ssnaruto/xtools/color"
	"github.com/ssnaruto/xtools/utils"
)

type esLog struct {
	Index         string
	BulkIndexer   esutil.BulkIndexer
	fields        Fields
	fieldsDefault Fields
	OnFail        func(IndexError)
}

type EsConfig struct {
	EsCfg           elasticsearch.Config
	Index           string
	FlushBytes      int
	FlushInterval   time.Duration
	NumWorkers      int
	OnFail          func(IndexError)
	DisableCloudLog bool
}

type IndexError struct {
	Index  string
	Reason string
}

func newEsLog(cfg EsConfig) (esLog, error) {
	logEngine := esLog{
		Index:  cfg.Index,
		OnFail: cfg.OnFail,
	}

	if cfg.DisableCloudLog == true {
		return logEngine, nil
	}

	if cfg.EsCfg.Transport == nil {
		cfg.EsCfg.Transport = &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
		}
	}
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 1
	}

	es, err := elasticsearch.NewClient(cfg.EsCfg)
	if err != nil {
		return logEngine, fmt.Errorf("Error creating the client: %s", err)
	}

	logEngine.BulkIndexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        es,                // The Elasticsearch client
		NumWorkers:    cfg.NumWorkers,    // The number of worker goroutines
		FlushBytes:    cfg.FlushBytes,    // The flush threshold in bytes
		FlushInterval: cfg.FlushInterval, // The periodic flush interval
	})
	if err != nil {
		return logEngine, fmt.Errorf("Error creating the Bulk Indexer: %s", err)
	}

	return logEngine, nil
}

func (e *esLog) Close() error {
	if e.BulkIndexer != nil {
		return e.BulkIndexer.Close(context.Background())
	}
	return nil
}

func (e *esLog) SetFieldsDefault(fields Fields) {
	e.fieldsDefault = fields
}

func (e *esLog) WithFields(fields Fields) esLog {
	return esLog{
		Index:         e.Index,
		BulkIndexer:   e.BulkIndexer,
		OnFail:        e.OnFail,
		fieldsDefault: e.fieldsDefault,
		fields:        fields,
	}
}

func (e *esLog) Debug(mss ...interface{}) error {
	fmt.Println(e.sprintWithColor(DEBUG, mss...))
	return e.cloudLog(DEBUG, mss...)
}
func (e *esLog) Debugf(format string, mss ...interface{}) error {
	msg := fmt.Sprintf(format, mss...)
	fmt.Println(e.sprintWithColor(DEBUG, msg))
	return e.cloudLog(DEBUG, msg)
}

func (e *esLog) Info(mss ...interface{}) error {
	fmt.Println(e.sprintWithColor(INFO, mss...))
	return e.cloudLog(INFO, mss...)
}
func (e *esLog) Infof(format string, mss ...interface{}) error {
	msg := fmt.Sprintf(format, mss...)
	fmt.Println(e.sprintWithColor(INFO, msg))
	return e.cloudLog(INFO, msg)
}

func (e *esLog) Warn(mss ...interface{}) error {
	fmt.Println(e.sprintWithColor(WARN, mss...))
	return e.cloudLog(WARN, mss...)
}
func (e *esLog) Warnf(format string, mss ...interface{}) error {
	msg := fmt.Sprintf(format, mss...)
	fmt.Println(e.sprintWithColor(WARN, msg))
	return e.cloudLog(WARN, msg)
}

func (e *esLog) Error(mss ...interface{}) error {
	fmt.Println(e.sprintWithColor(ERROR, mss...))
	return e.cloudLog(ERROR, mss...)
}
func (e *esLog) Errorf(format string, mss ...interface{}) error {
	msg := fmt.Sprintf(format, mss...)
	fmt.Println(e.sprintWithColor(ERROR, msg))
	return e.cloudLog(ERROR, msg)
}

func (e *esLog) Fatal(mss ...interface{}) error {
	e.cloudLog(FATAL, mss...)
	e.Close()
	log.Fatal(e.sprintWithColor(FATAL, mss...))
	return nil
}
func (e *esLog) Fatalf(format string, mss ...interface{}) error {
	msg := fmt.Sprintf(format, mss...)
	e.cloudLog(FATAL, msg)
	e.Close()
	log.Fatal(e.sprintWithColor(FATAL, msg))
	return nil
}

func (e *esLog) sprintWithColor(level LogLevel, mss ...interface{}) string {
	level.Name = getLevelInColor(level.Name)
	return e.sprint(level, mss...)
}

func (e *esLog) sprint(level LogLevel, mss ...interface{}) string {

	var msg string
	msg += fmt.Sprintf(
		"%s [%s] %s",
		time.Now().Format("2006-01-02 15:04:05 MST"),
		level.Name,
		fmt.Sprint(mss...),
	)

	if e.fieldsDefault != nil {
		for k, vl := range e.fieldsDefault {
			msg += fmt.Sprintf(" / %s=%s", k, fmt.Sprint(vl))
		}
	}
	if e.fields != nil {
		for k, vl := range e.fields {
			msg += fmt.Sprintf(" / %s=%s", k, fmt.Sprint(vl))
		}
	}

	if level.Scores >= LOG_LEVEL_ERROR {
		msg += "\n" + string(debug.Stack())
	}
	return msg
}

func (e *esLog) cloudLog(level LogLevel, mss ...interface{}) error {

	if e.BulkIndexer == nil || e.Index == "" {
		return nil
	}

	msg := fmt.Sprint(mss...)
	if level.Scores >= LOG_LEVEL_ERROR {
		msg += "\n" + string(debug.Stack())
	}

	data := Fields{
		"level":      level.Name,
		"message":    msg,
		"@timestamp": time.Now().Format("2006-01-02 15:04:05 MST"),
	}
	if e.fieldsDefault != nil {
		for k, vl := range e.fieldsDefault {
			data[k] = vl
		}
	}

	if e.fields != nil {
		for k, vl := range e.fields {
			data[k] = vl
		}
	}

	rawUuid, _ := uuid.NewV4()
	err := e.BulkIndexer.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			// Action field configures the operation to perform (index, create, delete, update)
			Index:      e.Index,
			Action:     "index",
			DocumentID: rawUuid.String(),
			Body:       bytes.NewReader(utils.ToByte(data)),
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			},
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				e.OnFail(IndexError{
					Index:  res.Index,
					Reason: res.Error.Reason,
				})
			},
		},
	)

	return err
}

func getLevelInColor(level string) string {
	level = strings.ToUpper(level)
	switch level {
	case "DEBUG":
		return color.InCyan(level)

	case "INFO":
		return color.InGreen(level)

	case "WARNING":
		return color.InYellow(level)

	case "ERROR":
		return color.InRed(level)

	case "FATAL":
		return color.InPurple(level)
	}

	return color.InWhite(level)
}
