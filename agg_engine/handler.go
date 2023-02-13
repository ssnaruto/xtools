package agg_engine

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/ssnaruto/xtools/logx"
	"github.com/ssnaruto/xtools/utils"
	// "github.com/puzpuzpuz/xsync"
)

func NewWorkerAGGHandler(cfg Config) *AGGHandler {
	handler := AGGHandler{
		name:         cfg.Name,
		wg:           &sync.WaitGroup{},
		InputHandler: cfg.InputHandler,
		AGGConfig:    cfg.AGG,
	}
	handler.AGGJob = []*AGGJob{}
	for _, agCf := range handler.AGGConfig {
		if agCf.JobHandler == nil {
			logx.Warn("AGGConfig.JobHandler is required to get started")
			continue
		}

		if agCf.PartitionKey != "" && !utils.InArrayString(agCf.PartitionKey, agCf.Dimensions) {
			agCf.PartitionKey = ""
			logx.Warnf("PartitionKey needs to be in Dimensions")
		}

		handler.AGGJob = append(handler.AGGJob, &AGGJob{
			AGGData: AGGData{
				AGGConfig: agCf,
				Caching:   NewMemCache(agCf.MaxItems),
			},
		})
	}

	return &handler
}

type AGGHandler struct {
	name    string
	counter int
	sync.Mutex
	wg *sync.WaitGroup

	InputHandler
	AGGConfig []AGGConfig
	AGGJob    []*AGGJob
}

func (w *AGGHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	w.wg.Add(1)
	var counter int
	for msg := range claim.Messages() {
		counter++
		rawInput, err := w.Parse(msg.Value)
		if err != nil {
			sess.MarkMessage(msg, "")
			continue
		}

		for _, job := range w.AGGJob {

			for _, inputData := range job.JobHandler.DataHandle(rawInput) {

				dimesions := map[string]string{}
				metrics := map[string]float64{}

				var uniqueKey string
				var partitionId string

				for _, dimensionKey := range job.Dimensions {
					var vlStr string
					if vl, ok := inputData[dimensionKey]; ok {
						vlStr = fmt.Sprintf("%v", vl)
						uniqueKey = uniqueKey + vlStr
						if dimensionKey == job.PartitionKey {
							partitionId = vlStr
						}
					}

					dimesions[dimensionKey] = vlStr
				}

				for _, metricsKey := range job.Metrics {
					metrics[metricsKey] = 0
					if vl, ok := inputData[metricsKey]; ok {
						switch metricValue := vl.(type) {
						case float64:
							metrics[metricsKey] = metricValue
						case float32:
							metrics[metricsKey] = float64(metricValue)
						case int64:
							metrics[metricsKey] = float64(metricValue)
						case int32:
							metrics[metricsKey] = float64(metricValue)
						case int:
							metrics[metricsKey] = float64(metricValue)
						case string:
							vlFloat64, _ := strconv.ParseFloat(metricValue, 64)
							metrics[metricsKey] = vlFloat64
						}
					}
				}

				cache := job.Caching.GetCachePartition(partitionId)
				cache.Lock()
				if cacheData, ok := cache.Get(uniqueKey); ok {
					for k, v := range metrics {
						cacheData.Metrics[k] = cacheData.Metrics[k] + v
					}
					cache.Set(uniqueKey, cacheData)
				} else {
					cache.Set(uniqueKey, MetricsData{
						Dimesions: dimesions,
						Metrics:   metrics,
					})
				}
				cache.Unlock()

			}

		}

		sess.MarkMessage(msg, "")
	}

	w.Lock()
	w.counter += counter
	w.Unlock()

	w.wg.Done()
	return nil

}

func (w *AGGHandler) Flush() {
	w.Lock()
	wg := &sync.WaitGroup{}
	for _, data := range w.AGGJob {
		data.Lock()
		wg.Add(1)
		go func(result AGGData) {
			result.Flush()
			wg.Done()
		}(AGGData{
			AGGConfig: data.AGGConfig,
			Caching:   data.Caching,
		})

		data.ResetCache()
		data.Unlock()
	}

	w.Unlock()
	wg.Wait()
	logx.Infof("%s / Flush data completed...", w.name)
}

func (w *AGGHandler) GetTotalItems() int {
	return w.counter
}

func (w *AGGHandler) Setup(_ sarama.ConsumerGroupSession) error {
	logx.Infof("%s / worker up and running...", w.name)
	return nil
}
func (w *AGGHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}
func (w *AGGHandler) Wait() {
	w.wg.Wait()
}

type AGGJob struct {
	AGGData
	sync.Mutex
}

type AGGData struct {
	AGGConfig
	Caching MemCache
}

func (a *AGGData) ResetCache() {
	a.Caching = NewMemCache(a.MaxItems)
}

func (a *AGGData) Flush() {
	for _, partition := range a.Caching.Workers {
		for _, item := range partition.Items() {
			output := OutputData{}
			for k, vl := range item.Dimesions {
				output[k] = vl
			}
			for k, vl := range item.Metrics {
				output[k] = vl
			}

			a.JobHandler.Flush(output)
		}
	}
}
