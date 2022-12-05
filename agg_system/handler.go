package agg_system

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	gojson "github.com/goccy/go-json"
	"github.com/ssnaruto/xtools/logx"
	"github.com/ssnaruto/xtools/utils"
	// "github.com/puzpuzpuz/xsync"
)

func NewWorkerAGGHandler(cfg Config) *AGGHandler {
	handler := AGGHandler{
		name:      cfg.Name,
		wg:        &sync.WaitGroup{},
		AGGConfig: cfg.AGG,
	}
	handler.AGGJob = []*AGGJob{}
	for _, agCf := range handler.AGGConfig {
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

	AGGConfig []AGGConfig
	AGGJob    []*AGGJob
}

func (w *AGGHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	w.wg.Add(1)
	var counter int
	for msg := range claim.Messages() {
		counter++
		var rawInput InputData
		err := gojson.Unmarshal(msg.Value, &rawInput)
		if err != nil {
			sess.MarkMessage(msg, "")
			continue
		}

		for _, job := range w.AGGJob {

			input := rawInput
			if job.Validate != nil {
				input, err = job.Validate(rawInput)
				if err != nil {
					continue
				}
			}

			dimesions := map[string]string{}
			metrics := map[string]float64{}

			var uniqueKey string
			var partitionId string
			for _, dimensionKey := range job.Dimensions {
				if vl, ok := input[dimensionKey]; ok {
					vlStr := fmt.Sprintf("%v", vl)
					dimesions[dimensionKey] = vlStr
					uniqueKey = uniqueKey + vlStr
					if dimensionKey == job.PartitionKey {
						partitionId = vlStr
					}
				}
			}

			for _, metricsKey := range job.Metrics {
				if vl, ok := input[metricsKey]; ok {
					metricData, _ := vl.(float64)
					metrics[metricsKey] = metricData
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

type InputData map[string]interface{}
type OutputData map[string]interface{}

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
	if a.Callback == nil {
		return
	}

	for _, partition := range a.Caching.Workers {
		for _, item := range partition.Items() {
			output := OutputData{}
			for k, vl := range item.Dimesions {
				output[k] = vl
			}
			for k, vl := range item.Metrics {
				output[k] = vl
			}

			a.Callback(output)
		}
	}
}
