package agg_system

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	gojson "github.com/goccy/go-json"
	"github.com/ssnaruto/xtools/logx"
)

func NewWorkerAGGHandler(cfg Config) *AGGHandler {
	handler := AGGHandler{
		name: fmt.Sprintf("%s (Kafka: %s / %s)", cfg.Name, cfg.Kafka.Topic, cfg.Kafka.ConsumerGroupId),
		wg:   &sync.WaitGroup{},
	}
	for _, agCf := range cfg.AGG {
		handler.AGGJob = append(handler.AGGJob, AGGJob{
			AGGConfig: agCf,
			Caching:   NewMemCache(agCf.MaxItems),
		})
	}

	return &handler
}

type AGGHandler struct {
	name    string
	counter int
	sync.Mutex
	wg *sync.WaitGroup

	AGGJob []AGGJob
}

func (w *AGGHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	w.wg.Add(1)
	var counter int

	for msg := range claim.Messages() {
		counter++

		var input InputData
		err := gojson.Unmarshal(msg.Value, &input)
		if err != nil {
			sess.MarkMessage(msg, "")
			continue
		}

		for _, job := range w.AGGJob {

			if job.Validate != nil {
				if job.Validate(input) != nil {
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
	for _, data := range w.AGGJob {
		go func(result AGGJob) {
			result.Flush()
		}(data)
	}
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
	AGGConfig
	Caching MemCache
}

func (a *AGGJob) Flush() {
	if a.Callback == nil {
		return
	}

	for _, partition := range a.Caching.Workers {
		for _, item := range partition.Records {
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
