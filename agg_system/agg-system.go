package agg_system

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ssnaruto/xtools/logx"
)

func New(cfg Config) AGGEngine {
	result := AGGEngine{
		Config: cfg,
		wg:     &sync.WaitGroup{},
	}

	if result.StartAggAfterSeconds <= 0 {
		result.StartAggAfterSeconds = 1 * 60 // default start AGG after 1 minutes
	}
	if result.FlushAfterSeconds <= 0 {
		result.FlushAfterSeconds = 15 // default start AGG after 15 seconds
	}

	return result
}

type AGGEngine struct {
	Config

	stopSignal bool
	wg         *sync.WaitGroup
}

func (a *AGGEngine) Start() {
	a.wg.Add(1)
	var timeCounter int
	for {
		if a.stopSignal {
			break
		}

		if timeCounter == a.StartAggAfterSeconds {
			timeCounter = 0
		}
		if timeCounter == 0 {
			a.StartAGG()
		}

		time.Sleep(1 * time.Second)
		timeCounter++
	}

	a.wg.Done()
}

func (a *AGGEngine) StartAGG() {

	logx.Infof("%s / Start AGG data in %v seconds", a.Name, a.FlushAfterSeconds)

	worker := NewKafkaJob(a.Config)
	for i := 1; i <= a.NumberOfWorker; i++ {
		go StartKafkaConsumer(
			a.Kafka,
			worker,
		)
	}

	time.Sleep(time.Duration(a.FlushAfterSeconds) * time.Second)
	worker.Stop()

	logx.Infof("%s / Completed to roll-up %v items, now start flushing data...", a.Name, worker.GetTotalItems())
	worker.Flush()

}

func (a *AGGEngine) Close() {
	a.stopSignal = true
	a.wg.Wait()
	logx.Infof("%s / Closed...", a.Name)
}

type Config struct {
	Name  string
	Kafka Kafka
	AGG   []AGGConfig

	StartAggAfterSeconds int
	FlushAfterSeconds    int
	NumberOfWorker       int
}

type Kafka struct {
	Topic           string
	ConsumerGroupId string
	Host            []string
	KafkaVersion    sarama.KafkaVersion
}

type AGGConfig struct {
	Dimensions []string
	Metrics    []string

	PartitionKey string
	MaxItems     float64

	Callback func(OutputData)
}
