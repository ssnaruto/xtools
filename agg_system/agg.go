package agg_system

import (
	"github.com/Shopify/sarama"
)

func New(cfg Config) AGGEngine {
	if cfg.Kafka != nil {
		return NewWorkerKafka(cfg)
	}

	return NewWorkerChannel(cfg)
}

type AGGEngine interface {
	Start()
	Add([]byte)
	Close()
}

type Config struct {
	Name string
	AGG  []AGGConfig

	Kafka     *Kafka
	InputChan chan []byte

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

	JobHandler
}

type JobHandler interface {
	Validate(InputData) (InputData, error)
	Flush(OutputData)
	Error(error, []byte)
}
