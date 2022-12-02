package agg_system

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ssnaruto/xtools/logx"
)

func NewWorkerKafka(cfg Config) *WorkerKafka {
	result := &WorkerKafka{
		Config: cfg,
	}

	if result.StartAggAfterSeconds <= 0 {
		result.StartAggAfterSeconds = 1 * 60 // default start AGG after 1 minutes
	}
	if result.FlushAfterSeconds <= 0 {
		result.FlushAfterSeconds = 15 // default start AGG after 15 seconds
	}
	return result
}

type WorkerKafka struct {
	Config
	c          CronJob
	stopSignal chan bool
	isRunning  bool
	isStarted  bool
}

func (a *WorkerKafka) Start() {
	if a.isStarted {
		return
	}

	a.isStarted = true
	a.c = NewCronJob(a.StartAggAfterSeconds)
	a.c.Add(a.StartAGG)
	a.c.Start()
}

func (a *WorkerKafka) StartAGG() {

	logx.Infof("%s / Start AGG data in %v seconds", a.Name, a.FlushAfterSeconds)

	a.stopSignal = make(chan bool)
	a.isRunning = true
	worker := NewKafkaJob(a.Config)

	for i := 1; i <= a.NumberOfWorker; i++ {
		go StartKafkaConsumer(
			a.Kafka,
			worker,
		)
	}

	go func() {
		time.Sleep(time.Duration(a.FlushAfterSeconds) * time.Second)
		a.Stop()
	}()

	<-a.stopSignal
	worker.Stop()

	logx.Infof("%s / Completed to roll-up %v items, now start flushing data...", a.Name, worker.GetTotalItems())
	worker.Flush()

}

func (a *WorkerKafka) Add(msg []byte) {}

func (a *WorkerKafka) Stop() {
	if a.isRunning {
		a.stopSignal <- true
	}
	a.isRunning = false
}

func (a *WorkerKafka) Close() {
	a.Stop()
	a.c.Stop()
	logx.Infof("%s / Closed...", a.Name)
}

type KafkaJob struct {
	ctx    context.Context
	cancel context.CancelFunc
	*AGGHandler
}

func (k *KafkaJob) Stop() {
	k.cancel()
	k.AGGHandler.Wait()
}

func NewKafkaJob(cfg Config) KafkaJob {
	ctx, cancel := context.WithCancel(context.Background())
	return KafkaJob{
		ctx:        ctx,
		cancel:     cancel,
		AGGHandler: NewWorkerAGGHandler(cfg),
	}
}

func StartKafkaConsumer(cfg *Kafka, job KafkaJob) {

	config := sarama.NewConfig()
	config.Version = cfg.KafkaVersion
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.ChannelBufferSize = 5000
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	client, err := sarama.NewConsumerGroup(cfg.Host, cfg.ConsumerGroupId, config)
	if err != nil {
		logx.FatalPf("Error creating consumer group client on Group Id %s: %s", cfg.Topic, err)
		return
	}

	// Track errors
	go func() {
		for err := range client.Errors() {
			logx.Warnf("Error in consumer group of topic %s: %s", cfg.Topic, err)
		}
	}()

	defer func() {
		if err = client.Close(); err != nil {
			logx.Errorf("%s: Error closing client: %s", cfg.Topic, err)
		}
	}()

	for {
		if err := client.Consume(job.ctx, []string{cfg.Topic}, job.AGGHandler); err != nil {
			logx.Errorf("%s: Error from consumer: %s", cfg.Topic, err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if job.ctx.Err() != nil {
			return
		}
	}

}
