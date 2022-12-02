package agg_system

import (
	"github.com/Shopify/sarama"
	"github.com/ssnaruto/xtools/logx"
)

func StartKafkaConsumer(cfg Kafka, job KafkaJob) {

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
