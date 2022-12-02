package agg_system

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/ssnaruto/xtools/logx"
)

func NewWorkerChannel(cfg Config) *WorkerChannel {
	result := &WorkerChannel{
		Config: cfg,
	}

	if result.FlushAfterSeconds <= 0 {
		result.FlushAfterSeconds = 15 // default start AGG after 15 seconds
	}
	return result
}

type Message struct {
	Value []byte
}

type WorkerChannel struct {
	Config
	buff      chan *sarama.ConsumerMessage
	isStarted bool
}

func (a *WorkerChannel) Start() {
	if a.isStarted {
		return
	}

	a.isStarted = true
	a.buff = make(chan *sarama.ConsumerMessage, 20000)
	worker := ChannelJob{
		NewWorkerAGGHandler(a.Config),
	}
	for i := 1; i <= a.NumberOfWorker; i++ {
		logx.Infof("%s / worker up and running...", a.Name)
		go worker.ConsumeClaim(
			&ChanSessison{},
			&ChanClaim{
				buff: a.buff,
			},
		)
	}
}

func (a *WorkerChannel) Add(msg []byte) {
	a.buff <- &sarama.ConsumerMessage{
		Value: msg,
	}
}

func (a *WorkerChannel) Close() {
	close(a.buff)
}

type ChannelJob struct {
	*AGGHandler
}

func (k *ChannelJob) Stop() {
	k.AGGHandler.Wait()
}

type ChanClaim struct {
	buff chan *sarama.ConsumerMessage
}

func (s *ChanClaim) Topic() string {
	return ""
}
func (s *ChanClaim) Partition() int32 {
	return 0
}
func (s *ChanClaim) InitialOffset() int64 {
	return 0
}
func (s *ChanClaim) HighWaterMarkOffset() int64 {
	return 0
}
func (s *ChanClaim) Messages() <-chan *sarama.ConsumerMessage {
	return s.buff
}

type ChanSessison struct{}

func (s *ChanSessison) Claims() map[string][]int32 {
	return map[string][]int32{}
}
func (s *ChanSessison) MemberID() string {
	return ""
}
func (s *ChanSessison) GenerationID() int32 {
	return 0
}
func (s *ChanSessison) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}
func (s *ChanSessison) Commit() {
}
func (s *ChanSessison) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}
func (s *ChanSessison) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
}
func (s *ChanSessison) Context() context.Context {
	return context.Background()
}
