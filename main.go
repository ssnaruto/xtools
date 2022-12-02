package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	gojson "github.com/goccy/go-json"
	"github.com/ssnaruto/xtools/agg_system"
	"github.com/ssnaruto/xtools/logx"
	"github.com/ssnaruto/xtools/shutdown"
	"github.com/ssnaruto/xtools/utils"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	logx.InfoP("Service started...")

	// go startWorker(ctx)
	go startWorkerChannel(ctx)

	signChan := make(chan os.Signal, 1)
	signal.Notify(signChan, os.Interrupt, syscall.SIGTERM)
	<-signChan

	logx.InfoP("Stop signal is ping, waiting all worker done to exit....")
	cancel()
	shutdown.Wait()

	logx.Info("All worker completed, now system is exit....")
	logx.Close()

	os.Exit(0)
}

type InputDemo struct {
	Time        string `json:"time"`
	SiteId      string `json:"siteId"`
	TagId       int    `json:"tagId"`
	CountryCode string `json:"countryCode"`

	BidRequest  int     `json:"bidRequest"`
	BidResponse int     `json:"bidResponse"`
	Impressions int     `json:"impressions"`
	Revenue     float64 `json:"revenue"`
}

func startWorker(ctx context.Context) {

	go func() {

		mssQue := NewSaramaAsyncProducer("test")
		for i := 0; i < 5; i++ {

			for i := 0; i < 500000; i++ {

				dt := InputDemo{
					Time:        "2022-11-29",
					SiteId:      "100",
					TagId:       222,
					CountryCode: "US",

					BidRequest:  1,
					BidResponse: 1,
					Impressions: 1,
					Revenue:     0.5,
				}

				if jsonByte, err := gojson.Marshal(dt); err == nil {
					mssQue.Input() <- &sarama.ProducerMessage{
						Topic: "test-worker",
						Value: sarama.ByteEncoder(jsonByte),
					}
				}

				dx := InputDemo{
					Time:        "2022-11-28",
					SiteId:      "100",
					TagId:       222,
					CountryCode: "US",

					BidRequest:  1,
					BidResponse: 1,
					Impressions: 1,
					Revenue:     0.5,
				}

				if jsonByte, err := gojson.Marshal(dx); err == nil {
					mssQue.Input() <- &sarama.ProducerMessage{
						Topic: "test-worker",
						Value: sarama.ByteEncoder(jsonByte),
					}
				}

				dc := InputDemo{
					Time:        "2022-11-28",
					SiteId:      "100",
					TagId:       88,
					CountryCode: "US",

					BidRequest:  1,
					BidResponse: 1,
					Impressions: 1,
					Revenue:     0.2,
				}

				if jsonByte, err := gojson.Marshal(dc); err == nil {
					mssQue.Input() <- &sarama.ProducerMessage{
						Topic: "test-worker",
						Value: sarama.ByteEncoder(jsonByte),
					}
				}

			}

		}

		mssQue.AsyncClose()
		time.Sleep(10 * time.Second)
		fmt.Println("INSERT DONE..........")

	}()

	shutdown.Add(1)
	worker := agg_system.New(
		agg_system.Config{
			Name: "Worker Kafka",
			Kafka: &agg_system.Kafka{
				Topic:           "test-worker",
				ConsumerGroupId: "test-worker",
				Host:            []string{"127.0.0.1:9092"},
				KafkaVersion:    sarama.V2_8_0_0,
			},

			AGG: []agg_system.AGGConfig{
				agg_system.AGGConfig{
					Dimensions:   []string{"time", "siteId", "tagId", "countryCode"},
					Metrics:      []string{"bidRequest", "bidResponse", "impressions", "revenue"},
					PartitionKey: "siteId",
					MaxItems:     10000,
					Callback: func(result agg_system.OutputData) {

						fmt.Println(utils.ToString(result))

					},
				},
				agg_system.AGGConfig{
					Dimensions:   []string{"siteId", "countryCode"},
					Metrics:      []string{"bidRequest", "bidResponse"},
					PartitionKey: "xxx",
					MaxItems:     10000,
					Callback: func(result agg_system.OutputData) {

						fmt.Println(utils.ToString(result))

					},
				},
			},

			StartAggAfterSeconds: 10,
			FlushAfterSeconds:    10,
			NumberOfWorker:       1,
		},
	)

	go worker.Start()

	select {
	case <-ctx.Done():
		worker.Close()
	}

	fmt.Println("COMPLETED......")
	shutdown.Done()

}

func NewSaramaAsyncProducer(producerName string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.RequiredAcks = sarama.WaitForLocal     // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy // Compress messages
	config.Producer.Flush.Frequency = 3 * time.Second      // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		logx.Fatalf("Failed to start Sarama producer %s: %s", producerName, err)
	}

	go func(pName string) {
		for err := range producer.Errors() {
			logx.Warnf("Failed to write message to kafka on producer %s: %s", producerName, err)
		}
	}(producerName)

	return producer
}

func startWorkerChannel(ctx context.Context) {

	shutdown.Add(1)
	worker := agg_system.New(
		agg_system.Config{
			Name: "Worker Channel",
			AGG: []agg_system.AGGConfig{
				agg_system.AGGConfig{
					Dimensions:   []string{"time", "siteId", "tagId", "countryCode"},
					Metrics:      []string{"bidRequest", "bidResponse", "impressions", "revenue"},
					PartitionKey: "siteId",
					MaxItems:     10000,
					Callback: func(result agg_system.OutputData) {

						time.Sleep(500 * time.Millisecond)
						fmt.Println(utils.ToString(result))

					},
				},
				agg_system.AGGConfig{
					Dimensions:   []string{"siteId", "countryCode"},
					Metrics:      []string{"bidRequest", "bidResponse"},
					PartitionKey: "siteId",
					MaxItems:     10000,
					Callback: func(result agg_system.OutputData) {

						fmt.Println("-----------------")
						fmt.Println(utils.ToString(result))
						fmt.Println("-----------------")

					},
				},
			},

			StartAggAfterSeconds: 20,
			FlushAfterSeconds:    10,
			NumberOfWorker:       1,
		},
	)

	go worker.Start()

	go func() {

		for i := 0; i < 5; i++ {

			for i := 0; i < 1000000; i++ {

				dt := InputDemo{
					Time:        "2022-11-29",
					SiteId:      "100",
					TagId:       222,
					CountryCode: "US",

					BidRequest:  1,
					BidResponse: 1,
					Impressions: 1,
					Revenue:     0.5,
				}

				if jsonByte, err := gojson.Marshal(dt); err == nil {
					worker.Add(jsonByte)
				}

				dx := InputDemo{
					Time:        "2022-11-28",
					SiteId:      "100",
					TagId:       222,
					CountryCode: "US",

					BidRequest:  1,
					BidResponse: 1,
					Impressions: 1,
					Revenue:     0.5,
				}

				if jsonByte, err := gojson.Marshal(dx); err == nil {
					worker.Add(jsonByte)
				}

				dc := InputDemo{
					Time:        "2022-11-28",
					SiteId:      "100",
					TagId:       88,
					CountryCode: "US",

					BidRequest:  1,
					BidResponse: 1,
					Impressions: 1,
					Revenue:     0.2,
				}

				if jsonByte, err := gojson.Marshal(dc); err == nil {
					worker.Add(jsonByte)
				}

			}

		}

		time.Sleep(10 * time.Second)
		fmt.Println("INSERT DONE..........")

	}()

	select {
	case <-ctx.Done():
		worker.Close()
	}

	fmt.Println("COMPLETED......")
	shutdown.Done()

}
