package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	topic := "HVES"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "foo",
		"acks":              "all",
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	go func() {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "foo",
			"auto.offset.reset": "smallest",
		})
		if err != nil {
			log.Fatal(err)
		}
		err = consumer.Subscribe(topic, nil)
		if err != nil {
			log.Fatal(err)
		}

		for {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("consumed message from the queue: %s\n", string(e.Value))
			case *kafka.Error:
				fmt.Printf("%s\n", e)
			}
		}
	}()

	delivery_chan := make(chan kafka.Event, 10000)
	for {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(kafka.PartitionAny)},
			Value:          []byte("FOO"),
		},
			delivery_chan,
		)
		if err != nil {
			log.Fatal(err)
		}
		<-delivery_chan
		time.Sleep(time.Second * 3)
	}
}
