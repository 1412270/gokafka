package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer      *kafka.Producer
	topic         string
	delivery_chan chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer:      p,
		topic:         topic,
		delivery_chan: make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) placeOrder(oderType string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %d", oderType, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &op.topic,
			Partition: int32(kafka.PartitionAny),
		},
		Value: payload,
	},
		op.delivery_chan,
	)
	if err != nil {
		log.Fatal(err)
	}
	<-op.delivery_chan
	fmt.Printf("placed order on the queue %s\n", format)
	return nil
}

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

	op := NewOrderPlacer(p, topic)
	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("market", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 3)
	}
}
