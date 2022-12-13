package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func Producer_Confluent() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": address, // 注意不能用localhost
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	for _, word := range []string{"c---1", "c---2", "c---3", "c---4"} {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(word),
		}, nil)

		if err != nil {
			log.Printf("produce to queue error: %v\n", err)
		} else {
			log.Printf("produce to queue success: %v\n", word)
		}
	}

	// Wait for message deliveries before shutting down
	p.Flush(5 * 1000)
}
