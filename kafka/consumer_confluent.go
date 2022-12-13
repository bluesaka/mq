package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func Consumer_Confluent() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  address,
		"group.id":           groupCf,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		panic(err)
	}
	//partitions := kafka.TopicPartitions{
	//	{Topic: &topic, Partition: 1},
	//}
	//c.Assign(partitions)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			log.Printf("Partition: %s, Message: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			log.Printf("Consumer error: %v, %v\n", err, msg)
		}
	}
}
