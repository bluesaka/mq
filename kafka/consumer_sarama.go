package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

func Consumr_Sarama() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(addressList, config)
	if err != nil {
		log.Fatalf("kafka new consumer error: %v\n", err)
	}
	defer consumer.Close()

	// topic的分区列表，每个分区都有自己的offset信息，根据情况为每个分区创建消费者进行消费
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("consumer get partition list error: %v\n", err)
	}
	if len(partitionList) == 0 {
		log.Printf("topic: %s has no partition\n", topic)
		return
	}
	fmt.Println("partitionList:", partitionList)

	var wg sync.WaitGroup
	wg.Add(len(partitionList))

	for partition := range partitionList {
		partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("kafka ConsumePartition error: %v\n", err)
		}

		go func() {
			defer wg.Done()
			defer partitionConsumer.Close()
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					fmt.Printf("consumer msg offset: %d, partition: %d, timestamp: %s, value: %s\n",
						msg.Offset, msg.Partition, msg.Timestamp.Format(timeFormat), string(msg.Value))

					// 业务处理 ...

					// 标记
					fmt.Println("water offset:", partitionConsumer.HighWaterMarkOffset())
				case err := <-partitionConsumer.Errors():
					fmt.Printf("consume error: %v\n", err)
				}
			}
		}()
	}

	wg.Wait()
}

func Consumr_Group_Sarama() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(addressList, group, config)
	if err != nil {
		log.Fatalf("kafka NewConsumerGroup error: %v\n", err)
	}
	defer consumerGroup.Close()

	consumer := Consumer{}
	var wg sync.WaitGroup
	wg.Add(1)
	ctx := context.Background()

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{topic}, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
		}
	}()

	wg.Wait()
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
}

func (c Consumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		// 业务处理...
		// 标记消息已消费，若不标记，下次还会再次消费
		session.MarkMessage(message, "")
		return nil
	}

	return nil
}
