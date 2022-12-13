package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func Producer_Sarama() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要leader和follower都确认
	//config.Producer.Partitioner = sarama.NewRandomPartitioner // 随机一个partition
	config.Producer.Partitioner = sarama.NewManualPartitioner // 选择message中的Partition，前提是要kafka的topic中有该分区
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回
	config.Producer.Return.Errors = true
	//config.Version = sarama.V0_11_0_2

	producer, err := sarama.NewAsyncProducer(addressList, config)
	if err != nil {
		log.Fatalf("kafka new producer error: %v", err)
	}
	defer producer.AsyncClose()

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Partition: 1, //搭配sarama.NewManualPartitioner手动指定分区，前提是该topic有该分区，不然会报错`kafka: partitioner returned an invalid partition index`
		Timestamp: time.Now(),
	}

	fmt.Println("input msg:")
	var value string
	for {
		fmt.Scanln(&value)
		msg.Value = sarama.ByteEncoder(value)
		fmt.Printf("stdin: %s\n", value)

		producer.Input() <- msg

		select {
		case success := <-producer.Successes():
			fmt.Printf("produce success, partition: %d, offset: %d, timestamp: %s\n", success.Partition, success.Offset, success.Timestamp.Format(timeFormat))
		case err := <-producer.Errors():
			fmt.Printf("produce error: %v\n", err.Err)
		}
	}
}
