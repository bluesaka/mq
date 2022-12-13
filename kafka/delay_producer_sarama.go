package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"strconv"
	"time"
)

// Delay_Producer_Sarama 生成消息到延迟队列
func Delay_Producer_Sarama() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer(addressList, config)
	if err != nil {
		log.Fatalf("kafka new producer error: %v", err)
	}
	defer producer.Close()

	for i := 1; i <= 5; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     topicDelay,
			Value:     sarama.ByteEncoder("delay-" + strconv.Itoa(i)),
			Timestamp: time.Now(),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("send delay msg error:", err)
		} else {
			log.Printf("delay msg partition: %d, offset: %d", partition, offset)
		}
		time.Sleep(time.Second)
	}
}
