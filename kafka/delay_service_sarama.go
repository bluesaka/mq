package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"sync"
	"time"
)

// Delay_Service_Sarama 消费延迟队列，并生成消息到真实队列
func Delay_Service_Sarama() {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerGroup, err := sarama.NewConsumerGroup(addressList, groupDelay, consumerConfig)
	if err != nil {
		log.Fatalf("kafka NewConsumerGroup error: %v", err)
	}
	defer consumerGroup.Close()

	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(addressList, producerConfig)
	if err != nil {
		log.Fatalf("kafka NewSyncProducer error: %v", err)
	}
	defer producer.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	ctx := context.Background()
	// 消息10秒过期
	delayConsumer := NewDelayConsumer(producer, 20*time.Second)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{topicDelay}, delayConsumer); err != nil {
				log.Println("consumer error:", err)
				break
			}
		}
	}()

	wg.Wait()
}

type DelayConsumer struct {
	producer sarama.SyncProducer
	delay    time.Duration
}

func NewDelayConsumer(producer sarama.SyncProducer, delay time.Duration) *DelayConsumer {
	return &DelayConsumer{
		producer: producer,
		delay:    delay,
	}
}

func (d DelayConsumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (d DelayConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (d DelayConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("consumer msg is: %v", string(msg.Value))
		// 如果消息已经超时，把消息发送到真实队列
		if time.Now().Sub(msg.Timestamp) >= d.delay {
			_, _, err := d.producer.SendMessage(&sarama.ProducerMessage{
				Topic: topicReal,
				Key:   sarama.ByteEncoder(msg.Key),
				Value: sarama.ByteEncoder(msg.Value),
			})
			if err == nil {
				log.Printf("msg: %s, ts: %d send to real topic", msg.Value, msg.Timestamp.Unix())
				session.MarkMessage(msg, "")
				//continue
			}
			return nil
		}
		time.Sleep(time.Second)
		return nil
	}

	return nil
}
