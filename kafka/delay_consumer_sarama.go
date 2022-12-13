package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

// Delay_Consumer_Sarama 消费真实队列
func Delay_Consumer_Sarama() {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerGroup, err := sarama.NewConsumerGroup(addressList, groupReal, consumerConfig)
	if err != nil {
		log.Fatalf("kafka NewConsumerGroup error: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	consumer := RealConsumer{
		ready: make(chan bool),
	}

	go func() {
		defer wg.Done()
		if err := consumerGroup.Consume(ctx, []string{topicReal}, &consumer); err != nil {
			log.Panicf("Error from consumer: %v", err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
		consumer.ready = make(chan bool)
	}()

	<-consumer.ready
	wg.Wait()
}

type RealConsumer struct {
	ready chan bool
}

func (r RealConsumer) Setup(session sarama.ConsumerGroupSession) error {
	close(r.ready)
	return nil
}

func (r RealConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (r RealConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(msg.Value), msg.Timestamp, msg.Topic)
			// 处理业务
			// 处理后标记消息完成，保存offset到消费组
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
