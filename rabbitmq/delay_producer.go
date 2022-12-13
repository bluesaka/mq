package rabbitmq

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"time"
)

// ProducerDelay producer delay
func ProducerDelay() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatalf("amqp dial error: %v\n", err)
	}
	defer conn.Close()

	// channel
	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("open channel error: %v\n", err)
	}
	defer channel.Close()

	args := amqp.Table{
		"x-dead-letter-exchange":    DelayExchange, // 死信队列 => 延时交换机
		"x-dead-letter-routing-key": DeadQueue,     // 路由键默认是队列名
		//"x-message-ttl": 10000, // 队列的所有消息过期时间，优先级小于消息指定的Expiration过期时间
	}

	// 声明死信队列
	if _, err := channel.QueueDeclare(DeadQueue, true, false, false, false, args); err != nil {
		log.Fatalf("DeadQueue declare error: %v\n", err)
	}

	// 声明延时队列
	if _, err := channel.QueueDeclare(DelayQueue, true, false, false, false, nil); err != nil {
		log.Fatalf("DelayQueue declare error: %v\n", err)
	}

	// 声明延时交换机
	if err := channel.ExchangeDeclare(DelayExchange, "direct", true, false, false, false, nil); err != nil {
		log.Fatalf("DelayExchange declare error: %v\n", err)
	}

	// 绑定死信队列+延时队列+延时交换机
	if err := channel.QueueBind(DelayQueue, DeadQueue, DelayExchange, false, nil); err != nil {
		log.Fatalf("QueueBind error: %v\n", err)
	}

	data, err := json.Marshal(map[string]interface{}{
		"name": "abc",
		"time": time.Now().Format("2006-01-02 15:04:05"),
	})
	if err != nil {
		log.Fatalf("Marshal error: %v\n", err)
	}

	err = channel.Publish("", DeadQueue, false, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         data,
		DeliveryMode: 2,
		Expiration:   TTL, // 过期时间，单位ms
	})
	if err != nil {
		log.Fatalf("publish msg error: %v\n", err)
	}

	log.Println("success")
}
