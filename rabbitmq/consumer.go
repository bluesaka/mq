package rabbitmq

import (
	"github.com/streadway/amqp"
	"log"
)

// Consumer consumer
func Consumer() {
	consume(QueueName)
}

// consume do consume
func consume(queueName string) {
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

	// queue declare
	queue, err := channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue declare error: %v\n", err)
	}
	log.Printf("queue: %+v\n", queue)

	msgChan, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume error: %v", err)
	}

	go func() {
		for msg := range msgChan {
			log.Printf("message: %+v, body string: %s\n", msg, string(msg.Body))
			if err := msg.Ack(true); err != nil {
				log.Println("ack error:", err)
			}
		}
	}()

	log.Println("connected to RabbitMQ, waiting for messages")
	select {}
}
