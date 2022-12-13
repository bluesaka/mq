package rabbitmq

import (
	"github.com/streadway/amqp"
	"log"
	"strconv"
)

// Producer producer
func Producer() {
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
	queue, err := channel.QueueDeclare(QueueName, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue declare error: %v\n", err)
	}
	log.Printf("queue: %+v\n", queue)

	// publish message
	for i := 1; i <= 10; i++ {
		err = channel.Publish("", QueueName, false, false, amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte("hello world " + strconv.Itoa(i)),
			DeliveryMode: 2,
		})
		if err != nil {
			log.Fatalf("publish msg error: %v\n", err)
		}
	}

	log.Println("success")
}
