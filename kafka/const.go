package kafka

const (
	address    = "127.0.0.1:9092"
	key        = "test-key"
	timeFormat = "2006-01-02 15:04:05"

	group      = "consumer-group-sarama"
	groupDelay = "consumer-group-delay-sarama"
	groupReal  = "consumer-group-real-sarama"

	groupCf      = "consumer-group-confluent"
	groupCfDelay = "consumer-group-delay-confluent"
	groupCfReal  = "consumer-group-real-confluent"
)

var (
	addressList = []string{"127.0.0.1:9092"}
	topic       = "go-test-topic"
	topicDelay  = "go-delay-topic"
	topicReal   = "go-real-topic"
)
