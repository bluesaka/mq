package main

import (
	"fmt"
	"github.com/zeromicro/go-queue/dq"
	"strconv"
	"time"
)

func main() {
	producer := dq.NewProducer([]dq.Beanstalk{
		{
			Endpoint: "localhost:11300",
			Tube:     "tube",
		},
		{
			Endpoint: "localhost:11301",
			Tube:     "tube",
		},
	})

	for i := 1; i < 5; i++ {
		resp, err := producer.Delay([]byte("delay msg: "+strconv.Itoa(i)), 5*time.Second)
		fmt.Println(resp)
		if err != nil {
			fmt.Println(err)
		}
	}
}
