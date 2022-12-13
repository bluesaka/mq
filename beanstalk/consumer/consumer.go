package main

import (
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	"time"
)

func main() {
	c, err := beanstalk.Dial("tcp", "localhost:11300")
	fmt.Println(c, err)

	id, body, err := c.Reserve(5 * time.Second)
	fmt.Println(id, body, err)
}
