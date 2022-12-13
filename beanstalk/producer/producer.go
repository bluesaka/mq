package main

import (
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	"time"
)

func main() {
	c, err := beanstalk.Dial("tcp", "localhost:11300")
	fmt.Println(c, err)

	id, err := c.Put([]byte("hello"), 1, 0, 120*time.Second)
	fmt.Println(id, err)
}
