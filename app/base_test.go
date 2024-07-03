package main

import (
	"net"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/database"
)

var (
	testCfg = config{}
)

const (
	host = "localhost"
)

func dialWithRetry(maxRetry int, host, port string) (net.Conn, error) {
	var conn net.Conn
	var err error
	for i := 0; i < maxRetry; i++ {
		conn, err = net.Dial("tcp", host+":"+port)
		if err == nil {
			return conn, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, err
}

var mockdbs = []*database.DB{
	database.NewDB(),
}
