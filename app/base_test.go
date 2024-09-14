package main

import (
	"net"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/database"
	"github.com/libp2p/go-reuseport"
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

func ports() (string, string) {
	masterPort := "8082"
	replicaPort := "8083"
	return masterPort, replicaPort
}

// setTestServerReusePort sets the reuseport option for the server and replicaServer
// it deals with the port issue in ci
func setTestServerReusePort(s *server, rs *replicaServer) {
	if s != nil {
		s.netConfig = &net.ListenConfig{
			Control: reuseport.Control,
		}
	}
	if rs != nil {
		rs.netConfig = &net.ListenConfig{
			Control: reuseport.Control,
		}
	}
}
