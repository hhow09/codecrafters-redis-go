package main

import (
	"fmt"
	"net"
	"os"
)

type replicaServer struct {
	*server
	*replicaConf
}

type replicaConf struct {
	masterOffset uint64
	masterHost   string
	masterPort   string
}

func newReplicaServer(host, port string, db *db, replicaConf *replicaConf) (*replicaServer, error) {
	role := RoleSlave
	s := newServer(host, port, db, role)
	return &replicaServer{
		server:      s,
		replicaConf: replicaConf,
	}, nil
}

func (s *replicaServer) Start() {
	err := s.sendHandshake()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	s.server.Start()
}

// handshake sends the handshake message to the master
func (s *replicaServer) sendHandshake() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s.masterHost, s.masterPort))
	if err != nil {
		return fmt.Errorf("Error connecting to master: %s", err.Error())
	}
	defer conn.Close()
	_, err = conn.Write(newArray([][]byte{newBulkString("PING")}))
	if err != nil {
		return fmt.Errorf("Error writing to connection: %s", err.Error())
	}
	return nil

	// TODO: The replica sends REPLCONF twice to the master (Next stages)
	// TODO: The replica sends PSYNC to the master (Next stages)

}
