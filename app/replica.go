package main

import (
	"bufio"
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
	_, err = bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("Error reading from connection: %s", err.Error())
	}
	// REPLCONF <option> <value> <option> <value> ...
	// ref: https://redis.io/docs/latest/commands/replconf/
	// ref: https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L2685
	// Set the slave port, so that Master's INFO command can list the slave listening port correctly.
	_, err = conn.Write(newArray([][]byte{newBulkString("REPLCONF"), newBulkString("listening-port"), newBulkString(s.port)}))
	if err != nil {
		return fmt.Errorf("Error writing to connection: %s", err.Error())
	}
	_, err = bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("Error reading from connection: %s", err.Error())
	}
	// Inform the master of our (slave) capabilities.
	// EOF / PSYNC2
	_, err = conn.Write(newArray([][]byte{newBulkString("REPLCONF"), newBulkString("capa"), newBulkString("psync2")}))
	if err != nil {
		return fmt.Errorf("Error writing to connection: %s", err.Error())
	}
	_, err = bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("Error reading from connection: %s", err.Error())
	}

	// TODO: The replica sends PSYNC to the master (Next stages)
	_, err = conn.Write(newArray([][]byte{newBulkString("PSYNC"), newBulkString("?"), newBulkString("-1")}))
	if err != nil {
		return fmt.Errorf("Error writing to connection: %s", err.Error())
	}
	_, err = bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("Error reading from connection: %s", err.Error())
	}
	return nil
}
