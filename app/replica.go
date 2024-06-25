package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type replicaServer struct {
	*server
	*replicaConf
	master *net.Conn
}

type replicaConf struct {
	masterOffset uint64
	masterHost   string
	masterPort   string
}

func newReplicaServer(host, port string, db *db, replicaConf *replicaConf) (*replicaServer, error) {
	s := newServer(host, port, db, RoleSlave)
	return &replicaServer{
		server:      s,
		replicaConf: replicaConf,
	}, nil
}

func (s *replicaServer) Start(shutdown chan os.Signal) {
	conn, err := s.sendHandshake()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	s.master = &conn

	// handle master connection (for replication)
	go func(conn net.Conn) {
		if err := s.replHandler(conn); err != nil {
			fmt.Println(err)
		}
	}(conn)

	// handle client connection
	s.server.Start(shutdown, s.server.handler)
}

// handshake sends the handshake message to the master
func (s *replicaServer) sendHandshake() (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s.masterHost, s.masterPort))
	if err != nil {
		return nil, fmt.Errorf("Error connecting to master: %s", err.Error())
	}
	r := bufio.NewReader(conn)
	_, err = conn.Write(newArray([][]byte{newBulkString("PING")}))
	if err != nil {
		return nil, fmt.Errorf("Error writing to connection: %s", err.Error())
	}
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Error reading from connection: %s", err.Error())
	}
	// REPLCONF <option> <value> <option> <value> ...
	// ref: https://redis.io/docs/latest/commands/replconf/
	// ref: https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L2685
	// Set the slave port, so that Master's INFO command can list the slave listening port correctly.
	_, err = conn.Write(newArray([][]byte{newBulkString("REPLCONF"), newBulkString("listening-port"), newBulkString(s.port)}))
	if err != nil {
		return nil, fmt.Errorf("Error writing to connection: %s", err.Error())
	}
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Error reading from connection: %s", err.Error())
	}
	// Inform the master of our (slave) capabilities.
	// EOF / PSYNC2
	_, err = conn.Write(newArray([][]byte{newBulkString("REPLCONF"), newBulkString("capa"), newBulkString("psync2")}))
	if err != nil {
		return nil, fmt.Errorf("Error writing to connection: %s", err.Error())
	}
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Error reading from connection: %s", err.Error())
	}

	// Connected to master

	// TODO: The replica sends PSYNC to the master (Next stages)
	_, err = conn.Write(newArray([][]byte{newBulkString("PSYNC"), newBulkString("?"), newBulkString("-1")}))
	if err != nil {
		return nil, fmt.Errorf("Error writing to connection: %s", err.Error())
	}
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Error reading from connection: %s", err.Error())
	}
	// here we assum FULLRESYNC is received

	// Read the RDB file from the master
	// size limit: https://github.com/redis/redis-doc/pull/1653
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("Error reading from connection: %s", err.Error())
	}
	// RDB file received
	return conn, nil
}

func (s *replicaServer) replHandler(conn net.Conn) error {
	r := bufio.NewReader(conn)
	defer conn.Close()
	for {
		typmsg, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("Error reading byte from connection: %s", err.Error())
		}
		typ := checkDataType(typmsg)
		if typ != typeArray {
			if _, err := conn.Write(newErrorMSG("expecting type array")); err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}
			return nil
		}
		arr, err := handleRESPArray(r)
		if err != nil {
			return fmt.Errorf("Error reading resp array from connection: %s", err.Error())
		}
		if len(arr) == 0 {
			if _, err := conn.Write(newErrorMSG("empty array")); err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}
		}
		switch arr[0] {
		// https://redis.io/docs/latest/commands/ping/
		// [PING]
		case "PING":
			if err := handlePing(conn); err != nil {
				return err
			}
		// https://redis.io/docs/latest/commands/echo/
		// [ECHO, message]
		case "ECHO":
			if err := handleEcho(conn, arr); err != nil {
				return err
			}
		// https://redis.io/docs/latest/commands/set/
		// [SET, key, value]
		case "SET":
			if err := handleSet(conn, arr, s.db); err != nil {
				return err
			}
		// [GET, key]
		case "GET":
			if err := handleGet(conn, arr, s.db); err != nil {
				return err
			}
		// TODO
		// REPLCONF <option> <value> <option> <value> ...
		// This command is used by a replica in order to configure the replication process before starting it with the SYNC command.
		// ref: https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L1114C4-L1114C50
		case "REPLCONF":
			if len(arr) != 3 {
				if _, err := conn.Write(newErrorMSG("expecting 3 arguments")); err != nil {
					return fmt.Errorf("Error writing to connection: %s", err.Error())
				}
				return nil
			}
			switch arr[1] {
			case "GETACK":
				if s.role == RoleSlave {
					if _, err := conn.Write(newArray([][]byte{newBulkString("REPLCONF"), newBulkString("ACK"), newBulkString("0")})); err != nil {
						return fmt.Errorf("Error writing to connection: %s", err.Error())
					}
				}
			}

		default:
			if _, err := conn.Write([]byte(newErrorMSG("unknown command " + arr[0]))); err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}
		}
	}
}
