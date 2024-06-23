package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	// Uncomment this block to pass the first stage
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/persistence"
)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

func main() {
	role := RoleMaster
	p := flag.String("port", "6379", "port to bind to")
	replicaOf := flag.String("replicaof", "", "replicaof host port")
	flag.Parse()

	var rpc *replicaConf
	switch *replicaOf {
	case "":
		s := newServer("localhost", *p, newDB(), role)
		s.Start()
	default:
		sl := strings.Split(*replicaOf, " ")
		masterHost, masterPort := sl[0], sl[1]
		rpc = &replicaConf{
			masterHost: masterHost,
			masterPort: masterPort,
		}
		rs, err := newReplicaServer("localhost", *p, newDB(), rpc)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		rs.Start()
	}
}

type server struct {
	host         string
	port         string
	db           *db
	role         string
	masterReplid string
	masterOffset uint64
	replicas     map[string]net.Conn
}

func newServer(host, port string, db *db, role string) *server {
	return &server{
		host:         host,
		port:         port,
		db:           db,
		masterReplid: generateRandomString(40),
		masterOffset: 0,

		role:     role,
		replicas: make(map[string]net.Conn, 0),
	}
}

func (s *server) Start() {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.host, s.port))
	if err != nil {
		err := fmt.Errorf("Error listening: %v", err.Error())
		fmt.Println(err)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go func(conn net.Conn) {
			if err := s.handler(conn); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}(conn)
	}
}

// expeting *1\r\n$4\r\nping\r\n
func (s *server) handler(conn net.Conn) error {
	defer conn.Close()
	for {
		fullCmdBuf := bytes.NewBuffer(nil)
		r := bufio.NewReader(io.TeeReader(conn, fullCmdBuf)) //
		typmsg, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("Error reading from connection: %s", err.Error())
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
			return fmt.Errorf("Error reading from connection: %s", err.Error())
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
			if _, err := conn.Write(newSimpleString("PONG")); err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}
		// https://redis.io/docs/latest/commands/echo/
		// [ECHO, message]
		case "ECHO":
			if len(arr) < 2 {
				if _, err := conn.Write(newErrorMSG("expecting 2 arguments")); err != nil {
					return fmt.Errorf("Error writing to connection: %s", err.Error())
				}
				return nil
			}
			if _, err := conn.Write(newBulkString(arr[1])); err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}
		// https://redis.io/docs/latest/commands/set/
		// [SET, key, value]
		case "SET":
			switch len(arr) {
			case 5:
				switch arr[3] {
				// PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
				case "px":
					exp, err := strconv.ParseInt(arr[4], 10, 64) // milliseconds
					if err != nil {
						_, err := conn.Write(newErrorMSG("invalid expire time"))
						if err != nil {
							return fmt.Errorf("Error writing to connection: %s", err.Error())
						}
						return nil
					}
					s.db.setExp(arr[1], arr[2], time.Now().UnixMilli()+exp)
				}
			default:
				s.db.set(arr[1], arr[2])
			}

			_, err := conn.Write(newSimpleString("OK"))
			if err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}
		// [GET, key]
		case "GET":
			if len(arr) < 2 {
				if _, err := conn.Write(newErrorMSG("expecting 2 arguments")); err != nil {
					return fmt.Errorf("Error writing to connection: %s", err.Error())
				}
			}
			value := s.db.get(arr[1])
			if value == "" {
				_, err := conn.Write(newNullBulkString())
				if err != nil {
					return fmt.Errorf("Error writing to connection: %s", err.Error())
				}
			} else {
				_, err := conn.Write(newBulkString(value))
				if err != nil {
					return fmt.Errorf("Error writing to connection: %s", err.Error())
				}
			}
		case "INFO":
			if len(arr) > 1 {
				if arr[1] == "replication" {
					if _, err := conn.Write(newBulkString(fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d", s.role, s.masterReplid, s.masterOffset))); err != nil {
						return fmt.Errorf("Error writing to connection: %s", err.Error())
					}

				}
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
			case "listening-port":
				// id: host-port
				id := fmt.Sprintf("%s-%s", conn.RemoteAddr(), arr[2])
				s.replicas[id] = conn
			case "capa":
				// TODO
			}
			if _, err := conn.Write(newSimpleString("OK")); err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}
		case "PSYNC":
			if len(arr) != 3 {
				if _, err := conn.Write(newErrorMSG("expecting 3 arguments")); err != nil {
					return fmt.Errorf("Error writing to connection: %s", err.Error())
				}
				return nil
			}
			// Send a FULLRESYNC reply in the specific case of a full resynchronization.
			// https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L674
			_, err := conn.Write((newSimpleString(fmt.Sprintf("FULLRESYNC %s %d", s.masterReplid, s.masterOffset))))
			if err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}

			rdbFile := persistence.RDB{
				Aux: &persistence.Aux{
					Version: "7.2.0",
					Bits:    64,
					Ctime:   1829289061,
					UsedMem: 2965639168,
				},
			}
			b, err := rdbFile.MarshalRDB()
			if err != nil {
				return fmt.Errorf("Error marshalling RDB file: %s", err.Error())
			}
			if _, err := conn.Write(newRDBFile(b)); err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}

		default:
			if _, err := conn.Write([]byte(newErrorMSG("unknown command " + arr[0]))); err != nil {
				return fmt.Errorf("Error writing to connection: %s", err.Error())
			}
		}
		if s.role == RoleMaster {
			switch arr[0] {
			case "SET", "GET":
				s.broadcast(fullCmdBuf.Bytes())
				fullCmdBuf.Reset()
			}
		}

	}
}

// Broadcast the command to all replicas
func (s *server) broadcast(msg []byte) {
	for _, rplc := range s.replicas {
		_, err := rplc.Write(msg)
		if err != nil {
			fmt.Println("Error writing to replica: ", err.Error())
		}
	}
}
