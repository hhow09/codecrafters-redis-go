package main

import (
	"bufio"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	// Uncomment this block to pass the first stage
	"net"
	"os"
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
		role = RoleSlave
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
}

func newServer(host, port string, db *db, role string) *server {
	return &server{
		host:         host,
		port:         port,
		db:           db,
		masterReplid: generateRandomString(40),
		masterOffset: 0,

		role: role,
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
		go s.handler(conn)
	}
}

// expeting *1\r\n$4\r\nping\r\n
func (s *server) handler(conn net.Conn) error {
	defer conn.Close()
	for {
		r := bufio.NewReader(conn)
		typmsg, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("Error reading from connection: %s", err.Error())
		}
		typ := checkDataType(typmsg)
		if typ != typeArray {
			conn.Write(newErrorMSG("expecting type array"))
			return nil
		}
		arr, err := handleRESPArray(r)
		if err != nil {
			return fmt.Errorf("Error reading from connection: %s", err.Error())
		}
		if len(arr) == 0 {
			conn.Write(newErrorMSG("empty array"))
		}
		switch arr[0] {
		// https://redis.io/docs/latest/commands/ping/
		// [PING]
		case "PING":
			conn.Write(newSimpleString("PONG"))
		// https://redis.io/docs/latest/commands/echo/
		// [ECHO, message]
		case "ECHO":
			if len(arr) < 2 {
				conn.Write(newErrorMSG("expecting 2 arguments"))
				return nil
			}
			conn.Write(newBulkString(arr[1]))
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
						conn.Write(newErrorMSG("invalid expire time"))
						return nil
					}
					s.db.setExp(arr[1], arr[2], time.Now().UnixMilli()+exp)
				}
			default:
				s.db.set(arr[1], arr[2])
			}

			conn.Write(newSimpleString("OK"))
		// [GET, key]
		case "GET":
			if len(arr) < 2 {
				conn.Write(newErrorMSG("expecting 2 arguments"))
			}
			value := s.db.get(arr[1])
			if value == "" {
				conn.Write(newNullBulkString())
			} else {
				conn.Write(newBulkString(value))
			}
		case "INFO":
			if len(arr) > 1 {
				if arr[1] == "replication" {
					conn.Write(newBulkString(fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d", s.role, s.masterReplid, s.masterOffset)))
				}
			} else {
				// TODO
			}
		default:
			conn.Write([]byte(newErrorMSG("unknown command " + arr[0])))
		}
	}
}
