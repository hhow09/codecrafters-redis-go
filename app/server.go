package main

import (
	"bufio"
	"flag"
	"fmt"
	"strconv"
	"time"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

var role = RoleMaster

func main() {
	p := flag.String("port", "6379", "port to bind to")
	replicaOf := flag.String("replicaof", "", "replicaof host port")
	flag.Parse()

	if *replicaOf != "" {
		role = RoleSlave
	}
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *p))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	db := newDB()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handler(conn, db)
	}
}

// expeting *1\r\n$4\r\nping\r\n
func handler(conn net.Conn, db *db) error {
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
					db.setExp(arr[1], arr[2], time.Now().UnixMilli()+exp)
				}
			default:
				db.set(arr[1], arr[2])
			}

			conn.Write(newSimpleString("OK"))
		// [GET, key]
		case "GET":
			if len(arr) < 2 {
				conn.Write(newErrorMSG("expecting 2 arguments"))
			}
			value := db.get(arr[1])
			if value == "" {
				conn.Write(newNullBulkString())
			} else {
				conn.Write(newBulkString(value))
			}
		case "INFO":
			if len(arr) > 1 {
				if arr[1] == "replication" {
					conn.Write(newBulkString(fmt.Sprintf("role:%s\n", role)))
				}
			} else {
				// TODO
			}
		default:
			conn.Write([]byte(newErrorMSG("unknown command " + arr[0])))
		}
	}
}
