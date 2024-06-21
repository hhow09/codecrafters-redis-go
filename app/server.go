package main

import (
	"bufio"
	"fmt"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arr[1]), arr[1])))
		// https://redis.io/docs/latest/commands/set/
		// [SET, key, value]
		case "SET":
			if len(arr) < 3 {
				conn.Write(newErrorMSG("expecting 3 arguments"))
			}
			db.set(arr[1], arr[2])
			conn.Write(newSimpleString("OK"))
		// [GET, key]
		case "GET":
			if len(arr) < 2 {
				conn.Write(newErrorMSG("expecting 2 arguments"))
			}
			value := db.get(arr[1])
			conn.Write(newBulkString(value))
		default:
			conn.Write([]byte(newErrorMSG("unknown command " + arr[0])))
		}
	}
}
