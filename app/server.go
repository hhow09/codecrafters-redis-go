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

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handlePing(conn)
	}
}

// expeting *1\r\n$4\r\nping\r\n
func handlePing(conn net.Conn) error {
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
		for _, v := range arr {
			switch v {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			default:
				conn.Write([]byte(newErrorMSG("unknown command " + arr[0])))
			}
		}
	}
}
