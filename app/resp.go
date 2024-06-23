package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// ref: https://redis.io/docs/latest/develop/reference/protocol-spec/

const (
	typeSimpleString = '+'
	typeBulkString   = '$'
	typeArray        = '*'
	typeError        = '-'
)

func handleRESPArray(r *bufio.Reader) ([]string, error) {
	elCountMsg, err := readRESPMsg(r)
	if err != nil {
		return nil, fmt.Errorf("Error reading from connection: %w", err)
	}
	// base-10 value.
	elCount, err := strconv.ParseInt(elCountMsg, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse element count: %w", err)
	}
	res := make([]string, elCount)
	for i := int64(0); i < elCount; i++ {
		typmsg, err := readRESPMsg(r)
		if err != nil {
			return nil, fmt.Errorf("Error reading from connection: %w", err)
		}
		switch typmsg[0] {
		case typeBulkString:
			len, err := strconv.ParseInt(typmsg[1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Unable to parse string length: %w", err)
			}
			s, err := readRESPMsg(r)
			if err != nil {
				return nil, fmt.Errorf("Error reading string: %w", err)
			}
			res[i] = s[:len]
			// TODO
		case typeSimpleString:
			// TODO
		case typeArray:
			// TODO
		}
	}
	return res, nil
}

// readRESPMsg reads a RESP message from the reader and trim the line break.
func readRESPMsg(r *bufio.Reader) (string, error) {
	msg, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("Error reading from connection: %s", err.Error())
	}
	// The \r\n (CRLF) is the protocol's terminator, which always separates its parts.
	return strings.TrimRight(msg, "\r\n"), nil
}

func checkDataType(b byte) byte {
	switch b {
	case typeSimpleString:
		return typeSimpleString
	case typeBulkString:
		return typeBulkString
	case typeArray:
		return typeArray
	default:
		return 0
	}
}

func newSimpleString(msg string) []byte {
	return []byte(fmt.Sprintf("%c%s\r\n", typeSimpleString, msg))
}

func newBulkString(msg string) []byte {
	return []byte(fmt.Sprintf("%c%d\r\n%s\r\n", typeBulkString, len(msg), msg))
}

func newNullBulkString() []byte {
	return []byte(fmt.Sprintf("%c-1\r\n", typeBulkString))
}

func newArray(arr [][]byte) []byte {
	prefix := []byte(fmt.Sprintf("%c%d\r\n", typeArray, len(arr)))
	for _, v := range arr {
		prefix = append(prefix, v...)
	}
	return prefix
}

func newErrorMSG(msg string) []byte {
	return []byte(fmt.Sprintf("%cERR %s\r\n", typeError, msg))
}

func newRDBFile(f []byte) []byte {
	prefix := []byte(fmt.Sprintf("%c%d\r\n", typeBulkString, len(f)))
	return append(prefix, f...)
}
