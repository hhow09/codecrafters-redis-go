package resp

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/database"
)

// ref: https://redis.io/docs/latest/develop/reference/protocol-spec/

const (
	TypeSimpleString = '+'
	TypeBulkString   = '$'
	TypeArray        = '*'
	TypeError        = '-'
	TypeInt          = ':'
)

type reader interface {
	ReadString(delim byte) (string, error)
	ReadByte() (byte, error)
}

func HandleRESPArray(r reader) ([]string, error) {
	elCountMsg, err := readRESPMsg(r)
	if err != nil {
		return nil, fmt.Errorf("error reading from connection: %w", err)
	}
	// base-10 value.
	elCount, err := strconv.ParseInt(elCountMsg, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse element count: %w", err)
	}
	res := make([]string, elCount)
	for i := int64(0); i < elCount; i++ {
		typmsg, err := readRESPMsg(r)
		if err != nil {
			return nil, fmt.Errorf("error reading from connection: %w", err)
		}
		switch typmsg[0] {
		case TypeBulkString:
			len, err := strconv.ParseInt(typmsg[1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("unable to parse string length: %w", err)
			}
			s, err := readRESPMsg(r)
			if err != nil {
				return nil, fmt.Errorf("error reading string: %w", err)
			}
			res[i] = s[:len]
			// TODO
		case TypeSimpleString:
			// TODO
		case TypeArray:
			// TODO
		}
	}
	return res, nil
}

// readRESPMsg reads a RESP message from the reader and trim the line break.
func readRESPMsg(r reader) (string, error) {
	msg, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("error reading from connection: %s", err.Error())
	}
	// The \r\n (CRLF) is the protocol's terminator, which always separates its parts.
	return strings.TrimRight(msg, "\r\n"), nil
}

func CheckDataType(r reader) (byte, error) {
	b, err := r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("error reading byte from connection: %s", err.Error())
	}

	switch b {
	case TypeSimpleString:
		return TypeSimpleString, nil
	case TypeBulkString:
		return TypeBulkString, nil
	case TypeArray:
		return TypeArray, nil
	default:
		return 0, fmt.Errorf("unknown type: %c", b)
	}
}

func NewSimpleString(msg string) []byte {
	return []byte(fmt.Sprintf("%c%s\r\n", TypeSimpleString, msg))
}

func NewBulkString(msg string) []byte {
	return []byte(fmt.Sprintf("%c%d\r\n%s\r\n", TypeBulkString, len(msg), msg))
}

func NewNullBulkString() []byte {
	return []byte(fmt.Sprintf("%c-1\r\n", TypeBulkString))
}

func NewArray(arr [][]byte) []byte {
	prefix := []byte(fmt.Sprintf("%c%d\r\n", TypeArray, len(arr)))
	for _, v := range arr {
		prefix = append(prefix, v...)
	}
	return prefix
}

func NewErrorMSG(msg string) []byte {
	return []byte(fmt.Sprintf("%cERR %s\r\n", TypeError, msg))
}

func NewRDBFile(f []byte) []byte {
	prefix := []byte(fmt.Sprintf("%c%d\r\n", TypeBulkString, len(f)))
	return append(prefix, f...)
}

func NewSetCmd(arr []string) []byte {
	a := [][]byte{NewBulkString("SET"), NewBulkString(arr[1]), NewBulkString(arr[2])}
	if len(arr) > 3 {
		a = append(a, NewBulkString(arr[3]), NewBulkString(arr[4]))
	}
	return NewArray(a)
}

func NewInt(i int) []byte {
	return []byte(fmt.Sprintf("%c%d\r\n", TypeInt, i))
}

func NewStreamEntries(ents []database.Entry) []byte {
	res := make([][]byte, len(ents))
	for i, e := range ents {
		kvs := make([][]byte, len(e.KVs)*2)
		for j, kv := range e.KVs {
			kvs[j*2] = NewBulkString(kv.Key)
			kvs[j*2+1] = NewBulkString(kv.Value)
		}

		res[i] = NewArray([][]byte{
			NewBulkString(database.StreamEntryID(e.Ts, e.Seq)),
			NewArray(kvs),
		})
	}
	return NewArray(res)
}
