package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	appbufio "github.com/codecrafters-io/redis-starter-go/app/bufio"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type replicaServer struct {
	*server
	*replicaConf
	master *bufio.Reader
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
	// handle master connection (for replication)
	go func() {
		r, wc, err := s.sendHandshake()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		s.master = r
		if err := s.replHandler(r, wc); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()

	// handle client connection
	s.server.Start(shutdown, s.server.handler)
}

// handshake sends the handshake message to the master
func (s *replicaServer) sendHandshake() (*bufio.Reader, io.WriteCloser, error) {
	if s.role != RoleSlave {
		return nil, nil, fmt.Errorf("replica role is not slave")
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s.masterHost, s.masterPort))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	r := bufio.NewReader(conn)
	wc := io.WriteCloser(conn)
	_, err = wc.Write(resp.NewArray([][]byte{resp.NewBulkString("PING")}))
	if err != nil {
		return nil, nil, fmt.Errorf("error writing to connection: %s", err.Error())
	}
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("error reading from connection: %s", err.Error())
	}
	// REPLCONF <option> <value> <option> <value> ...
	// ref: https://redis.io/docs/latest/commands/replconf/
	// ref: https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L2685
	// Set the slave port, so that Master's INFO command can list the slave listening port correctly.
	_, err = wc.Write(resp.NewArray([][]byte{resp.NewBulkString("REPLCONF"), resp.NewBulkString("listening-port"), resp.NewBulkString(s.port)}))
	if err != nil {
		return nil, nil, fmt.Errorf("error writing to connection: %s", err.Error())
	}
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("error reading from connection: %s", err.Error())
	}
	// Inform the master of our (slave) capabilities.
	// EOF / PSYNC2
	_, err = wc.Write(resp.NewArray([][]byte{resp.NewBulkString("REPLCONF"), resp.NewBulkString("capa"), resp.NewBulkString("psync2")}))
	if err != nil {
		return nil, nil, fmt.Errorf("error writing to connection: %s", err.Error())
	}
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("error reading from connection: %s", err.Error())
	}

	// Connected to master
	_, err = wc.Write(resp.NewArray([][]byte{resp.NewBulkString("PSYNC"), resp.NewBulkString("?"), resp.NewBulkString("-1")}))
	if err != nil {
		return nil, nil, fmt.Errorf("error writing to connection: %s", err.Error())
	}
	_, err = r.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("error reading from connection: %s", err.Error())
	}
	// here we assum FULLRESYNC is received

	// Read the RDB file from the master
	// size limit: https://github.com/redis/redis-doc/pull/1653
	prefix, err := r.ReadString('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("error reading from connection: %s", err.Error())
	}
	dbFileLen, err := strconv.ParseInt(prefix[1:len(prefix)-2], 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing length: %s", err.Error())
	}
	db := make([]byte, dbFileLen)
	if _, err = io.ReadFull(r, db); err != nil {
		return nil, nil, fmt.Errorf("error reading from connection: %s", err.Error())
	}
	return r, wc, nil
}

func (s *replicaServer) replHandler(ir *bufio.Reader, wc io.WriteCloser) error {
	r := appbufio.NewTrackedBufioReader(ir)
	defer wc.Close()
	for {
		typ, err := resp.CheckDataType(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error reading from connection: %s", err.Error())
		}
		if typ != resp.TypeArray {
			if _, err := wc.Write(resp.NewErrorMSG("expecting type array")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
			return nil
		}
		arr, err := resp.HandleRESPArray(r)
		if err != nil {
			return fmt.Errorf("error reading resp array from connection: %s", err.Error())
		}
		if len(arr) == 0 {
			if _, err := wc.Write(resp.NewErrorMSG("empty array")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		}
		switch arr[0] {
		// All other propagated commands (like PING, SET etc.) should be read and processed, but a response should not be sent back to the master.
		case "PING": // https://redis.io/docs/latest/commands/ping/
		// https://redis.io/docs/latest/commands/echo/
		// [ECHO, message]
		case "ECHO":
		// https://redis.io/docs/latest/commands/set/
		// [SET, key, value]
		case "SET":
			if err := handleSet(wc, arr, s.db); err != nil {
				return err
			}
		// [GET, key]
		// All other propagated commands (like PING, SET etc.) should be read and processed, but a response should not be sent back to the master.
		case "GET":
		// TODO
		// REPLCONF <option> <value> <option> <value> ...
		// This command is used by a replica in order to configure the replication process before starting it with the SYNC command.
		// ref: https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L1114C4-L1114C50
		case "REPLCONF":
			if len(arr) != 3 {
				if _, err := wc.Write(resp.NewErrorMSG("expecting 3 arguments")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return nil
			}
			switch arr[1] {
			case "GETACK":
				// The offset should only include the number of bytes of commands processed before receiving the current REPLCONF GETACK command.
				offset := fmt.Sprintf("%v", s.replicaConf.masterOffset)
				if _, err := wc.Write(resp.NewArray([][]byte{resp.NewBulkString("REPLCONF"), resp.NewBulkString("ACK"), resp.NewBulkString(offset)})); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
			}

		default:
			if _, err := wc.Write([]byte(resp.NewErrorMSG("unknown command " + arr[0]))); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		}
		s.replicaConf.masterOffset += r.NAndReset()
	}
}
