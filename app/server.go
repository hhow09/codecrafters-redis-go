package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

const (
	RoleMaster = "master"
	RoleSlave  = "slave"
)

const (
	backlogSizePerReplica = 1000
)

func main() {
	role := RoleMaster
	p := flag.String("port", "6379", "port to bind to")
	replicaOf := flag.String("replicaof", "", "replicaof host port")
	dir := flag.String("dir", "", "directory to store db file")
	dbfilename := flag.String("dbfilename", "dump.rdb", "rdb file name")
	flag.Parse()
	cfg := config{}
	if *dir == "" && *dbfilename != "" {
		panic("dbfilename should be provided with dir")
	}
	cfg = config{
		persistence: persistenceCfg{
			dir:        *dir,
			dbfilename: *dbfilename,
		},
	}

	var rpc *replicaConf
	shutdown := make(chan os.Signal, 1)
	switch *replicaOf {
	case "":
		s := newServer("localhost", *p, newDB(), role, cfg)
		s.Start(shutdown, s.handler)
	default:
		sl := strings.Split(*replicaOf, " ")
		masterHost, masterPort := sl[0], sl[1]
		rpc = &replicaConf{
			masterHost: masterHost,
			masterPort: masterPort,
		}
		rs, err := newReplicaServer("localhost", *p, newDB(), rpc, cfg)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		rs.Start(shutdown)
	}
}

type server struct {
	host               string
	port               string
	db                 *db
	role               string
	masterReplid       string
	masterOffset       uint64
	replicationBacklog *replication.ReplicatinoBacklog
	config             config
}

type config struct {
	persistence persistenceCfg
}

type persistenceCfg struct {
	dir        string
	dbfilename string
}

func newServer(host, port string, db *db, role string, config config) *server {
	return &server{
		host:         host,
		port:         port,
		db:           db,
		masterReplid: replication.GenReplicationID(),
		masterOffset: 0,

		role:               role,
		replicationBacklog: replication.NewReplicationBacklog(backlogSizePerReplica),
		config:             config,
	}
}

func (s *server) Start(shutdown chan os.Signal, h func(net.Conn) error) {
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.host, s.port))
	if err != nil {
		err := fmt.Errorf("error listening: %v", err.Error())
		fmt.Println(err)
		os.Exit(1)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("error accepting connection: ", err.Error())
				os.Exit(1)
			}
			go func(conn net.Conn) {
				if err := h(conn); err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}(conn)
		}
	}()
	sig := <-shutdown
	fmt.Printf("[%s, %s] Shutting down server: %v\n", s.port, s.role, sig)
}

func (s *server) handler(conn net.Conn) (err error) {
	r := bufio.NewReader(conn)
	defer conn.Close()
	for {
		typ, err := resp.CheckDataType(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error reading byte from connection: %s", err.Error())
		}
		if typ != resp.TypeArray {
			if _, err := conn.Write(resp.NewErrorMSG("expecting type array")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
			return nil
		}
		arr, err := resp.HandleRESPArray(r)
		if err != nil {
			return fmt.Errorf("error reading resp array from connection: %s", err.Error())
		}
		if len(arr) == 0 {
			if _, err := conn.Write(resp.NewErrorMSG("empty array")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
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
			if s.role == RoleMaster {
				// store commands in replication buffer
				msg := replication.Msg{
					Data:               resp.NewSetCmd(arr),
					ShouldWaitResponse: false,
				}
				s.replicationBacklog.BroardcastBacklog(msg)
				_, err := conn.Write(resp.NewSimpleString("OK"))
				if err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
			}
		// [GET, key]
		case "GET":
			if err := handleGet(conn, arr, s.db); err != nil {
				return err
			}
		case "INFO":
			if len(arr) > 1 {
				if arr[1] == "replication" {
					if _, err := conn.Write(resp.NewBulkString(fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d", s.role, s.masterReplid, s.masterOffset))); err != nil {
						return fmt.Errorf("error writing to connection: %s", err.Error())
					}

				}
			}
			// TODO
		// REPLCONF <option> <value> <option> <value> ...
		// This command is used by a replica in order to configure the replication process before starting it with the SYNC command.
		// ref: https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L1114C4-L1114C50
		case "REPLCONF":
			if len(arr) != 3 {
				if _, err := conn.Write(resp.NewErrorMSG("expecting 3 arguments")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return nil
			}
			switch arr[1] {
			case "listening-port":
				// should hand over the connection ownership to replica connection and not use the reader here anymore.
				return s.handleReiplicaHanshake(conn, r, arr[2])

			case "GETACK":
				msg := replication.Msg{
					Data:               resp.NewArray([][]byte{resp.NewBulkString(arr[0]), resp.NewBulkString(arr[1]), resp.NewBulkString(arr[2])}),
					ShouldWaitResponse: false,
				}
				s.replicationBacklog.BroardcastBacklog(msg)
			}
			if _, err := conn.Write(resp.NewSimpleString("OK")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}

		// [WAIT numreplicas timeout]
		// ref: https://redis.io/docs/latest/commands/wait/
		// The WAIT command should return when either (a) the specified number of replicas have acknowledged the command, or (b) the timeout expires.
		// The WAIT command should always return the number of replicas that have acknowledged the command, even if the timeout expires.
		// The returned number of replicas might be lesser than or greater than the expected number of replicas specified in the WAIT command.
		// ref: https://app.codecrafters.io/courses/redis/stages/na2
		case "WAIT":
			if err := handleWait(conn, arr, s.replicationBacklog); err != nil {
				return err
			}

		// CONFIG GET parameter [parameter ...]
		// ref: https://redis.io/docs/latest/commands/config-get/
		case "CONFIG":
			if len(arr) < 3 {
				if _, err := conn.Write(resp.NewErrorMSG("expecting 3 arguments")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return nil
			}
			switch arr[1] {
			case "GET":
				res := [][]byte{}
				for i := 2; i < len(arr); i++ {
					switch arr[i] {
					case "dir":
						res = append(res, resp.NewBulkString("dir"), resp.NewBulkString(s.config.persistence.dir)) // key, value
					case "dbfilename":
						res = append(res, resp.NewBulkString("dbfilename"), resp.NewBulkString(s.config.persistence.dbfilename)) // key, value
					}
				}
				if _, err := conn.Write(resp.NewArray(res)); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
			}

		default:
			if _, err := conn.Write([]byte(resp.NewErrorMSG("unknown command " + arr[0]))); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		}
	}
}

func handlePing(conn net.Conn) error {
	if _, err := conn.Write(resp.NewSimpleString("PONG")); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}

func handleEcho(conn net.Conn, arr []string) error {
	if len(arr) < 2 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting 2 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	if _, err := conn.Write(resp.NewBulkString(arr[1])); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}

func handleSet(conn io.Writer, arr []string, db *db) error {
	switch len(arr) {
	case 5:
		switch arr[3] {
		// PX milliseconds -- Set the specified expire time, in milliseconds (a positive integer).
		case "px":
			exp, err := strconv.ParseInt(arr[4], 10, 64) // milliseconds
			if err != nil {
				if _, werr := conn.Write(resp.NewErrorMSG("invalid expire time")); werr != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return fmt.Errorf("error parsing expire time: %s", err.Error())
			}
			db.setExp(arr[1], arr[2], time.Now().UnixMilli()+exp)
		}
	default:
		db.set(arr[1], arr[2])
	}
	return nil
}

func handleGet(conn net.Conn, arr []string, db *db) error {
	if len(arr) < 2 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting 2 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	value := db.get(arr[1])
	if value == "" {
		if _, err := conn.Write(resp.NewNullBulkString()); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	} else {
		if _, err := conn.Write(resp.NewBulkString(value)); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	return nil
}

func handleWait(conn net.Conn, arr []string, backlog *replication.ReplicatinoBacklog) error {
	if len(arr) != 3 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting 3 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	replCount, err := strconv.Atoi(arr[1])
	if err != nil || replCount < 0 {
		if _, err := conn.Write(resp.NewErrorMSG("invalid numreplicas")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	timeout, err := strconv.Atoi(arr[2])
	if err != nil {
		if _, err := conn.Write(resp.NewErrorMSG("invalid timeout")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	if replCount == 0 {
		if _, err := conn.Write(resp.NewInt(0)); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	count := backlog.InSyncReplicas(time.Millisecond*time.Duration(timeout), replCount)
	if _, err := conn.Write(resp.NewInt(count)); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}
