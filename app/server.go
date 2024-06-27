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

	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
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
	flag.Parse()

	var rpc *replicaConf
	shutdown := make(chan os.Signal, 1)
	switch *replicaOf {
	case "":
		s := newServer("localhost", *p, newDB(), role)
		s.Start(shutdown, s.handler)
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
}

func newServer(host, port string, db *db, role string) *server {
	return &server{
		host:         host,
		port:         port,
		db:           db,
		masterReplid: generateRandomString(40),
		masterOffset: 0,

		role:               role,
		replicationBacklog: replication.NewReplicationBacklog(backlogSizePerReplica),
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
	if s.role == RoleMaster {
		go s.replicationBacklog.SendBacklog(shutdown)
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

func (s *server) handler(conn net.Conn) error {
	r := bufio.NewReader(conn)
	defer conn.Close()
	for {
		typmsg, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("error reading byte from connection: %s", err.Error())
		}
		typ := checkDataType(typmsg)
		if typ != typeArray {
			if _, err := conn.Write(newErrorMSG("expecting type array")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
			return nil
		}
		arr, err := handleRESPArray(r)
		if err != nil {
			return fmt.Errorf("error reading resp array from connection: %s", err.Error())
		}
		if len(arr) == 0 {
			if _, err := conn.Write(newErrorMSG("empty array")); err != nil {
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
				s.replicationBacklog.InsertBacklog(newSetCmd(arr))
				_, err := conn.Write(newSimpleString("OK"))
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
					if _, err := conn.Write(newBulkString(fmt.Sprintf("role:%s\nmaster_replid:%s\nmaster_repl_offset:%d", s.role, s.masterReplid, s.masterOffset))); err != nil {
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
				if _, err := conn.Write(newErrorMSG("expecting 3 arguments")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return nil
			}
			switch arr[1] {
			case "listening-port":
				// id: host-port
				id := fmt.Sprintf("%s-%s", strings.Split(conn.RemoteAddr().String(), ":")[0], arr[2])
				s.replicationBacklog.AddReplica(id, conn)
			case "capa":
				// TODO
			case "GETACK":
				// store commands in replication buffer
				s.replicationBacklog.InsertBacklog(newArray([][]byte{newBulkString(arr[0]), newBulkString(arr[1]), newBulkString(arr[2])}))
				_, err := conn.Write(newSimpleString("OK"))
				if err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
			}
			if _, err := conn.Write(newSimpleString("OK")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		case "PSYNC":
			if len(arr) != 3 {
				if _, err := conn.Write(newErrorMSG("expecting 3 arguments")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return nil
			}
			// Send a FULLRESYNC reply in the specific case of a full resynchronization.
			// https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L674
			_, err := conn.Write((newSimpleString(fmt.Sprintf("FULLRESYNC %s %d", s.masterReplid, s.masterOffset))))
			if err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
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
				return fmt.Errorf("error marshalling RDB file: %s", err.Error())
			}
			if _, err := conn.Write(newRDBFile(b)); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		// [WAIT numreplicas timeout]
		// https://redis.io/docs/latest/commands/wait/
		case "WAIT":
			if len(arr) != 3 {
				if _, err := conn.Write(newErrorMSG("expecting 3 arguments")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return nil
			}
			_, err := strconv.Atoi(arr[1])
			if err != nil {
				if _, err := conn.Write(newErrorMSG("invalid numreplicas")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return nil
			}
			if _, err := conn.Write(newInt(0)); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}

		default:
			if _, err := conn.Write([]byte(newErrorMSG("unknown command " + arr[0]))); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		}
	}
}

func handlePing(conn net.Conn) error {
	if _, err := conn.Write(newSimpleString("PONG")); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}

func handleEcho(conn net.Conn, arr []string) error {
	if len(arr) < 2 {
		if _, err := conn.Write(newErrorMSG("expecting 2 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	if _, err := conn.Write(newBulkString(arr[1])); err != nil {
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
				if _, werr := conn.Write(newErrorMSG("invalid expire time")); werr != nil {
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
		if _, err := conn.Write(newErrorMSG("expecting 2 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	value := db.get(arr[1])
	if value == "" {
		if _, err := conn.Write(newNullBulkString()); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	} else {
		if _, err := conn.Write(newBulkString(value)); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	return nil
}
