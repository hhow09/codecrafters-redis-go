package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/database"
	"github.com/codecrafters-io/redis-starter-go/app/persistence"
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
	dbfilename := flag.String("dbfilename", "", "rdb file name")
	flag.Parse()
	if *dir == "" && *dbfilename != "" {
		panic("dbfilename should be provided with dir")
	}
	cfg := config{
		persistence: persistence.Config{
			Dir:        *dir,
			Dbfilename: *dbfilename,
		},
	}
	dbs, err := persistence.LoadRDB(cfg.persistence)
	if err != nil {
		panic(fmt.Errorf("fail to load RDB from confog: %w", err))
	}
	var rpc *replicaConf
	shutdown := make(chan os.Signal, 1)
	switch *replicaOf {
	case "":
		s := newServer("localhost", *p, dbs, role, cfg)
		s.Start(shutdown, s.handler)
	default:
		sl := strings.Split(*replicaOf, " ")
		masterHost, masterPort := sl[0], sl[1]
		rpc = &replicaConf{
			masterHost: masterHost,
			masterPort: masterPort,
		}
		rs, err := newReplicaServer("localhost", *p, dbs, rpc, cfg)
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
	dbs                []*database.DB
	db                 *database.DB // selected db
	role               string
	masterReplid       string
	masterOffset       uint64
	replicationBacklog *replication.ReplicatinoBacklog
	config             config
}

type config struct {
	persistence persistence.Config
}

const defaultDBIdx = 0

func newServer(host, port string, dbs []*database.DB, role string, config config) *server {
	return &server{
		host:         host,
		port:         port,
		dbs:          dbs,
		masterReplid: replication.GenReplicationID(),
		masterOffset: 0,

		role:               role,
		replicationBacklog: replication.NewReplicationBacklog(backlogSizePerReplica),
		config:             config,
		db:                 dbs[defaultDBIdx],
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

type clientState struct {
	isMulti  bool
	cmdQueue [][]string
}

func (s *server) handler(conn net.Conn) (err error) {
	r := bufio.NewReader(conn)
	defer conn.Close()
	state := &clientState{}
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
		// these are command need to handle before queueing
		switch strings.ToUpper(arr[0]) {
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
			}
		// https://redis.io/docs/latest/commands/exec/
		case "EXEC":
			if err := s.handleExec(conn, state); err != nil {
				return err
			}
			continue
			// https://redis.io/docs/latest/commands/discard/
		case "DISCARD":
			if state.isMulti {
				state.isMulti = false
				state.cmdQueue = nil
				if _, err := conn.Write(resp.NewSimpleString("OK")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
			} else {
				if _, err := conn.Write(resp.NewErrorMSG("DISCARD without MULTI")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
			}
			continue
		}

		if state.isMulti {
			if err := s.handleQueuing(conn, arr, state); err != nil {
				return err
			}
			continue
		}
		if err := s.handleWriteOnlyCmd(conn, arr, state); err != nil {
			return err
		}
	}
}

func (s *server) handleQueuing(conn io.Writer, arr []string, state *clientState) error {
	state.cmdQueue = append(state.cmdQueue, arr)
	if _, err := conn.Write(resp.NewSimpleString("QUEUED")); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}

// handleExec execute the queued commands.
func (s *server) handleExec(conn io.Writer, state *clientState) error {
	if !state.isMulti {
		if _, err := conn.Write(resp.NewErrorMSG("EXEC without MULTI")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	} else {
		res := make([][]byte, len(state.cmdQueue))
		for i, cmd := range state.cmdQueue {
			buff := bytes.NewBuffer(nil)
			if err := s.handleWriteOnlyCmd(buff, cmd, state); err != nil {
				return err
			}
			res[i] = buff.Bytes()
		}
		if _, err := conn.Write(resp.NewArray(res)); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	state.isMulti = false
	state.cmdQueue = nil
	return nil
}

// when entering this function, we do not read from connection anymore.
func (s *server) handleWriteOnlyCmd(conn io.Writer, arr []string, state *clientState) error {
	switch strings.ToUpper(arr[0]) {
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

	// https://redis.io/docs/latest/commands/incr/
	case "INCR":
		if err := handleIncr(conn, arr, s.db); err != nil {
			return err
		}

	case "XADD":
		if err := handleXAdd(conn, arr, s.db); err != nil {
			return err
		}
	// https://redis.io/docs/latest/commands/xrange/
	// XRANGE key start end [COUNT count]
	case "XRANGE":
		if err := handleXRange(conn, arr, s.db); err != nil {
			return err
		}
	// https://redis.io/docs/latest/commands/xread/
	// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
	case "XREAD":
		if err := handleXRead(conn, arr, s.db); err != nil {
			return err
		}
	// https://redis.io/docs/latest/commands/multi/
	// https://redis.io/docs/latest/develop/interact/transactions/
	case "MULTI":
		if state.isMulti {
			if _, err := conn.Write(resp.NewErrorMSG("already in MULTI")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		}
		state.isMulti = true
		if _, err := conn.Write(resp.NewSimpleString("OK")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	// https://redis.io/docs/latest/commands/type/
	case "TYPE":
		if len(arr) != 2 {
			if _, err := conn.Write(resp.NewErrorMSG("expecting 2 arguments")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
			return nil
		}
		typ := s.db.Type(arr[1])
		if typ == "" {
			if _, err := conn.Write(resp.NewSimpleString("none")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		} else {
			if _, err := conn.Write(resp.NewBulkString(typ)); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
		}
		// https://redis.io/docs/latest/commands/keys/
	case "KEYS":
		if err := handleKeys(conn, arr, s.db); err != nil {
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
					res = append(res, resp.NewBulkString("dir"), resp.NewBulkString(s.config.persistence.Dir)) // key, value
				case "dbfilename":
					res = append(res, resp.NewBulkString("dbfilename"), resp.NewBulkString(s.config.persistence.Dbfilename)) // key, value
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
	return nil
}

func handlePing(conn io.Writer) error {
	if _, err := conn.Write(resp.NewSimpleString("PONG")); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}

func handleEcho(conn io.Writer, arr []string) error {
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

func handleSet(conn io.Writer, arr []string, db *database.DB) error {
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
			db.SetExp(arr[1], arr[2], time.Now().UnixMilli()+exp)
		}
	default:
		db.Set(arr[1], arr[2])
	}
	return nil
}

func handleGet(conn io.Writer, arr []string, db *database.DB) error {
	if len(arr) < 2 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting 2 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	value := db.Get(arr[1])
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

func handleIncr(conn io.Writer, arr []string, db *database.DB) error {
	if len(arr) != 2 {
		return fmt.Errorf("expecting 2 arguments")
	}
	value := db.Get(arr[1])
	parsedIntVal := 0 // default
	var err error
	if value != "" {
		// The string stored at the key is interpreted as a base-10 64 bit signed integer to execute the operation.
		parsedIntVal, err = strconv.Atoi(value)
		if err != nil {
			if _, err := conn.Write(resp.NewErrorMSG("value is not an integer or out of range")); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
			return nil
		}
	}
	increasedVal := parsedIntVal + 1
	db.Set(arr[1], strconv.Itoa(increasedVal))
	if _, err := conn.Write(resp.NewInt(increasedVal)); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil

}

func handleWait(conn io.Writer, arr []string, backlog *replication.ReplicatinoBacklog) error {
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

func handleKeys(conn io.Writer, arr []string, db *database.DB) error {
	if len(arr) != 2 {
		return fmt.Errorf("invliad keys length")
	}
	pattern := arr[1]
	if pattern == "*" {
		pattern = ".*"
	}
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invliad patter:  %w", err)
	}
	keys := db.Keys(regex)
	res := make([][]byte, len(keys))
	for i, k := range keys {
		res[i] = resp.NewBulkString(k)
	}
	if _, err := conn.Write(resp.NewArray(res)); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}

func handleXAdd(conn io.Writer, arr []string, db *database.DB) error {
	if len(arr) < 4 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting >= 5 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	if (len(arr)-3)%2 != 0 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting even key value arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	lkv := (len(arr) - 3) / 2
	kvs := make([]database.KeyValue, lkv)
	for i := 0; i < lkv; i++ {
		kvs[i] = database.KeyValue{
			Key:   arr[3+i*2],
			Value: arr[3+i*2+1],
		}
	}
	id, err := db.XAdd(arr[1], arr[2], kvs)
	if err != nil {
		if _, err := conn.Write(resp.NewErrorMSG(err.Error())); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	if _, err := conn.Write(resp.NewBulkString(id)); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}

func handleXRange(conn io.Writer, arr []string, db *database.DB) error {
	if len(arr) < 4 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting >= 4 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	key, start, end := arr[1], arr[2], arr[3]
	ents, err := db.Xrange(key, start, end)
	if err != nil {
		if _, err := conn.Write(resp.NewErrorMSG(err.Error())); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}

	if _, err := conn.Write(resp.NewStreamEntries(ents)); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}

func handleXRead(conn io.Writer, arr []string, db *database.DB) error {
	if len(arr) < 4 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting >= 4 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	streamArgsIdx := 0
	var block time.Duration
	for i, s := range arr {
		if strings.ToUpper(s) == "BLOCK" {
			blockMS, err := strconv.Atoi(arr[i+1])
			if err != nil {
				if _, err := conn.Write(resp.NewErrorMSG("invliad BLOCK argument, expeting numeric")); err != nil {
					return fmt.Errorf("error writing to connection: %s", err.Error())
				}
				return nil
			}
			block = time.Duration(blockMS) * time.Millisecond
		}
		if strings.ToUpper(s) == "STREAMS" {
			streamArgsIdx = i + 1
			break
		}
	}
	if (len(arr)-streamArgsIdx)%2 != 0 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting pairs of stream and id")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	streamsCount := (len(arr) - streamArgsIdx) / 2
	keys := arr[streamArgsIdx : streamArgsIdx+streamsCount]
	ids := arr[streamArgsIdx+streamsCount : streamArgsIdx+2*streamsCount]
	res := [][]byte{}

	if block > 0 {
		time.Sleep(block)
	}

	for i, key := range keys {
		startID := ids[i]
		ents, err := db.Xrange(key, startID, "+")
		if err != nil {
			if _, err := conn.Write(resp.NewErrorMSG(err.Error())); err != nil {
				return fmt.Errorf("error writing to connection: %s", err.Error())
			}
			return nil
		}
		firstEnt := ents[0]
		if database.StreamEntryID(firstEnt.Ts, firstEnt.Seq) == startID {
			ents = ents[1:]
		}
		resEntries := resp.NewStreamEntries(ents)
		if len(ents) > 0 {
			res = append(res, resp.NewArray([][]byte{resp.NewBulkString(key), resEntries}))
		}
	}
	if len(res) == 0 {
		if _, err := conn.Write(resp.NewNullBulkString()); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	if _, err := conn.Write(resp.NewArray(res)); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	return nil
}
