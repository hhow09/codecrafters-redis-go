package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

// handleReiplicaHanshake handles the initial handshake from a replica
func (s *server) handleReiplicaHanshake(conn net.Conn, r *bufio.Reader, port string) error {
	// respond to REPLCONF listening-port <PORT>
	if _, err := conn.Write(resp.NewSimpleString("OK")); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	typ, err := resp.CheckDataType(r)
	if err != nil {
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
	if len(arr) != 3 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting 3 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	if arr[0] != "REPLCONF" && arr[1] != "capa" {
		if _, err := conn.Write(resp.NewErrorMSG("expecting REPLCONF capa")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	if arr[2] != "psync2" {
		if _, err := conn.Write(resp.NewErrorMSG("unknown sync")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	// respond to REPLCONF capa psync2
	if _, err := conn.Write(resp.NewSimpleString("OK")); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	typ, err = resp.CheckDataType(r)
	if err != nil {
		return fmt.Errorf("error reading byte from connection: %s", err.Error())
	}
	if typ != resp.TypeArray {
		if _, err := conn.Write(resp.NewErrorMSG("expecting type array")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}

	// Connected to master
	arr, err = resp.HandleRESPArray(r)
	if err != nil {
		return fmt.Errorf("error reading resp array from connection: %s", err.Error())
	}
	if len(arr) != 3 {
		if _, err := conn.Write(resp.NewErrorMSG("expecting 3 arguments")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		return nil
	}
	if arr[0] != "PSYNC" {
		if _, err := conn.Write(resp.NewErrorMSG("expecting PSYNC")); err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
	}
	// Send a FULLRESYNC reply in the specific case of a full resynchronization.
	// ref: https://github.com/redis/redis/blob/811c5d7aeb0b76494d78efe61e418f574c310ec0/src/replication.c#L674
	_, err = conn.Write((resp.NewSimpleString(fmt.Sprintf("FULLRESYNC %s %d", s.masterReplid, s.masterOffset))))
	if err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}

	// hard coded RDB file
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
	if _, err := conn.Write(resp.NewRDBFile(b)); err != nil {
		return fmt.Errorf("error writing to connection: %s", err.Error())
	}
	addr := conn.RemoteAddr().String()
	return s.handleReplica(addr, conn, r, port)
}

// handleReplica handles the replication to replicas
// it reads from the replication backlog and writes to the replica
func (s *server) handleReplica(addr string, conn io.Writer, r *bufio.Reader, port string) error {
	id := replicaID(addr, port)
	replicaBacklog := s.replicationBacklog.RegisterReplica(id)

	// read from master broadcast channel
	for msg := range replicaBacklog.Broadcast {
		_, err := conn.Write(msg.Data)
		if err != nil {
			return fmt.Errorf("error writing to connection: %s", err.Error())
		}
		if msg.ShouldWaitResponse {
			done := make(chan struct{})
			errCh := make(chan error)
			go func() {
				typ, err := resp.CheckDataType(r)
				if err != nil {
					errCh <- fmt.Errorf("error reading byte from connection: %s", err.Error())
					return
				}
				// wait for response when sending REPLCONF GETACK *
				// handle other cases in the future
				if typ != resp.TypeArray {
					if _, err := conn.Write(resp.NewErrorMSG("expecting type array")); err != nil {
						errCh <- fmt.Errorf("error writing to connection: %s", err.Error())
						return
					}
					done <- struct{}{}
					return
				}
				arr, err := resp.HandleRESPArray(r)
				if err != nil {
					errCh <- fmt.Errorf("error reading resp array from connection: %s", err.Error())
					return
				}
				if len(arr) != 3 {
					if _, err := conn.Write(resp.NewErrorMSG("expecting 3 arguments")); err != nil {
						errCh <- fmt.Errorf("error writing to connection: %s", err.Error())
						return
					}
					done <- struct{}{}
					return
				}
				offset, err := strconv.Atoi(arr[2])
				if err != nil {
					if _, err := conn.Write(resp.NewErrorMSG("invalid offset")); err != nil {
						errCh <- fmt.Errorf("error writing to connection: %s", err.Error())
						return
					}
					done <- struct{}{}
					return
				}
				replicaBacklog.Ack(uint64(offset))
				done <- struct{}{}
			}()

			select {
			case err := <-errCh:
				return err
			case <-done:
			case <-time.After(msg.WaitTimeout):
				// the replica might not be able to ACK the offset in time
				continue
			}
		}
	}
	return nil
}

func replicaID(remoteAddr, port string) string {
	return fmt.Sprintf("%s-%s", strings.Split(remoteAddr, ":")[0], port)
}
