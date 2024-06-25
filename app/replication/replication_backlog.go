package replication

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

type ReplicatinoBacklog struct {
	mu           sync.Mutex
	replicastore map[string]*replica
	maxSize      int
}

type replicaStore struct {
	mu    sync.Mutex
	store map[string]*replica
}

type replica struct {
	conn net.Conn
	buf  [][]byte // replication buffer, https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
}

func NewReplicationBacklog(maxSize int) *ReplicatinoBacklog {
	return &ReplicatinoBacklog{
		replicastore: make(map[string]*replica),
		maxSize:      maxSize,
	}
}

func (r *ReplicatinoBacklog) AddReplica(id string, conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicastore[id] = &replica{
		conn: conn,
	}
}

func (r *ReplicatinoBacklog) RemoveReplica(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.replicastore, id)
}

func (r *ReplicatinoBacklog) InsertBacklog(val []byte) {
	for _, rplc := range r.replicastore {
		rplc.buf = append(rplc.buf, val)
		if len(rplc.buf) > r.maxSize {
			diff := len(rplc.buf) - r.maxSize
			rplc.buf = rplc.buf[diff:]
		}
	}

}

func (r *ReplicatinoBacklog) SendBacklog(stopC chan os.Signal) {
	for {
		select {
		case <-stopC:
			fmt.Println("Stopping replication backlog")
			return
		default:
			r.mu.Lock()
			for id, rplc := range r.replicastore {
				if len(rplc.buf) > 0 {
					for rplc.buf != nil {
						fmt.Printf("Sending backlog to replica %s : %v\n", id, string(bytes.Join(rplc.buf, nil)))
						_, err := rplc.conn.Write(bytes.Join(rplc.buf, nil))
						if err != nil {
							fmt.Printf("Error writing to replica %s: %v \n", id, err.Error())
							break
						}
						rplc.buf = nil
					}
				}
			}
			r.mu.Unlock()
			time.Sleep(1 * time.Second)
		}
	}
}
