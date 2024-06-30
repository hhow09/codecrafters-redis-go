package replication

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type ReplicatinoBacklog struct {
	mu      sync.RWMutex
	maxSize int
	backlog map[string]*replicaBacklog
}

type replicaBacklog struct {
	Broadcast      chan Msg
	expectedOffset uint64
	currentOffset  uint64
}

// Msg represents a message in backlog
type Msg struct {
	Data               []byte
	ShouldWaitResponse bool // currently only used for GETACK
	WaitTimeout        time.Duration
}

func NewReplicationBacklog(maxSize int) *ReplicatinoBacklog {
	return &ReplicatinoBacklog{
		backlog: make(map[string]*replicaBacklog),
		maxSize: maxSize,
	}
}

// RegisterReplica registers a replica to the replication backlog
func (r *ReplicatinoBacklog) RegisterReplica(id string) *replicaBacklog {
	r.mu.Lock()
	defer r.mu.Unlock()
	broadcast := make(chan Msg, r.maxSize)
	r.backlog[id] = &replicaBacklog{
		Broadcast: broadcast,
	}
	return r.backlog[id]
}

// BroardcastBacklog add the message to all the replica's backlog
func (r *ReplicatinoBacklog) BroardcastBacklog(msg Msg) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, bl := range r.backlog {
		bl.AddBacklog(msg)
	}
}

// InSyncReplicas checks the count of replicas that are in sync with the master
func (r *ReplicatinoBacklog) InSyncReplicas(timeout time.Duration, waitCount int) int {
	r.mu.RLock()
	insyncCount := &atomic.Int32{}
	for id, rplc := range r.backlog {
		go r.checkInSync(id, rplc, insyncCount, timeout)
	}
	r.mu.RUnlock()

	<-time.After(timeout + 100*time.Millisecond)
	c := int(insyncCount.Load())
	return c
}

func (r *ReplicatinoBacklog) checkInSync(id string, bl *replicaBacklog, count *atomic.Int32, timeout time.Duration) {
	// not supposed to send a GETACK to the replicas if there was nothing to sent before
	if bl.expectedOffset == 0 {
		count.Add(1)
		return
	}
	// because replica is acking the offset BEFORE processing [REPLCONF GETACK *]
	expOffset := bl.expectedOffset
	msg := Msg{
		Data:               resp.NewArray([][]byte{resp.NewBulkString("REPLCONF"), resp.NewBulkString("GETACK"), resp.NewBulkString("*")}),
		ShouldWaitResponse: true,
		WaitTimeout:        timeout,
	}
	bl.AddBacklog(msg)

	<-time.After(timeout + time.Millisecond)
	updatedBl := r.backlog[id]
	if updatedBl.currentOffset == expOffset {
		count.Add(1)
		return
	}
}

func (bl *replicaBacklog) Ack(offset uint64) {
	bl.currentOffset = offset
}

func (bl *replicaBacklog) AddBacklog(msg Msg) {
	bl.Broadcast <- msg
	bl.expectedOffset += uint64(len(msg.Data))
}
