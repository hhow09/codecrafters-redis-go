package replication

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReplicatinoBacklog(t *testing.T) {
	c := make(chan os.Signal)
	rb := NewReplicationBacklog(20)
	c1, s1 := net.Pipe()
	c2, s2 := net.Pipe()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		rb.SendBacklog(c)
	}()

	rb.AddReplica("s1", s1)

	go func() {
		r1 := bufio.NewReader(c1)
		for i := 0; i < 20; i++ {
			val, err := r1.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%d\n", i), val)
		}
		wg.Done()
	}()

	for i := 0; i < 10; i++ {
		rb.InsertBacklog([]byte(fmt.Sprintf("%d\n", i)))
	}
	time.Sleep(1 * time.Second)

	rb.AddReplica("s2", s2)

	go func() {
		r2 := bufio.NewReader(c2)
		for i := 10; i < 20; i++ {
			val, err := r2.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%d\n", i), val)
		}
		wg.Done()
	}()

	for i := 10; i < 20; i++ {
		rb.InsertBacklog([]byte(fmt.Sprintf("%d\n", i)))
	}

	wg.Wait()
	c <- os.Interrupt

}
