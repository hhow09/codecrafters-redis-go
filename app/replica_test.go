package main

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandshake(t *testing.T) {
	masterPort := "8084"
	master := newServer("localhost", masterPort, newDB(), RoleMaster)
	c := make(chan os.Signal, 1)
	defer close(c)
	go master.Start(c)

	rs, err := newReplicaServer("localhost", "8085", newDB(), &replicaConf{masterHost: "localhost", masterPort: masterPort})
	require.NoError(t, err)
	conn, err := rs.sendHandshake()
	require.NoError(t, err)
	require.NotNil(t, conn)

	_, err = conn.Write(newArray([][]byte{newBulkString("PING")}))
	require.NoError(t, err)
	s, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, string(newSimpleString("PONG")), s)
}

func TestPropogate(t *testing.T) {
	masterPort := "8086"
	replicaPort := "8087"
	master := newServer("localhost", masterPort, newDB(), RoleMaster)
	c := make(chan os.Signal, 1)
	defer close(c)
	go master.Start(c)

	rs, err := newReplicaServer("localhost", replicaPort, newDB(), &replicaConf{masterHost: "localhost", masterPort: masterPort})
	require.NoError(t, err)
	c2 := make(chan os.Signal, 1)
	go rs.Start(c2)
	defer close(c2)

	time.Sleep(1 * time.Second) // wait replica handshake complete

	masterConn, err := dialWithRetry(3, "localhost", masterPort)
	require.NoError(t, err)

	_, err = masterConn.Write(newArray([][]byte{newBulkString("SET"), newBulkString("key2"), newBulkString("value2")}))
	_, err = masterConn.Write(newArray([][]byte{newBulkString("SET"), newBulkString("key3"), newBulkString("value3")}))
	require.NoError(t, err)
	_, err = bufio.NewReader(masterConn).ReadString('\n')
	require.NoError(t, err)

	time.Sleep(1 * time.Second) // wait for replication

	replicaConn, err := dialWithRetry(3, "localhost", replicaPort)
	require.NoError(t, err)
	verifyValue(t, replicaConn, "key2", "value2")
	verifyValue(t, replicaConn, "key3", "value3")
	require.NoError(t, masterConn.Close())
	require.NoError(t, replicaConn.Close())
}

func verifyValue(t *testing.T, conn net.Conn, key string, expected string) {
	maxRetry := 5
	for i := 0; i < maxRetry; i++ {
		_, err := conn.Write(newArray([][]byte{newBulkString("GET"), newBulkString(key)}))
		require.NoError(t, err)
		r := bufio.NewReader(conn)
		res, err := r.ReadString('\n')
		require.NoError(t, err)

		expectedVal := string(newBulkString(expected))
		if res == string(newNullBulkString()) {
			if i == maxRetry-1 {
				require.Fail(t, "Unexpected response", res, "expected", expectedVal)
			}
			continue
		} else if res == expectedVal[:4] {
			res2, err := r.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, expectedVal, res+res2, "verifying %s Unexpected response %s", expectedVal, res+res2)
		} else {
			require.Fail(t, "Unexpected response", res)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
