package main

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/stretchr/testify/require"
)

func TestHandshake(t *testing.T) {
	masterPort, replicaPort := ports()
	master := newServer("localhost", masterPort, mockdbs, RoleMaster, testCfg)
	setTestServerReusePort(master, nil)
	c := make(chan os.Signal, 1)
	defer close(c)
	go master.Start(c, master.handler)

	rs, err := newReplicaServer("localhost", replicaPort, mockdbs, &replicaConf{masterHost: "localhost", masterPort: masterPort}, testCfg)
	setTestServerReusePort(nil, rs)
	require.NoError(t, err)
	r, wc, err := rs.sendHandshake()
	require.NoError(t, err)
	require.NotNil(t, r)
	require.NotNil(t, wc)
	wc.Close()
}

func TestPropogate(t *testing.T) {
	masterPort, replicaPort := ports()
	master := newServer("localhost", masterPort, mockdbs, RoleMaster, testCfg)
	setTestServerReusePort(master, nil)
	c := make(chan os.Signal, 1)
	defer close(c)
	go master.Start(c, master.handler)

	rs, err := newReplicaServer("localhost", replicaPort, mockdbs, &replicaConf{masterHost: "localhost", masterPort: masterPort}, testCfg)
	setTestServerReusePort(nil, rs)
	require.NoError(t, err)
	c2 := make(chan os.Signal, 1)
	go rs.Start(c2)
	defer close(c2)

	time.Sleep(1 * time.Second) // wait replica handshake complete

	masterConn, err := dialWithRetry(3, "localhost", masterPort)
	require.NoError(t, err)

	_, err = masterConn.Write(resp.NewArray([][]byte{resp.NewBulkString("SET"), resp.NewBulkString("key2"), resp.NewBulkString("value2")}))
	require.NoError(t, err)
	_, err = masterConn.Write(resp.NewArray([][]byte{resp.NewBulkString("SET"), resp.NewBulkString("key3"), resp.NewBulkString("value3")}))
	require.NoError(t, err)
	_, err = bufio.NewReader(masterConn).ReadString('\n')
	require.NoError(t, err)

	time.Sleep(1 * time.Second) // wait for replication

	replicaConn, err := dialWithRetry(3, "localhost", replicaPort)
	require.NoError(t, err)
	verifyValue(t, replicaConn, "key2", "value2")
	verifyValue(t, replicaConn, "key3", "value3")
	masterConn.Close()
	replicaConn.Close()
}

func verifyValue(t *testing.T, conn net.Conn, key string, expected string) {
	maxRetry := 5
	for i := 0; i < maxRetry; i++ {
		_, err := conn.Write(resp.NewArray([][]byte{resp.NewBulkString("GET"), resp.NewBulkString(key)}))
		require.NoError(t, err)
		r := bufio.NewReader(conn)
		res, err := r.ReadString('\n')
		require.NoError(t, err)

		expectedVal := string(resp.NewBulkString(expected))
		if res == string(resp.NewNullBulkString()) {
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

func TestPropogateAndAck(t *testing.T) {
	masterPort, replicaPort := ports()
	master := newServer("localhost", masterPort, mockdbs, RoleMaster, testCfg)
	setTestServerReusePort(master, nil)
	c := make(chan os.Signal, 1)
	defer close(c)
	go master.Start(c, master.handler)

	rs, err := newReplicaServer("localhost", replicaPort, mockdbs, &replicaConf{masterHost: "localhost", masterPort: masterPort}, testCfg)
	setTestServerReusePort(nil, rs)
	require.NoError(t, err)
	c2 := make(chan os.Signal, 1)
	go rs.Start(c2)
	defer close(c2)

	time.Sleep(1 * time.Second) // wait replica handshake complete

	masterConn, err := dialWithRetry(3, "localhost", masterPort)
	require.NoError(t, err)

	_, err = masterConn.Write(resp.NewArray([][]byte{resp.NewBulkString("SET"), resp.NewBulkString("key2"), resp.NewBulkString("value2")}))
	require.NoError(t, err)
	setRes, err := bufio.NewReader(masterConn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, string(resp.NewSimpleString("OK")), setRes)
	_, err = masterConn.Write(resp.NewArray([][]byte{resp.NewBulkString("WAIT"), resp.NewBulkString("1"), resp.NewBulkString("500")}))
	require.NoError(t, err)
	waitRes, err := bufio.NewReader(masterConn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, string(resp.NewInt(1)), waitRes)

	_, err = masterConn.Write(resp.NewArray([][]byte{resp.NewBulkString("SET"), resp.NewBulkString("key3"), resp.NewBulkString("value3")}))
	require.NoError(t, err)
	setRes, err = bufio.NewReader(masterConn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, string(resp.NewSimpleString("OK")), setRes)
	_, err = masterConn.Write(resp.NewArray([][]byte{resp.NewBulkString("WAIT"), resp.NewBulkString("1"), resp.NewBulkString("500")}))
	require.NoError(t, err)
	waitRes, err = bufio.NewReader(masterConn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, string(resp.NewInt(1)), waitRes)

	time.Sleep(50 * time.Millisecond) // wait for replication

	replicaConn, err := dialWithRetry(3, "localhost", replicaPort)
	require.NoError(t, err)
	verifyValue(t, replicaConn, "key2", "value2")
	verifyValue(t, replicaConn, "key3", "value3")
	masterConn.Close()
	replicaConn.Close()
}
