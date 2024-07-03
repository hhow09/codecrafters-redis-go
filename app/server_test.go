package main

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/database"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/stretchr/testify/require"
)

func TestPing(t *testing.T) {
	client, server := net.Pipe()

	require.NoError(t, client.SetDeadline(time.Now().Add(2*time.Second)))
	defer client.Close()
	s := newServer(host, "8081", mockdbs, RoleMaster, testCfg)
	go func() {
		err := s.handler(server)
		require.NoError(t, err)
	}()

	_, err := client.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	require.NoError(t, err)
	bfio := bufio.NewReader(client)
	res, err := bfio.ReadBytes('\n')
	require.NoError(t, err)
	require.Equal(t, "+PONG\r\n", string(res))
}

func TestEcho(t *testing.T) {
	client, server := net.Pipe()

	require.NoError(t, client.SetDeadline(time.Now().Add(2*time.Second)))
	defer client.Close()
	s := newServer(host, "8082", mockdbs, RoleMaster, testCfg)
	go func() {
		err := s.handler(server)
		require.NoError(t, err)
	}()

	_, err := client.Write([]byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"))
	require.NoError(t, err)
	bfio := bufio.NewReader(client)
	res1, err := bfio.ReadBytes('\n')
	require.NoError(t, err)
	res2, err := bfio.ReadBytes('\n')
	require.NoError(t, err)
	require.Equal(t, "$3\r\nhey\r\n", string(append(res1, res2...)))
}

func TestSetGet(t *testing.T) {
	port := "8083"
	s := newServer(host, port, mockdbs, RoleMaster, testCfg)
	c := make(chan os.Signal, 1)
	defer close(c)
	go s.Start(c, s.handler)

	conn, err := dialWithRetry(3, "localhost", port)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))

	_, err = conn.Write(resp.NewArray([][]byte{resp.NewBulkString("SET"), resp.NewBulkString("key1"), resp.NewBulkString("value1")}))
	require.NoError(t, err)

	r := bufio.NewReader(conn)
	res1, err := r.ReadBytes('\n')
	require.NoError(t, err)
	require.Equal(t, resp.NewSimpleString("OK"), res1)

	_, err = conn.Write(resp.NewArray([][]byte{resp.NewBulkString("GET"), resp.NewBulkString("key1")}))
	require.NoError(t, err)

	res2_1, err := r.ReadBytes('\n')
	require.NoError(t, err)
	res2_2, err := r.ReadBytes('\n')
	require.NoError(t, err)
	require.Equal(t, resp.NewBulkString("value1"), append(res2_1, res2_2...))
}

func TestIncr(t *testing.T) {
	port := "8090"
	dbs := []*database.DB{
		database.NewDB(),
	}
	dbs[defaultDBIdx].Set("intKey", "1")
	dbs[defaultDBIdx].Set("nonIntKey", "abc")
	s := newServer(host, port, dbs, RoleMaster, testCfg)
	c := make(chan os.Signal, 1)
	defer close(c)
	go s.Start(c, s.handler)

	conn, err := dialWithRetry(3, host, port)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))

	_, err = conn.Write(resp.NewArray([][]byte{resp.NewBulkString("INCR"), resp.NewBulkString("key1")}))
	require.NoError(t, err)

	r := bufio.NewReader(conn)
	res1, err := r.ReadBytes('\n')
	require.NoError(t, err)
	require.Equal(t, resp.NewInt(1), res1)

	_, err = conn.Write(resp.NewArray([][]byte{resp.NewBulkString("INCR"), resp.NewBulkString("nonIntKey")}))
	require.NoError(t, err)
	res2, err := r.ReadBytes('\n')
	require.NoError(t, err)
	require.Equal(t, resp.NewErrorMSG("value is not an integer or out of range"), res2)
}
