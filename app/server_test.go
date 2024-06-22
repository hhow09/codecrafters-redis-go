package main

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPing(t *testing.T) {
	client, server := net.Pipe()

	require.NoError(t, client.SetDeadline(time.Now().Add(2*time.Second)))
	defer client.Close()
	s := newServer("localhost", "8080", newDB(), "master")
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
	s := newServer("localhost", "8080", newDB(), "master")
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
