package persistence

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// https://github.com/codecrafters-io/redis-tester/blob/main/internal/assets/empty_rdb_hex.md
func TestAuxMarshalRDB(t *testing.T) {
	a := Aux{
		Version: "7.2.0",
		Bits:    64,
		Ctime:   1829289061,
		UsedMem: 2965639168,
	}
	b, err := a.MarshalRDB()
	require.NoError(t, err)
	s := hex.EncodeToString(b)
	require.Equal(t, "fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000", s)
}

func TestMarshalRDB(t *testing.T) {
	a := &Aux{
		Version: "7.2.0",
		Bits:    64,
		Ctime:   1829289061,
		UsedMem: 2965639168,
	}
	rdb := RDB{
		Aux: a,
	}

	b, err := rdb.MarshalRDB()
	require.NoError(t, err)
	s := hex.EncodeToString(b)
	require.Equal(t, "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2", s)
}
