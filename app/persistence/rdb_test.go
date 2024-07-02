package persistence

import (
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	mockAux = &Aux{
		Version: "7.2.0",
		Bits:    64,
		Ctime:   1829289061,
		UsedMem: 2965639168,
	}
)

// https://github.com/codecrafters-io/redis-tester/blob/main/internal/assets/empty_rdb_hex.md
func TestAuxMarshalAux(t *testing.T) {
	a := mockAux
	b, err := a.MarshalAux()
	require.NoError(t, err)
	s := hex.EncodeToString(b)
	require.Equal(t, "fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000", s)
}

func TestMarshalRDB(t *testing.T) {
	rdb := RDB{
		Aux: mockAux,
	}

	b, err := rdb.MarshalRDB()
	require.NoError(t, err)
	s := hex.EncodeToString(b)
	require.Equal(t, "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2", s)
}

func TestUnMarshalRDB(t *testing.T) {
	// hexdump
	// 00000000  52 45 44 49 53 30 30 31  31 fa 09 72 65 64 69 73  |REDIS0011..redis|
	// 00000010  2d 76 65 72 05 37 2e 32  2e 34 fa 0a 72 65 64 69  |-ver.7.2.4..redi|
	// 00000020  73 2d 62 69 74 73 c0 40  fa 05 63 74 69 6d 65 c2  |s-bits.@..ctime.|
	// 00000030  0b af 82 66 fa 08 75 73  65 64 2d 6d 65 6d c2 78  |...f..used-mem.x|
	// 00000040  d1 0e 00 fa 08 61 6f 66  2d 62 61 73 65 c0 00 fe  |.....aof-base...|
	// 00000050  00 fb 01 00 00 05 6d 79  6b 65 79 05 6d 79 76 61  |......mykey.myva|
	// 00000060  6c ff 8f 79 cd e2 e3 86  b3 57                    |l..y.....W|
	// 0000006a
	b, err := os.ReadFile("./mykey_myval.rdb")
	require.NoError(t, err)

	rdb, err := UnMarshalRDB(b)
	require.NoError(t, err)
	db := rdb.DBs[0]
	require.Empty(t, db.ExpiredDatas)
	require.Len(t, db.ValidDatas, 1)
	require.Equal(t, "7.2.4", rdb.Aux.Version)
	require.Equal(t, uint8(64), rdb.Aux.Bits)
	require.Equal(t, uint32(1719840523), rdb.Aux.Ctime)
	data, ok := db.ValidDatas["mykey"]
	require.True(t, ok)
	require.Equal(t, "myval", data.Value)
	require.False(t, data.Expired)
	require.Empty(t, data.ExpireTimestampMS)
}
