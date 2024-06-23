package persistence

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// ref: https://rdb.fnordig.de/file_format.html#auxiliary-fields

const version = 11

const (
	opCodeAux = 0xFA
	// C0 is the special flag. Since it starts with the bit pattern 1100____, we read the next 2 bytes as an integer. This is the third entry of the list
	IntFlag2Byte = 0xC0
)

type RDB struct {
	*Aux
}

type Aux struct {
	Version string
	Bits    uint8
	Ctime   uint32
	UsedMem uint64
}

//
// ----------------------------#
// 52 45 44 49 53              # Magic String "REDIS"
// 30 30 30 33                 # RDB Version Number as ASCII string. "0003" = 3
// ----------------------------
// FA                          # Auxiliary field
// $string-encoded-key         # May contain arbitrary metadata
// $string-encoded-value       # such as Redis version, creation time, used memory, ...
// ----------------------------
// ...
// FF                          ## End of RDB file indicator
// 8-byte-checksum             ## CRC64 checksum of the entire file.

func (d *RDB) MarshalRDB() ([]byte, error) {
	b := []byte("REDIS")
	b = append(b, []byte(fmt.Sprintf("%04d", version))...)

	aux, err := d.Aux.MarshalRDB()
	if err != nil {
		return nil, err
	}
	b = append(b, aux...)
	// TODO: hard-coded checksum here
	// ref: https://github.com/reborndb/go/blob/master/redis/rdb/digest/crc64.go
	bts, err := hex.DecodeString("fff06e3bfec0ff5aa2")
	if err != nil {
		return nil, err
	}
	b = append(b, bts...)
	return b, nil
}

func (a *Aux) MarshalRDB() ([]byte, error) {
	b := make([]byte, 0)
	b = append(b, opCodeAux)
	b = append(b, lengthEncodedStr("redis-ver")...)
	b = append(b, lengthEncodedStr(a.Version)...)

	b = append(b, opCodeAux)
	b = append(b, lengthEncodedStr("redis-bits")...)
	b = append(b, encodedInt8(a.Bits)...)

	b = append(b, opCodeAux)
	b = append(b, lengthEncodedStr("ctime")...)
	b = append(b, encodedUInt32(a.Ctime)...)

	b = append(b, opCodeAux)
	b = append(b, lengthEncodedStr("used-mem")...)
	b = append(b, encodedUInt32(uint32(a.UsedMem))...)

	b = append(b, opCodeAux)
	b = append(b, lengthEncodedStr("aof-base")...)
	b = append(b, encodedInt8(0)...)
	return b, nil
}

func lengthEncodedStr(s string) []byte {
	return append([]byte{byte(len(s))}, []byte(s)...)
}

func encodedInt8(i uint8) []byte {
	b := make([]byte, 0)
	b = append(b, IntFlag2Byte)
	b = append(b, byte(i))
	return b
}

func encodedUInt32(i uint32) []byte {
	b := make([]byte, 0)
	b = append(b, 0xC2)
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, i)
	b = append(b, bs...)
	return b
}
