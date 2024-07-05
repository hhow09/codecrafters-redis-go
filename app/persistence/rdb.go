package persistence

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/database"
)

// ref: https://rdb.fnordig.de/file_fo	rmat.html#auxiliary-fields

const (
	redisDefaultDBSize = 16
	version            = 11
)

const (
	magicString = "REDIS"
)

const (
	opCodeAux          = 0xFA
	opCodeDatabaseSec  = 0xFE
	opCodeHashSize     = 0xFB
	opCodeEOF          = 0xFF
	opCodeExpireTimeMS = 0xFC
	opCodeExpireTime   = 0xFD
)

const (
	// C0 is the special flag. Since it starts with the bit pattern 1100____, we read the next 2 bytes as an integer. This is the third entry of the list
	stringEncoding = 0x00
)

// Encoding
const (
	redisInt8          = 0b00
	redisInt16         = 0b01
	redisInt32         = 0b10
	redisCompressedStr = 0b11
)

type RDB struct {
	*Aux
	DBs []*Database
}

type Database struct {
	Index int
	Datas map[string]database.Data
}

type Aux struct {
	Version string
	Bits    uint8
	Ctime   uint32
	UsedMem uint32
	AofBase uint8
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
// FE                       // Indicates the start of a database subsection.
// 00                       /* The index of the database (size encoded).
// FF                          ## End of RDB file indicator
// 8-byte-checksum             ## CRC64 checksum of the entire file.

func UnMarshalRDB(b []byte) (*RDB, error) {
	rdb := &RDB{
		Aux: &Aux{},
		DBs: make([]*Database, redisDefaultDBSize),
	}
	for i := 0; i < redisDefaultDBSize; i++ {
		rdb.DBs[i] = &Database{
			Index: i,
		}
	}

	idx := 0
	if string(b[0:len(magicString)]) != magicString {
		return nil, fmt.Errorf("invalid magic string")
	}
	idx += len(magicString)
	verStr := string(b[idx : idx+4])
	_, err := strconv.Atoi(verStr)
	if err != nil {
		return nil, fmt.Errorf("invalid version: %w", err)
	}
	rdb.Version = verStr
	idx = idx + 4
	buf := bytes.NewBuffer(b[idx:])
	aux, err := UnMarshalAux(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal aux: %w", err)
	}
	rdb.Aux = aux

	// db
	currDB := rdb.DBs[0]
	for {
		opCode, err := buf.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("fail to read opCode: %w", err)
		}
		switch opCode {
		case opCodeEOF:
			if err := verifyChecksum(buf); err != nil {
				return nil, fmt.Errorf("fail to verify checksum: %w", err)
			}
			return rdb, nil
		case opCodeDatabaseSec:
			sz, special, err := decodeSizeUint(buf)
			if err != nil {
				return nil, fmt.Errorf("fail to decode Size Uint: %w", err)
			}
			if special {
				return nil, fmt.Errorf("wrong resize db encoding found")
			}
			currDB = rdb.DBs[int(sz)]
		case opCodeHashSize:
			t, err := UnMarshalHashTable(buf)
			if err != nil {
				return nil, fmt.Errorf("fail unmarshal hash table: %w", err)
			}
			currDB.Datas = t
		default:
			return nil, fmt.Errorf("unknown opCode: %d", opCode)
		}
	}
}

// The op code is followed by two Redis Strings, representing the key and value of a setting. Unknown fields should be ignored by a parser.
func UnMarshalAux(buf *bytes.Buffer) (*Aux, error) {
	aux := &Aux{}
	for buf.Len() > 0 {
		fb, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		// break
		if fb == opCodeDatabaseSec {
			if err := buf.UnreadByte(); err != nil {
				return nil, fmt.Errorf("fail to unread byte")
			}
			return aux, nil
		}
		if fb != opCodeAux {
			return nil, fmt.Errorf("invalid opCode %c", fb)
		}

		key, _, err := readStringEncoding(buf)
		if err != nil {
			return nil, err
		}

		switch key {
		case "redis-ver":
			version, _, err := readStringEncoding(buf)
			if err != nil {
				return nil, err
			}
			aux.Version = version
		case "redis-bits":
			_, bits, err := readStringEncoding(buf)
			if err != nil {
				return nil, fmt.Errorf("fail to read bits: %w", err)
			}
			aux.Bits = uint8(bits)
		case "ctime":
			_, ctime, err := readStringEncoding(buf)
			if err != nil {
				return nil, fmt.Errorf("fail to read ctime: %w", err)
			}
			aux.Ctime = ctime
		case "used-mem":
			_, usedMem, err := readStringEncoding(buf)
			if err != nil {
				return nil, fmt.Errorf("fail to read used-mem: %w", err)
			}
			aux.UsedMem = usedMem
		case "aof-base":
			// Assuming aof-base is always 0 as per MarshalAux
			_, i, err := readStringEncoding(buf)
			if err != nil {
				return nil, fmt.Errorf("fail to read aof-base: %w", err)
			}
			aux.AofBase = uint8(i)

		// No corresponding field in Aux struct for "aof-base" shown, assuming it's informational
		default:
			return nil, fmt.Errorf("unknown key: %s", key)
		}
	}
	return aux, nil
}

func UnMarshalHashTable(buf *bytes.Buffer) (map[string]database.Data, error) {
	tableSize, spf, err := decodeSizeUint(buf)
	if err != nil {
		return nil, fmt.Errorf("fail to decode size encoding: %w", err)
	}
	if spf {
		return nil, fmt.Errorf("wrong resize db encoding found")
	}
	expiryTableSize, spf, err := decodeSizeUint(buf)
	if err != nil {
		return nil, fmt.Errorf("fail to decode size encoding: %w", err)
	}
	if spf {
		return nil, fmt.Errorf("wrong resize db encoding found")
	}
	log.Printf("UnMarshalHashTable tableSize: %d, expiryTableSize: %d\n", tableSize, expiryTableSize)
	if tableSize > 0 {
		tb, err := readTable(buf, tableSize)
		if err != nil {
			return nil, fmt.Errorf("fail read table: %w", err)
		}
		return tb, nil
	}
	return nil, nil
}

func readTable(buf *bytes.Buffer, size uint32) (map[string]database.Data, error) {
	count := uint32(0)
	m := map[string]database.Data{}
	for count < size {
		firstByte, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		ts := uint64(0)
		switch firstByte {
		case opCodeExpireTimeMS:
			timestampB := make([]byte, 8)
			_, err := buf.Read(timestampB)
			if err != nil {
				return m, fmt.Errorf("fail to read timestamp")
			}
			ts = binary.LittleEndian.Uint64(timestampB)

		case opCodeExpireTime:
			timestampB := make([]byte, 4)
			_, err := buf.Read(timestampB)
			if err != nil {
				return m, fmt.Errorf("fail to read timestamp")
			}
			tsS := binary.LittleEndian.Uint32(timestampB)
			ts = uint64(tsS * 1000)
		default:
			// for normal key-value, the there is no opCode
			// since the firstByte is actually the keyType
			if err := buf.UnreadByte(); err != nil {
				return m, fmt.Errorf("fail to unread byte")
			}
		}
		keyType, err := buf.ReadByte()
		if err != nil {
			return m, fmt.Errorf("fail to read key type")
		}
		switch keyType {
		case stringEncoding:
			key, _, err := readStringEncoding(buf)
			if err != nil {
				return m, fmt.Errorf("fail to read key")
			}
			val, _, err := readStringEncoding(buf)
			if err != nil {
				return m, fmt.Errorf("fail to read val")
			}
			m[key] = database.NewData(database.TypeString, val, ts)
			count += 1
		default:
			return m, fmt.Errorf("key type %v not supported yet", keyType)
		}

	}
	return m, nil
}

func (d *RDB) MarshalRDB() ([]byte, error) {
	b := []byte(magicString)
	b = append(b, []byte(fmt.Sprintf("%04d", version))...)

	aux, err := d.Aux.MarshalAux()
	if err != nil {
		return nil, err
	}
	b = append(b, aux...)
	// TODO
	// MarshalHashTable

	// TODO: implement checksum
	// ref: https://github.com/reborndb/go/blob/master/redis/rdb/digest/crc64.go
	bts, err := hex.DecodeString("fff06e3bfec0ff5aa2") //hard-coded checksum here
	if err != nil {
		return nil, err
	}
	b = append(b, bts...)
	return b, nil
}

func (a Aux) MarshalAux() ([]byte, error) {
	b := make([]byte, 0)
	b = append(b, opCodeAux)
	b = append(b, encodeToString("redis-ver")...)
	b = append(b, encodeToString(a.Version)...)

	b = append(b, opCodeAux)
	b = append(b, encodeToString("redis-bits")...)
	b = append(b, encodeToString(a.Bits)...)

	b = append(b, opCodeAux)
	b = append(b, encodeToString("ctime")...)
	b = append(b, encodeToString(a.Ctime)...)

	b = append(b, opCodeAux)
	b = append(b, encodeToString("used-mem")...)
	b = append(b, encodeToString(a.UsedMem)...)

	b = append(b, opCodeAux)
	b = append(b, encodeToString("aof-base")...)
	b = append(b, encodeToString(a.AofBase)...)
	return b, nil
}

func encodeToString(v any) []byte {
	switch v := v.(type) {
	case string:
		return append([]byte{byte(len(v))}, []byte(v)...)
	case uint8:
		b := make([]byte, 0)
		b = append(b, 0xC0)
		b = append(b, byte(v))
		return b
	case uint16:
		b := make([]byte, 0)
		b = append(b, 0xC1)
		bs := make([]byte, 2)
		binary.BigEndian.PutUint16(bs, v)
		return append(b, bs...)
	case uint32:
		b := make([]byte, 0)
		b = append(b, 0xC2)
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, v)
		return append(b, bs...)
	default:
		return nil
	}
}

func readStringEncoding(b *bytes.Buffer) (string, uint32, error) {
	sz, specialfmt, err := decodeSizeUint(b)
	if err != nil {
		return "", 0, fmt.Errorf("failed to decode Size Uint: %w", err)
	}
	if specialfmt {
		switch sz {
		case redisInt8:
			val, err := b.ReadByte()
			if err != nil {
				return "", 0, fmt.Errorf("fail to read byte: %w", err)
			}
			return "", uint32(val), nil
		case redisInt16:
			buf := make([]byte, 2)
			_, err := b.Read(buf)
			if err != nil {
				return "", 0, fmt.Errorf("fail to read byte: %w", err)
			}
			return "", uint32(binary.LittleEndian.Uint16(buf)), nil
		case redisInt32:
			buf := make([]byte, 4)
			_, err := b.Read(buf)
			if err != nil {
				return "", 0, fmt.Errorf("fail to read byte: %w", err)
			}
			return "", binary.LittleEndian.Uint32(buf), nil
		case redisCompressedStr:
			res, err := readCompressedStr(b)
			if err != nil {
				return "", 0, fmt.Errorf("fail to read compressed string: %w", err)
			}
			return res, 0, nil
		}
	}
	s := make([]byte, sz)
	n, err := b.Read(s)
	if err != nil {
		return "", 0, fmt.Errorf("fail to read string")
	}
	if n != len(s) {
		return "", 0, fmt.Errorf("invalid string length")
	}
	return string(s), 0, nil
}

func decodeSizeUint(b *bytes.Buffer) (size uint32, special bool, err error) {
	firstByte, err := b.ReadByte()
	if err != nil {
		return 0, false, fmt.Errorf("input slice is too short")
	}
	switch firstByte >> 6 { // Shift right to get the first two bits
	case 0b00:
		// The size is the remaining 6 bits of the byte.
		size = uint32(firstByte & 0b00111111) // Mask to get the last 6 bits
	case 0b01:
		secondByte, err := b.ReadByte()
		if err != nil {
			return 0, false, fmt.Errorf("input slice is too short: %w", err)
		}
		size = uint32(binary.BigEndian.Uint16([]byte{firstByte & 0b11000000, secondByte})) // Combine the last 6 bits of the first byte and the next byte
	case 0b10:
		next4B := make([]byte, 4)
		n, err := b.Read(next4B)
		if err != nil {
			return 0, false, fmt.Errorf("input slice is too short: %w", err)
		}
		if n != 4 {
			return 0, false, fmt.Errorf("input slice is too short")
		}
		size = binary.BigEndian.Uint32(next4B) // Next 4 bytes
	case 0b11:
		// The remaining 6 bits specify a type of string encoding.
		//  See string encoding section.
		return uint32(firstByte & 63), true, nil
	default:
		return 0, false, fmt.Errorf("unknown encoding")
	}

	return size, false, nil
}

func verifyChecksum(b *bytes.Buffer) error {
	// TODO verify
	return nil
}

func readCompressedStr(r *bytes.Buffer) (string, error) {
	return "", errors.New("not implemented yet")
}
