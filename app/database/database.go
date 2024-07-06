package database

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NO_EXPIRY = 0
)

type DB struct {
	datas map[string]*Data
	mu    sync.RWMutex
}

type Data struct {
	Type              string
	Value             string
	ExpireTimestampMS uint64
	Entries           []Entry
}

type Entry struct {
	Ts  uint64
	Seq uint64
	KVs []KeyValue
}

type KeyValue struct {
	Key   string
	Value string
}

func NewString(value string, expireTimestampMS uint64) *Data {
	return &Data{
		Type:              TypeString,
		Value:             value,
		ExpireTimestampMS: expireTimestampMS,
	}
}

func NewDB() *DB {
	return &DB{
		datas: make(map[string]*Data),
	}
}

func NewFromLoad(datas map[string]*Data) *DB {
	if datas == nil {
		return NewDB()
	}
	return &DB{
		datas: datas,
	}
}

func (d *DB) Get(key string) string {
	return d.get(key).Value
}

func (d *DB) get(key string) Data {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if data, ok := d.datas[key]; ok {
		if data.ExpireTimestampMS != NO_EXPIRY && uint64(time.Now().UnixMilli()) > data.ExpireTimestampMS {
			delete(d.datas, key)
			return Data{}
		}
		return *data
	}
	return Data{}
}

func (d *DB) Type(key string) string {
	return d.get(key).Type
}

func (d *DB) Set(key, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.datas[key] = NewString(value, NO_EXPIRY)
}

func (d *DB) SetExp(key, value string, exp int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.datas[key] = NewString(value, uint64(exp))
}

func (d *DB) Keys(reg *regexp.Regexp) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	keys := make([]string, 0)
	for key := range d.datas {
		if reg.MatchString(key) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (d *DB) XAdd(key, inputEntryID string, kvs []KeyValue) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if data, ok := d.datas[key]; ok {
		if data.Type != TypeStream {
			return "", ErrWrongType
		}
		ts, seq, err := validateAndGenerateEntryID(inputEntryID, &data.Entries[len(data.Entries)-1])
		if err != nil {
			return "", err
		}
		ent := Entry{
			Ts:  ts,
			Seq: seq,
			KVs: kvs,
		}
		data.Entries = append(data.Entries, ent)
		return fmt.Sprintf("%d-%d", ts, seq), nil
	}
	ts, seq, err := validateAndGenerateEntryID(inputEntryID, nil)
	if err != nil {
		return "", err
	}
	ent := Entry{
		Ts:  ts,
		Seq: seq,
		KVs: kvs,
	}
	d.datas[key] = &Data{
		Type:    TypeStream,
		Entries: []Entry{ent},
	}
	return fmt.Sprintf("%d-%d", ts, seq), nil
}

func validateAndGenerateEntryID(entryID string, lastEntry *Entry) (uint64, uint64, error) {
	if entryID == "" {
		return 0, 0, ErrInvalidEntryID
	}
	var timeStamp uint64
	var seq uint64
	var err error
	autoGenSeq := false
	if entryID == "*" {
		// fully auto generate entryID
		timeStamp = uint64(time.Now().UnixMilli())
		autoGenSeq = true
	} else {
		arr := strings.Split(entryID, "-")
		if len(arr) != 2 {
			return 0, 0, ErrInvalidEntryID
		}
		timeStamp, err = strconv.ParseUint(arr[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("%v: %v", ErrInvalidEntryID, err)
		}
		// partial entryID
		if arr[1] == "*" {
			autoGenSeq = true
		} else {
			// specific entryID
			seq, err = strconv.ParseUint(arr[1], 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("%v: %v", ErrInvalidEntryID, err)
			}
			if seq == 0 && timeStamp == 0 {
				return 0, 0, ErrIDMinVal
			}
		}
	}
	if autoGenSeq {
		if lastEntry != nil && timeStamp == lastEntry.Ts {
			return timeStamp, lastEntry.Seq + 1, nil
		} else {
			if timeStamp == 0 {
				return timeStamp, 1, nil
			}
			return timeStamp, 0, nil
		}
	}
	if lastEntry == nil {
		return timeStamp, seq, nil
	}
	if timeStamp < lastEntry.Ts {
		return 0, 0, ErrIDTooSmall
	}
	if timeStamp == lastEntry.Ts && seq <= lastEntry.Seq {
		return 0, 0, ErrIDTooSmall
	}
	return timeStamp, seq, nil
}
