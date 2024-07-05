package database

import (
	"regexp"
	"sync"
	"time"
)

const (
	NO_EXPIRY = 0
)

type DB struct {
	datas map[string]Data
	mu    sync.RWMutex
}

type Data struct {
	Type              string
	Value             string
	ExpireTimestampMS uint64
	Entries           []Entry
}

type Entry struct {
	ID  string
	KVs []KeyValue
}

type KeyValue struct {
	Key   string
	Value string
}

func NewString(value string, expireTimestampMS uint64) Data {
	return Data{
		Type:              TypeString,
		Value:             value,
		ExpireTimestampMS: expireTimestampMS,
	}
}

func NewDB() *DB {
	return &DB{
		datas: make(map[string]Data),
	}
}

func NewFromLoad(datas map[string]Data) *DB {
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
		return data
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

func (d *DB) XAdd(key, entryID string, kvs []KeyValue) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	ent := Entry{
		ID:  entryID,
		KVs: kvs,
	}
	if data, ok := d.datas[key]; ok {
		if data.Type != TypeStream {
			return "", ErrWrongType
		}
		data.Entries = append(data.Entries, ent)
	}
	d.datas[key] = Data{
		Type:    TypeStream,
		Entries: []Entry{ent},
	}
	return entryID, nil
}
