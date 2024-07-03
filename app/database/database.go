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
	Value             string
	ExpireTimestampMS uint64
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
	d.mu.RLock()
	defer d.mu.RUnlock()
	if data, ok := d.datas[key]; ok {
		if data.ExpireTimestampMS != NO_EXPIRY && uint64(time.Now().UnixMilli()) > data.ExpireTimestampMS {
			delete(d.datas, key)
			return ""
		}
		return data.Value
	}
	return ""
}

func (d *DB) Set(key, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.datas[key] = Data{
		Value:             value,
		ExpireTimestampMS: NO_EXPIRY,
	}
}

func (d *DB) SetExp(key, value string, exp int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.datas[key] = Data{
		Value:             value,
		ExpireTimestampMS: uint64(exp),
	}
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
