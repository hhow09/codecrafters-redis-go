package main

import (
	"sync"
	"time"
)

const (
	NO_EXPIRY = -1
)

type data struct {
	value string
	exp   int64
}

type db struct {
	datas map[string]*data
	mu    sync.RWMutex
}

func newDB() *db {
	return &db{
		datas: make(map[string]*data),
	}
}

func (d *db) get(key string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if data, ok := d.datas[key]; ok {
		if data.exp != -1 && time.Now().UnixMilli() > data.exp {
			delete(d.datas, key)
			return ""
		}
		return data.value
	}
	return ""
}

func (d *db) set(key, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.datas[key] = &data{
		value: value,
		exp:   NO_EXPIRY,
	}
}

func (d *db) setExp(key, value string, exp int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.datas[key] = &data{
		value: value,
		exp:   exp,
	}
}
