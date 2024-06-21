package main

import "sync"

type db struct {
	data map[string]string
	mu   sync.RWMutex
}

func newDB() *db {
	return &db{
		data: make(map[string]string),
	}
}

func (d *db) get(key string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.data[key]
}

func (d *db) set(key, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data[key] = value
}
