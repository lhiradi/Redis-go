package main

import (
	"fmt"
	"sync"
	"time"
)

type DB struct {
	data    map[string]cacheValue
	streams map[string][]streamEntry
	mu      sync.RWMutex
}

type streamEntry struct {
	id     string
	fileds map[string]string
}

type cacheValue struct {
	value string
	ttl   int64
}

func New() *DB {
	return &DB{
		data:    make(map[string]cacheValue),
		streams: make(map[string][]streamEntry),
	}
}

func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, ok := db.data[key]
	if !ok {
		return "", false
	}

	if val.ttl > 0 && time.Now().UnixMilli() > val.ttl {

		delete(db.data, key)
		return "", false
	}
	return val.value, true
}

func (db *DB) Set(key, value string, ttlMilSec int64) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var ttl int64
	if ttlMilSec > 0 {
		ttl = time.Now().UnixMilli() + ttlMilSec
	}
	db.data[key] = cacheValue{value: value, ttl: ttl}
}

func (db *DB) XAdd(key, id string, fields map[string]string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.data[key]; ok {
		return "", fmt.Errorf("wrong key type")
	}

	entry := streamEntry{
		id:     id,
		fileds: fields,
	}

	db.streams[key] = append(db.streams[key], entry)
	return entry.id, nil
}
