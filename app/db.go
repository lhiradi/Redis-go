package main

import (
	"sync"
	"time"
)

type DB struct {
	data map[string]cacheValue
	mu   sync.RWMutex
}

type cacheValue struct {
	value string
	ttl   int64 // Unix timestamp in seconds; 0 means no expiration
}

func New() *DB {
	return &DB{
		data: make(map[string]cacheValue),
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
		// Expired
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
