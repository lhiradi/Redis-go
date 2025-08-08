package main

import (
	"fmt"
	"strings"
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

func (db *DB) GetType(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if _, ok := db.data[key]; ok {
		return "string"
	}
	if _, ok := db.streams[key]; ok {
		return "stream"
	}
	return "none"
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
	var newID string

	if _, ok := db.data[key]; ok {
		return "", fmt.Errorf("wrong key type")
	}
	sepratedId := strings.Split(id, "-")
	lastEntry := db.streams[key][len(db.streams[key])-1]
	lastSepratedId := strings.Split(lastEntry.id, "-")

	if id == "0-0" {
		return "", fmt.Errorf("the ID specified in XADD must be greater than 0-0")
	}
	if sepratedId[0] > lastSepratedId[0] {
		newID = id
	} else if sepratedId[0] == lastSepratedId[0] {
		if sepratedId[1] > lastSepratedId[1] {
			newID = id
		} else {
			return "", fmt.Errorf("the ID specified in XADD is equal or smaller than the target stream top item")
		}
	} else {
		return "", fmt.Errorf("the ID specified in XADD is equal or smaller than the target stream top item")
	}

	entry := streamEntry{
		id:     newID,
		fileds: fields,
	}

	db.streams[key] = append(db.streams[key], entry)
	return entry.id, nil
}
