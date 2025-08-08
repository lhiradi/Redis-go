package main

import (
	"fmt"
	"strconv"
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

	// Check if the key exists but is not a stream
	if _, ok := db.data[key]; ok {
		return "", fmt.Errorf("wrong key type")
	}

	var finalID string
	lastEntry, streamExists := db.streams[key]

	// Handle the case of an empty or new stream.
	if !streamExists || len(lastEntry) == 0 {
		if id == "0-0" {
			return "", fmt.Errorf(" The ID specified in XADD must be greater than 0-0")
		} else {
			finalID = id
		}
	} else { // Handle existing stream
		lastID := lastEntry[len(lastEntry)-1].id

		// Compare user-provided ID with the last one
		lastParts := strings.Split(lastID, "-")
		lastMs, _ := strconv.ParseInt(lastParts[0], 10, 64)
		lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

		idParts := strings.Split(id, "-")
		idMs, _ := strconv.ParseInt(idParts[0], 10, 64)
		idSeq, _ := strconv.ParseInt(idParts[1], 10, 64)

		if idMs < lastMs || (idMs == lastMs && idSeq <= lastSeq) {
			return "", fmt.Errorf(" The ID specified in XADD is equal or smaller than the target stream top item")
		}
		finalID = id
	}

	entry := streamEntry{
		id:     finalID,
		fileds: fields,
	}

	db.streams[key] = append(db.streams[key], entry)
	return finalID, nil
}
