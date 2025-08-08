package db

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DB struct {
	Data    map[string]cacheValue
	Streams map[string][]streamEntry
	mu      sync.RWMutex
}

type streamEntry struct {
	ID     string
	Fields map[string]string
}

type cacheValue struct {
	Value string
	Ttl   int64
}

func New() *DB {
	return &DB{
		Data:    make(map[string]cacheValue),
		Streams: make(map[string][]streamEntry),
	}
}

func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, ok := db.Data[key]
	if !ok {
		return "", false
	}

	if val.Ttl > 0 && time.Now().UnixMilli() > val.Ttl {

		delete(db.Data, key)
		return "", false
	}
	return val.Value, true
}

func (db *DB) GetType(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if _, ok := db.Data[key]; ok {
		return "string"
	}
	if _, ok := db.Streams[key]; ok {
		return "stream"
	}
	return "none"
}
func (db *DB) Set(key, Value string, ttlMilSec int64) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var Ttl int64
	if ttlMilSec > 0 {
		Ttl = time.Now().UnixMilli() + ttlMilSec
	}
	db.Data[key] = cacheValue{Value: Value, Ttl: Ttl}
}

func (db *DB) XAdd(key, ID string, fields map[string]string) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if the key exists but is not a stream
	if _, ok := db.Data[key]; ok {
		return "", fmt.Errorf("wrong key type")
	}

	var finalID string
	lastEntry, streamExists := db.Streams[key]

	// Handle the case of an empty or new stream.
	if !streamExists || len(lastEntry) == 0 {
		if ID == "0-0" {
			return "", fmt.Errorf(" The ID specified in XADD must be greater than 0-0")
		} else {
			finalID = ID
		}
	} else { // Handle existing stream
		if ID == "0-0" {
			return "", fmt.Errorf(" The ID specified in XADD must be greater than 0-0")
		}
		lastID := lastEntry[len(lastEntry)-1].ID

		// Compare user-provIDed ID with the last one
		lastParts := strings.Split(lastID, "-")
		lastMs, _ := strconv.ParseInt(lastParts[0], 10, 64)
		lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

		IDParts := strings.Split(ID, "-")
		IDMs, _ := strconv.ParseInt(IDParts[0], 10, 64)
		IDSeq, _ := strconv.ParseInt(IDParts[1], 10, 64)

		if IDMs < lastMs || (IDMs == lastMs && IDSeq <= lastSeq) {
			return "", fmt.Errorf(" The ID specified in XADD is equal or smaller than the target stream top item")
		}
		finalID = ID
	}

	entry := streamEntry{
		ID:     finalID,
		Fields: fields,
	}

	db.Streams[key] = append(db.Streams[key], entry)
	return finalID, nil
}
