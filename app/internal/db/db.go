package db

import (
	"fmt"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
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

	lastID := ""
	if stream, streamExists := db.Streams[key]; streamExists && len(stream) > 0 {
		lastID = stream[len(stream)-1].ID
	}

	finalID, err := utils.ValidateStreamID(ID, lastID)
	if err != nil {
		return "", err
	}

	entry := streamEntry{
		ID:     finalID,
		Fields: fields,
	}

	db.Streams[key] = append(db.Streams[key], entry)
	return finalID, nil
}
