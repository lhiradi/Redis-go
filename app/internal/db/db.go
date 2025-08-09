package db

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type DB struct {
	Data    map[string]cacheValue
	Streams map[string][]StreamEntry
	mu      sync.RWMutex
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type StreamReadEntry struct {
	Key     string
	Entries []StreamEntry
}

type cacheValue struct {
	Value string
	Ttl   int64
}

func New() *DB {
	return &DB{
		Data:    make(map[string]cacheValue),
		Streams: make(map[string][]StreamEntry),
	}
}

func (db *DB) GetLastID(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if stream, ok := db.Streams[key]; ok && len(stream) > 0 {
		return stream[len(stream)-1].ID
	}
	return "0-0"
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

	entry := StreamEntry{
		ID:     finalID,
		Fields: fields,
	}

	db.Streams[key] = append(db.Streams[key], entry)
	return finalID, nil
}

func (db *DB) XRange(key, start, end string) []StreamEntry {
	entries, ok := db.Streams[key]
	if !ok {
		return nil
	}

	startMs, startSeq := utils.ParsID(start) // (if start == "-"") strings.split will split it into 0,0
	var endMs, endSeq int64
	if end == "+" {
		endMs = math.MaxInt64
		endSeq = math.MaxInt64
	} else {
		endMs, endSeq = utils.ParsID(end)
	}
	var wantedEntries []StreamEntry

	for _, entry := range entries {
		entryMs, entrySeq := utils.ParsID(entry.ID)
		if utils.CompareIDs(entryMs, entrySeq, startMs, startSeq) >= 0 &&
			utils.CompareIDs(entryMs, entrySeq, endMs, endSeq) <= 0 {
			wantedEntries = append(wantedEntries, entry)
		}
	}

	return wantedEntries
}

func (db *DB) XREAD(key, ID string) []StreamEntry {
	entries, ok := db.Streams[key]
	if !ok {
		return nil
	}
	IDMs, IDSeq := utils.ParsID(ID)
	var wantedEntries []StreamEntry

	for _, entry := range entries {
		entryMs, entrySeq := utils.ParsID(entry.ID)
		if utils.CompareIDs(entryMs, entrySeq, IDMs, IDSeq) > 0 {
			wantedEntries = append(wantedEntries, entry)
		}
	}

	return wantedEntries
}

func (db *DB) INCR(key string) int64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	val, ok := db.Data[key]
	if !ok {
		db.Set(key, "1", 0)
		return 1
	}

	intVal, _ := strconv.ParseInt(val.Value, 10, 64)
	val.Value = strconv.Itoa(int(intVal + 1))
	return intVal + 1
}
