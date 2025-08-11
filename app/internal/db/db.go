package db

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type ReplicaConn struct {
	Conn net.Conn
	Mu   sync.Mutex
}

type DB struct {
	Data            map[string]cacheValue
	Streams         map[string][]StreamEntry
	Mu              sync.RWMutex
	Role            string
	ID              string
	Offset          int
	Replicas        []*ReplicaConn
	ReplicaMu       sync.RWMutex
	NumAcksRecieved int64
	RDBFileDir      string
	RDBFileName     string
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

func New(role string) *DB {
	return &DB{
		Data:            make(map[string]cacheValue),
		Streams:         make(map[string][]StreamEntry),
		Role:            role,
		ID:              utils.GenerateReplicaID(),
		Offset:          0,
		Replicas:        make([]*ReplicaConn, 0),
		NumAcksRecieved: 0,
	}
}

func (db *DB) ParseAndLoadRDBFile() error {
	data, err := ParseRDBFile(db.RDBFileDir, db.RDBFileName)
	if err != nil {
		return fmt.Errorf("failed to parse RDB file: %w", err)
	}

	db.Data = data
	return nil
}

func (db *DB) UpdateOffset(length int) {
	db.Offset = db.Offset + length
}

func (db *DB) AddReplica(conn net.Conn) {
	db.ReplicaMu.Lock()
	defer db.ReplicaMu.Unlock()

	db.Replicas = append(db.Replicas, &ReplicaConn{Conn: conn})
	fmt.Printf("Added replica connection. Total replicas: %d \n", len(db.Replicas))
}

func (db *DB) RemoveReplica(conn net.Conn) {
	db.ReplicaMu.Lock()
	defer db.ReplicaMu.Unlock()
	for i, r := range db.Replicas {
		if r.Conn == conn {
			fmt.Printf("Removing replica connection from address: %s\n", conn.RemoteAddr().String())
			_ = r.Conn.Close()
			db.Replicas = append(db.Replicas[:i], db.Replicas[i+1:]...)
			break
		}
	}
}

// PropagateCommand now locks per-replica before writing
func (db *DB) PropagateCommand(args []string) {
	db.ReplicaMu.RLock()
	defer db.ReplicaMu.RUnlock()

	respCmd := utils.FormatRESPArray(args)

	for _, r := range db.Replicas {
		r.Mu.Lock()
		_, err := r.Conn.Write([]byte(respCmd))
		r.Mu.Unlock()
		if err != nil {
			fmt.Printf("Failed to propagate command to replica: %v\n", err)
			// Note: we don't remove here; removal happens in other code paths (or optionally add removal logic)
			continue
		}
	}
}

func (db *DB) GetLastID(key string) string {
	db.Mu.RLock()
	defer db.Mu.RUnlock()

	if stream, ok := db.Streams[key]; ok && len(stream) > 0 {
		return stream[len(stream)-1].ID
	}
	return "0-0"
}

func (db *DB) Get(key string) (string, bool) {
	db.Mu.RLock()
	defer db.Mu.RUnlock()

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
	db.Mu.RLock()
	defer db.Mu.RUnlock()

	if _, ok := db.Data[key]; ok {
		return "string"
	}
	if _, ok := db.Streams[key]; ok {
		return "stream"
	}
	return "none"
}
func (db *DB) Set(key, Value string, ttlMilSec int64) {
	db.Mu.Lock()
	defer db.Mu.Unlock()

	var Ttl int64
	if ttlMilSec > 0 {
		Ttl = time.Now().UnixMilli() + ttlMilSec
	}
	db.Data[key] = cacheValue{Value: Value, Ttl: Ttl}
}

func (db *DB) XAdd(key, ID string, fields map[string]string) (string, error) {
	db.Mu.Lock()
	defer db.Mu.Unlock()

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
	db.Mu.Lock()
	defer db.Mu.Unlock()

	val, ok := db.Data[key]
	if !ok {
		db.Data[key] = cacheValue{Value: "1", Ttl: 0}
		return int64(1)
	}

	intVal, err := strconv.ParseInt(val.Value, 10, 64)
	if err != nil {
		return -1
	}
	val.Value = strconv.Itoa(int(intVal + 1))
	db.Data[key] = val
	return intVal + 1
}
